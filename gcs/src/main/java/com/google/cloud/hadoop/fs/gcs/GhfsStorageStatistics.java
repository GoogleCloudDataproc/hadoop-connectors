/*
 * Copyright 2023 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.fs.gcs;

import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.FILES_CREATED;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.INVOCATION_GET_FILE_CHECKSUM;
import static com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemBase.InvocationRaisingIOE;
import com.google.common.base.Stopwatch;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nonnull;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.StorageStatistics;

/** Storage statistics for GCS */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class GhfsStorageStatistics extends StorageStatistics {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  /** {@value} The key that stores all the registered metrics */
  public static final String NAME = "GhfsStorageStatistics";

  public static final int LATENCY_LOGGING_THRESHOLD_MS = 150;

  private final Map<GhfsStatistic, AtomicLong> opsCount = new EnumMap<>(GhfsStatistic.class);
  private final Map<GhfsStatistic, AtomicLong> minimums = new EnumMap<>(GhfsStatistic.class);
  private final Map<GhfsStatistic, AtomicLong> maximums = new EnumMap<>(GhfsStatistic.class);
  private final Map<GhfsStatistic, MeanStatistic> means = new EnumMap<>(GhfsStatistic.class);

  public GhfsStorageStatistics() {
    super(NAME);
    for (GhfsStatistic opType : GhfsStatistic.values()) {
      opsCount.put(opType, new AtomicLong(0));

      if (opType.getType() == GhfsStatisticTypeEnum.TYPE_DURATION) {
        minimums.put(opType, null);
        maximums.put(opType, new AtomicLong(0));
        means.put(opType, new MeanStatistic());
      }
    }
  }

  static <B> B trackDuration(
      @Nonnull GhfsStorageStatistics stats,
      GhfsStatistic statistic,
      Object context,
      InvocationRaisingIOE<B> operation)
      throws IOException {
    Stopwatch stopwatch = Stopwatch.createStarted();
    try {
      stats.increment(statistic);
      return operation.apply();
    } finally {
      stats.updateStats(statistic, stopwatch.elapsed().toMillis(), context);
    }
  }

  private long increment(GhfsStatistic statistic) {
    return incrementCounter(statistic, 1);
  }

  /**
   * Increment a specific counter.
   *
   * @param op operation
   * @param count increment value
   * @return the new value
   */
  long incrementCounter(GhfsStatistic op, long count) {
    return opsCount.get(op).addAndGet(count);
  }

  @Override
  public void reset() {
    for (AtomicLong value : opsCount.values()) {
      value.set(0);
    }
  }

  void updateStats(GhfsStatistic statistic, long durationMs, Object context) {
    updateMinAndMax(statistic, durationMs, durationMs, context);

    if (means.containsKey(statistic)) {
      means.get(statistic).addSample(durationMs);
    }
  }

  void updateStats(
      GhfsStatistic statistic,
      long minLatency,
      long maxLatency,
      long totalDuration,
      int count,
      Object context) {

    updateMinAndMax(statistic, minLatency, maxLatency, context);
    means.get(statistic).addSample(totalDuration, count);
    opsCount.get(statistic).addAndGet(count);
  }

  private void updateMinAndMax(
      GhfsStatistic statistic, long minDuration, long maxDuration, Object context) {
    checkArgument(
        statistic.getType() == GhfsStatisticTypeEnum.TYPE_DURATION,
        String.format("Unexpected instrumentation type %s", statistic));

    AtomicLong minVal = minimums.get(statistic);
    if (minVal == null) {
      // There can be race here. It is ok to have the last write win.
      minimums.put(statistic, new AtomicLong(minDuration));
    } else if (minDuration < minVal.get()) {
      minVal.set(minDuration);
    }

    AtomicLong maxVal = maximums.get(statistic);
    if (maxDuration > maxVal.get()) {
      if (maxDuration > LATENCY_LOGGING_THRESHOLD_MS) {
        logger.atWarning().log(
            "Detected potential high latency for operation %s. latencyMs=%s; previousMaxLatencyMs=%s; operationCount=%s; context=%s",
            statistic, maxDuration, maxVal.get(), opsCount.get(statistic), context);
      }

      // There can be race here and can have some data points get missed. This is a corner case.
      // Since this function can be called quite frequently, opting for performance over consistency
      // here.
      maxVal.set(maxDuration);
    }
  }

  void streamReadBytes(int bytesRead) {
    incrementCounter(GhfsStatistic.STREAM_READ_BYTES, bytesRead);
  }

  /** If more data was requested than was actually returned, this was an incomplete read. */
  void streamReadOperationInComplete(int requested, int actual) {
    if (requested > actual) {
      increment(GhfsStatistic.STREAM_READ_OPERATIONS_INCOMPLETE);
    }
  }

  void streamReadSeekBackward(long negativeOffset) {
    increment(GhfsStatistic.STREAM_READ_SEEK_BACKWARD_OPERATIONS);
    incrementCounter(GhfsStatistic.STREAM_READ_SEEK_BYTES_BACKWARDS, -negativeOffset);
  }

  void streamReadSeekForward(long skipped) {
    if (skipped > 0) {
      incrementCounter(GhfsStatistic.STREAM_READ_SEEK_BYTES_SKIPPED, skipped);
    }

    increment(GhfsStatistic.STREAM_READ_SEEK_FORWARD_OPERATIONS);
  }

  void streamWriteBytes(int bytesWritten) {
    incrementCounter(GhfsStatistic.STREAM_WRITE_BYTES, bytesWritten);
  }

  void filesCreated() {
    increment(FILES_CREATED);
  }

  void getFileCheckSum() {
    increment(INVOCATION_GET_FILE_CHECKSUM);
  }

  private class LongIterator implements Iterator<LongStatistic> {
    // TODO: Include statistic related metrics as well.
    private Iterator<Map.Entry<GhfsStatistic, AtomicLong>> iterator =
        Collections.unmodifiableSet(opsCount.entrySet()).iterator();

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public LongStatistic next() {
      if (!iterator.hasNext()) {
        throw new NoSuchElementException();
      }
      final Map.Entry<GhfsStatistic, AtomicLong> entry = iterator.next();
      return new LongStatistic(entry.getKey().getSymbol(), entry.getValue().get());
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public Iterator<LongStatistic> getLongStatistics() {
    return new LongIterator();
  }

  @Override
  public Long getLong(String key) {
    final GhfsStatistic type = GhfsStatistic.fromSymbol(key);
    return type == null ? 0L : opsCount.get(type).get();
  }

  @Override
  public boolean isTracked(String key) {
    return GhfsStatistic.fromSymbol(key) != null;
  }

  /**
   * To get the minimum value which is stored with MINIMUM extension
   *
   * @param symbol
   * @return minimum statistic value
   */
  public Long getMin(String symbol) {
    return getStatisticValue(symbol, minimums);
  }

  /**
   * To get the maximum value which is stored with MAXIMUM extension
   *
   * @param symbol
   * @return maximum statistic value
   */
  public Long getMax(String symbol) {
    return getStatisticValue(symbol, maximums);
  }

  /**
   * To get the mean value which is stored with MEAN extension
   *
   * @param symbol
   * @return mean statistic value
   */
  public double getMean(String key) {
    final GhfsStatistic type = GhfsStatistic.fromSymbol(key);
    MeanStatistic val = means.get(type);
    if (val == null) {
      return 0;
    }

    return val.getValue();
  }

  private long getStatisticValue(String key, Map<GhfsStatistic, AtomicLong> stats) {
    final GhfsStatistic stat = GhfsStatistic.fromSymbol(key);
    if (stat == null) {
      return 0L;
    }

    AtomicLong val = stats.get(stat);
    if (val == null) {
      return 0L;
    }

    return val.get();
  }

  /** This class keeps track of mean statistics by keeping track of sum and number of samples. */
  static class MeanStatistic {
    private int sample;

    private long sum;

    synchronized void addSample(long val) {
      addSample(val, 1);
    }

    double getValue() {
      if (sample == 0) {
        return 0;
      }

      return sum / sample;
    }

    synchronized void addSample(long total, int count) {
      sample += count;
      sum += total;
    }
  }
}
