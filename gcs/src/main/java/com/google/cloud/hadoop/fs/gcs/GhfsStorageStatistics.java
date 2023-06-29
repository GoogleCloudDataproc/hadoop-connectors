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
import java.util.ArrayList;
import java.util.HashMap;
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

  public static final int LATENCY_LOGGING_THRESHOLD_MS = 100;

  private final Map<String, AtomicLong> opsCount = new HashMap<>();
  private final Map<String, AtomicLong> minimums = new HashMap<>();
  private final Map<String, AtomicLong> maximums = new HashMap<>();
  private final Map<String, MeanStatistic> means = new HashMap<>();

  public GhfsStorageStatistics() {
    super(NAME);
    for (GhfsStatistic opType : GhfsStatistic.values()) {
      String symbol = opType.getSymbol();
      opsCount.put(symbol, new AtomicLong(0));

      if (opType.getType() == GhfsStatisticTypeEnum.TYPE_DURATION) {
        minimums.put(getMinKey(symbol), null);
        maximums.put(getMaxKey(symbol), new AtomicLong(0));
        means.put(getMeanKey(symbol), new MeanStatistic());
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
    return opsCount.get(op.getSymbol()).addAndGet(count);
  }

  @Override
  public void reset() {
    resetMetrics(opsCount);
    resetMetrics(maximums);

    for (String ms : means.keySet()) {
      means.get(ms).reset();
    }

    for (String ms : minimums.keySet()) {
      minimums.put(ms, null);
    }
  }

  private void resetMetrics(Map<String, AtomicLong> metrics) {
    for (AtomicLong value : metrics.values()) {
      value.set(0);
    }
  }

  void updateStats(GhfsStatistic statistic, long durationMs, Object context) {
    checkArgument(
        statistic.getType() == GhfsStatisticTypeEnum.TYPE_DURATION,
        String.format("Unexpected instrumentation type %s", statistic));
    String minKey = getMinKey(statistic.getSymbol());

    AtomicLong minVal = minimums.get(minKey);
    if (minVal == null) {
      // There can be race here. It is ok to have the last write win.
      minimums.put(minKey, new AtomicLong(durationMs));
    } else if (durationMs < minVal.get()) {
      minVal.set(durationMs);
    }

    String maxKey = getMaxKey(statistic.getSymbol());
    AtomicLong maxVal = maximums.get(maxKey);
    if (durationMs > maxVal.get()) {
      if (durationMs > LATENCY_LOGGING_THRESHOLD_MS) {
        logger.atWarning().log(
            "Detected potential high latency for operation %s. latencyMs=%s; previousMaxLatencyMs=%s; operationCount=%s; context=%s",
            statistic, durationMs, maxVal.get(), opsCount.get(statistic.getSymbol()), context);
      }

      // There can be race here and can have some data points get missed. This is a corner case.
      // Since this function can be called quite frequently, opting for performance over consistency
      // here.
      maxVal.set(durationMs);
    }

    String meanKey = getMeanKey(statistic.getSymbol());
    if (means.containsKey(meanKey)) {
      means.get(meanKey).addSample(durationMs);
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
    private Iterator<String> iterator = getMetricNames();

    private Iterator<String> getMetricNames() {
      ArrayList<String> metrics = new ArrayList<>();

      metrics.addAll(opsCount.keySet());
      metrics.addAll(minimums.keySet());
      metrics.addAll(maximums.keySet());
      metrics.addAll(means.keySet());

      return metrics.iterator();
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public LongStatistic next() {
      if (!iterator.hasNext()) {
        throw new NoSuchElementException();
      }

      final String entry = iterator.next();
      return new LongStatistic(entry, getValue(entry));
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  private long getValue(String key) {
    if (opsCount.containsKey(key)) {
      return opsCount.get(key).longValue();
    }

    if (maximums.containsKey(key)) {
      return maximums.get(key).longValue();
    }

    if (minimums.containsKey(key) && minimums.get(key) != null) {
      return minimums.get(key).longValue();
    }

    if (means.containsKey(key)) {
      return Math.round(means.get(key).getValue());
    }

    return 0L;
  }

  @Override
  public Iterator<LongStatistic> getLongStatistics() {
    return new LongIterator();
  }

  @Override
  public Long getLong(String key) {
    return this.getValue(key);
  }

  @Override
  public boolean isTracked(String key) {
    return opsCount.containsKey(key)
        || maximums.containsKey(key)
        || minimums.containsKey(key)
        || means.containsKey(key);
  }

  /**
   * To get the minimum value which is stored with MINIMUM extension
   *
   * @param symbol
   * @return minimum statistic value
   */
  public Long getMin(String symbol) {
    AtomicLong minValue = minimums.get(getMinKey(symbol));
    if (minValue == null) {
      return 0L;
    }

    return minValue.longValue();
  }

  private String getMinKey(String symbol) {
    return symbol + "_min";
  }

  private String getMaxKey(String symbol) {
    return symbol + "_max";
  }

  private String getMeanKey(String symbol) {
    return symbol + "_mean";
  }

  /**
   * To get the maximum value which is stored with MAXIMUM extension
   *
   * @param symbol
   * @return maximum statistic value
   */
  public Long getMax(String symbol) {
    AtomicLong maxValue = maximums.get(getMaxKey(symbol));
    if (maxValue == null) {
      return 0L;
    }

    return maxValue.longValue();
  }

  /**
   * To get the mean value which is stored with MEAN extension
   *
   * @param key
   * @return mean statistic value
   */
  public double getMean(String key) {
    MeanStatistic val = means.get(getMeanKey(key));
    if (val == null) {
      return 0;
    }

    return val.getValue();
  }

  /** This class keeps track of mean statistics by keeping track of sum and number of samples. */
  static class MeanStatistic {
    private int sample;

    private long sum;

    synchronized void addSample(long val) {
      sample++;
      sum += val;
    }

    double getValue() {
      if (sample == 0) {
        return 0;
      }

      return sum / Math.max(1, sample); // to protect against the race with reset().
    }

    public void reset() {
      sum = 0;
      sample = 0;
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (Iterator<LongStatistic> it = this.getLongStatistics(); it.hasNext(); ) {
      LongStatistic statistic = it.next();
      if (sb.length() != 0) {
        sb.append(", ");
      }

      sb.append(String.format("%s=%s", statistic.getName(), statistic.getValue()));
    }

    return String.format("[%s]", sb);
  }
}
