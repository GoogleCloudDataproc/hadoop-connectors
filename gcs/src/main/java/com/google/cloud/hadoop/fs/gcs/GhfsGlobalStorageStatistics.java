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

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.EXCEPTION_COUNT;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.GCS_CLIENT_RATE_LIMIT_COUNT;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.GCS_CLIENT_SIDE_ERROR_COUNT;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.GCS_REQUEST_COUNT;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.GCS_SERVER_SIDE_ERROR_COUNT;
import static com.google.cloud.hadoop.gcsio.StatisticTypeEnum.TYPE_DURATION;
import static com.google.common.base.Preconditions.checkArgument;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpResponseException;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics;
import com.google.cloud.hadoop.util.GcsRequestExecutionEvent;
import com.google.cloud.hadoop.util.GoogleCloudStorageEventBus.StatisticsType;
import com.google.cloud.hadoop.util.ITraceFactory;
import com.google.cloud.hadoop.util.ITraceOperation;
import com.google.common.base.Stopwatch;
import com.google.common.eventbus.Subscribe;
import com.google.common.flogger.GoogleLogger;
import io.grpc.Status;
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
import org.apache.hadoop.fs.statistics.DurationTrackerFactory;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding;
import org.apache.hadoop.util.functional.CallableRaisingIOE;

/** Storage statistics for GCS */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class GhfsGlobalStorageStatistics extends StorageStatistics {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  /** {@value} The key that stores all the registered metrics */
  public static final String NAME = "GhfsStorageStatistics";

  public static final int LATENCY_LOGGING_THRESHOLD_MS = 500;

  // Instance to be used if it encounters any error while registering to Global Statistics.
  // Error can happen for e.g. when different class loaders are used.
  // If this instance is used, the metrics will not be reported to metrics sinks.
  static final GhfsGlobalStorageStatistics DUMMY_INSTANCE = new GhfsGlobalStorageStatistics();

  private final Map<String, AtomicLong> opsCount = new HashMap<>();
  private final Map<String, AtomicLong> minimums = new HashMap<>();
  private final Map<String, AtomicLong> maximums = new HashMap<>();
  private final Map<String, MeanStatistic> means = new HashMap<>();

  public GhfsGlobalStorageStatistics() {
    super(NAME);

    for (GoogleCloudStorageStatistics opType : GoogleCloudStorageStatistics.values()) {
      String symbol = opType.getSymbol();
      opsCount.put(symbol, new AtomicLong(0));
    }

    for (GhfsStatistic opType : GhfsStatistic.values()) {
      String symbol = opType.getSymbol();
      opsCount.put(symbol, new AtomicLong(0));

      if (opType.getType() == TYPE_DURATION) {
        minimums.put(getMinKey(symbol), null);
        maximums.put(getMaxKey(symbol), new AtomicLong(0));
        means.put(getMeanKey(symbol), new MeanStatistic());
      }
    }
  }

  static <B> B trackDuration(
      DurationTrackerFactory factory,
      @Nonnull GhfsGlobalStorageStatistics stats,
      GhfsStatistic statistic,
      Object context,
      ITraceFactory traceFactory,
      CallableRaisingIOE<B> operation)
      throws IOException {
    Stopwatch stopwatch = Stopwatch.createStarted();

    try (ITraceOperation opFS =
        traceFactory.createRootWithLogging(statistic.getSymbol(), context)) {
      stats.increment(statistic);
      return IOStatisticsBinding.trackDuration(factory, statistic.getSymbol(), operation);
    } finally {
      stats.updateStats(statistic, stopwatch.elapsed().toMillis(), context);
    }
  }

  private long increment(GhfsStatistic statistic) {
    return incrementCounter(statistic, 1);
  }

  private void increment(GoogleCloudStorageStatistics statistic) {
    incrementCounter(statistic, 1);
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

  /**
   * Increment a specific counter.
   *
   * @param op operation
   * @param count increment value
   */
  void incrementCounter(GoogleCloudStorageStatistics op, long count) {
    opsCount.get(op.getSymbol()).addAndGet(count);
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
        statistic.getType() == TYPE_DURATION,
        String.format("Unexpected instrumentation type %s", statistic));
    updateMinMaxStats(statistic, durationMs, durationMs, context);

    addMeanStatistic(statistic, durationMs, 1);
  }

  private void addMeanStatistic(GhfsStatistic statistic, long totalDurationMs, int count) {
    String meanKey = getMeanKey(statistic.getSymbol());
    if (means.containsKey(meanKey)) {
      means.get(meanKey).addSample(totalDurationMs, count);
    }
  }

  void updateStats(
      GhfsStatistic statistic,
      long minLatency,
      long maxLatency,
      long totalDuration,
      int count,
      Object context) {

    updateMinMaxStats(statistic, minLatency, maxLatency, context);
    addMeanStatistic(statistic, totalDuration, count);
    opsCount.get(statistic.getSymbol()).addAndGet(count);
  }

  private void updateMinMaxStats(
      GhfsStatistic statistic, long minDurationMs, long maxDurationMs, Object context) {
    String minKey = getMinKey(statistic.getSymbol());

    AtomicLong minVal = minimums.get(minKey);
    if (minVal == null) {
      // There can be race here. It is ok to have the last write win.
      minimums.put(minKey, new AtomicLong(minDurationMs));
    } else if (minDurationMs < minVal.get()) {
      minVal.set(minDurationMs);
    }

    String maxKey = getMaxKey(statistic.getSymbol());
    AtomicLong maxVal = maximums.get(maxKey);
    if (maxDurationMs > maxVal.get()) {

      // Log is avoided if the first request exceedes threshold
      if (maxDurationMs > LATENCY_LOGGING_THRESHOLD_MS
          && opsCount.get(statistic.getSymbol()).get() > 0) {
        logger.atInfo().log(
            "Detected potential high latency for operation %s. latencyMs=%s; previousMaxLatencyMs=%s; operationCount=%s; context=%s",
            statistic, maxDurationMs, maxVal.get(), opsCount.get(statistic.getSymbol()), context);
      }

      // There can be race here and can have some data points get missed. This is a corner case.
      // Since this function can be called quite frequently, opting for performance over consistency
      // here.
      maxVal.set(maxDurationMs);
    }
  }

  /**
   * Updating the required gcs specific statistics based on httpresponse.
   *
   * @param statusCode
   */
  private void updateGcsIOSpecificStatistics(int statusCode) {

    if (statusCode >= 400 && statusCode < 500) {
      incrementGcsClientSideCounter();

      if (statusCode == 429) {
        incrementRateLimitingCounter();
      }
    }

    if (statusCode >= 500 && statusCode < 600) {
      incrementGcsServerSideCounter();
    }
  }

  private int grpcToHttpStatusCodeMapping(Status grpcStatusCode) {
    // using code.proto as reference
    // https://github.com/googleapis/googleapis/blob/master/google/rpc/code.proto
    switch (grpcStatusCode.getCode()) {
      case OK:
        return 200;
      case CANCELLED:
        return 499;
      case INVALID_ARGUMENT:
      case FAILED_PRECONDITION:
      case OUT_OF_RANGE:
        return 400;
      case DEADLINE_EXCEEDED:
        return 504;
      case NOT_FOUND:
        return 404;
      case ALREADY_EXISTS:
      case ABORTED:
        return 409;
      case PERMISSION_DENIED:
        return 403;
      case RESOURCE_EXHAUSTED:
        return 429;
      case UNIMPLEMENTED:
        return 501;
      case UNAVAILABLE:
        return 503;
      case UNAUTHENTICATED:
        return 401;
      case UNKNOWN:
      case INTERNAL:
      case DATA_LOSS:
      default:
        return 500;
    }
  }

  /**
   * Updating the required gcs specific statistics based on HttpResponseException.
   *
   * @param responseException contains statusCode based on which metrics are updated
   */
  @Subscribe
  private void subscriberOnHttpResponseException(@Nonnull HttpResponseException responseException) {
    updateGcsIOSpecificStatistics(responseException.getStatusCode());
  }

  /**
   * Updating the required gcs specific statistics based on GoogleJsonResponseException.
   *
   * @param responseException contains statusCode based on which metrics are updated
   */
  @Subscribe
  private void subscriberOnGoogleJsonResponseException(
      @Nonnull GoogleJsonResponseException responseException) {
    updateGcsIOSpecificStatistics(responseException.getStatusCode());
  }

  /**
   * Updating the required gcs specific statistics based on HttpResponse.
   *
   * @param responseStatus responseStatus status code from HTTP response
   */
  @Subscribe
  private void subscriberOnHttpResponseStatus(@Nonnull Integer responseStatus) {
    updateGcsIOSpecificStatistics(responseStatus);
  }

  @Subscribe
  private void subscriberOnGcsRequest(@Nonnull GcsRequestExecutionEvent event) {
    incrementGcsTotalRequestCount();
  }

  @Subscribe
  private void subscriberOnGrpcStatus(@Nonnull Status status) {
    updateGcsIOSpecificStatistics(grpcToHttpStatusCodeMapping(status));
  }

  /**
   * Updating the EXCEPTION_COUNT
   *
   * @param exception
   */
  @Subscribe
  private void subscriberOnException(IOException exception) {
    incrementGcsExceptionCount();
  }

  /**
   * Updating the corresponding statistics
   *
   * @param strType
   */
  @Subscribe
  private void subscriberOnStatisticsType(StatisticsType strType) {
    if (strType == StatisticsType.DIRECTORIES_DELETED) {
      incrementDirectoriesDeleted();
    }
  }

  private void incrementDirectoriesDeleted() {
    increment(GhfsStatistic.DIRECTORIES_DELETED);
  }

  private void incrementGcsExceptionCount() {
    increment(EXCEPTION_COUNT);
  }

  private void incrementGcsTotalRequestCount() {
    increment(GCS_REQUEST_COUNT);
  }

  private void incrementRateLimitingCounter() {
    increment(GCS_CLIENT_RATE_LIMIT_COUNT);
  }

  private void incrementGcsClientSideCounter() {
    increment(GCS_CLIENT_SIDE_ERROR_COUNT);
  }

  private void incrementGcsServerSideCounter() {
    increment(GCS_SERVER_SIDE_ERROR_COUNT);
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
      addSample(val, 1);
    }

    synchronized void addSample(long val, int count) {
      sample += count;
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
