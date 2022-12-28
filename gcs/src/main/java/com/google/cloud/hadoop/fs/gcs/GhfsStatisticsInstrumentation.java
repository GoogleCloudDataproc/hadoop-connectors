/*
 * Copyright 2021 Google Inc. All Rights Reserved.
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

import static org.apache.hadoop.fs.statistics.StoreStatisticNames.SUFFIX_FAILURES;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.iostatisticsStore;

import com.google.common.flogger.GoogleLogger;
import java.io.Closeable;
import java.net.URI;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.statistics.DurationTracker;
import org.apache.hadoop.fs.statistics.DurationTrackerFactory;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStoreBuilder;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.impl.MetricsSystemImpl;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.metrics2.lib.MutableMetric;

/**
 * Instrumentation of GHFS IO Statistics.
 *
 * <p>Counters and metrics are generally addressed in code by their name or {@link GhfsStatistic}
 * key. There <i>may</i> be some statistics which do not have an entry here. To avoid attempts to
 * access such counters failing, the operations to increment/query metric values are designed to
 * handle lookup failures.
 *
 * <p>GHFS {@code StorageStatistics} are dynamically derived from the {@code IOStatistics}.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class GhfsStatisticsInstrumentation
    implements Closeable, MetricsSource, IOStatisticsSource, DurationTrackerFactory {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private static final String METRICS_SOURCE_BASENAME = "GsMetrics";

  /**
   * {@value} Currently all gcs metrics are placed in a single "context". Distinct contexts may be
   * used in the future.
   */
  private static final String CONTEXT = "gsFileSystem";

  /** {@value} The name of the gcs-specific metrics system instance used for gcs metrics. */
  private static final String METRICS_SYSTEM_NAME = "gs-file-system";

  /**
   * {@value} The name of a field added to metrics records that uniquely identifies a specific
   * FileSystem instance.
   */
  private static final String METRIC_TAG_FILESYSTEM_ID = "gsFileSystemId";

  /**
   * {@value} The name of a field added to metrics records that indicates the hostname portion of
   * the FS URL.
   */
  private static final String METRIC_TAG_BUCKET = "bucket";

  private static MetricsSystem metricsSystem = new MetricsSystemImpl().init(METRICS_SYSTEM_NAME);
  private static final AtomicLong metricsSourceNameCounter = new AtomicLong(0);
  private static final AtomicLong metricsSourceActiveCounter = new AtomicLong(0);

  private final MetricsRegistry registry = new MetricsRegistry(CONTEXT).setContext(CONTEXT);

  /**
   * This is the IOStatistics store for the GoogleHadoopFileSystem instance. It is not kept in sync
   * with the rest of the {@link GhfsStatisticsInstrumentation}. Most inner statistics
   * implementation classes only update this store when it is pushed back, such as in close().
   */
  private final IOStatisticsStore ioStatisticsStore;

  /** Duration Tracker Factory for the Instrumentation */
  private final DurationTrackerFactory durationTrackerFactory;

  private final String metricsSourceName;

  /**
   * Construct the instrumentation for a filesystem.
   *
   * @param name URI of filesystem.
   */
  public GhfsStatisticsInstrumentation(URI name) {
    registry
        .tag(
            METRIC_TAG_FILESYSTEM_ID,
            "A unique identifier for the instance",
            UUID.randomUUID().toString())
        .tag(METRIC_TAG_BUCKET, "Hostname from the FS URL", name.getHost());

    // Register with Hadoop MetricsSystem
    metricsSourceActiveCounter.incrementAndGet();
    long number = metricsSourceNameCounter.incrementAndGet();
    metricsSourceName = METRICS_SOURCE_BASENAME + number + "-" + name.getHost();
    metricsSystem.register(metricsSourceName, /* desc= */ "", /* source= */ this);

    ioStatisticsStore = createIoStatisticsStore();

    durationTrackerFactory =
        IOStatisticsBinding.pairedTrackerFactory(
            new MetricDurationTrackerFactory(), ioStatisticsStore);
  }

  public synchronized void close() {
    metricsSystem.unregisterSource(metricsSourceName);
    long activeSources = metricsSourceActiveCounter.decrementAndGet();
    if (activeSources == 0) {
      logger.atInfo().log("Shutting down metrics publisher");
      metricsSystem.publishMetricsNow();
      metricsSystem.shutdown();
      metricsSystem = null;
    }
  }

  /**
   * Get the instance IO Statistics.
   *
   * @return statistics.
   */
  @Override
  public IOStatisticsStore getIOStatistics() {
    return ioStatisticsStore;
  }

  /**
   * Increments a mutable counter and the matching instance IOStatistics counter. No-op if the
   * counter is not defined, or the count == 0.
   *
   * @param op operation
   * @param count increment value
   */
  public void incrementCounter(GhfsStatistic op, long count) {
    if (count == 0) {
      return;
    }
    String name = op.getSymbol();
    incrementMutableCounter(name, count);
    ioStatisticsStore.incrementCounter(name, count);
  }

  /**
   * Create a counter in the registry.
   *
   * @param name counter name
   * @param desc counter description
   * @return a new counter
   */
  protected final MutableCounterLong counter(String name, String desc) {
    return registry.newCounter(name, desc, 0L);
  }

  /**
   * Create a counter in the registry.
   *
   * @param op statistic to count
   * @return a new counter
   */
  protected final MutableCounterLong counter(GhfsStatistic op) {
    return counter(op.getSymbol(), op.getDescription());
  }

  /**
   * Registering a duration adds the success and failure counters.
   *
   * @param op statistic to track
   */
  protected final void duration(GhfsStatistic op) {
    counter(op.getSymbol(), op.getDescription());
    counter(op.getSymbol() + SUFFIX_FAILURES, op.getDescription());
  }

  /**
   * Create a gauge in the registry.
   *
   * @param name name gauge name
   * @param desc description
   * @return the gauge
   */
  protected final MutableGaugeLong gauge(String name, String desc) {
    return registry.newGauge(name, desc, 0L);
  }

  /**
   * Create a gauge in the registry.
   *
   * @param op statistic to count
   * @return the gauge
   */
  protected final MutableGaugeLong gauge(GhfsStatistic op) {
    return gauge(op.getSymbol(), op.getDescription());
  }

  /**
   * Get the metrics registry.
   *
   * @return the registry
   */
  public MetricsRegistry getRegistry() {
    return registry;
  }

  /**
   * Look up a metric from both the registered set and the lighter weight stream entries.
   *
   * @param name metric name
   * @return the metric or null
   */
  public MutableMetric lookupMetric(String name) {
    return getRegistry().get(name);
  }

  /**
   * Lookup a counter by name. Return null if it is not known.
   *
   * @param name counter name
   * @return the counter
   * @throws IllegalStateException if the metric is not a counter
   */
  private MutableCounterLong lookupCounter(String name) {
    MutableMetric metric = lookupMetric(name);
    if (metric == null) {
      return null;
    }
    if (!(metric instanceof MutableCounterLong)) {
      throw new IllegalStateException(
          String.format(
              "Metric %s is not a MutableCounterLong: %s (type: %s)",
              name, metric, metric.getClass()));
    }
    return (MutableCounterLong) metric;
  }

  /**
   * Increments a Mutable counter. No-op if not a positive integer.
   *
   * @param name counter name.
   * @param count increment value
   */
  private void incrementMutableCounter(String name, long count) {
    if (count > 0) {
      MutableCounterLong counter = lookupCounter(name);
      if (counter != null) {
        counter.incr(count);
      }
    }
  }

  /**
   * A duration tracker which updates a mutable counter with a metric. The metric is updated with
   * the count on start; after a failure the failures count is incremented by one.
   */
  private final class MetricUpdatingDurationTracker implements DurationTracker {

    /** Name of the statistics value to be updated */
    private final String symbol;

    private boolean failed;

    private MetricUpdatingDurationTracker(String symbol, long count) {
      this.symbol = symbol;
      incrementMutableCounter(symbol, count);
    }

    @Override
    public void failed() {
      failed = true;
    }

    /** Close: on failure increment any mutable counter of failures. */
    @Override
    public void close() {
      if (failed) {
        incrementMutableCounter(symbol + SUFFIX_FAILURES, 1);
      }
    }
  }

  /**
   * Get the duration tracker factory.
   *
   * @return duration tracking for the instrumentation.
   */
  public DurationTrackerFactory getDurationTrackerFactory() {
    return durationTrackerFactory;
  }

  private final class MetricDurationTrackerFactory implements DurationTrackerFactory {

    /**
     * The duration tracker updates the metrics with the count and IOStatistics will full duration
     * information.
     *
     * @param key statistic key prefix
     * @param count #of times to increment the matching counter in this operation.
     * @return a duration tracker.
     */
    @Override
    public DurationTracker trackDuration(String key, long count) {
      return new MetricUpdatingDurationTracker(key, count);
    }
  }

  /**
   * The duration tracker updates the metrics with the count and IOStatistics will full duration
   * information.
   *
   * @param key statistic key prefix
   * @param count #of times to increment the matching counter in this operation.
   * @return a duration tracker.
   */
  @Override
  public DurationTracker trackDuration(String key, long count) {
    return durationTrackerFactory.trackDuration(key, count);
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {}

  private IOStatisticsStore createIoStatisticsStore() {
    IOStatisticsStoreBuilder storeBuilder = iostatisticsStore();
    for (GhfsStatistic stat : GhfsStatistic.values()) {
      switch (stat.getType()) {
        case COUNTER:
          counter(stat);
          storeBuilder.withCounters(stat.getSymbol());
          break;
        case DURATION:
          duration(stat);
          storeBuilder.withDurationTracking(stat.getSymbol());
          break;
        case GAUGE:
          gauge(stat);
          storeBuilder.withGauges(stat.getSymbol());
          break;
        default:
          throw new IllegalStateException("Unknown statistic type: " + stat.getType());
      }
    }
    return storeBuilder.build();
  }
}
