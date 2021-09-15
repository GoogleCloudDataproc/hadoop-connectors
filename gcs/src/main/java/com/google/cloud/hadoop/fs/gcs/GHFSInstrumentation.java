/*
 * Copyright 2013 Google Inc. All Rights Reserved.
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

import com.google.common.flogger.GoogleLogger;
import java.io.Closeable;
import java.net.URI;
import java.time.Duration;
import java.util.EnumSet;
import java.util.UUID;
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
 * Instrumentation of GCS.
 *
 * <p>History
 *
 * <ol>
 *   <li>HADOOP-13028. Initial implementation. Derived from the {@code S3AInstrumentation}.
 *   <li>Broadly (and directly) used in gcs. The use of direct references causes "problems" in
 *       mocking tests.
 *   <li>HADOOP-16830. IOStatistics. Move to an interface and implementation design for the
 *       different inner classes.
 * </ol>
 *
 * <p>Counters and metrics are generally addressed in code by their name or {@link GHFSStatistic}
 * key. There <i>may</i> be some Statistics which do not have an entry here. To avoid attempts to
 * access such counters failing, the operations to increment/query metric values are designed to
 * handle lookup failures.
 *
 * <p>GoogleHadoopFileSystem StorageStatistics are dynamically derived from the IOStatistics.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
class GHFSInstrumentation
    implements Closeable, MetricsSource, IOStatisticsSource, DurationTrackerFactory {
  public static final String CONTEXT = "DFSClient";
  public static final String METRICS_SYSTEM_NAME = "google-hadoop-file-system";
  public static final String METRIC_TAG_FILESYSTEM_ID = "gcsFilesystemId";
  public static final String METRIC_TAG_BUCKET = "bucket";
  private static final Object METRICS_SYSTEM_LOCK = new Object();
  private final MetricsRegistry registry =
      new MetricsRegistry("googleHadoopFilesystem").setContext(CONTEXT);
  private IOStatisticsStore instanceIOStatistics;
  private DurationTrackerFactory durationTrackerFactory;
  private static int metricsSourceNameCounter = 0;
  private static int metricsSourceActiveCounter = 0;
  private String metricsSourceName;
  private static final String METRICS_SOURCE_BASENAME = "GCSMetrics";
  private static MetricsSystem metricsSystem = null;
  private static final GoogleLogger LOG = GoogleLogger.forEnclosingClass();

  public GHFSInstrumentation(URI name) {
    UUID fileSystemInstanceID = UUID.randomUUID();
    registry.tag(
        METRIC_TAG_FILESYSTEM_ID,
        "A unique identifier for the instance",
        fileSystemInstanceID.toString());
    registry.tag(METRIC_TAG_BUCKET, "Hostname from the FS URL", name.getHost());
    IOStatisticsStoreBuilder storeBuilder = IOStatisticsBinding.iostatisticsStore();
    // declare all counter statistics
    EnumSet.allOf(GHFSStatistic.class).stream()
        .filter(GHFSStatistic -> GHFSStatistic.getType() == GHFSStatisticTypeEnum.TYPE_COUNTER)
        .forEach(
            stat -> {
              counter(stat);
              storeBuilder.withCounters(stat.getSymbol());
            });
    // declare all gauge statistics
    EnumSet.allOf(GHFSStatistic.class).stream()
        .filter(statistic -> statistic.getType() == GHFSStatisticTypeEnum.TYPE_GAUGE)
        .forEach(
            stat -> {
              gauge(stat);
              storeBuilder.withGauges(stat.getSymbol());
            });

    // and durations
    EnumSet.allOf(GHFSStatistic.class).stream()
        .filter(statistic -> statistic.getType() == GHFSStatisticTypeEnum.TYPE_DURATION)
        .forEach(
            stat -> {
              duration(stat);
              storeBuilder.withDurationTracking(stat.getSymbol());
            });
    // register with Hadoop metrics
    registerAsMetricsSource(name);
    // and build the IO Statistics
    instanceIOStatistics = storeBuilder.build();
    // duration track metrics (Success/failure) and IOStatistics.
    durationTrackerFactory =
        IOStatisticsBinding.pairedTrackerFactory(
            instanceIOStatistics, new MetricDurationTrackerFactory());
  }

  public void close() {
    LOG.atFine().log("Close");
  }

  /**
   * Get the instance IO Statistics.
   *
   * @return statistics.
   */
  @Override
  public IOStatisticsStore getIOStatistics() {
    return instanceIOStatistics;
  }

  /**
   * Increments a mutable counter and the matching instance IOStatistics counter. No-op if the
   * counter is not defined, or the count == 0.
   *
   * @param op operation
   * @param count increment value
   */
  public void incrementCounter(GHFSStatistic op, long count) {
    if (count != 0) {
      String name = op.getSymbol();
      incrementMutableCounter(name, count);
      instanceIOStatistics.incrementCounter(name, count);
    }
  }

  /**
   * Register this instance as a metrics source.
   *
   * @param name gs:// URI for the associated FileSystem instance
   */
  private void registerAsMetricsSource(URI name) {
    int number;
    synchronized (METRICS_SYSTEM_LOCK) {
      getMetricsSystem();

      metricsSourceActiveCounter++;
      number = ++metricsSourceNameCounter;
    }
    String msName = METRICS_SOURCE_BASENAME + number;
    metricsSourceName = msName + "-" + name.getHost();
    metricsSystem.register(metricsSourceName, "", this);
  }

  public MetricsSystem getMetricsSystem() {
    synchronized (METRICS_SYSTEM_LOCK) {
      if (metricsSystem == null) {
        metricsSystem = new MetricsSystemImpl();
        metricsSystem.init(METRICS_SYSTEM_NAME);
      }
    }
    return metricsSystem;
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
  protected final MutableCounterLong counter(GHFSStatistic op) {
    return counter(op.getSymbol(), op.getDescription());
  }

  /**
   * Registering a duration adds the success and failure counters.
   *
   * @param op statistic to track
   */
  protected final void duration(GHFSStatistic op) {
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
  protected final MutableGaugeLong gauge(GHFSStatistic op) {
    return gauge(op.getSymbol(), op.getDescription());
  }

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
    MutableMetric metric = getRegistry().get(name);
    return metric;
  }

  private MutableCounterLong lookupCounter(String name) {
    MutableMetric metric = lookupMetric(name);
    if (metric == null) {
      return null;
    }
    if (!(metric instanceof MutableCounterLong)) {
      throw new IllegalStateException(
          "Metric "
              + name
              + " is not a MutableCounterLong: "
              + metric
              + " (type: "
              + metric.getClass()
              + ")");
    }
    return (MutableCounterLong) metric;
  }

  /**
   * Increments a Mutable counter. No-op if not a positive integer.
   *
   * @param name counter name.
   * @param count increment value
   */
  private void incrementMutableCounter(final String name, final long count) {
    if (count > 0) {
      MutableCounterLong counter = lookupCounter(name);
      if (counter != null) {
        counter.incr(count);
      }
    }
  }

  private final class MetricUpdatingDurationTracker implements DurationTracker {

    private final String symbol;

    private boolean failed;

    private MetricUpdatingDurationTracker(final String symbol, final long count) {
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

    @Override
    public DurationTracker trackDuration(final String key, final long count) {
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
  public DurationTracker trackDuration(final String key, final long count) {
    return durationTrackerFactory.trackDuration(key, count);
  }

  /**
   * Add the duration as a timed statistic, deriving statistic name from the operation symbol and
   * the outcome.
   *
   * @param op operation
   * @param success was the operation a success?
   * @param duration how long did it take
   */
  public void recordDuration(
      final GHFSStatistic op, final boolean success, final Duration duration) {
    String name = op.getSymbol() + (success ? "" : SUFFIX_FAILURES);
    instanceIOStatistics.addTimedOperation(name, duration);
  }

  @Override
  public void getMetrics(MetricsCollector metricsCollector, boolean b) {}

  /** Indicate that GCS created a file. */
  public void fileCreated() {
    incrementCounter(GHFSStatistic.FILES_CREATED, 1);
  }

  /**
   * Indicate that GCS deleted one or more files.
   *
   * @param count number of files.
   */
  public void fileDeleted(int count) {
    incrementCounter(GHFSStatistic.FILES_DELETED, count);
  }
}
