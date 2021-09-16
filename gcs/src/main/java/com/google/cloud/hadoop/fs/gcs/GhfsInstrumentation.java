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

import static org.apache.hadoop.fs.statistics.IOStatisticsSupport.snapshotIOStatistics;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.SUFFIX_FAILURES;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.iostatisticsStore;

import com.google.common.flogger.GoogleLogger;
import java.io.Closeable;
import java.net.URI;
import java.time.Duration;
import java.util.EnumSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.statistics.*;
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
 * <p>Counters and metrics are generally addressed in code by their name or {@link GhfsStatistic}
 * key. There <i>may</i> be some Statistics which do not have an entry here. To avoid attempts to
 * access such counters failing, the operations to increment/query metric values are designed to
 * handle lookup failures.
 *
 * <p>GoogleHadoopFileSystem StorageStatistics are dynamically derived from the IOStatistics.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
class GhfsInstrumentation
    implements Closeable, MetricsSource, IOStatisticsSource, DurationTrackerFactory {
  public static final String CONTEXT = "gcsFilesystem";
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

  public GhfsInstrumentation(URI name) {
    UUID fileSystemInstanceID = UUID.randomUUID();
    registry.tag(
        METRIC_TAG_FILESYSTEM_ID,
        "A unique identifier for the instance",
        fileSystemInstanceID.toString());
    registry.tag(METRIC_TAG_BUCKET, "Hostname from the FS URL", name.getHost());
    IOStatisticsStoreBuilder storeBuilder = IOStatisticsBinding.iostatisticsStore();
    // declare all counter statistics
    EnumSet.allOf(GhfsStatistic.class).stream()
        .filter(GhfsStatistic -> GhfsStatistic.getType() == GhfsStatisticTypeEnum.TYPE_COUNTER)
        .forEach(
            stat -> {
              counter(stat);
              storeBuilder.withCounters(stat.getSymbol());
            });
    // declare all gauge statistics
    EnumSet.allOf(GhfsStatistic.class).stream()
        .filter(statistic -> statistic.getType() == GhfsStatisticTypeEnum.TYPE_GAUGE)
        .forEach(
            stat -> {
              gauge(stat);
              storeBuilder.withGauges(stat.getSymbol());
            });

    // and durations
    EnumSet.allOf(GhfsStatistic.class).stream()
        .filter(statistic -> statistic.getType() == GhfsStatisticTypeEnum.TYPE_DURATION)
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
  public void incrementCounter(GhfsStatistic op, long count) {
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
          final GhfsStatistic op, final boolean success, final Duration duration) {
    String name = op.getSymbol() + (success ? "" : SUFFIX_FAILURES);
    instanceIOStatistics.addTimedOperation(name, duration);
  }

  @Override
  public void getMetrics(MetricsCollector metricsCollector, boolean b) {}

  /** Indicate that GCS created a file. */
  public void fileCreated() {
    incrementCounter(GhfsStatistic.FILES_CREATED, 1);
  }

  /**
   * Indicate that GCS deleted one or more files.
   *
   * @param count number of files.
   */
  public void fileDeleted(int count) {
    incrementCounter(GhfsStatistic.FILES_DELETED, count);
  }
  /**
   * Create a stream input statistics instance.
   *
   * @return the new instance
   * @param filesystemStatistics FS Statistics to update in close().
   */
  public GhfsInputStreamStatistics newInputStreamStatistics(
      @Nullable final FileSystem.Statistics filesystemStatistics) {
    return new InputStreamStatistics(filesystemStatistics);
  }

  /**
   * Create a stream output statistics instance.
   *
   * @param filesystemStatistics thread-local FS statistics.
   * @return the new instance
   */
  public GhfsOutputStreamStatistics newOutputStreamStatistics(
      FileSystem.Statistics filesystemStatistics) {
    return new OutputStreamStatistics(filesystemStatistics);
  }

  /**
   * Statistics updated by an GoogleHadoopFSDataInputStream during its actual operation.
   *
   * <p>When {@code close()} is called, the final set of numbers are propagated to the
   * GoogleHadoopFileSystem metrics. The {@link FileSystem.Statistics} statistics passed in are also
   * updated. This ensures that whichever thread calls close() gets the total count of bytes read,
   * even if any work is done in other threads.
   */
  private final class InputStreamStatistics extends AbstractGhfsStatisticsSource
      implements GhfsInputStreamStatistics {

    /** Distance used when incrementing FS stats. */
    private static final int DISTANCE = 5;

    /** FS statistics for the thread creating the stream. */
    private final FileSystem.Statistics filesystemStatistics;

    /** The statistics from the last merge. */
    private IOStatisticsSnapshot mergedStats;
    /*
    The core counters are extracted to atomic longs for slightly
    faster resolution on the critical paths, especially single byte
    reads and the like.
    */

    private final AtomicLong backwardSeekOperations;
    private final AtomicLong bytesBackwardsOnSeek;
    /** Bytes read by the application. */
    private final AtomicLong bytesRead;
    private final AtomicLong bytesSkippedOnSeek;
    private final AtomicLong forwardSeekOperations;
    private final AtomicLong readExceptions;
    private final AtomicLong readsIncomplete;
    private final AtomicLong readOperations;
    private final AtomicLong seekOperations;

    /** Bytes read by the application and any when draining streams . */
    private final AtomicLong totalBytesRead;

    /**
     * Instantiate.
     *
     * @param filesystemStatistics FS Statistics to update in close().
     */
    private InputStreamStatistics(@Nullable FileSystem.Statistics filesystemStatistics) {
      this.filesystemStatistics = filesystemStatistics;
      IOStatisticsStore st =
          iostatisticsStore()
              .withCounters(
                  StreamStatisticNames.STREAM_READ_CLOSE_OPERATIONS,
                  StreamStatisticNames.STREAM_READ_BYTES,
                  StreamStatisticNames.STREAM_READ_EXCEPTIONS,
                  StreamStatisticNames.STREAM_READ_OPERATIONS,
                  StreamStatisticNames.STREAM_READ_OPERATIONS_INCOMPLETE,
                  StreamStatisticNames.STREAM_READ_SEEK_OPERATIONS,
                  StreamStatisticNames.STREAM_READ_SEEK_BACKWARD_OPERATIONS,
                  StreamStatisticNames.STREAM_READ_SEEK_FORWARD_OPERATIONS,
                  StreamStatisticNames.STREAM_READ_SEEK_BYTES_BACKWARDS,
                  StreamStatisticNames.STREAM_READ_SEEK_BYTES_SKIPPED,
                  StreamStatisticNames.STREAM_READ_TOTAL_BYTES)
              .build();
      setIOStatistics(st);
      backwardSeekOperations =
          st.getCounterReference(StreamStatisticNames.STREAM_READ_SEEK_BACKWARD_OPERATIONS);
      bytesBackwardsOnSeek =
          st.getCounterReference(StreamStatisticNames.STREAM_READ_SEEK_BYTES_BACKWARDS);
      bytesRead = st.getCounterReference(StreamStatisticNames.STREAM_READ_BYTES);
      bytesSkippedOnSeek =
          st.getCounterReference(StreamStatisticNames.STREAM_READ_SEEK_BYTES_SKIPPED);
      forwardSeekOperations =
          st.getCounterReference(StreamStatisticNames.STREAM_READ_SEEK_FORWARD_OPERATIONS);
      readExceptions = st.getCounterReference(StreamStatisticNames.STREAM_READ_EXCEPTIONS);
      readsIncomplete =
          st.getCounterReference(StreamStatisticNames.STREAM_READ_OPERATIONS_INCOMPLETE);
      readOperations = st.getCounterReference(StreamStatisticNames.STREAM_READ_OPERATIONS);
      seekOperations = st.getCounterReference(StreamStatisticNames.STREAM_READ_SEEK_OPERATIONS);
      totalBytesRead = st.getCounterReference(StreamStatisticNames.STREAM_READ_TOTAL_BYTES);
      setIOStatistics(st);
      // create initial snapshot of merged statistics
      mergedStats = snapshotIOStatistics(st);
    }
    /**
     * Increment a named counter by one.
     *
     * @param name counter name
     * @return the new value
     */
    private long increment(String name) {
      return increment(name, 1);
    }

    /**
     * Increment a named counter by a given value.
     *
     * @param name counter name
     * @param value value to increment by.
     * @return the new value
     */
    private long increment(String name, long value) {
      return incCounter(name, value);
    }

    /**
     * Get the inner class's IO Statistics. This is needed to avoid findbugs warnings about
     * ambiguity.
     *
     * @return the Input Stream's statistics.
     */
    private IOStatisticsStore localIOStatistics() {
      return InputStreamStatistics.super.getIOStatistics();
    }

    /**
     * {@inheritDoc}. Increments the number of seek operations, and backward seek operations. The
     * offset is inverted and used as the increment of {@link #bytesBackwardsOnSeek}.
     */
    @Override
    public void seekBackwards(long negativeOffset) {
      seekOperations.incrementAndGet();
      backwardSeekOperations.incrementAndGet();
      bytesBackwardsOnSeek.addAndGet(-negativeOffset);
    }

    /**
     * {@inheritDoc}. Increment the number of seek and forward seek operations, as well as counters
     * of bytes skipped in seek, where appropriate.
     */
    @Override
    public void seekForwards(final long skipped) {
      seekOperations.incrementAndGet();
      forwardSeekOperations.incrementAndGet();
      if (skipped > 0) {
        bytesSkippedOnSeek.addAndGet(skipped);
      }
    }

    /** {@inheritDoc}. */
    @Override
    public void readException() {
      readExceptions.incrementAndGet();
    }

    /** {@inheritDoc}. If the byte counter is positive, increment bytesRead and totalBytesRead. */
    @Override
    public void bytesRead(long bytes) {
      if (bytes > 0) {
        bytesRead.addAndGet(bytes);
        totalBytesRead.addAndGet(bytes);
      }
    }

    @Override
    public void readOperationStarted(long pos, long len) {
      readOperations.incrementAndGet();
    }

    /**
     * {@inheritDoc}. If more data was requested than was actually returned, this was an incomplete
     * read. Increment {@link #readsIncomplete}.
     */
    @Override
    public void readOperationCompleted(int requested, int actual) {
      if (requested > actual) {
        readsIncomplete.incrementAndGet();
      }
    }

    /**
     * {@code close()} merges the stream statistics into the filesystem's instrumentation instance.
     */
    @Override
    public void close() {
      increment(StreamStatisticNames.STREAM_READ_CLOSE_OPERATIONS);
      merge(true);
    }

    @Override
    public long getCloseOperations() {
      return lookupCounterValue(StreamStatisticNames.STREAM_READ_CLOSE_OPERATIONS);
    }

    @Override
    public long getForwardSeekOperations() {
      return lookupCounterValue(StreamStatisticNames.STREAM_READ_SEEK_FORWARD_OPERATIONS);
    }

    @Override
    public long getBackwardSeekOperations() {
      return lookupCounterValue(StreamStatisticNames.STREAM_READ_SEEK_BACKWARD_OPERATIONS);
    }

    @Override
    public long getBytesRead() {
      return lookupCounterValue(StreamStatisticNames.STREAM_READ_BYTES);
    }

    @Override
    public long getTotalBytesRead() {
      return lookupCounterValue(StreamStatisticNames.STREAM_READ_TOTAL_BYTES);
    }

    @Override
    public long getBytesSkippedOnSeek() {
      return lookupCounterValue(StreamStatisticNames.STREAM_READ_SEEK_BYTES_SKIPPED);
    }

    @Override
    public long getBytesBackwardsOnSeek() {
      return lookupCounterValue(StreamStatisticNames.STREAM_READ_SEEK_BYTES_BACKWARDS);
    }

    @Override
    public long getSeekOperations() {
      return lookupCounterValue(StreamStatisticNames.STREAM_READ_SEEK_OPERATIONS);
    }

    @Override
    public long getReadExceptions() {
      return lookupCounterValue(StreamStatisticNames.STREAM_READ_EXCEPTIONS);
    }

    @Override
    public long getReadOperations() {
      return lookupCounterValue(StreamStatisticNames.STREAM_READ_OPERATIONS);
    }

    @Override
    public long getReadsIncomplete() {
      return lookupCounterValue(StreamStatisticNames.STREAM_READ_OPERATIONS_INCOMPLETE);
    }

    /**
     * Merge the statistics into the filesystem's instrumentation instance.
     *
     * <p>If the merge is invoked because the stream has been closed, then all statistics are
     * merged, and the filesystem statistics of {@link #filesystemStatistics} updated with the bytes
     * read values.
     *
     * <p>Whichever thread close()d the stream will have its counters updated.
     *
     * @param isClosed is this merge invoked because the stream is closed?
     */
    private void merge(boolean isClosed) {

      IOStatisticsStore ioStatistics = localIOStatistics();
      promoteInputStreamCountersToMetrics();
      mergedStats = snapshotIOStatistics(localIOStatistics());

      if (isClosed) {
        // stream is being closed.
        // merge in all the IOStatistics
        GhfsInstrumentation.this.getIOStatistics().aggregate(ioStatistics);

        // increment the filesystem statistics for this thread.
        if (filesystemStatistics != null) {
          long t = getTotalBytesRead();
          filesystemStatistics.incrementBytesRead(t);
          filesystemStatistics.incrementBytesReadByDistance(DISTANCE, t);
        }
      }
    }

    /**
     * Merge in the statistics of a single input stream into the filesystem-wide metrics counters.
     * This does not update the FS IOStatistics values.
     */
    private void promoteInputStreamCountersToMetrics() {
      // iterate through all the counters
      localIOStatistics().counters().keySet().stream().forEach(e -> promoteIOCounter(e));
    }
    /**
     * Propagate a counter from the instance-level statistics to the GHFS instrumentation,
     * subtracting the previous merged value.
     *
     * @param name statistic to promote
     */
    void promoteIOCounter(String name) {
      incrementMutableCounter(name, lookupCounterValue(name) - mergedStats.counters().get(name));
    }
  }

  /**
   * Statistics updated by an output stream during its actual operation.
   *
   * <p>Some of these stats are propagated to any passed in {@link FileSystem.Statistics} instance;
   * this is done in close() for better cross-thread accounting.
   *
   * <p>Some of the collected statistics are not directly served via IOStatistics. They are added to
   * the instrumentation IOStatistics and metric counters during the {@link
   * #mergeOutputStreamStatistics(OutputStreamStatistics)} operation.
   */
  private final class OutputStreamStatistics extends AbstractGhfsStatisticsSource
      implements GhfsOutputStreamStatistics {
    private final AtomicLong bytesWritten;
    private final AtomicLong writeExceptions;
    private final FileSystem.Statistics filesystemStatistics;

    /**
     * Instantiate.
     *
     * @param filesystemStatistics FS Statistics to update in close().
     */
    private OutputStreamStatistics(@Nullable FileSystem.Statistics filesystemStatistics) {
      this.filesystemStatistics = filesystemStatistics;
      IOStatisticsStore st =
          iostatisticsStore()
              .withCounters(
                  GhfsStatistic.STREAM_WRITE_BYTES.getSymbol(),
                  GhfsStatistic.STREAM_WRITE_EXCEPTIONS.getSymbol())
              .build();
      setIOStatistics(st);
      // these are extracted to avoid lookups on heavily used counters.

      bytesWritten = st.getCounterReference(StreamStatisticNames.STREAM_WRITE_BYTES);
      writeExceptions = st.getCounterReference(StreamStatisticNames.STREAM_WRITE_EXCEPTIONS);
    }

    /**
     * Get the inner class's IO Statistics. This is needed to avoid findbugs warnings about
     * ambiguity.
     *
     * @return the Input Stream's statistics.
     */
    private IOStatisticsStore localIOStatistics() {
      return OutputStreamStatistics.super.getIOStatistics();
    }

    @Override
    public void close() {
      mergeOutputStreamStatistics(this);
      // and patch the FS statistics.
      // provided the stream is closed in the worker thread, this will
      // ensure that the thread-specific worker stats are updated.
      if (filesystemStatistics != null) {
        long t = getBytesWritten();
        filesystemStatistics.incrementBytesWritten(t);
      }
    }

    /**
     * Record bytes written.
     *
     * @param count number of bytes
     */
    @Override
    public void writeBytes(long count) {
      bytesWritten.addAndGet(count);
    }

    /** {@inheritDoc}. */
    @Override
    public void writeException() {
      writeExceptions.incrementAndGet();
    }

    /**
     * Get the current count of bytes written.
     *
     * @return the counter value.
     */
    @Override
    public long getBytesWritten() {
      return bytesWritten.get();
    }

    @Override
    public long getWriteExceptions() {
      return lookupCounterValue(StreamStatisticNames.STREAM_WRITE_EXCEPTIONS);
    }

  }
  /**
   * Merge in the statistics of a single output stream into the filesystem-wide statistics.
   *
   * @param source stream statistics
   */
  private void mergeOutputStreamStatistics(OutputStreamStatistics source) {

    incrementCounter(
        GhfsStatistic.STREAM_WRITE_EXCEPTIONS,
        source.lookupCounterValue(StreamStatisticNames.STREAM_WRITE_EXCEPTIONS));
    // merge in all the IOStatistics
    this.getIOStatistics().aggregate(source.getIOStatistics());
  }
}
