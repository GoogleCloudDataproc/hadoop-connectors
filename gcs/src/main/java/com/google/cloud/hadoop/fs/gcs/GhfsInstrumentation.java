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

import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.DELEGATION_TOKENS_ISSUED;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.DIRECTORIES_CREATED;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.DIRECTORIES_DELETED;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.FILES_CREATED;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.FILES_DELETED;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.INVOCATION_HFLUSH;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.INVOCATION_HSYNC;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.STREAM_WRITE_BYTES;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.STREAM_WRITE_CLOSE_OPERATIONS;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.STREAM_WRITE_EXCEPTIONS;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.STREAM_WRITE_OPERATIONS;
import static org.apache.hadoop.fs.statistics.IOStatisticsSupport.snapshotIOStatistics;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.SUFFIX_FAILURES;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.iostatisticsStore;

import com.google.common.flogger.GoogleLogger;
import java.io.Closeable;
import java.net.URI;
import java.util.EnumSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.statistics.DurationTracker;
import org.apache.hadoop.fs.statistics.DurationTrackerFactory;
import org.apache.hadoop.fs.statistics.IOStatisticsSnapshot;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.fs.statistics.StreamStatisticNames;
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
 * <p>Counters and metrics are generally addressed in code by their name or {@link GhfsStatistic}
 * key. There <i>may</i> be some Statistics which do not have an entry here. To avoid attempts to
 * access such counters failing, the operations to increment/query metric values are designed to
 * handle lookup failures.
 *
 * <p>GoogleHadoopFileSystem StorageStatistics are dynamically derived from the IOStatistics.
 */
public class GhfsInstrumentation
    implements Closeable, MetricsSource, IOStatisticsSource, DurationTrackerFactory {

  private static final String METRICS_SOURCE_BASENAME = "GCSMetrics";

  /**
   * {@value} Currently all gcs metrics are placed in a single "context". Distinct contexts may be
   * used in the future.
   */
  private static final String CONTEXT = "GoogleHadoopFilesystem";

  /** {@value} The name of the gcs-specific metrics system instance used for gcs metrics. */
  private static final String METRICS_SYSTEM_NAME = "google-hadoop-file-system";

  /**
   * {@value} The name of a field added to metrics records that uniquely identifies a specific
   * FileSystem instance.
   */
  private static final String METRIC_TAG_FILESYSTEM_ID = "gcsFilesystemId";

  /**
   * {@value} The name of a field added to metrics records that indicates the hostname portion of
   * the FS URL.
   */
  private static final String METRIC_TAG_BUCKET = "bucket";

  /**
   * metricsSystemLock must be used to synchronize modifications to metricsSystem and the following
   * counters.
   */
  private static final Object METRICS_SYSTEM_LOCK = new Object();

  private static MetricsSystem metricsSystem = null;
  private static int metricsSourceNameCounter = 0;
  private static int metricsSourceActiveCounter = 0;

  private final MetricsRegistry registry =
      new MetricsRegistry("googleHadoopFilesystem").setContext(CONTEXT);

  /**
   * This is the IOStatistics store for the GoogleHadoopFileSystem instance. It is not kept in sync
   * with the rest of the Ghfsinstrumentation. Most inner statistics implementation classes only
   * update this store when it is pushed back, such as in close().
   */
  private final IOStatisticsStore instanceIOStatistics;

  /** Duration Tracker Factory for the Instrumentation */
  private final DurationTrackerFactory durationTrackerFactory;

  private String metricsSourceName;

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  /**
   * Construct the instrumentation for a filesystem.
   *
   * @param name URI of filesystem.
   */
  public GhfsInstrumentation(URI name) {
    UUID fileSystemInstanceID = UUID.randomUUID();
    registry.tag(
        METRIC_TAG_FILESYSTEM_ID,
        "A unique identifier for the instance",
        fileSystemInstanceID.toString());
    registry.tag(METRIC_TAG_BUCKET, "Hostname from the FS URL", name.getHost());
    IOStatisticsStoreBuilder storeBuilder = createStoreBuilder();

    // register with Hadoop metrics
    registerAsMetricsSource(name);
    // and build the IO Statistics
    instanceIOStatistics = storeBuilder.build();
    // duration track metrics (Success/failure) and IOStatistics.
    durationTrackerFactory =
        IOStatisticsBinding.pairedTrackerFactory(
            new MetricDurationTrackerFactory(), instanceIOStatistics);
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
    metricsSourceName = METRICS_SOURCE_BASENAME + number + "-" + name.getHost();
    metricsSystem.register(metricsSourceName, "", this);
  }

  public void close() {
    synchronized (METRICS_SYSTEM_LOCK) {
      metricsSystem.unregisterSource(metricsSourceName);
      metricsSourceActiveCounter--;
      int activeSources = metricsSourceActiveCounter;
      if (activeSources == 0) {
        logger.atInfo().log("Shutting down metrics publisher");
        metricsSystem.publishMetricsNow();
        metricsSystem.shutdown();
        metricsSystem = null;
      }
    }
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
    if (count == 0) {
      return;
    }
    String name = op.getSymbol();
    incrementMutableCounter(name, count);
    instanceIOStatistics.incrementCounter(name, count);
  }

  /**
   * Get the metrics system.
   *
   * @return metricsSystem
   */
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
  public void getMetrics(MetricsCollector metricsCollector, boolean b) {}

  /** Indicate that GCS created a file. */
  public void fileCreated() {
    incrementCounter(FILES_CREATED, 1);
  }

  /** Indicate that GCS created a directory. */
  public void directoryCreated() {
    incrementCounter(DIRECTORIES_CREATED, 1);
  }

  /** Indicate that GCS just deleted a directory. */
  public void directoryDeleted() {
    incrementCounter(DIRECTORIES_DELETED, 1);
  }

  /**
   * Indicate that GCS deleted one or more files.
   *
   * @param count number of files.
   */
  public void fileDeleted(int count) {
    incrementCounter(FILES_DELETED, count);
  }

  /**
   * Create a stream input statistics instance.
   *
   * @param filesystemStatistics FS Statistics to update in close().
   * @return the new instance
   */
  public GhfsInputStreamStatistics newInputStreamStatistics(
      @Nullable FileSystem.Statistics filesystemStatistics) {
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
                  StreamStatisticNames.STREAM_READ_BYTES,
                  StreamStatisticNames.STREAM_READ_EXCEPTIONS,
                  StreamStatisticNames.STREAM_READ_OPERATIONS_INCOMPLETE,
                  StreamStatisticNames.STREAM_READ_SEEK_BACKWARD_OPERATIONS,
                  StreamStatisticNames.STREAM_READ_SEEK_FORWARD_OPERATIONS,
                  StreamStatisticNames.STREAM_READ_SEEK_BYTES_BACKWARDS,
                  StreamStatisticNames.STREAM_READ_SEEK_BYTES_SKIPPED,
                  StreamStatisticNames.STREAM_READ_TOTAL_BYTES)
              .withDurationTracking(
                  GhfsStatistic.STREAM_READ_SEEK_OPERATIONS.getSymbol(),
                  GhfsStatistic.STREAM_READ_CLOSE_OPERATIONS.getSymbol(),
                  GhfsStatistic.STREAM_READ_OPERATIONS.getSymbol())
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
      return incrementCounter(name, value);
    }

    /**
     * Get the inner class's IO Statistics. This is needed to avoid findbugs warnings about
     * ambiguity.
     *
     * @return the Input Stream's statistics.
     */
    private IOStatisticsStore localIOStatistics() {
      return super.getIOStatistics();
    }

    /**
     * Increments the number of seek operations, and backward seek operations. The offset is
     * inverted and used as the increment of {@link #bytesBackwardsOnSeek}.
     */
    @Override
    public void seekBackwards(long negativeOffset) {
      backwardSeekOperations.incrementAndGet();
      bytesBackwardsOnSeek.addAndGet(-negativeOffset);
    }

    /**
     * Increment the number of seek and forward seek operations, as well as counters of bytes
     * skipped in seek, where appropriate.
     */
    @Override
    public void seekForwards(long skipped) {
      if (skipped > 0) {
        bytesSkippedOnSeek.addAndGet(skipped);
      }
      forwardSeekOperations.incrementAndGet();
    }

    /** An ignored stream read exception was received. */
    @Override
    public void readException() {
      readExceptions.incrementAndGet();
    }

    /** If the byte counter is positive, increment bytesRead and totalBytesRead. */
    @Override
    public void bytesRead(long bytes) {
      if (bytes > 0) {
        bytesRead.addAndGet(bytes);
        totalBytesRead.addAndGet(bytes);
      }
    }

    /**
     * A read() operation in the input stream has started.
     *
     * @param pos starting position of the read
     * @param len length of bytes to read
     */
    @Override
    public void readOperationStarted(long pos, long len) {
      readOperations.incrementAndGet();
    }

    /**
     * If more data was requested than was actually returned, this was an incomplete read. Increment
     * {@link #readsIncomplete}.
     */
    @Override
    public void readOperationCompleted(int requested, int actual) {
      if (requested > actual) {
        readsIncomplete.incrementAndGet();
      }
    }

    /**
     * {@code close()} merges the stream statistics into the filesystem's instrumentation instance.
     * The filesystem statistics of {@link #filesystemStatistics} updated with the bytes read
     * values. When the input stream is closed, corresponding counters will be updated.
     */
    @Override
    public void close() {
      IOStatisticsStore ioStatistics = localIOStatistics();
      promoteInputStreamCountersToMetrics();
      mergedStats = snapshotIOStatistics(localIOStatistics());

      // stream is being closed.
      // merge in all the IOStatistics
      GhfsInstrumentation.this.getIOStatistics().aggregate(ioStatistics);
    }

    /**
     * The total number of times the input stream has been closed.
     *
     * @return the number of close calls.
     */
    @Override
    public long getCloseOperations() {
      return lookupCounterValue(StreamStatisticNames.STREAM_READ_CLOSE_OPERATIONS);
    }

    /**
     * The total number of executed seek operations which went forward in an input stream.
     *
     * @return the number of Forward seek operations.
     */
    @Override
    public long getForwardSeekOperations() {
      return lookupCounterValue(StreamStatisticNames.STREAM_READ_SEEK_FORWARD_OPERATIONS);
    }

    /**
     * The total number of executed seek operations which went backward in an input stream.
     *
     * @return the number of Backward seek operations
     */
    @Override
    public long getBackwardSeekOperations() {
      return lookupCounterValue(StreamStatisticNames.STREAM_READ_SEEK_BACKWARD_OPERATIONS);
    }

    /**
     * The bytes read in read() operations.
     *
     * @return the number of bytes returned to the caller.
     */
    @Override
    public long getBytesRead() {
      return lookupCounterValue(StreamStatisticNames.STREAM_READ_BYTES);
    }

    /**
     * The total number of bytes read, including all read and discarded when closing streams or
     * skipped during seek calls.
     *
     * @return the total number of bytes read from GHFS.
     */
    @Override
    public long getTotalBytesRead() {
      return lookupCounterValue(StreamStatisticNames.STREAM_READ_TOTAL_BYTES);
    }

    /**
     * The total number of bytes skipped during seek calls.
     *
     * @return the number of bytes skipped.
     */
    @Override
    public long getBytesSkippedOnSeek() {
      return lookupCounterValue(StreamStatisticNames.STREAM_READ_SEEK_BYTES_SKIPPED);
    }

    /**
     * The total number of bytes skipped during backward seek calls.
     *
     * @return the number of bytes skipped.
     */
    @Override
    public long getBytesBackwardsOnSeek() {
      return lookupCounterValue(StreamStatisticNames.STREAM_READ_SEEK_BYTES_BACKWARDS);
    }

    /**
     * The total number of seek operations in an input stream
     *
     * @return the number of bytes skipped.
     */
    @Override
    public long getSeekOperations() {
      return lookupCounterValue(StreamStatisticNames.STREAM_READ_SEEK_OPERATIONS);
    }

    /**
     * The total number of exceptions raised during input stream reads
     *
     * @return the count of read Exceptions.
     */
    @Override
    public long getReadExceptions() {
      return lookupCounterValue(StreamStatisticNames.STREAM_READ_EXCEPTIONS);
    }

    /**
     * The total number of times the read() operation in an input stream has been called.
     *
     * @return the count of read operations.
     */
    @Override
    public long getReadOperations() {
      return lookupCounterValue(StreamStatisticNames.STREAM_READ_OPERATIONS);
    }

    /**
     * The total number of Incomplete read() operations
     *
     * @return the count of Incomplete read operations.
     */
    @Override
    public long getReadsIncomplete() {
      return lookupCounterValue(StreamStatisticNames.STREAM_READ_OPERATIONS_INCOMPLETE);
    }

    /**
     * Merge in the statistics of a single input stream into the filesystem-wide metrics counters.
     * This does not update the FS IOStatistics values.
     */
    private void promoteInputStreamCountersToMetrics() {
      // iterate through all the counters
      localIOStatistics().counters().keySet().forEach(this::promoteIOCounter);
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
              .withCounters(STREAM_WRITE_BYTES.getSymbol(), STREAM_WRITE_EXCEPTIONS.getSymbol())
              .withDurationTracking(
                  STREAM_WRITE_CLOSE_OPERATIONS.getSymbol(),
                  STREAM_WRITE_OPERATIONS.getSymbol(),
                  INVOCATION_HFLUSH.getSymbol(),
                  INVOCATION_HSYNC.getSymbol())
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
      return super.getIOStatistics();
    }

    @Override
    public void close() {
      // provided the stream is closed in the worker thread, this will
      // ensure that the thread-specific worker stats are updated.
      mergeOutputStreamStatistics(this);
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

    /** An ignored stream write exception was received. */
    @Override
    public void writeException() {
      writeExceptions.incrementAndGet();
    }

    /** Syncable.hflush() has been invoked. */
    @Override
    public void hflushInvoked() {
      incrementCounter(INVOCATION_HFLUSH.getSymbol(), 1);
    }

    /** Syncable.hsync() has been invoked. */
    @Override
    public void hsyncInvoked() {
      incrementCounter(INVOCATION_HSYNC.getSymbol(), 1);
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

    /**
     * The total number of exceptions raised during ouput stream write
     *
     * @return the count of write Exceptions.
     */
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
        STREAM_WRITE_EXCEPTIONS,
        source.lookupCounterValue(StreamStatisticNames.STREAM_WRITE_EXCEPTIONS));
    // merge in all the IOStatistics
    getIOStatistics().aggregate(source.getIOStatistics());
  }

  /**
   * Create a delegation token statistics instance.
   *
   * @return an instance of delegation token statistics
   */
  public DelegationTokenStatistics newDelegationTokenStatistics() {
    return new DelegationTokenStatisticsImpl();
  }

  /**
   * Instrumentation exported to GCS Delegation Token support. The {@link #tokenIssued()} call is a
   * no-op; This statistics class doesn't collect any local statistics. Instead it directly updates
   * the Ghfs Instrumentation
   */
  private final class DelegationTokenStatisticsImpl extends AbstractGhfsStatisticsSource
      implements DelegationTokenStatistics {

    private DelegationTokenStatisticsImpl() {
      IOStatisticsStore st =
          iostatisticsStore().withCounters(DELEGATION_TOKENS_ISSUED.getSymbol()).build();
    }

    /**
     * Get the inner class's IO Statistics. This is needed to avoid findbugs warnings about
     * ambiguity.
     *
     * @return the Input Stream's statistics.
     */
    private IOStatisticsStore localIOStatistics() {
      return super.getIOStatistics();
    }

    /**
     * Merge in the statistics of a single output stream into the filesystem-wide statistics.
     *
     * @param source stream statistics
     */
    private void mergeDelegationTokenStatistics(DelegationTokenStatistics source) {

      // merge in all the IOStatistics
      getIOStatistics().aggregate(source.getIOStatistics());
    }

    /** A token has been issued. */
    @Override
    public void tokenIssued() {}

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
      return getDurationTrackerFactory().trackDuration(key, count);
    }
  }

  private IOStatisticsStoreBuilder createStoreBuilder() {
    IOStatisticsStoreBuilder storeBuilder = iostatisticsStore();
    EnumSet.allOf(GhfsStatistic.class)
        .forEach(
            stat -> {
              // declare all counter statistics
              if (stat.getType() == GhfsStatisticTypeEnum.TYPE_COUNTER) {
                counter(stat);
                storeBuilder.withCounters(stat.getSymbol());
                // declare all gauge statistics
              } else if (stat.getType() == GhfsStatisticTypeEnum.TYPE_GAUGE) {
                gauge(stat);
                storeBuilder.withGauges(stat.getSymbol());
                // and durations
              } else if (stat.getType() == GhfsStatisticTypeEnum.TYPE_DURATION) {
                duration(stat);
                storeBuilder.withDurationTracking(stat.getSymbol());
              }
            });

    return storeBuilder;
  }
}
