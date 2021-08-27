package com.google.cloud.hadoop.fs.gcs;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.statistics.*;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStoreBuilder;
import org.apache.hadoop.metrics2.*;
import org.apache.hadoop.metrics2.impl.MetricsSystemImpl;
import org.apache.hadoop.metrics2.lib.*;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.net.URI;
import java.time.Duration;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;



import static com.google.cloud.hadoop.fs.gcs.Constants.STREAM_READ_GAUGE_INPUT_POLICY;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.demandStringifyIOStatistics;
import static org.apache.hadoop.fs.statistics.IOStatisticsSupport.snapshotIOStatistics;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.ACTION_HTTP_GET_REQUEST;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.SUFFIX_FAILURES;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_UNBUFFERED;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.iostatisticsStore;

public class GHFSInstrumentation implements Closeable, MetricsSource, IOStatisticsSource, DurationTrackerFactory{
    public static final String CONTEXT="DFSClient";
    public static final String METRICS_SYSTEM_NAME = "google-hadoop-file-system";
    public static final String METRIC_TAG_FILESYSTEM_ID="gcsFilesystemId";
    public static final String METRIC_TAG_BUCKET="bucket";
    private static final Object METRICS_SYSTEM_LOCK = new Object();
    private final MetricsRegistry registry=new MetricsRegistry("googleHadoopFilesystem").setContext(CONTEXT);
    private IOStatisticsStore instanceIOStatistics;
    private DurationTrackerFactory durationTrackerFactory;
    private static int metricsSourceNameCounter = 0;
    private static int metricsSourceActiveCounter = 0;
    private String metricsSourceName;
    private static final String METRICS_SOURCE_BASENAME = "GCSMetrics";
    private static MetricsSystem metricsSystem = null;
    private static final Logger LOG = LoggerFactory.getLogger(
            GHFSInstrumentation.class);

    public GHFSInstrumentation(URI name){
        UUID fileSystemInstanceID=UUID.randomUUID();
        registry.tag(METRIC_TAG_FILESYSTEM_ID,"A unique identifier for the instance",
                fileSystemInstanceID.toString());
        registry.tag(METRIC_TAG_BUCKET,"Hostname from the FS URL",name.getHost());
        IOStatisticsStoreBuilder storeBuilder= IOStatisticsBinding.iostatisticsStore();
        // declare all counter statistics
        EnumSet.allOf(GHFSStatistic.class).stream()
                .filter(GHFSStatistic -> GHFSStatistic.getType() == GHFSStatisticTypeEnum.TYPE_COUNTER)
                .forEach(stat -> {
                    counter(stat);
                    storeBuilder.withCounters(stat.getSymbol());
                });
        // declare all gauge statistics
        EnumSet.allOf(GHFSStatistic.class).stream()
                .filter(statistic ->
                        statistic.getType() == GHFSStatisticTypeEnum.TYPE_GAUGE)
                .forEach(stat -> {
                    gauge(stat);
                    storeBuilder.withGauges(stat.getSymbol());
                });

        // and durations
        EnumSet.allOf(GHFSStatistic.class).stream()
                .filter(statistic ->
                        statistic.getType() == GHFSStatisticTypeEnum.TYPE_DURATION)
                .forEach(stat -> {
                    duration(stat);
                    storeBuilder.withDurationTracking(stat.getSymbol());
                });
        // register with Hadoop metrics
        registerAsMetricsSource(name);
        // and build the IO Statistics
        instanceIOStatistics=storeBuilder.build();
        // duration track metrics (Success/failure) and IOStatistics.
        durationTrackerFactory=IOStatisticsBinding.pairedTrackerFactory(instanceIOStatistics,new MetricDurationTrackerFactory());


    }
    public void close(){
        System.out.println("Close");
    }

    /**
     * Get the instance IO Statistics.
     * @return statistics.
     */
    @Override
    public IOStatisticsStore getIOStatistics() {
        return instanceIOStatistics;
    }
    /**
     * Get the duration tracker factory.
     * @return duration tracking for the instrumentation.
     */
    public DurationTrackerFactory getDurationTrackerFactory() {
        return durationTrackerFactory;
    }



    /**
     * Indicate that GCS created a file.
     */
    public void fileCreated() {
        incrementCounter(GHFSStatistic.FILES_CREATED, 1);
    }


    /**
     * Register this instance as a metrics source.
     * @param name s3a:// URI for the associated FileSystem instance
     */
    private void registerAsMetricsSource(URI name) {
        int number;
        synchronized(METRICS_SYSTEM_LOCK) {
            getMetricsSystem();

            metricsSourceActiveCounter++;
            number = ++metricsSourceNameCounter;
        }
        String msName = METRICS_SOURCE_BASENAME + number;
        metricsSourceName = msName + "-" + name.getHost();
        metricsSystem.register(metricsSourceName, "", this);
    }


    @VisibleForTesting
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
     * Indicate that GCS deleted one or more files.
     * @param count number of files.
     */
    public void fileDeleted(int count) {
        incrementCounter(GHFSStatistic.FILES_DELETED, count);
    }
    /**
     * Create a counter in the registry.
     * @param name counter name
     * @param desc counter description
     * @return a new counter
     */
    protected final MutableCounterLong counter(String name, String desc) {
        return registry.newCounter(name, desc, 0L);
    }

    /**
     * Create a counter in the registry.
     * @param op statistic to count
     * @return a new counter
     */
    protected final MutableCounterLong counter(GHFSStatistic op) {
        return counter(op.getSymbol(), op.getDescription());
    }

    /**
     * Registering a duration adds the success and failure counters.
     * @param op statistic to track
     */
    protected final void duration(GHFSStatistic op) {
        counter(op.getSymbol(), op.getDescription());
        counter(op.getSymbol() + SUFFIX_FAILURES, op.getDescription());
    }

    /**
     * Create a gauge in the registry.
     * @param name name gauge name
     * @param desc description
     * @return the gauge
     */
    protected final MutableGaugeLong gauge(String name, String desc) {
        return registry.newGauge(name, desc, 0L);
    }

    /**
     * Create a gauge in the registry.
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
     * Look up a metric from both the registered set and the lighter weight
     * stream entries.
     * @param name metric name
     * @return the metric or null
     */
    public MutableMetric lookupMetric(String name) {
        MutableMetric metric = getRegistry().get(name);
        return metric;
    }
    /**
     * Get the value of a counter.
     * @param statistic the operation
     * @return its value, or 0 if not found.
     */
    public long getCounterValue(GHFSStatistic statistic) {
        return getCounterValue(statistic.getSymbol());
    }
    /**
     * Get the value of a counter.
     * If the counter is null, return 0.
     * @param name the name of the counter
     * @return its value.
     */
    public long getCounterValue(String name) {
        MutableCounterLong counter = lookupCounter(name);
        return counter == null ? 0 : counter.value();
    }

    private MutableCounterLong lookupCounter(String name) {
        MutableMetric metric = lookupMetric(name);
        if (metric == null) {
            return null;
        }
        if (!(metric instanceof MutableCounterLong)) {
            throw new IllegalStateException("Metric " + name
                    + " is not a MutableCounterLong: " + metric
                    + " (type: " + metric.getClass() +")");
        }
        return (MutableCounterLong) metric;
    }
    /**
     * Look up a gauge.
     * @param name gauge name
     * @return the gauge or null
     * @throws ClassCastException if the metric is not a Gauge.
     */
    public MutableGaugeLong lookupGauge(String name) {
        MutableMetric metric = lookupMetric(name);
        if (metric == null) {
            LOG.debug("No gauge {}", name);
        }
        return (MutableGaugeLong) metric;
    }

    /**
     * The duration tracker updates the metrics with the count
     * and IOStatistics will full duration information.
     * @param key statistic key prefix
     * @param count  #of times to increment the matching counter in this
     * operation.
     * @return a duration tracker.
     */
    @Override
    public DurationTracker trackDuration(final String key, final long count) {
        return durationTrackerFactory.trackDuration(key, count);
    }
    /**
     * String representation. Includes the IOStatistics
     * when logging is at DEBUG.
     * @return a string form.
     */
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(
                "S3AInstrumentation{");
        if (LOG.isDebugEnabled()) {
            sb.append("instanceIOStatistics=").append(instanceIOStatistics);
        }
        sb.append('}');
        return sb.toString();
    }

    /**
     * Indicate that fake directory request was made.
     * @param count number of directory entries included in the delete request.
     */
    public void fakeDirsDeleted(int count) {
        incrementCounter(GHFSStatistic.FAKE_DIRECTORIES_DELETED, count);
    }

    /**
     * Indicate that gcs created a directory.
     */
    public void directoryCreated() {
        incrementCounter(GHFSStatistic.DIRECTORIES_CREATED, 1);
    }

    /**
     * Indicate that gcs just deleted a directory.
     */
    public void directoryDeleted() {
        incrementCounter(GHFSStatistic.DIRECTORIES_DELETED, 1);
    }

    /**
     * Indicate that gcs copied some files within the store.
     *
     * @param files number of files
     * @param size total size in bytes
     */
    public void filesCopied(int files, long size) {
        incrementCounter(GHFSStatistic.FILES_COPIED, files);
        incrementCounter(GHFSStatistic.FILES_COPIED_BYTES, size);
    }

    /**
     * Note that an error was ignored.
     */
    public void errorIgnored() {
        incrementCounter(GHFSStatistic.IGNORED_ERRORS, 1);
    }
    /**
     * Increments a mutable counter and the matching
     * instance IOStatistics counter.
     * No-op if the counter is not defined, or the count == 0.
     * @param op operation
     * @param count increment value
     */

    public void incrementCounter(GHFSStatistic op, long count) {
        String name = op.getSymbol();
        if (count != 0) {
            incrementMutableCounter(name, count);
            instanceIOStatistics.incrementCounter(name, count);
        }
    }
    /**
     * Increments a mutable counter and the matching
     * instance IOStatistics counter with the value of
     * the atomic long.
     * No-op if the counter is not defined, or the count == 0.
     * @param op operation
     * @param count atomic long containing value
     */
    public void incrementCounter(GHFSStatistic op, AtomicLong count) {
        incrementCounter(op, count.get());
    }

    /**
     * Increment a specific gauge.
     * No-op if not defined.
     * @param op operation
     * @param count increment value
     * @throws ClassCastException if the metric is of the wrong type
     */
    public void incrementGauge(GHFSStatistic op, long count) {
        MutableGaugeLong gauge = lookupGauge(op.getSymbol());
        if (gauge != null) {
            gauge.incr(count);
        } else {
            LOG.debug("No Gauge: "+ op);
        }
    }

    /**
     * Decrement a specific gauge.
     * No-op if not defined.
     * @param op operation
     * @param count increment value
     * @throws ClassCastException if the metric is of the wrong type
     */
    public void decrementGauge(GHFSStatistic op, long count) {
        MutableGaugeLong gauge = lookupGauge(op.getSymbol());
        if (gauge != null) {
            gauge.decr(count);
        } else {
            LOG.debug("No Gauge: {}", op);
        }
    }

    /**
     * Increments a Mutable counter.
     * No-op if not a positive integer.
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


    /**
     * Add the duration as a timed statistic, deriving
     * statistic name from the operation symbol and the outcome.
     * @param op operation
     * @param success was the operation a success?
     * @param duration how long did it take
     */

    public void recordDuration(final GHFSStatistic op,
                               final boolean success,
                               final Duration duration) {
        String name = op.getSymbol()
                + (success ? "" : SUFFIX_FAILURES);
        instanceIOStatistics.addTimedOperation(name, duration);
    }
    /**
     * Copy all the metrics to a map of (name, long-value).
     * @return a map of the metrics
     */
    public Map<String, Long> toMap() {
        MetricsToMap metricBuilder = new MetricsToMap(null);
        registry.snapshot(metricBuilder, true);
        return metricBuilder.getMap();
    }
    @Override
    public void getMetrics(MetricsCollector metricsCollector, boolean b) {
//        System.out.println("getMetrics");
    }


    /**
     * Create a stream input statistics instance.
     * @return the new instance
     * @param filesystemStatistics FS Statistics to update in close().
     */
    public GHFSInputStreamStatistics newInputStreamStatistics(
            @Nullable final FileSystem.Statistics filesystemStatistics) {
        return new InputStreamStatistics(filesystemStatistics);
    }

    /**
     * Create a stream output statistics instance.
     * @param filesystemStatistics thread-local FS statistics.
     * @return the new instance
     */
//    public GHFSOutputStreamStatistics newOutputStreamStatistics(
//            FileSystem.Statistics filesystemStatistics) {
//        return new OutputStreamStatistics(filesystemStatistics);
//    }
    /**
     * Statistics updated by an GoogleHadoopFSInputStream during its actual operation.
     * <p>
     * When {@code unbuffer()} is called, the changed numbers are propagated
     * to the S3AFileSystem metrics.
     * </p>
     * <p>
     * When {@code close()} is called, the final set of numbers are propagated
     * to the S3AFileSystem metrics.
     * </p>
     * The {@link FileSystem.Statistics} statistics passed in are also
     * updated. This ensures that whichever thread calls close() gets the
     * total count of bytes read, even if any work is done in other
     * threads.
     *
     */
    /**
     * Merge in the statistics of a single output stream into
     * the filesystem-wide statistics.
     * @param source stream statistics
     */
    private void mergeOutputStreamStatistics(
            OutputStreamStatistics source) {

        incrementCounter(GHFSStatistic.STREAM_WRITE_EXCEPTIONS,
                source.lookupCounterValue(
                        StreamStatisticNames.STREAM_WRITE_EXCEPTIONS));
        // merge in all the IOStatistics
        this.getIOStatistics().aggregate(source.getIOStatistics());
    }
    private final class InputStreamStatistics  extends AbstractGHFSStatisticsSource implements GHFSInputStreamStatistics{

        /**
         * Distance used when incrementing FS stats.
         */
        private static final int DISTANCE = 5;

        /**
         * FS statistics for the thread creating the stream.
         */
        private final FileSystem.Statistics filesystemStatistics;

        /**
         * The statistics from the last merge.
         */
        private IOStatisticsSnapshot mergedStats;
        /*
   The core counters are extracted to atomic longs for slightly
   faster resolution on the critical paths, especially single byte
   reads and the like.
    */
        private final AtomicLong aborted;
        private final AtomicLong backwardSeekOperations;
        private final AtomicLong bytesBackwardsOnSeek;
        private final AtomicLong bytesDiscardedInAbort;
        /** Bytes read by the application. */
        private final AtomicLong bytesRead;
        private final AtomicLong bytesDiscardedInClose;
        private final AtomicLong bytesDiscardedOnSeek;
        private final AtomicLong bytesSkippedOnSeek;
        private final AtomicLong closed;
        private final AtomicLong forwardSeekOperations;
        private final AtomicLong openOperations;
        private final AtomicLong readExceptions;
        private final AtomicLong readsIncomplete;
        private final AtomicLong readOperations;
        private final AtomicLong readFullyOperations;
        private final AtomicLong seekOperations;

        /** Bytes read by the application and any when draining streams . */
        private final AtomicLong totalBytesRead;

        /**
         * Instantiate.
         * @param filesystemStatistics FS Statistics to update in close().
         */

        private InputStreamStatistics(
                @Nullable FileSystem.Statistics filesystemStatistics) {
            this.filesystemStatistics = filesystemStatistics;
            IOStatisticsStore st = iostatisticsStore()
                    .withCounters(
                            StreamStatisticNames.STREAM_READ_ABORTED,
                            StreamStatisticNames.STREAM_READ_BYTES_DISCARDED_ABORT,
                            StreamStatisticNames.STREAM_READ_CLOSED,
                            StreamStatisticNames.STREAM_READ_BYTES_DISCARDED_CLOSE,
                            StreamStatisticNames.STREAM_READ_CLOSE_OPERATIONS,
                            StreamStatisticNames.STREAM_READ_OPENED,
                            StreamStatisticNames.STREAM_READ_BYTES,
                            StreamStatisticNames.STREAM_READ_EXCEPTIONS,
                            StreamStatisticNames.STREAM_READ_FULLY_OPERATIONS,
                            StreamStatisticNames.STREAM_READ_OPERATIONS,
                            StreamStatisticNames.STREAM_READ_OPERATIONS_INCOMPLETE,
                            StreamStatisticNames.STREAM_READ_SEEK_OPERATIONS,
                            StreamStatisticNames.STREAM_READ_SEEK_POLICY_CHANGED,
                            StreamStatisticNames.STREAM_READ_SEEK_BACKWARD_OPERATIONS,
                            StreamStatisticNames.STREAM_READ_SEEK_FORWARD_OPERATIONS,
                            StreamStatisticNames.STREAM_READ_SEEK_BYTES_BACKWARDS,
                            StreamStatisticNames.STREAM_READ_SEEK_BYTES_DISCARDED,
                            StreamStatisticNames.STREAM_READ_SEEK_BYTES_SKIPPED,
                            StreamStatisticNames.STREAM_READ_TOTAL_BYTES,
                            StreamStatisticNames.STREAM_READ_UNBUFFERED,
                            StreamStatisticNames.STREAM_READ_VERSION_MISMATCHES)
                    .withGauges(STREAM_READ_GAUGE_INPUT_POLICY)
                    .withDurationTracking(ACTION_HTTP_GET_REQUEST)
                    .build();
            setIOStatistics(st);
            aborted = st.getCounterReference(
                    StreamStatisticNames.STREAM_READ_ABORTED);
            backwardSeekOperations = st.getCounterReference(
                    StreamStatisticNames.STREAM_READ_SEEK_BACKWARD_OPERATIONS);
            bytesBackwardsOnSeek = st.getCounterReference(
                    StreamStatisticNames.STREAM_READ_SEEK_BYTES_BACKWARDS);
            bytesDiscardedInAbort = st.getCounterReference(
                    StreamStatisticNames.STREAM_READ_BYTES_DISCARDED_ABORT);
            bytesRead = st.getCounterReference(
                    StreamStatisticNames.STREAM_READ_BYTES);
            bytesDiscardedInClose = st.getCounterReference(
                    StreamStatisticNames.STREAM_READ_BYTES_DISCARDED_CLOSE);
            bytesDiscardedOnSeek = st.getCounterReference(
                    StreamStatisticNames.STREAM_READ_SEEK_BYTES_DISCARDED);
            bytesSkippedOnSeek = st.getCounterReference(
                    StreamStatisticNames.STREAM_READ_SEEK_BYTES_SKIPPED);
            closed = st.getCounterReference(
                    StreamStatisticNames.STREAM_READ_CLOSED);
            forwardSeekOperations = st.getCounterReference(
                    StreamStatisticNames.STREAM_READ_SEEK_FORWARD_OPERATIONS);
            openOperations = st.getCounterReference(
                    StreamStatisticNames.STREAM_READ_OPENED);
            readExceptions = st.getCounterReference(
                    StreamStatisticNames.STREAM_READ_EXCEPTIONS);
            readsIncomplete = st.getCounterReference(
                    StreamStatisticNames.STREAM_READ_OPERATIONS_INCOMPLETE);
            readOperations = st.getCounterReference(
                    StreamStatisticNames.STREAM_READ_OPERATIONS);
            readFullyOperations = st.getCounterReference(
                    StreamStatisticNames.STREAM_READ_FULLY_OPERATIONS);
            seekOperations = st.getCounterReference(
                    StreamStatisticNames.STREAM_READ_SEEK_OPERATIONS);
            totalBytesRead = st.getCounterReference(
                    StreamStatisticNames.STREAM_READ_TOTAL_BYTES);
            setIOStatistics(st);
            // create initial snapshot of merged statistics
            mergedStats = snapshotIOStatistics(st);


        }
        /**
         * Increment a named counter by one.
         * @param name counter name
         * @return the new value
         */
        private long increment(String name) {
            return increment(name, 1);
        }

        /**
         * Increment a named counter by a given value.
         * @param name counter name
         * @param value value to increment by.
         * @return the new value
         */
        private long increment(String name, long value) {
            return incCounter(name, value);
        }
        /**
         * Get the inner class's IO Statistics. This is
         * needed to avoid findbugs warnings about ambiguity.
         * @return the Input Stream's statistics.
         */
        private IOStatisticsStore localIOStatistics() {
            return InputStreamStatistics.super.getIOStatistics();
        }
        /**
         * {@inheritDoc}.
         * Increments the number of seek operations,
         * and backward seek operations.
         * The offset is inverted and used as the increment
         * of {@link #bytesBackwardsOnSeek}.
         */
        @Override
        public void seekBackwards(long negativeOffset) {
            seekOperations.incrementAndGet();
            backwardSeekOperations.incrementAndGet();
            bytesBackwardsOnSeek.addAndGet(-negativeOffset);
        }

        /**
         * {@inheritDoc}.
         * Increment the number of seek and forward seek
         * operations, as well as counters of bytes skipped
         * and bytes read in seek, where appropriate.
         * Bytes read in seek are also added to the totalBytesRead
         * counter.
         */
        @Override
        public void seekForwards(final long skipped,
                                 long bytesReadInSeek) {
            seekOperations.incrementAndGet();
            forwardSeekOperations.incrementAndGet();
            if (skipped > 0) {
                bytesSkippedOnSeek.addAndGet(skipped);
            }
            if (bytesReadInSeek > 0) {
                bytesDiscardedOnSeek.addAndGet(bytesReadInSeek);
                totalBytesRead.addAndGet(bytesReadInSeek);
            }
        }

        /**
         * {@inheritDoc}.
         * Use {@code getAnIncrement()} on {@link #openOperations}
         * so that on invocation 1 it returns 0.
         * The caller will know that this is the first invocation.
         */
        @Override
        public long streamOpened() {
            return openOperations.getAndIncrement();
        }

        /**
         * {@inheritDoc}.
         * If the connection was aborted, increment {@link #aborted}
         * and add the byte's remaining count to {@link #bytesDiscardedInAbort}.
         * If not aborted, increment {@link #closed} and
         * then {@link #bytesDiscardedInClose} and {@link #totalBytesRead}
         * with the bytes remaining value.
         */
        @Override
        public void streamClose(boolean abortedConnection,
                                long remainingInCurrentRequest) {
            if (abortedConnection) {
                // the connection was aborted.
                // update the counter of abort() calls and bytes discarded
                aborted.incrementAndGet();
                bytesDiscardedInAbort.addAndGet(remainingInCurrentRequest);
            } else {
                // connection closed, possibly draining the stream of surplus
                // bytes.
                closed.incrementAndGet();
                bytesDiscardedInClose.addAndGet(remainingInCurrentRequest);
                totalBytesRead.addAndGet(remainingInCurrentRequest);
            }
        }

        /**
         * {@inheritDoc}.
         */
        @Override
        public void readException() {
            readExceptions.incrementAndGet();
        }

        /**
         * {@inheritDoc}.
         * If the byte counter is positive, increment bytesRead and totalBytesRead.
         */
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

        @Override
        public void readFullyOperationStarted(long pos, long len) {
            readFullyOperations.incrementAndGet();
        }

        /**
         * {@inheritDoc}.
         * If more data was requested than was actually returned, this
         * was an incomplete read. Increment {@link #readsIncomplete}.
         */
        @Override
        public void readOperationCompleted(int requested, int actual) {
            if (requested > actual) {
                readsIncomplete.incrementAndGet();
            }
        }

        /**
         * {@code close()} merges the stream statistics into the filesystem's
         * instrumentation instance.
         */
        @Override
        public void close() {
            System.out.println("Close in GHFSInstrumentation is called");
            increment(StreamStatisticNames.STREAM_READ_CLOSE_OPERATIONS);
            merge(true);
        }

        /**
         * {@inheritDoc}.
         * As well as incrementing the {@code STREAM_READ_SEEK_POLICY_CHANGED}
         * counter, the
         * {@code STREAM_READ_GAUGE_INPUT_POLICY} gauge is set to the new value.
         *
         */
        @Override
        public void inputPolicySet(int updatedPolicy) {
            increment(StreamStatisticNames.STREAM_READ_SEEK_POLICY_CHANGED);
            localIOStatistics().setGauge(STREAM_READ_GAUGE_INPUT_POLICY,
                    updatedPolicy);
        }
        /**
         * {@inheritDoc}
         * Increment the counter {@code STREAM_READ_UNBUFFERED}
         * and then merge the current set of statistics into the
         * FileSystem's statistics through {@link #merge(boolean)}.
         */
        @Override
        public void unbuffered() {
            increment(STREAM_READ_UNBUFFERED);
            merge(false);
        }


        @Override
        public long getCloseOperations() {
            return lookupCounterValue(
                    StreamStatisticNames.STREAM_READ_CLOSE_OPERATIONS);
        }

        @Override
        public long getClosed() {
            return lookupCounterValue(StreamStatisticNames.STREAM_READ_CLOSED);
        }

        @Override
        public long getAborted() {
            return lookupCounterValue(StreamStatisticNames.STREAM_READ_ABORTED);
        }

        @Override
        public long getForwardSeekOperations() {
            return lookupCounterValue(
                    StreamStatisticNames.STREAM_READ_SEEK_FORWARD_OPERATIONS);
        }

        @Override
        public long getBackwardSeekOperations() {
            return lookupCounterValue(
                    StreamStatisticNames.STREAM_READ_SEEK_BACKWARD_OPERATIONS);
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
            return lookupCounterValue(
                    StreamStatisticNames.STREAM_READ_SEEK_BYTES_SKIPPED);
        }

        @Override
        public long getBytesBackwardsOnSeek() {
            return lookupCounterValue(
                    StreamStatisticNames.STREAM_READ_SEEK_BYTES_BACKWARDS);
        }

        @Override
        public long getBytesReadInClose() {
            return lookupCounterValue(
                    StreamStatisticNames.STREAM_READ_BYTES_DISCARDED_CLOSE);
        }

        @Override
        public long getBytesDiscardedInAbort() {
            return lookupCounterValue(
                    StreamStatisticNames.STREAM_READ_BYTES_DISCARDED_ABORT);
        }

        @Override
        public long getOpenOperations() {
            return lookupCounterValue(StreamStatisticNames.STREAM_READ_OPENED);
        }

        @Override
        public long getSeekOperations() {
            return lookupCounterValue(
                    StreamStatisticNames.STREAM_READ_SEEK_OPERATIONS);
        }

        @Override
        public long getReadExceptions() {
            return lookupCounterValue(
                    StreamStatisticNames.STREAM_READ_EXCEPTIONS);
        }

        @Override
        public long getReadOperations() {
            return lookupCounterValue(StreamStatisticNames.STREAM_READ_OPERATIONS);
        }

        @Override
        public long getReadFullyOperations() {
            return lookupCounterValue(
                    StreamStatisticNames.STREAM_READ_FULLY_OPERATIONS);
        }

        @Override
        public long getReadsIncomplete() {
            return lookupCounterValue(
                    StreamStatisticNames.STREAM_READ_OPERATIONS_INCOMPLETE);
        }

        @Override
        public long getPolicySetCount() {
            return lookupCounterValue(
                    StreamStatisticNames.STREAM_READ_SEEK_POLICY_CHANGED);
        }


        @Override
        public long getInputPolicy() {
            return localIOStatistics().gauges()
                    .get(STREAM_READ_GAUGE_INPUT_POLICY);
        }

        @Override
        public DurationTracker initiateGetRequest() {
            return trackDuration(ACTION_HTTP_GET_REQUEST);
        }


        /**
         * Merge the statistics into the filesystem's instrumentation instance.
         * <p>
         *   If the merge is invoked because the stream has been closed,
         *   then all statistics are merged, and the filesystem
         *   statistics of {@link #filesystemStatistics} updated
         *   with the bytes read values.
         * </p>
         * <p>
         *   Whichever thread close()d the stream will have its counters
         *   updated.
         * </p>
         * <p>
         *   If the merge is due to an unbuffer() call, the change in all
         *   counters since the last merge will be pushed to the Instrumentation's
         *   counters.
         * </p>
         *
         * @param isClosed is this merge invoked because the stream is closed?
         */
        private void merge(boolean isClosed) {

            IOStatisticsStore ioStatistics = localIOStatistics();
            LOG.debug("Merging statistics into FS statistics in {}: {}",
                    (isClosed ? "close()" : "unbuffer()"),
                    demandStringifyIOStatistics(ioStatistics));
            promoteInputStreamCountersToMetrics();
            mergedStats = snapshotIOStatistics(localIOStatistics());

            if (isClosed) {
                // stream is being closed.
                // merge in all the IOStatistics
                GHFSInstrumentation.this.getIOStatistics().aggregate(ioStatistics);

                // increment the filesystem statistics for this thread.
                if (filesystemStatistics != null) {
                    long t = getTotalBytesRead();
                    filesystemStatistics.incrementBytesRead(t);
                    filesystemStatistics.incrementBytesReadByDistance(DISTANCE, t);
                }
            }
        }

        /**
         * Merge in the statistics of a single input stream into
         * the filesystem-wide metrics counters.
         * This does not update the FS IOStatistics values.
         */
        private void promoteInputStreamCountersToMetrics() {
            // iterate through all the counters
            localIOStatistics().counters()
                    .keySet().stream()
                    .forEach(e -> promoteIOCounter(e));
        }
        /**
         * Propagate a counter from the instance-level statistics
         * to the S3A instrumentation, subtracting the previous merged value.
         * @param name statistic to promote
         */
        void promoteIOCounter(String name) {
            incrementMutableCounter(name,
                    lookupCounterValue(name)
                            - mergedStats.counters().get(name));
        }

    }


    private final class OutputStreamStatistics
            extends AbstractGHFSStatisticsSource
            implements GHFSOutputStreamStatistics {
        private final AtomicLong bytesWritten;
        private final AtomicLong transferDuration = new AtomicLong(0);
        private final AtomicLong queueDuration = new AtomicLong(0);
        private final FileSystem.Statistics filesystemStatistics;

        /**
         * Instantiate.
         * @param filesystemStatistics FS Statistics to update in close().
         */
        private OutputStreamStatistics(
                @Nullable FileSystem.Statistics filesystemStatistics) {
            this.filesystemStatistics = filesystemStatistics;
            IOStatisticsStore st = iostatisticsStore()
                    .withCounters(
                            GHFSStatistic.STREAM_WRITE_BYTES.getSymbol(),
                            GHFSStatistic.STREAM_WRITE_EXCEPTIONS.getSymbol(),
                            GHFSStatistic.INVOCATION_HFLUSH.getSymbol(),
                            GHFSStatistic.INVOCATION_HSYNC.getSymbol())
                    .build();
            setIOStatistics(st);
            // these are extracted to avoid lookups on heavily used counters.

            bytesWritten = st.getCounterReference(
                    StreamStatisticNames.STREAM_WRITE_BYTES);
        }

        /**
         * Increment the Statistic gauge and the local IOStatistics
         * equivalent.
         * @param statistic statistic
         * @param v value.
         * @return local IOStatistic value
         */
        private long incAllGauges(GHFSStatistic statistic, long v) {
            incrementGauge(statistic, v);
            return incGauge(statistic.getSymbol(), v);
        }




        /**
         * Get the inner class's IO Statistics. This is
         * needed to avoid findbugs warnings about ambiguity.
         * @return the Input Stream's statistics.
         */
        private IOStatisticsStore localIOStatistics() {
            return OutputStreamStatistics.super.getIOStatistics();
        }

        @Override
        public void hflushInvoked() {
            incCounter(GHFSStatistic.INVOCATION_HFLUSH.getSymbol(), 1);
        }

        @Override
        public void hsyncInvoked() {
            incCounter(GHFSStatistic.INVOCATION_HSYNC.getSymbol(), 1);
        }

        @Override
        public void close() {

            mergeOutputStreamStatistics(this);
            // and patch the FS statistics.
            // provided the stream is closed in the worker thread, this will
            // ensure that the thread-specific worker stats are updated.
            if (filesystemStatistics != null) {
                // filesystemStatistics.incrementBytesWritten(bytesUploaded.get());
            }
        }


        /**
         * Record bytes written.
         * @param count number of bytes
         */
        @Override
        public void writeBytes(long count) {
            bytesWritten.addAndGet(count);
        }

        /**
         * Get the current count of bytes written.
         * @return the counter value.
         */
        @Override
        public long getBytesWritten() {
            return bytesWritten.get();
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder(
                    "OutputStreamStatistics{");
//            sb.append(localIOStatistics().toString());
//            sb.append(", blocksActive=").append(blocksActive);
//            sb.append(", blockUploadsCompleted=").append(blockUploadsCompleted);
//            sb.append(", blocksAllocated=").append(blocksAllocated);
//            sb.append(", blocksReleased=").append(blocksReleased);
//            sb.append(", blocksActivelyAllocated=")
//                    .append(getBlocksActivelyAllocated());
            sb.append(", transferDuration=").append(transferDuration).append(" ms");
//            sb.append(", totalUploadDuration=").append(totalUploadDuration())
//                    .append(" ms");
//            sb.append(", effectiveBandwidth=").append(effectiveBandwidth())
//                    .append(" bytes/s");
//            sb.append('}');
            return sb.toString();
        }
    }


    private final class MetricUpdatingDurationTracker
            implements DurationTracker {

        private final String symbol;

        private boolean failed;

        private MetricUpdatingDurationTracker(
                final String symbol,
                final long count) {
            this.symbol = symbol;
            incrementMutableCounter(symbol, count);
        }

        @Override
        public void failed() {
            failed = true;
        }

        /**
         * Close: on failure increment any mutable counter of
         * failures.
         */
        @Override
        public void close() {
            if (failed) {
                incrementMutableCounter(symbol + SUFFIX_FAILURES, 1);
            }
        }
    }

    private final class MetricDurationTrackerFactory
            implements DurationTrackerFactory {

        @Override
        public DurationTracker trackDuration(final String key, final long count) {
            return new MetricUpdatingDurationTracker(key, count);
        }

    }
    /**
     * Convert all metrics to a map.
     */
    private static class MetricsToMap extends MetricsRecordBuilder {
        private final MetricsCollector parent;
        private final Map<String, Long> map =
                new HashMap<>();

        MetricsToMap(MetricsCollector parent) {
            this.parent = parent;
        }

        @Override
        public MetricsRecordBuilder tag(MetricsInfo info, String value) {
            return this;
        }

        @Override
        public MetricsRecordBuilder add(MetricsTag tag) {
            return this;
        }

        @Override
        public MetricsRecordBuilder add(AbstractMetric metric) {
            return this;
        }

        @Override
        public MetricsRecordBuilder setContext(String value) {
            return this;
        }

        @Override
        public MetricsRecordBuilder addCounter(MetricsInfo info, int value) {
            return tuple(info, value);
        }

        @Override
        public MetricsRecordBuilder addCounter(MetricsInfo info, long value) {
            return tuple(info, value);
        }

        @Override
        public MetricsRecordBuilder addGauge(MetricsInfo info, int value) {
            return tuple(info, value);
        }

        @Override
        public MetricsRecordBuilder addGauge(MetricsInfo info, long value) {
            return tuple(info, value);
        }

        public MetricsToMap tuple(MetricsInfo info, long value) {
            return tuple(info.name(), value);
        }

        public MetricsToMap tuple(String name, long value) {
            map.put(name, value);
            return this;
        }

        @Override
        public MetricsRecordBuilder addGauge(MetricsInfo info, float value) {
            return tuple(info, (long) value);
        }

        @Override
        public MetricsRecordBuilder addGauge(MetricsInfo info, double value) {
            return tuple(info, (long) value);
        }

        @Override
        public MetricsCollector parent() {
            return parent;
        }

        /**
         * Get the map.
         * @return the map of metrics
         */
        public Map<String, Long> getMap() {
            return map;
        }
    }

}
