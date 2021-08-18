package com.google.cloud.hadoop.fs.gcs;


import org.apache.hadoop.fs.statistics.DurationTracker;
import org.apache.hadoop.fs.statistics.DurationTrackerFactory;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStoreBuilder;
import org.apache.hadoop.metrics2.*;
import org.apache.hadoop.metrics2.impl.MetricsSystemImpl;
import org.apache.hadoop.metrics2.lib.*;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.net.URI;
import java.time.Duration;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.hadoop.fs.statistics.StoreStatisticNames.SUFFIX_FAILURES;

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
