package com.google.cloud.hadoop.fs.gcs;


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
import org.apache.hadoop.metrics2.lib.*;

import java.io.Closeable;
import java.net.URI;
import java.util.EnumSet;
import java.util.UUID;

import static org.apache.hadoop.fs.statistics.StoreStatisticNames.SUFFIX_FAILURES;

public class GHFSInstrumentation implements Closeable, MetricsSource, IOStatisticsSource{
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
public GHFSInstrumentation(URI name){
    UUID tempFileSystemID=UUID.randomUUID();
    registry.tag(METRIC_TAG_FILESYSTEM_ID,"A unique identifier for the instance",
            tempFileSystemID.toString());
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
    registerAsMetricsSource(name);
    instanceIOStatistics=storeBuilder.build();
    durationTrackerFactory=IOStatisticsBinding.pairedTrackerFactory(instanceIOStatistics,new MetricDurationTrackerFactory());


}
    public void close(){
        System.out.println("Close");
    }

    @Override
    public IOStatisticsStore getIOStatistics() {
        return instanceIOStatistics;
    }

    public void incrementCounter(GHFSStatistic op, long count) {
        String name = op.getSymbol();
        if (count != 0) {
            incrementMutableCounter(name, count);
            instanceIOStatistics.incrementCounter(name, count);
        }
    }
    public void fileOpened() {
        incrementCounter(GHFSStatistic.INVOCATION_OPEN, 1);
    }

    public void fileCreated() {
        incrementCounter(GHFSStatistic.FILES_CREATED, 1);
    }
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

    /**
     * Create a quantiles in the registry.
     * @param op  statistic to collect
     * @param sampleName sample name of the quantiles
     * @param valueName value name of the quantiles
     * @param interval interval of the quantiles in seconds
     * @return the created quantiles metric
     */
    protected final MutableQuantiles quantiles(GHFSStatistic op,
                                               String sampleName,
                                               String valueName,
                                               int interval) {
        return registry.newQuantiles(op.getSymbol(), op.getDescription(),
                sampleName, valueName, interval);
    }

    public MetricsRegistry getRegistry() {
        return registry;
    }
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
            throw new IllegalStateException("Metric " + name
                    + " is not a MutableCounterLong: " + metric
                    + " (type: " + metric.getClass() +")");
        }
        return (MutableCounterLong) metric;
    }
    private void incrementMutableCounter(final String name, final long count) {
        if (count > 0) {
            MutableCounterLong counter = lookupCounter(name);
            if (counter != null) {
                counter.incr(count);
            }
        }
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

}
