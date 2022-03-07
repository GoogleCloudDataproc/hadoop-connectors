package com.google.cloud.hadoop.gcsio;

import static io.opencensus.common.Duration.fromMillis;

import com.google.auth.Credentials;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.flogger.GoogleLogger;
import io.opencensus.common.Scope;
import io.opencensus.contrib.grpc.metrics.RpcViews;
import io.opencensus.exporter.stats.stackdriver.StackdriverStatsConfiguration;
import io.opencensus.exporter.stats.stackdriver.StackdriverStatsExporter;
import io.opencensus.stats.Aggregation;
import io.opencensus.stats.Aggregation.Count;
import io.opencensus.stats.Aggregation.Distribution;
import io.opencensus.stats.BucketBoundaries;
import io.opencensus.stats.Measure.MeasureLong;
import io.opencensus.stats.Stats;
import io.opencensus.stats.StatsRecorder;
import io.opencensus.stats.View;
import io.opencensus.stats.View.Name;
import io.opencensus.stats.ViewManager;
import io.opencensus.tags.TagContext;
import io.opencensus.tags.TagContextBuilder;
import io.opencensus.tags.TagKey;
import io.opencensus.tags.TagMetadata;
import io.opencensus.tags.TagMetadata.TagTtl;
import io.opencensus.tags.TagValue;
import io.opencensus.tags.Tagger;
import io.opencensus.tags.Tags;
import java.io.IOException;
import java.util.List;
import javax.annotation.concurrent.GuardedBy;

/** Publishes open-census measurements to StackDriver */
class CloudMonitoringMetricsRecorder implements MetricsRecorder {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  @VisibleForTesting static final Object monitor = new Object();

  private static final int EXPORT_INTERVAL = 5000;

  @GuardedBy("monitor")
  private static boolean initialized = false;

  private static final TagMetadata TAG_METADATA_NO_PROPAGATION =
      TagMetadata.create(TagTtl.NO_PROPAGATION);

  static final String MS = "ms";
  static final String BY = "By";

  static final MeasureLong LATENCY_MS =
      MeasureLong.create("gcsio/latency", "The latency in milliseconds ", MS);

  static final MeasureLong MESSAGE_LATENCY_MS =
      MeasureLong.create(
          "gcsio/message/latency", "The latency in milliseconds per gcs message loop", MS);

  static final MeasureLong REQUESTS =
      MeasureLong.create("gcsio/requests", "The distribution of retry attempts for gcs calls", BY);

  static final MeasureLong REQUEST_RETRIES =
      MeasureLong.create("gcsio/retries", "The distribution of retry attempts for gcs calls", BY);

  static final TagKey METHOD = TagKey.create("method");
  static final TagKey STATUS = TagKey.create("status");
  static final TagKey ERROR = TagKey.create("error");
  static final TagKey THREAD = TagKey.create("thread");
  static final TagKey PROTOCOL = TagKey.create("protocol");

  static final List<TagKey> TAG_KEYS = ImmutableList.of(METHOD, STATUS, ERROR, THREAD, PROTOCOL);

  static final List<Double> RPC_MILLIS_BUCKET_BOUNDARIES =
      ImmutableList.of(
          0.0, 0.01, 0.05, 0.1, 0.3, 0.6, 0.8, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 8.0, 10.0, 13.0, 16.0,
          20.0, 25.0, 30.0, 40.0, 50.0, 65.0, 80.0, 100.0, 130.0, 160.0, 200.0, 250.0, 300.0, 400.0,
          500.0, 650.0, 800.0, 1000.0, 2000.0, 5000.0, 10000.0, 20000.0, 50000.0, 100000.0);

  static final Aggregation AGGREGATION_WITH_MILLIS_HISTOGRAM =
      Distribution.create(BucketBoundaries.create(RPC_MILLIS_BUCKET_BOUNDARIES));

  static final Aggregation COUNT = Count.create();

  private static final Tagger tagger = Tags.getTagger();
  private static final StatsRecorder statsRecorder = Stats.getStatsRecorder();

  public static MetricsRecorder create(String projectId, Credentials credentials) {
    try {
      registerAllViews();
      setupCloudMonitoringExporter(projectId, credentials);
    } catch (Exception e) {
      logger.atWarning().withCause(e).log("Exception while registering metrics publisher");
      return new NoOpMetricsRecorder();
    }
    return new CloudMonitoringMetricsRecorder();
  }

  private static void setupCloudMonitoringExporter(String projectId, Credentials credentials)
      throws IOException {
    synchronized (monitor) {
      if (!initialized) {
        StackdriverStatsExporter.createAndRegister(
            StackdriverStatsConfiguration.builder()
                .setCredentials(credentials)
                .setProjectId(projectId)
                .setExportInterval(fromMillis(EXPORT_INTERVAL))
                .build());
        Runtime.getRuntime().addShutdownHook(new Thread(StackdriverStatsExporter::unregister));
        initialized = true;
      }
    }
  }

  private static void registerAllViews() {
    // Define the views
    View[] views =
        new View[] {
          View.create(
              Name.create("gcsio/latency"),
              "The distribution of latencies for a  method",
              LATENCY_MS,
              AGGREGATION_WITH_MILLIS_HISTOGRAM,
              TAG_KEYS),
          View.create(
              Name.create("gcsio/message/latency"),
              "The distribution of latencies at a message level in a streaming context",
              MESSAGE_LATENCY_MS,
              AGGREGATION_WITH_MILLIS_HISTOGRAM,
              TAG_KEYS),
          View.create(
              Name.create("gcsio/retries"),
              "The distribution of retry attempts for a method",
              REQUEST_RETRIES,
              COUNT,
              TAG_KEYS),
          View.create(
              Name.create("gcsio/requests"),
              "The distribution of request counts for a method",
              REQUESTS,
              COUNT,
              TAG_KEYS),
        };

    ViewManager viewManager = Stats.getViewManager();

    for (View view : views) {
      viewManager.registerView(view);
    }

    // register views for native grpc client metrics
    RpcViews.registerAllGrpcViews();
    RpcViews.registerRealTimeMetricsViews();
  }

  @Override
  public void recordTaggedStat(TagKey key, String value, MeasureLong ml, Long n) {
    TagContext tagContext =
        tagger
            .emptyBuilder()
            .put(
                THREAD,
                TagValue.create(String.valueOf(Thread.currentThread().getId())),
                TAG_METADATA_NO_PROPAGATION)
            .put(key, TagValue.create(value), TAG_METADATA_NO_PROPAGATION)
            .build();
    try (Scope ignored = tagger.withTagContext(tagContext)) {
      statsRecorder.newMeasureMap().put(ml, n).record();
    }
  }

  @Override
  public void recordLong(TagKey[] keys, String[] values, MeasureLong ml, Long n) {
    TagContextBuilder builder = tagger.emptyBuilder();
    for (int i = 0; i < keys.length; i++) {
      builder.put(keys[i], TagValue.create(values[i]), TAG_METADATA_NO_PROPAGATION);
    }
    builder.put(
        THREAD,
        TagValue.create(String.valueOf(Thread.currentThread().getId())),
        TAG_METADATA_NO_PROPAGATION);
    TagContext tagContext = builder.build();

    try (Scope ignored = tagger.withTagContext(tagContext)) {
      statsRecorder.newMeasureMap().put(ml, n).record();
    }
  }
}
