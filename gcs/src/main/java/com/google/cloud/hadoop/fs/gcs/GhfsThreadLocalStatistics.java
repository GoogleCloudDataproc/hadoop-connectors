/*
 * Copyright 2025 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.fs.gcs;

import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.GCS_CONNECTOR_TIME;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.STREAM_READ_VECTORED_OPERATIONS;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.STREAM_READ_VECTORED_READ_COMBINED_RANGES;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.fs.StorageStatistics;

class GhfsThreadLocalStatistics extends StorageStatistics {
  static final String NAME = "GhfsThreadLocalStatistics";
  private Map<String, Metric> metrics = new HashMap<>();

  GhfsThreadLocalStatistics() {
    super(NAME);
    Arrays.stream(Metric.values()).forEach(x -> metrics.put(x.metricName, x));
  }

  @Override
  public Long getLong(String s) {
    if (!metrics.containsKey(s)) {
      return 0L;
    }

    return metrics.get(s).metricValue.getValue();
  }

  @Override
  public boolean isTracked(String s) {
    return metrics.containsKey(s);
  }

  @Override
  public void reset() {
    for (Metric s : metrics.values()) {
      s.reset();
    }
  }

  void increment(GhfsStatistic statistic, long count) {
    if (statistic == GCS_CONNECTOR_TIME) {
      Metric.HADOOP_API_TIME.increment(count);
    } else if (statistic == STREAM_READ_VECTORED_OPERATIONS) {
      Metric.STREAM_READ_VECTORED_COUNT.increment(count);
    } else if (statistic == STREAM_READ_VECTORED_READ_COMBINED_RANGES) {
      Metric.STREAM_READ_VECTORED_RANGE_COUNT.increment(count);
    } else if (statistic.getIsHadoopApi()) {
      Metric.HADOOP_API_COUNT.increment(count);
    }
  }

  void increment(GoogleCloudStorageStatistics op, long count) {
    if (op == GoogleCloudStorageStatistics.GCS_API_TIME) {
      Metric.GCS_API_TIME.increment(count);
    } else if (op == GoogleCloudStorageStatistics.GCS_API_REQUEST_COUNT) {
      Metric.GCS_API_COUNT.increment(count);
    } else if (op == GoogleCloudStorageStatistics.GCS_BACKOFF_COUNT) {
      Metric.BACKOFF_COUNT.increment(count);
    } else if (op == GoogleCloudStorageStatistics.GCS_BACKOFF_TIME) {
      Metric.BACKOFF_TIME.increment(count);
    }
  }

  void increment(String s, long count) {
    Metric m = Metric.getMetricByName(s);
    if (m != null) {
      m.increment(count);
    }
  }

  @Override
  public Iterator<LongStatistic> getLongStatistics() {
    return this.metrics.entrySet().stream()
        .map(entry -> new LongStatistic(entry.getKey(), entry.getValue().metricValue.getValue()))
        .iterator();
  }

  private static class ThreadLocalValue {
    private ThreadLocal<Long> value = ThreadLocal.withInitial(() -> 0L);

    void increment(long count) {
      value.set(value.get() + count);
    }

    Long getValue() {
      return value.get();
    }

    void reset() {
      value.set(0L);
    }
  }

  private enum Metric {
    HADOOP_API_COUNT("hadoopApiCount"),
    HADOOP_API_TIME("hadoopApiTime"),
    GCS_API_COUNT("gcsApiCount"),
    GCS_API_TIME("gcsApiTime"),
    BACKOFF_COUNT("backoffCount"),
    BACKOFF_TIME("backoffTime"),
    STREAM_READ_VECTORED_COUNT("readVectoredCount"),
    STREAM_READ_VECTORED_RANGE_COUNT("readVectoredRangeCount");

    private static final Map<String, Metric> BY_LABEL = new HashMap<>();

    static {
      for (Metric e : values()) {
        BY_LABEL.put(e.metricName, e);
      }
    }

    private final String metricName;
    private final ThreadLocalValue metricValue;

    Metric(String metricName) {
      this.metricName = metricName;
      this.metricValue = new ThreadLocalValue();
    }

    public static Metric getMetricByName(String label) {
      return BY_LABEL.get(label);
    }

    void reset() {
      metricValue.reset();
    }

    void increment(long count) {
      metricValue.increment(count);
    }
  }
}
