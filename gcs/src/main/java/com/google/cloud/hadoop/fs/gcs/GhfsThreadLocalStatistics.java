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

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.fs.StorageStatistics;

class GhfsThreadLocalStatistics extends StorageStatistics {
  static final String NAME = "GhfsThreadLocalStatistics";
  static final String HADOOP_API_COUNT = "hadoopApiCount";
  static final String HADOOP_API_TIME = "hadoopApiTime";
  static final String GCS_API_COUNT = "gcsApiCount";
  static final String GCS_API_TIME = "gcsApiTime";
  static final String BACKOFF_COUNT = "backoffCount";
  static final String BACKOFF_TIME = "backoffTime";

  private final ThreadLocalValue hadoopApiCount;
  private final ThreadLocalValue hadoopApiTime;
  private final ThreadLocalValue gcsApiCount;
  private final ThreadLocalValue gcsApiTime;
  private final ThreadLocalValue backoffCount;
  private final ThreadLocalValue backoffTime;

  private Map<String, ThreadLocalValue> metrics = new HashMap<>();

  GhfsThreadLocalStatistics() {
    super(NAME);
    this.hadoopApiCount = createMetric(HADOOP_API_COUNT);
    this.hadoopApiTime = createMetric(HADOOP_API_TIME);
    this.gcsApiCount = createMetric(GCS_API_COUNT);
    this.gcsApiTime = createMetric(GCS_API_TIME);
    this.backoffCount = createMetric(BACKOFF_COUNT);
    this.backoffTime = createMetric(BACKOFF_TIME);
  }

  private ThreadLocalValue createMetric(String name) {
    ThreadLocalValue result = new ThreadLocalValue();
    metrics.put(name, result);

    return result;
  }

  @Override
  public Long getLong(String s) {
    if (!metrics.containsKey(s)) {
      return 0L;
    }

    return metrics.get(s).getValue();
  }

  @Override
  public boolean isTracked(String s) {
    return metrics.containsKey(s);
  }

  @Override
  public void reset() {
    for (ThreadLocalValue s : metrics.values()) {
      s.reset();
    }
  }

  void increment(GhfsStatistic statistic, long count) {
    if (statistic == GCS_CONNECTOR_TIME) {
      this.hadoopApiTime.increment(count);
    } else if (statistic.getIsHadoopApi()) {
      this.hadoopApiCount.increment(count);
    }
  }

  void increment(GoogleCloudStorageStatistics op, long count) {
    if (op == GoogleCloudStorageStatistics.GCS_API_TIME) {
      this.gcsApiTime.increment(count);
    } else if (op == GoogleCloudStorageStatistics.GCS_API_REQUEST_COUNT) {
      this.gcsApiCount.increment(count);
    } else if (op == GoogleCloudStorageStatistics.GCS_BACKOFF_COUNT) {
      this.backoffCount.increment(count);
    } else if (op == GoogleCloudStorageStatistics.GCS_BACKOFF_TIME) {
      this.backoffTime.increment(count);
    }
  }

  @Override
  public Iterator<LongStatistic> getLongStatistics() {
    return this.metrics.entrySet().stream()
        .map(entry -> new LongStatistic(entry.getKey(), entry.getValue().getValue()))
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
}
