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

import static com.google.common.truth.Truth.assertThat;
import static java.util.Map.entry;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GhfsThreadLocalStatisticsTest {
  private GhfsThreadLocalStatistics statistics;
  private Map<String, Long> expected;

  private static final String GCS_API_COUNT = "gcsApiCount";
  private static final String GCS_API_TIME = "gcsApiTime";
  private static final String BACKOFF_COUNT = "backoffCount";
  private static final String BACKOFF_TIME = "backoffTime";
  private static final String HADOOP_API_COUNT = "hadoopApiCount";
  private static final String HADOOP_API_TIME = "hadoopApiTime";
  private static final String STREAM_READ_VECTORED_COUNT = "readVectoredCount";
  private static final String STREAM_READ_VECTORED_RANGE_COUNT = "readVectoredRangeCount";

  private static Map<GoogleCloudStorageStatistics, String> typeToNameMapping =
      Map.of(
          GoogleCloudStorageStatistics.GCS_API_REQUEST_COUNT, GCS_API_COUNT,
          GoogleCloudStorageStatistics.GCS_API_TIME, GCS_API_TIME,
          GoogleCloudStorageStatistics.GCS_BACKOFF_COUNT, BACKOFF_COUNT,
          GoogleCloudStorageStatistics.GCS_BACKOFF_TIME, BACKOFF_TIME);

  @Before
  public void init() {
    this.statistics = new GhfsThreadLocalStatistics();
    this.expected = getInitMetrics();
    this.statistics.reset();
  }

  private Map<String, Long> getInitMetrics() {
    Map<String, Long> result =
        new HashMap<>(
            Map.ofEntries(
                entry(BACKOFF_COUNT, 0L),
                entry(BACKOFF_TIME, 0L),
                entry(HADOOP_API_COUNT, 0L),
                entry(HADOOP_API_TIME, 0L),
                entry(GCS_API_COUNT, 0L),
                entry(GCS_API_TIME, 0L),
                entry(STREAM_READ_VECTORED_COUNT, 0L),
                entry(STREAM_READ_VECTORED_RANGE_COUNT, 0L)));

    return result;
  }

  @Test
  public void testInitialState() {
    verify(expected, statistics);
  }

  @Test
  public void testNotTracked() {
    verify(expected, statistics);
    assertThat(statistics.isTracked("notfound")).isFalse();
    assertThat(statistics.getLong("notfound")).isEqualTo(0);
  }

  @Test
  public void testHadoopApiMetricsTest() {
    runHadoopApiTests(expected, statistics);
  }

  @Test
  public void testGcsApiMetricsTest() {
    runGcsAPITests(this.expected, statistics);
  }

  @Test
  public void testReset() {
    runGcsAPITests(this.expected, statistics);
    statistics.reset();

    for (String metric : expected.keySet()) {
      expected.put(metric, 0L);
    }

    verify(expected, statistics);
  }

  @Test
  public void multiThreadTest() {
    IntStream.range(0, 5000)
        .parallel()
        .forEach(
            i -> {
              Map<String, Long> expectedMetrics = getThreadLocalMetrics(statistics);
              runGcsAPITests(expectedMetrics, statistics);
              runHadoopApiTests(expectedMetrics, statistics);
            });
  }

  private static void runHadoopApiTests(
      Map<String, Long> expectedMetrics, GhfsThreadLocalStatistics actualMetrics) {
    for (GhfsStatistic ghfsStatistic : GhfsStatistic.VALUES) {
      actualMetrics.increment(ghfsStatistic, 1);
      if (ghfsStatistic.getIsHadoopApi()) {
        expectedMetrics.merge(HADOOP_API_COUNT, 1L, Long::sum);
      } else if (ghfsStatistic == GhfsStatistic.GCS_CONNECTOR_TIME) {
        expectedMetrics.merge(HADOOP_API_TIME, 1L, Long::sum);
      } else if (ghfsStatistic == GhfsStatistic.STREAM_READ_VECTORED_OPERATIONS) {
        expectedMetrics.merge(STREAM_READ_VECTORED_COUNT, 1L, Long::sum);
      } else if (ghfsStatistic == GhfsStatistic.STREAM_READ_VECTORED_READ_COMBINED_RANGES) {
        expectedMetrics.merge(STREAM_READ_VECTORED_RANGE_COUNT, 1L, Long::sum);
      }

      verify(expectedMetrics, actualMetrics);
    }

    for (GhfsStatistic ghfsStatistic : GhfsStatistic.VALUES) {
      long theValue = Math.abs(ThreadLocalRandom.current().nextLong(1, 2000));
      actualMetrics.increment(ghfsStatistic, theValue);
      if (ghfsStatistic.getIsHadoopApi()) {
        expectedMetrics.merge(HADOOP_API_COUNT, theValue, Long::sum);
      } else if (ghfsStatistic == GhfsStatistic.GCS_CONNECTOR_TIME) {
        expectedMetrics.merge(HADOOP_API_TIME, theValue, Long::sum);
      } else if (ghfsStatistic == GhfsStatistic.STREAM_READ_VECTORED_OPERATIONS) {
        expectedMetrics.merge(STREAM_READ_VECTORED_COUNT, theValue, Long::sum);
      } else if (ghfsStatistic == GhfsStatistic.STREAM_READ_VECTORED_READ_COMBINED_RANGES) {
        expectedMetrics.merge(STREAM_READ_VECTORED_RANGE_COUNT, theValue, Long::sum);
      }

      verify(expectedMetrics, actualMetrics);
    }
  }

  private static void runGcsAPITests(
      Map<String, Long> expectedMetrics, GhfsThreadLocalStatistics actualMetrics) {
    verify(expectedMetrics, actualMetrics);

    for (GoogleCloudStorageStatistics theStat : typeToNameMapping.keySet()) {
      actualMetrics.increment(theStat, 1);
      expectedMetrics.merge(typeToNameMapping.get(theStat), 1L, Long::sum);
      verify(expectedMetrics, actualMetrics);
    }

    for (int i = 0; i < 10; i++) {
      for (GoogleCloudStorageStatistics theStat : typeToNameMapping.keySet()) {
        Long theValue = ThreadLocalRandom.current().nextLong(1, Integer.MAX_VALUE);
        actualMetrics.increment(theStat, theValue);
        expectedMetrics.merge(typeToNameMapping.get(theStat), theValue, Long::sum);
        verify(expectedMetrics, actualMetrics);
      }
    }
  }

  private static void verify(
      Map<String, Long> expectedMetrics, GhfsThreadLocalStatistics actualMetrics) {
    expectedMetrics.forEach((key, value) -> checkTracked(key, value, actualMetrics));
    assertThat(getThreadLocalMetrics(actualMetrics)).isEqualTo(expectedMetrics);
  }

  private static Map<String, Long> getThreadLocalMetrics(GhfsThreadLocalStatistics statistics) {
    Map<String, Long> values = new HashMap<>();
    statistics
        .getLongStatistics()
        .forEachRemaining(theStat -> values.put(theStat.getName(), theStat.getValue()));
    return values;
  }

  private static void checkTracked(
      String metric, long expected, GhfsThreadLocalStatistics statistics) {
    assertThat(statistics.isTracked(metric)).isTrue();
    assertThat(statistics.getLong(metric)).isEqualTo(expected);
  }
}
