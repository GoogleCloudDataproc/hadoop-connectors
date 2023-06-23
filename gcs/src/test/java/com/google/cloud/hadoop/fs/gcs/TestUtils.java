package com.google.cloud.hadoop.fs.gcs;

/*
 * Copyright 2023 Google Inc. All Rights Reserved.
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

import static com.google.common.truth.Truth.assertThat;

import org.apache.hadoop.fs.GlobalStorageStatistics;

class TestUtils {

  static void verifyDurationMetric(
      GhfsStorageStatistics stats, GhfsStatistic statistic, int expected) {
    String symbol = statistic.getSymbol();
    long minValue = stats.getMin(symbol);
    long maxValue = stats.getMax(symbol);
    long meanValue = Double.valueOf(stats.getMean(symbol)).longValue();

    assertThat(stats.getLong(symbol).equals(expected));
    assertThat(minValue).isLessThan(maxValue + 1);
    assertThat(minValue).isLessThan(meanValue + 1);
    assertThat(meanValue).isLessThan(maxValue + 1);
  }

  static GhfsStorageStatistics getStorageStatistics() {
    GhfsStorageStatistics stats =
        (GhfsStorageStatistics) GlobalStorageStatistics.INSTANCE.get(GhfsStorageStatistics.NAME);
    stats.reset();
    return stats;
  }

  static void verifyCounter(GhfsStorageStatistics stats, GhfsStatistic statName, int expected) {
    assertThat(stats.getLong(statName.getSymbol())).isEqualTo(expected);
  }
}
