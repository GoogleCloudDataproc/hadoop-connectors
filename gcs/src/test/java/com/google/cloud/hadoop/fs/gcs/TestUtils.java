package com.google.cloud.hadoop.fs.gcs;

import static com.google.common.truth.Truth.assertThat;

import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.StoreStatisticNames;

class TestUtils {
  static void verifyDurationMetric(IOStatistics ioStatistics, String symbol, int expected) {
    assertThat(ioStatistics.counters().get(symbol)).isEqualTo(expected);
    String minKey = String.format("%s%s", symbol, StoreStatisticNames.SUFFIX_MIN);
    String meanKey = String.format("%s%s", symbol, StoreStatisticNames.SUFFIX_MEAN);
    String maxKey = String.format("%s%s", symbol, StoreStatisticNames.SUFFIX_MAX);

    Long minValue = ioStatistics.minimums().get(minKey);
    Long maxValue = ioStatistics.maximums().get(maxKey);
    long meanValue = Double.valueOf(ioStatistics.meanStatistics().get(meanKey).mean()).longValue();

    assertThat(minValue).isLessThan(maxValue + 1);
    assertThat(minValue).isLessThan(meanValue + 1);
    assertThat(meanValue).isLessThan(maxValue + 1);
  }
}
