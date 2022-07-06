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

import java.util.Map;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.MeanStatistic;
import org.apache.hadoop.fs.statistics.impl.StorageStatisticsFromIOStatistics;

/** Storage statistics for GCS, dynamically generated from the IOStatistics. */
public class GhfsStorageStatistics extends StorageStatisticsFromIOStatistics {

  /** {@value} The key that stores all the registered metrics */
  public static final String NAME = "GhfsStorageStatistics";

  /** Exention for minimum */
  private static final String MINIMUM = ".min";
  /** Exention for maximum */
  private static final String MAXIMUM = ".max";
  /** Exention for mean */
  private static final String MEAN = ".mean";

  /** IOStatistics Instance */
  private final IOStatistics ioStatistics;

  /** Create the Storage Statistics instance from the IOStatistics */
  public GhfsStorageStatistics(IOStatistics ioStatistics) {
    super(NAME, "Ghfs", ioStatistics);
    this.ioStatistics = ioStatistics;
  }

  /**
   * Map of minimums
   *
   * @return current map of minimums
   */
  private Map<String, Long> minimums() {
    return this.ioStatistics.minimums();
  }

  /**
   * Map of maximums
   *
   * @return current map of maximums
   */
  private Map<String, Long> maximums() {
    return this.ioStatistics.maximums();
  }

  /**
   * Map of meanStatistics
   *
   * @return current map of MeanStatistic statistics
   */
  private Map<String, MeanStatistic> meanStatistics() {
    return this.ioStatistics.meanStatistics();
  }

  /**
   * To get the minimum value which is stored with MINIMUM extension
   *
   * @param symbol
   * @return minimum statistic value
   */
  public Long getMin(String symbol) {
    return (Long) this.minimums().get(symbol + MINIMUM);
  }

  /**
   * To get the maximum value which is stored with MAXIMUM extension
   *
   * @param symbol
   * @return maximum statistic value
   */
  public Long getMax(String symbol) {
    return (Long) this.maximums().get(symbol + MAXIMUM);
  }

  /**
   * To get the mean value which is stored with MEAN extension
   *
   * @param symbol
   * @return mean statistic value
   */
  public double getMean(String symbol) {
    return this.meanStatistics().get(symbol + MEAN).mean();
  }
}
