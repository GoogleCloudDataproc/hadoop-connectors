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

import com.google.common.base.MoreObjects;
import org.apache.hadoop.fs.statistics.DurationTracker;
import org.apache.hadoop.fs.statistics.DurationTrackerFactory;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;

/**
 * Abstract Base class for IOStatistics. This class is expected to implement IOStatistics Sources in
 * Input Stream, Output Stream.
 */
abstract class AbstractGhfsStatisticsSource implements IOStatisticsSource, DurationTrackerFactory {

  /** IOStatisticsStore to track the statistics */
  private IOStatisticsStore ioStatistics;

  protected AbstractGhfsStatisticsSource() {}

  /**
   * Set the IOStatistics with the iostatistics returned from the subclass.
   *
   * @param ioStatistics statistics to set
   */
  protected void setIOStatistics(IOStatisticsStore ioStatistics) {
    this.ioStatistics = ioStatistics;
  }

  /**
   * Increment a named counter by 1.
   *
   * @param name counter name
   * @return the updated value or, if the counter is unknown: 0
   */
  public long incrementCounter(String name) {
    return incrementCounter(name, 1);
  }

  /**
   * Increment a named counter by 1.
   *
   * @param name counter name
   * @param value value to increment by
   * @return the updated value or, if the counter is unknown: 0
   */
  public long incrementCounter(String name, long value) {
    return ioStatistics.incrementCounter(name, value);
  }

  /**
   * Get the value of a counter.
   *
   * @param name counter name
   * @return the value or null if no matching counter was found.
   */
  public Long lookupCounterValue(String name) {
    return ioStatistics.counters().get(name);
  }

  /**
   * Get the value of a gauge.
   *
   * @param name gauge name
   * @return the value or null if no matching gauge was found.
   */
  public Long lookupGaugeValue(String name) {
    return ioStatistics.gauges().get(name);
  }

  /**
   * Increment the value of a gauge.
   *
   * @param name gauge name
   * @param v value to increment
   * @return the value or 0 if no matching gauge was found.
   */
  public long incrementGauge(String name, long v) {
    return ioStatistics.incrementGauge(name, v);
  }

  /**
   * Increment the value of a gauge by 1.
   *
   * @param name gauge name
   * @return the value or 0 if no matching gauge was found.
   */
  public long incrementGauge(String name) {
    return incrementGauge(name, 1);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(getClass().getSimpleName())
        .add("IOStatistics ", ioStatistics)
        .toString();
  }

  /**
   * The duration tracker updates the metrics with the count and IOStatistics will full duration
   * information.
   *
   * @param key statistic key prefix
   * @param count #of times to increment the matching counter in this operation.
   * @return a duration tracker.
   */
  @Override
  public DurationTracker trackDuration(String key, long count) {
    return ioStatistics.trackDuration(key, count);
  }

  /**
   * Get the instance IO Statistics.
   *
   * @return IOstatistics.
   */
  @Override
  public IOStatisticsStore getIOStatistics() {
    return ioStatistics;
  }
}
