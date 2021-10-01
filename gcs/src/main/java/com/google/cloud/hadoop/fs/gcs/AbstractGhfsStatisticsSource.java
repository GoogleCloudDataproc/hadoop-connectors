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
 * Base class for implementing IOStatistics sources in the gcs module.
 *
 * <p>A lot of the methods are very terse, because GHFSInstrumentation has verbose methods of
 * similar names; the short ones always refer to the inner class and not any superclass method.
 */
abstract class AbstractGhfsStatisticsSource implements IOStatisticsSource, DurationTrackerFactory {
  /** IOStatisticsStore to track the statistics */
  private IOStatisticsStore ioStatistics;

  protected AbstractGhfsStatisticsSource() {}

  /**
   * Setter. this must be called in the subclass constructor with whatever
   *
   * @param statistics statistics to set
   */
  protected void setIOStatistics(IOStatisticsStore statistics) {
    this.ioStatistics = statistics;
  }

  /**
   * Increment a named counter by 1.
   *
   * @param name counter name
   * @return the updated value or, if the counter is unknown: 0
   */
  public long incCounter(String name) {
    return incCounter(name, 1);
  }

  /**
   * Increment a named counter by 1.
   *
   * @param name counter name
   * @param value value to increment by
   * @return the updated value or, if the counter is unknown: 0
   */
  public long incCounter(String name, long value) {
    return ioStatistics.incrementCounter(name, value);
  }

  /**
   * Get the value of a counter.
   *
   * @param name counter name
   * @return the value or null if no matching counter was found.
   */
  public Long lookupCounterValue(final String name) {
    return ioStatistics.counters().get(name);
  }

  /**
   * Get the value of a gauge.
   *
   * @param name gauge name
   * @return the value or null if no matching gauge was found.
   */
  public Long lookupGaugeValue(final String name) {
    return ioStatistics.gauges().get(name);
  }

  /**
   * Increment the value of a gauge.
   *
   * @param name gauge name
   * @param v value to increment
   * @return the value or 0 if no matching gauge was found.
   */
  public long incGauge(String name, long v) {
    return ioStatistics.incrementGauge(name, v);
  }

  /**
   * Increment the value of a gauge by 1.
   *
   * @param name gauge name
   * @return the value or 0 if no matching gauge was found.
   */
  public long incGauge(String name) {
    return incGauge(name, 1);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this.getClass().getSimpleName())
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
  public DurationTracker trackDuration(final String key, final long count) {
    return getIOStatistics().trackDuration(key, count);
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
