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

package com.google.cloud.hadoop.fs.gcs;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.StorageStatistics;

/** Storage statistics for GCS */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class GhfsStorageStatistics extends StorageStatistics {

  /** {@value} The key that stores all the registered metrics */
  public static final String NAME = "GhfsStorageStatistics";

  /** Exention for minimum */
  private static final String MINIMUM = ".min";
  /** Exention for maximum */
  private static final String MAXIMUM = ".max";
  /** Exention for mean */
  private static final String MEAN = ".max";

  private final Map<GhfsStatistic, AtomicLong> opsCount = new EnumMap<>(GhfsStatistic.class);

  public GhfsStorageStatistics() {
    super(NAME);
    for (GhfsStatistic opType : GhfsStatistic.values()) {
      opsCount.put(opType, new AtomicLong(0));
    }
  }

  /**
   * Increment a specific counter.
   *
   * @param op operation
   * @param count increment value
   * @return the new value
   */
  public long incrementCounter(GhfsStatistic op, long count) {
    return opsCount.get(op).addAndGet(count);
  }

  @Override
  public void reset() {
    for (AtomicLong value : opsCount.values()) {
      value.set(0);
    }
  }

  private class LongIterator implements Iterator<LongStatistic> {
    private Iterator<Map.Entry<GhfsStatistic, AtomicLong>> iterator =
        Collections.unmodifiableSet(opsCount.entrySet()).iterator();

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public LongStatistic next() {
      if (!iterator.hasNext()) {
        throw new NoSuchElementException();
      }
      final Map.Entry<GhfsStatistic, AtomicLong> entry = iterator.next();
      return new LongStatistic(entry.getKey().name(), entry.getValue().get());
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public Iterator<LongStatistic> getLongStatistics() {
    return new LongIterator();
  }

  @Override
  public Long getLong(String key) {
    final GhfsStatistic type = GhfsStatistic.fromSymbol(key);
    return type == null ? null : opsCount.get(type).get();
  }

  @Override
  public boolean isTracked(String key) {
    return GhfsStatistic.fromSymbol(key) == null;
  }

  /**
   * To get the minimum value which is stored with MINIMUM extension
   *
   * @param symbol
   * @return minimum statistic value
   */
  public Long getMin(String symbol) {
    return 0L; // TODO: Update this once duration instrumentations are added
  }

  /**
   * To get the maximum value which is stored with MAXIMUM extension
   *
   * @param symbol
   * @return maximum statistic value
   */
  public Long getMax(String symbol) {
    return 0L; // TODO: Update this once duration instrumentations are added
  }

  /**
   * To get the mean value which is stored with MEAN extension
   *
   * @param symbol
   * @return mean statistic value
   */
  public double getMean(String symbol) {
    return 0L; // TODO: Update this once duration instrumentations are added
  }

  /**
   * Map of minimums
   *
   * @return current map of minimums
   */
  private Map<String, Long> minimums() {
    return null; // TODO: Update this once duration instrumentations are added
  }

  /**
   * Map of maximums
   *
   * @return current map of maximums
   */
  private Map<String, Long> maximums() {
    return null; // TODO: Update this once duration instrumentations are added
  }
}
