/*
 * Copyright 2022 Google LLC. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.hadoop.gcsio;

import io.opencensus.stats.Measure.MeasureLong;
import io.opencensus.tags.TagKey;

/** Interface for exposing utility methods to publish metrics via open-census api-spec. */
interface MetricsRecorder {

  /**
   * Publishes metric for specified open-census measurement
   *
   * @param key represents filterable attribute
   * @param value for the TagKey
   * @param ml - Measurement to be recorded
   * @param n - Value of the measurement recorded
   */
  void recordTaggedStat(TagKey key, String value, MeasureLong ml, Long n);

  /**
   * Publishes metric for specified open-census measurement
   *
   * @param keys - List of TagKeys attributes used for filtration
   * @param values - corresponding values for list of TagKeys
   * @param ml - Measurement to be recorded
   * @param n - Value of the measurement recorded
   */
  void recordLong(TagKey[] keys, String[] values, MeasureLong ml, Long n);
}

/** No-Op metrics publisher */
class NoOpMetricsRecorder implements MetricsRecorder {

  @Override
  public void recordTaggedStat(TagKey key, String value, MeasureLong ml, Long n) {
    // No-op
  }

  @Override
  public void recordLong(TagKey[] keys, String[] values, MeasureLong ml, Long n) {
    // No-op
  }
}
