/*
 * Copyright 2026 Google LLC
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

package com.google.cloud.hadoop.gcsio;

import static com.google.common.truth.Truth.assertThat;

import org.junit.Test;

public class TrackedFeaturesTest {
  @Test
  public void allBitPositions_areCorrect() {
    // This test ensures that the bit positions of all tracked features remain consistent and is not
    // changed accidentally.
    java.util.Map<TrackedFeatures, Integer> expectedBitPositions =
        com.google.common.collect.ImmutableMap.<TrackedFeatures, Integer>builder()
            .put(TrackedFeatures.FADVISE_AUTO, 0)
            .put(TrackedFeatures.FADVISE_RANDOM, 1)
            .put(TrackedFeatures.FADVISE_SEQUENTIAL, 2)
            .put(TrackedFeatures.FADVISE_AUTORANDOM, 3)
            .put(TrackedFeatures.HIERARCHICAL_NAMESPACE_ENABLED, 4)
            .put(TrackedFeatures.HNS_OPTIMIZATIONS_ENABLED, 5)
            .put(TrackedFeatures.PERFORMANCE_CACHE_ENABLED, 6)
            .put(TrackedFeatures.CLOUD_LOGGING_ENABLED, 7)
            .put(TrackedFeatures.TRACE_LOG_ENABLED, 8)
            .put(TrackedFeatures.OPERATION_TRACE_LOG_ENABLED, 9)
            .put(TrackedFeatures.DIRECT_UPLOAD_ENABLED, 10)
            .put(TrackedFeatures.BIDI_ENABLED, 11)
            .put(TrackedFeatures.RENAME_API, 12)
            .build();

    // Verifies that no new features have been added without updating this test.
    assertThat(TrackedFeatures.values()).hasLength(expectedBitPositions.size());

    // Verifies that the bit position for each feature is correct.
    for (java.util.Map.Entry<TrackedFeatures, Integer> entry : expectedBitPositions.entrySet()) {
      assertThat(entry.getKey().getBitPosition()).isEqualTo(entry.getValue());
    }
  }
}
