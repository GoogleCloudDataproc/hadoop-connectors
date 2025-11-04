package com.google.cloud.hadoop.gcsio;

import static com.google.common.truth.Truth.assertThat;

import org.junit.Test;

public class TrackedFeaturesTest {
  @Test
  public void allBitPositions_areCorrect() {
    // This test ensures that the bit positions of all tracked features remain consistent and is not
    // changed accidentally.
    assertThat(TrackedFeatures.FADVISE_AUTO.getBitPosition()).isEqualTo(0);
    assertThat(TrackedFeatures.FADVISE_RANDOM.getBitPosition()).isEqualTo(1);
    assertThat(TrackedFeatures.FADVISE_SEQUENTIAL.getBitPosition()).isEqualTo(2);
    assertThat(TrackedFeatures.FADVISE_AUTORANDOM.getBitPosition()).isEqualTo(3);
    assertThat(TrackedFeatures.HIERARCHICAL_NAMESPACE_ENABLED.getBitPosition()).isEqualTo(4);
    assertThat(TrackedFeatures.PERFORMANCE_CACHE_ENABLED.getBitPosition()).isEqualTo(5);
    assertThat(TrackedFeatures.CLOUD_LOGGING_ENABLED.getBitPosition()).isEqualTo(6);
    assertThat(TrackedFeatures.TRACE_LOG_ENABLED.getBitPosition()).isEqualTo(7);
    assertThat(TrackedFeatures.OPERATION_TRACE_LOG_ENABLED.getBitPosition()).isEqualTo(8);
    assertThat(TrackedFeatures.DIRECT_UPLOAD_ENABLED.getBitPosition()).isEqualTo(9);
    assertThat(TrackedFeatures.BIDI_ENABLED.getBitPosition()).isEqualTo(10);
    assertThat(TrackedFeatures.RENAME_API.getBitPosition()).isEqualTo(11);
  }
}
