/*
 * Copyright 2026 Google LLC
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
package com.google.cloud.hadoop.gcsio;

import static com.google.cloud.hadoop.gcsio.FeatureHeaderGenerator.BITMASK_SIZE;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.Fadvise;
import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Unit tests for {@link FeatureHeaderGenerator}. This class contains non-parameterized tests for
 * the encoding logic. The parameterized tests for feature flag generation are in the inner class
 * {@link FeatureFlagGenerationTest}.
 */
@RunWith(JUnit4.class)
public class FeatureHeaderGeneratorTest {

  @Before
  public void setUp() {
    // Clear any request-specific features.
    FeatureHeaderGenerator.requestFeatures.remove();
  }

  // --- Tests for the encode method ---

  @Test
  public void encode_withEmptyBitmask_returnsNull() {
    assertThat(FeatureHeaderGenerator.encode(new BitSet(BITMASK_SIZE))).isNull();
  }

  @Test
  public void encode_withLowBitsSet_returnsCorrectString() {
    BitSet features = new BitSet(BITMASK_SIZE);
    features.set(TrackedFeatures.FADVISE_RANDOM.getBitPosition());
    features.set(TrackedFeatures.OPERATION_TRACE_LOG_ENABLED.getBitPosition());
    assertThat(FeatureHeaderGenerator.encode(features)).isEqualTo("AgI=");
  }

  @Test
  public void encode_withHighBitsSet_returnsCorrectString() {
    BitSet features = new BitSet(BITMASK_SIZE);
    // Hardcoded bit 65 is preserved here intentionally. The encoder operates on 64-bit
    // word boundaries, and we explicitly test that the bitset encoder correctly handles
    // bits that overflow into the second byte boundary, regardless of the enum.
    features.set(65);
    assertThat(FeatureHeaderGenerator.encode(features)).isEqualTo("AgAAAAAAAAAA");
  }

  @Test
  public void encode_withHighAndLowBitsSet_returnsCorrectString() {
    BitSet features = new BitSet(BITMASK_SIZE);
    features.set(0);
    features.set(64);
    assertThat(FeatureHeaderGenerator.encode(features)).isEqualTo("AQAAAAAAAAAB");
  }

  // --- Tests for the track method ---

  @Test
  public void track_withCallable_setsAndClearsFeature() throws IOException {
    FeatureHeaderGenerator header =
        new FeatureHeaderGenerator(GoogleCloudStorageFileSystemOptions.DEFAULT);

    // Dynamically generate the expected Base64 initial header string
    BitSet expectedInitialFeatures = new BitSet(BITMASK_SIZE);
    expectedInitialFeatures.set(TrackedFeatures.FADVISE_AUTO.getBitPosition());
    expectedInitialFeatures.set(TrackedFeatures.HIERARCHICAL_NAMESPACE_ENABLED.getBitPosition());
    String initialHeader = FeatureHeaderGenerator.encode(expectedInitialFeatures);

    assertThat(header.getValue()).isEqualTo(initialHeader);

    // A feature to track that is not part of the default set
    TrackedFeatures testFeature = TrackedFeatures.RENAME_API;

    String result =
        FeatureHeaderGenerator.track( // track is still static for request-level features
            testFeature,
            () -> {
              // Inside track, the header should include the new feature
              String trackedHeader = header.getValue();
              assertThat(trackedHeader).isNotNull();

              // Dynamically generate the expected tracked header
              BitSet expectedTrackedFeatures = (BitSet) expectedInitialFeatures.clone();
              expectedTrackedFeatures.set(testFeature.getBitPosition());
              assertThat(trackedHeader)
                  .isEqualTo(FeatureHeaderGenerator.encode(expectedTrackedFeatures));

              return "success";
            });

    assertThat(result).isEqualTo("success");

    // After track, the header should be back to the initial state
    String finalHeader = header.getValue();
    assertThat(finalHeader).isEqualTo(initialHeader);
  }

  @Test
  public void track_clearsFeatureOnException() {
    FeatureHeaderGenerator header =
        new FeatureHeaderGenerator(GoogleCloudStorageFileSystemOptions.DEFAULT);
    String initialHeader = header.getValue();
    TrackedFeatures testFeature = TrackedFeatures.RENAME_API; // bit 12

    IOException thrown =
        assertThrows(
            IOException.class,
            () ->
                FeatureHeaderGenerator.track(
                    testFeature,
                    () -> {
                      throw new IOException("test exception");
                    }));

    assertThat(thrown).hasMessageThat().isEqualTo("test exception");
    assertThat(header.getValue()).isEqualTo(initialHeader);
  }

  /** Parameterized tests for feature flag generation based on GCS options. */
  @RunWith(Parameterized.class)
  public static class FeatureFlagGenerationTest {

    String testName;
    private final GoogleCloudStorageFileSystemOptions options;
    private final BitSet expectedBitSet;

    public FeatureFlagGenerationTest(
        String testName, GoogleCloudStorageFileSystemOptions options, BitSet expectedBitSet) {
      this.testName = testName;
      this.options = options;
      this.expectedBitSet = expectedBitSet;
    }

    // Refactored helper method to accept enum values instead of hardcoded ints
    private static BitSet createBitSet(TrackedFeatures... features) {
      BitSet bitSet = new BitSet(BITMASK_SIZE);
      for (TrackedFeatures feature : features) {
        bitSet.set(feature.getBitPosition());
      }
      return bitSet;
    }

    @Parameters(name = "{0}")
    public static Collection<Object[]> data() {
      // Define constants for the default features to prevent repeating them
      TrackedFeatures fadviseAuto = TrackedFeatures.FADVISE_AUTO;
      TrackedFeatures hnsEnabled = TrackedFeatures.HIERARCHICAL_NAMESPACE_ENABLED;

      return Arrays.asList(
          new Object[][] {
            {
              "Default Options",
              GoogleCloudStorageFileSystemOptions.DEFAULT,
              createBitSet(fadviseAuto, hnsEnabled)
            },
            {
              "Fadvise Random",
              buildOptionsWithFadvise(Fadvise.RANDOM),
              createBitSet(TrackedFeatures.FADVISE_RANDOM, hnsEnabled)
            },
            {
              "Fadvise Sequential",
              buildOptionsWithFadvise(Fadvise.SEQUENTIAL),
              createBitSet(TrackedFeatures.FADVISE_SEQUENTIAL, hnsEnabled)
            },
            {
              "Fadvise AutoRandom",
              buildOptionsWithFadvise(Fadvise.AUTO_RANDOM),
              createBitSet(TrackedFeatures.FADVISE_AUTORANDOM, hnsEnabled)
            },
            {
              "Hierarchical Namespace",
              GoogleCloudStorageFileSystemOptions.DEFAULT.toBuilder()
                  .setCloudStorageOptions(
                      GoogleCloudStorageOptions.DEFAULT.toBuilder()
                          .setHnBucketRenameEnabled(true)
                          .build())
                  .build(),
              createBitSet(fadviseAuto, hnsEnabled)
            },
            {
              "HNS Optimizations Enabled",
              GoogleCloudStorageFileSystemOptions.DEFAULT.toBuilder()
                  .setCloudStorageOptions(
                      GoogleCloudStorageOptions.DEFAULT.toBuilder()
                          .setHnOptimizationEnabled(true)
                          .build())
                  .build(),
              createBitSet(fadviseAuto, hnsEnabled, TrackedFeatures.HNS_OPTIMIZATIONS_ENABLED)
            },
            {
              "Performance Cache",
              GoogleCloudStorageFileSystemOptions.DEFAULT.toBuilder()
                  .setPerformanceCacheEnabled(true)
                  .build(),
              createBitSet(fadviseAuto, hnsEnabled, TrackedFeatures.PERFORMANCE_CACHE_ENABLED)
            },
            {
              "Cloud Logging Enabled",
              GoogleCloudStorageFileSystemOptions.DEFAULT.toBuilder()
                  .setCloudLoggingEnabled(true)
                  .build(),
              createBitSet(fadviseAuto, hnsEnabled, TrackedFeatures.CLOUD_LOGGING_ENABLED)
            },
            {
              "Trace Log Enabled",
              GoogleCloudStorageFileSystemOptions.DEFAULT.toBuilder()
                  .setCloudStorageOptions(
                      GoogleCloudStorageOptions.DEFAULT.toBuilder()
                          .setTraceLogEnabled(true)
                          .build())
                  .build(),
              createBitSet(fadviseAuto, hnsEnabled, TrackedFeatures.TRACE_LOG_ENABLED)
            },
            {
              "Operation Trace Log Enabled",
              GoogleCloudStorageFileSystemOptions.DEFAULT.toBuilder()
                  .setCloudStorageOptions(
                      GoogleCloudStorageOptions.DEFAULT.toBuilder()
                          .setOperationTraceLogEnabled(true)
                          .build())
                  .build(),
              createBitSet(fadviseAuto, hnsEnabled, TrackedFeatures.OPERATION_TRACE_LOG_ENABLED)
            },
            {
              "Direct Upload",
              GoogleCloudStorageFileSystemOptions.DEFAULT.toBuilder()
                  .setCloudStorageOptions(
                      GoogleCloudStorageOptions.DEFAULT.toBuilder()
                          .setWriteChannelOptions(
                              GoogleCloudStorageOptions.DEFAULT.getWriteChannelOptions().toBuilder()
                                  .setDirectUploadEnabled(true)
                                  .build())
                          .build())
                  .build(),
              createBitSet(fadviseAuto, hnsEnabled, TrackedFeatures.DIRECT_UPLOAD_ENABLED)
            },
            {
              "Bidi Enabled",
              buildOptionsWithBidi(),
              createBitSet(fadviseAuto, hnsEnabled, TrackedFeatures.BIDI_ENABLED)
            }
          });
    }

    @Test
    public void generatesCorrectHeaderValue() {
      FeatureHeaderGenerator header = new FeatureHeaderGenerator(options);
      String expectedHeaderValue = FeatureHeaderGenerator.encode(expectedBitSet);
      assertThat(header.getValue()).isEqualTo(expectedHeaderValue);
    }
  }

  // --- Helper Methods ---

  private static GoogleCloudStorageFileSystemOptions buildOptionsWithFadvise(Fadvise fadvise) {
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.DEFAULT.toBuilder().setFadvise(fadvise).build();
    GoogleCloudStorageOptions storageOptions =
        GoogleCloudStorageOptions.DEFAULT.toBuilder().setReadChannelOptions(readOptions).build();
    return GoogleCloudStorageFileSystemOptions.DEFAULT.toBuilder()
        .setCloudStorageOptions(storageOptions)
        .build();
  }

  private static GoogleCloudStorageFileSystemOptions buildOptionsWithBidi() {
    GoogleCloudStorageOptions storageOptions =
        GoogleCloudStorageOptions.DEFAULT.toBuilder().setBidiEnabled(true).build();
    return GoogleCloudStorageFileSystemOptions.DEFAULT.toBuilder()
        .setCloudStorageOptions(storageOptions)
        .build();
  }
}
