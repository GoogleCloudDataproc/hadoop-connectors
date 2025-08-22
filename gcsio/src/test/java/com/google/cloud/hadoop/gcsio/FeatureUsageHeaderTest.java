/*
 * Copyright 2024 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may
 * obtain a copy of the License at
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
import static org.junit.Assert.assertThrows;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.Fadvise;
import java.nio.ByteBuffer;
import java.util.Base64;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class FeatureUsageHeaderTest {

  @Test
  public void encode_withEmptyBitmask_returnsNull() {
    long[] features = new long[FeatureUsageHeader.BITMASK_SIZE];
    assertThat(FeatureUsageHeader.encode(features)).isNull();
  }

  @Test
  public void encode_withLowBitsSet_returnsCorrectString() {
    long[] features = new long[FeatureUsageHeader.BITMASK_SIZE];
    features[FeatureUsageHeader.LOW_BITS_INDEX] = 1L << 1 | 1L << 9; // 2 | 512 = 514
    // 514 = 0x0202
    // Expected byte array: [2, 2]
    // Base64 of [2, 2] is "AgI="
    assertThat(FeatureUsageHeader.encode(features)).isEqualTo("AgI=");
  }

  @Test
  public void encode_withHighBitsSet_returnsCorrectString() {
    long[] features = new long[FeatureUsageHeader.BITMASK_SIZE];
    features[FeatureUsageHeader.HIGH_BITS_INDEX] = 1L << 1; // Bit 65
    // Expected byte array: [0, ..., 0, 2, 0, ..., 0] (16 bytes total)
    // Trimmed: [2, 0, 0, 0, 0, 0, 0, 0, 0]
    // Base64 is "AgAAAAAAAAAA"
    assertThat(FeatureUsageHeader.encode(features)).isEqualTo("AgAAAAAAAAAA");
  }

  @Test
  public void encode_withHighAndLowBitsSet_returnsCorrectString() {
    long[] features = new long[FeatureUsageHeader.BITMASK_SIZE];
    features[FeatureUsageHeader.HIGH_BITS_INDEX] = 1L; // Bit 64
    features[FeatureUsageHeader.LOW_BITS_INDEX] = 1L; // Bit 0
    // Expected byte array: [0,...,0,1, 0,...,0,1] (16 bytes total)
    // Trimmed: [1, 0, 0, 0, 0, 0, 0, 0, 1]
    // Base64 is "AQAAAAAAAAAB"
    assertThat(FeatureUsageHeader.encode(features)).isEqualTo("AQAAAAAAAAAB");
  }

  @Test
  public void encode_throwsOnInvalidBitmaskSize() {
    long[] features = new long[1];
    assertThrows(IllegalArgumentException.class, () -> FeatureUsageHeader.encode(features));
  }

  @Test
  public void getValue_withDefaultOptions_setsCorrectFeatures() {
    // Default options enable:
    // - Fadvise.AUTO (bit 0)
    // - GCS_STATUS_PARALLEL_ENABLED (bit 9)
    // - GCS_COPY_WITH_REWRITE_ENABLED (bit 10)
    GoogleCloudStorageFileSystemOptions options = GoogleCloudStorageFileSystemOptions.DEFAULT;

    long expectedLowBits = (1L) | (1L << 9) | (1L << 10); // 1 | 512 | 1024 = 1537
    verifyHeader(options, 0L, expectedLowBits);
  }

  @Test
  public void getValue_withFadviseRandom() {
    GoogleCloudStorageFileSystemOptions options = buildOptionsWithFadvise(Fadvise.RANDOM);
    // FADVISE_RANDOM (bit 1) is set, FADVISE_AUTO (bit 0) is not.
    long expectedLowBits = (1L << 1) | (1L << 9) | (1L << 10);
    verifyHeader(options, 0L, expectedLowBits);
  }

  @Test
  public void getValue_withFadviseSequential() {
    GoogleCloudStorageFileSystemOptions options = buildOptionsWithFadvise(Fadvise.SEQUENTIAL);
    // FADVISE_SEQUENTIAL (bit 2) is set, FADVISE_AUTO (bit 0) is not.
    long expectedLowBits = (1L << 2) | (1L << 9) | (1L << 10);
    verifyHeader(options, 0L, expectedLowBits);
  }

  @Test
  public void getValue_withCopyWithRewriteDisabled() {
    GoogleCloudStorageOptions storageOptions =
        GoogleCloudStorageOptions.DEFAULT.toBuilder().setCopyWithRewriteEnabled(false).build();
    GoogleCloudStorageFileSystemOptions options =
        GoogleCloudStorageFileSystemOptions.DEFAULT.toBuilder()
            .setCloudStorageOptions(storageOptions)
            .build();

    // GCS_COPY_WITH_REWRITE_ENABLED (bit 10) is not set.
    long expectedLowBits = (1L) | (1L << 9);
    verifyHeader(options, 0L, expectedLowBits);
  }

  @Test
  public void getValue_withStatusParallelDisabled() {
    GoogleCloudStorageFileSystemOptions options =
        GoogleCloudStorageFileSystemOptions.DEFAULT.toBuilder()
            .setStatusParallelEnabled(false)
            .build();

    // GCS_STATUS_PARALLEL_ENABLED (bit 9) is not set.
    long expectedLowBits = (1L) | (1L << 10);
    verifyHeader(options, 0L, expectedLowBits);
  }

  @Test
  public void getValue_withHierarchicalNamespace() {
    GoogleCloudStorageOptions storageOptions =
        GoogleCloudStorageOptions.DEFAULT.toBuilder().setHnBucketRenameEnabled(true).build();
    GoogleCloudStorageFileSystemOptions options =
        GoogleCloudStorageFileSystemOptions.DEFAULT.toBuilder()
            .setCloudStorageOptions(storageOptions)
            .build();

    // HIERARCHICAL_NAMESPACE_ENABLED is bit 4
    long expectedLowBits = (1L << 0) | (1L << 4) | (1L << 9) | (1L << 10);
    verifyHeader(options, 0L, expectedLowBits);
  }

  @Test
  public void getValue_withPerformanceCache() {
    GoogleCloudStorageFileSystemOptions options =
        GoogleCloudStorageFileSystemOptions.DEFAULT.toBuilder()
            .setPerformanceCacheEnabled(true)
            .build();

    // PERFORMANCE_CACHE_ENABLED is bit 5
    long expectedLowBits = (1L) | (1L << 5) | (1L << 9) | (1L << 10);
    verifyHeader(options, 0L, expectedLowBits);
  }

  @Test
  public void getValue_withDirectUpload() {
    GoogleCloudStorageOptions storageOptions =
        GoogleCloudStorageOptions.DEFAULT.toBuilder()
            .setWriteChannelOptions(
                GoogleCloudStorageOptions.DEFAULT.getWriteChannelOptions().toBuilder()
                    .setDirectUploadEnabled(true)
                    .build())
            .build();
    GoogleCloudStorageFileSystemOptions options =
        GoogleCloudStorageFileSystemOptions.DEFAULT.toBuilder()
            .setCloudStorageOptions(storageOptions)
            .build();

    // DIRECT_UPLOAD_ENABLED is bit 11
    long expectedLowBits = (1L) | (1L << 9) | (1L << 10) | (1L << 11);
    verifyHeader(options, 0L, expectedLowBits);
  }

  @Test
  public void getValue_withBidiEnabled() {
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.DEFAULT.toBuilder().setBidiEnabled(true).build();
    GoogleCloudStorageOptions storageOptions =
        GoogleCloudStorageOptions.DEFAULT.toBuilder().setReadChannelOptions(readOptions).build();
    GoogleCloudStorageFileSystemOptions options =
        GoogleCloudStorageFileSystemOptions.DEFAULT.toBuilder()
            .setCloudStorageOptions(storageOptions)
            .build();

    // BIDI_ENABLED is bit 12
    long expectedLowBits = (1L) | (1L << 9) | (1L << 10) | (1L << 12);
    verifyHeader(options, 0L, expectedLowBits);
  }

  @Test
  public void getValue_withTraceLogEnabled() {
    GoogleCloudStorageOptions storageOptions =
        GoogleCloudStorageOptions.DEFAULT.toBuilder().setTraceLogEnabled(true).build();
    GoogleCloudStorageFileSystemOptions options =
        GoogleCloudStorageFileSystemOptions.DEFAULT.toBuilder()
            .setCloudStorageOptions(storageOptions)
            .build();

    // TRACE_LOG_ENABLED is bit 7
    long expectedLowBits = (1L) | (1L << 7) | (1L << 9) | (1L << 10);
    verifyHeader(options, 0L, expectedLowBits);
  }

  @Test
  public void getValue_withOperationTraceLogEnabled() {
    GoogleCloudStorageOptions storageOptions =
        GoogleCloudStorageOptions.DEFAULT.toBuilder().setOperationTraceLogEnabled(true).build();
    GoogleCloudStorageFileSystemOptions options =
        GoogleCloudStorageFileSystemOptions.DEFAULT.toBuilder()
            .setCloudStorageOptions(storageOptions)
            .build();

    // OPERATION_TRACE_LOG_ENABLED is bit 8
    long expectedLowBits = (1L) | (1L << 8) | (1L << 9) | (1L << 10);
    verifyHeader(options, 0L, expectedLowBits);
  }

  private static GoogleCloudStorageFileSystemOptions buildOptionsWithFadvise(Fadvise fadvise) {
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.DEFAULT.toBuilder().setFadvise(fadvise).build();
    GoogleCloudStorageOptions storageOptions =
        GoogleCloudStorageOptions.DEFAULT.toBuilder().setReadChannelOptions(readOptions).build();
    return GoogleCloudStorageFileSystemOptions.DEFAULT.toBuilder()
        .setCloudStorageOptions(storageOptions)
        .build();
  }

  private static void verifyHeader(
      GoogleCloudStorageFileSystemOptions options, long expectedHigh, long expectedLow) {
    FeatureUsageHeader.setConfigFeatures(options);
    String headerValue = FeatureUsageHeader.getValue();
    assertThat(headerValue).isNotNull();
    byte[] decodedBytes = Base64.getDecoder().decode(headerValue);
    long[] actualFeatures = bytesToLongs(decodedBytes);
    assertThat(actualFeatures[FeatureUsageHeader.HIGH_BITS_INDEX]).isEqualTo(expectedHigh);
    assertThat(actualFeatures[FeatureUsageHeader.LOW_BITS_INDEX]).isEqualTo(expectedLow);
  }

  /** Converts a byte array (potentially trimmed) back into a 2-element long array. */
  private static long[] bytesToLongs(byte[] bytes) {
    byte[] fullArray = new byte[16];
    System.arraycopy(bytes, 0, fullArray, 16 - bytes.length, bytes.length);
    ByteBuffer buffer = ByteBuffer.wrap(fullArray);
    long high = buffer.getLong();
    long low = buffer.getLong();
    return new long[] {high, low};
  }
}
