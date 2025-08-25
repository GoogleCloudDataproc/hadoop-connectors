package com.google.cloud.hadoop.gcsio;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.Fadvise;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Unit tests for {@link FeatureUsageHeader}. This class contains non-parameterized tests for the
 * encoding logic. The parameterized tests for feature flag generation are in the inner class {@link
 * FeatureFlagGenerationTest}.
 */
@RunWith(JUnit4.class)
public class FeatureUsageHeaderTest {

  // --- Tests for the encode method ---

  @Test
  public void encode_withEmptyBitmask_returnsNull() {
    long[] features = new long[FeatureUsageHeader.BITMASK_SIZE];
    assertThat(FeatureUsageHeader.encode(features)).isNull();
  }

  @Test
  public void encode_withLowBitsSet_returnsCorrectString() {
    long[] features = new long[FeatureUsageHeader.BITMASK_SIZE];
    features[FeatureUsageHeader.LOW_BITS_INDEX] = 1L << 1 | 1L << 9; // 514
    assertThat(FeatureUsageHeader.encode(features)).isEqualTo("AgI=");
  }

  @Test
  public void encode_withHighBitsSet_returnsCorrectString() {
    long[] features = new long[FeatureUsageHeader.BITMASK_SIZE];
    features[FeatureUsageHeader.HIGH_BITS_INDEX] = 1L << 1; // Bit 65
    assertThat(FeatureUsageHeader.encode(features)).isEqualTo("AgAAAAAAAAAA");
  }

  @Test
  public void encode_withHighAndLowBitsSet_returnsCorrectString() {
    long[] features = new long[FeatureUsageHeader.BITMASK_SIZE];
    features[FeatureUsageHeader.HIGH_BITS_INDEX] = 1L; // Bit 64
    features[FeatureUsageHeader.LOW_BITS_INDEX] = 1L; // Bit 0
    assertThat(FeatureUsageHeader.encode(features)).isEqualTo("AQAAAAAAAAAB");
  }

  @Test
  public void encode_throwsOnInvalidBitmaskSize() {
    long[] features = new long[1];
    assertThrows(IllegalArgumentException.class, () -> FeatureUsageHeader.encode(features));
  }

  /** Parameterized tests for feature flag generation based on GCS options. */
  @RunWith(Parameterized.class)
  public static class FeatureFlagGenerationTest {

    private final String testName;
    private final GoogleCloudStorageFileSystemOptions options;
    private final long expectedHighBits;
    private final long expectedLowBits;

    public FeatureFlagGenerationTest(
        String testName,
        GoogleCloudStorageFileSystemOptions options,
        long expectedHighBits,
        long expectedLowBits) {
      this.testName = testName;
      this.options = options;
      this.expectedHighBits = expectedHighBits;
      this.expectedLowBits = expectedLowBits;
    }

    @Parameters(name = "{0}")
    public static Collection<Object[]> data() {
      final long defaultLowBits = (1L) | (1L << 9) | (1L << 10);

      return Arrays.asList(
          new Object[][] {
            {"Default Options", GoogleCloudStorageFileSystemOptions.DEFAULT, 0L, defaultLowBits},
            {
              "Fadvise Random",
              buildOptionsWithFadvise(Fadvise.RANDOM),
              0L,
              (defaultLowBits & ~(1L)) | (1L << 1)
            },
            {
              "Fadvise Sequential",
              buildOptionsWithFadvise(Fadvise.SEQUENTIAL),
              0L,
              (defaultLowBits & ~(1L)) | (1L << 2)
            },
            {
              "Copy With Rewrite Disabled",
              GoogleCloudStorageFileSystemOptions.DEFAULT.toBuilder()
                  .setCloudStorageOptions(
                      GoogleCloudStorageOptions.DEFAULT.toBuilder()
                          .setCopyWithRewriteEnabled(false)
                          .build())
                  .build(),
              0L,
              defaultLowBits & ~(1L << 10)
            },
            {
              "Status Parallel Disabled",
              GoogleCloudStorageFileSystemOptions.DEFAULT.toBuilder()
                  .setStatusParallelEnabled(false)
                  .build(),
              0L,
              defaultLowBits & ~(1L << 9)
            },
            {
              "Hierarchical Namespace",
              GoogleCloudStorageFileSystemOptions.DEFAULT.toBuilder()
                  .setCloudStorageOptions(
                      GoogleCloudStorageOptions.DEFAULT.toBuilder()
                          .setHnBucketRenameEnabled(true)
                          .build())
                  .build(),
              0L,
              defaultLowBits | (1L << 4)
            },
            {
              "Performance Cache",
              GoogleCloudStorageFileSystemOptions.DEFAULT.toBuilder()
                  .setPerformanceCacheEnabled(true)
                  .build(),
              0L,
              defaultLowBits | (1L << 5)
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
              0L,
              defaultLowBits | (1L << 11)
            },
            {"Bidi Enabled", buildOptionsWithBidi(true), 0L, defaultLowBits | (1L << 12)},
            {
              "Trace Log Enabled",
              GoogleCloudStorageFileSystemOptions.DEFAULT.toBuilder()
                  .setCloudStorageOptions(
                      GoogleCloudStorageOptions.DEFAULT.toBuilder()
                          .setTraceLogEnabled(true)
                          .build())
                  .build(),
              0L,
              defaultLowBits | (1L << 7)
            },
            {
              "Operation Trace Log Enabled",
              GoogleCloudStorageFileSystemOptions.DEFAULT.toBuilder()
                  .setCloudStorageOptions(
                      GoogleCloudStorageOptions.DEFAULT.toBuilder()
                          .setOperationTraceLogEnabled(true)
                          .build())
                  .build(),
              0L,
              defaultLowBits | (1L << 8)
            }
          });
    }

    @Test
    public void generatesCorrectHeaderValue() {
      verifyHeader(options, expectedHighBits, expectedLowBits);
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

  private static GoogleCloudStorageFileSystemOptions buildOptionsWithBidi(boolean enabled) {
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.DEFAULT.toBuilder().setBidiEnabled(enabled).build();
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

  private static long[] bytesToLongs(byte[] bytes) {
    byte[] fullArray = new byte[16];
    System.arraycopy(bytes, 0, fullArray, 16 - bytes.length, bytes.length);
    ByteBuffer buffer = ByteBuffer.wrap(fullArray);
    long high = buffer.getLong();
    long low = buffer.getLong();
    return new long[] {high, low};
  }
}
