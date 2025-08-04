package com.google.cloud.hadoop.gcsio;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.Fadvise;
import com.google.common.annotations.VisibleForTesting;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;

/**
 * Helper class for generating client-side metrics, such as the x-goog-storage-client-features
 * header.
 */
public final class ClientSideMetrics {

  // The bitmask is represented by a long array.
  // Index 0: high 64 bits, Index 1: low 64 bits.
  @VisibleForTesting static final int BITMASK_SIZE = 2;
  @VisibleForTesting static final int HIGH_BITS_INDEX = 0;
  @VisibleForTesting static final int LOW_BITS_INDEX = 1;

  private ClientSideMetrics() {}

  /**
   * Generates the value for the x-goog-storage-client-features header by combining
   * configuration-derived and request-specific features.
   *
   * @param options The GCS options containing configuration-derived features.
   * @return The Base64 encoded string representation of the feature bitmask, or {@code null} if no
   *     features are set.
   */
  public static String getClientFeaturesValue(GoogleCloudStorageOptions options) {
    long[] features = new long[BITMASK_SIZE];

    // 1. Get configuration-derived features.
    addConfigurationFeatures(features, options);

    // 2. Get request-specific features from the ThreadLocal context and merge them.
    long[] requestFeatures = FeatureTracking.get();
    features[HIGH_BITS_INDEX] |= requestFeatures[HIGH_BITS_INDEX];
    features[LOW_BITS_INDEX] |= requestFeatures[LOW_BITS_INDEX];

    // 3. Encode the combined bitmask.
    return encode(features);
  }

  private static void addConfigurationFeatures(long[] features, GoogleCloudStorageOptions options) {
    Fadvise fadvise = options.getReadChannelOptions().getFadvise();
    if (fadvise == null) {
      return;
    }

    switch (fadvise) {
      case AUTO:
        setBit(features, TrackedFeatures.FADVISE_AUTO.getBitPosition());
        break;
      case RANDOM:
        setBit(features, TrackedFeatures.FADVISE_RANDOM.getBitPosition());
        break;
      case SEQUENTIAL:
        setBit(features, TrackedFeatures.FADVISE_SEQUENTIAL.getBitPosition());
        break;
    }
  }

  @VisibleForTesting
  static String encode(long[] features) {
    checkArgument(features.length == BITMASK_SIZE, "Bitmask must be 128 bits (2 longs).");
    long highBits = features[HIGH_BITS_INDEX];
    long lowBits = features[LOW_BITS_INDEX];

    if (highBits == 0L && lowBits == 0L) {
      return null;
    }

    ByteBuffer buffer = ByteBuffer.allocate(16);
    buffer.putLong(highBits);
    buffer.putLong(lowBits);
    byte[] fullArray = buffer.array();

    int firstNonZeroByte =
        (highBits != 0L)
            ? Long.numberOfLeadingZeros(highBits) / 8
            : 8 + (Long.numberOfLeadingZeros(lowBits) / 8);

    byte[] trimmedArray = Arrays.copyOfRange(fullArray, firstNonZeroByte, fullArray.length);

    return Base64.getEncoder().encodeToString(trimmedArray);
  }

  private static void setBit(long[] features, int bitPosition) {
    checkArgument(features.length == BITMASK_SIZE, "Bitmask must be 128 bits (2 longs).");
    if (bitPosition < 0 || bitPosition >= 128) {
      return;
    }
    if (bitPosition < 64) {
      features[LOW_BITS_INDEX] |= (1L << bitPosition);
    } else {
      features[HIGH_BITS_INDEX] |= (1L << (bitPosition - 64));
    }
  }
}
