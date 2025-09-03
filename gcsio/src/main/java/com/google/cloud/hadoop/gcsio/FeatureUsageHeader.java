package com.google.cloud.hadoop.gcsio;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.Fadvise;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;
import org.apache.hadoop.util.functional.CallableRaisingIOE;

/**
 * Generates the x-goog-storage-hadoop-connector-features header value by combining
 * configuration-derived and request-specific features.
 */
public class FeatureUsageHeader {

  @VisibleForTesting static final int BITMASK_SIZE = 2;
  @VisibleForTesting static final int HIGH_BITS_INDEX = 0;
  @VisibleForTesting static final int LOW_BITS_INDEX = 1;

  @VisibleForTesting
  static final InheritableThreadLocal<long[]> requestFeatures =
      new InheritableThreadLocal<>() {
        @Override
        protected long[] initialValue() {
          return new long[BITMASK_SIZE];
        }
      };

  public static final String NAME = "X-Goog-Storage-Hadoop-Connector-Features";
  private final long[] configFeatures;

  public FeatureUsageHeader(GoogleCloudStorageFileSystemOptions options) {
    this.configFeatures = new long[BITMASK_SIZE];
    populateBitMask(configFeatures, options);
  }

  /**
   * Generates the value for the x-goog-storage-hadoop-connector-features header.
   *
   * @return The Base64 encoded string for the header, or {@code null} if no features are set.
   */
  public String getValue() {
    long[] features = new long[BITMASK_SIZE];
    features[HIGH_BITS_INDEX] =
        configFeatures[HIGH_BITS_INDEX] | requestFeatures.get()[HIGH_BITS_INDEX];
    features[LOW_BITS_INDEX] =
        configFeatures[LOW_BITS_INDEX] | requestFeatures.get()[LOW_BITS_INDEX];
    return encode(features);
  }

  /**
   * Executes a block of code with a specific feature tracked. The feature bit is set before
   * execution and cleared in a finally block, ensuring it doesn't leak to other requests.
   */
  public static <B> B track(TrackedFeatures feature, CallableRaisingIOE<B> operation)
      throws IOException {
    setBit(requestFeatures.get(), feature.getBitPosition());
    try {
      return operation.apply();
    } finally {
      clearBit(requestFeatures.get(), feature.getBitPosition());
    }
  }

  /** Populates the bitmask with features derived from connector-level options. */
  private void populateBitMask(long[] features, GoogleCloudStorageFileSystemOptions fsOptions) {
    GoogleCloudStorageOptions storageOptions = fsOptions.getCloudStorageOptions();
    // Fadvise options
    Fadvise fadvise = storageOptions.getReadChannelOptions().getFadvise();
    if (fadvise != null) {
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

    // Hierarchical Namespace
    if (storageOptions.isHnBucketRenameEnabled()) {
      setBit(features, TrackedFeatures.HIERARCHICAL_NAMESPACE_ENABLED.getBitPosition());
    }

    // Trace Logging
    if (storageOptions.isTraceLogEnabled()) {
      setBit(features, TrackedFeatures.TRACE_LOG_ENABLED.getBitPosition());
    }
    if (storageOptions.isOperationTraceLogEnabled()) {
      setBit(features, TrackedFeatures.OPERATION_TRACE_LOG_ENABLED.getBitPosition());
    }

    // Direct Upload
    if (storageOptions.getWriteChannelOptions().isDirectUploadEnabled()) {
      setBit(features, TrackedFeatures.DIRECT_UPLOAD_ENABLED.getBitPosition());
    }

    // Bidirectional Support
    if (storageOptions.getReadChannelOptions().isBidiEnabled()) {
      setBit(features, TrackedFeatures.BIDI_ENABLED.getBitPosition());
    }

    // Performance Cache
    if (fsOptions.isPerformanceCacheEnabled()) {
      setBit(features, TrackedFeatures.PERFORMANCE_CACHE_ENABLED.getBitPosition());
    }

    // Cloud Logging
    if (fsOptions.isCloudLoggingEnabled()) {
      setBit(features, TrackedFeatures.CLOUD_LOGGING_ENABLED.getBitPosition());
    }
  }

  /**
   * Base64-encodes the 128-bit feature mask into a string, trimming leading zero bytes. Returns
   * {@code null} if the bitmask is zero.
   */
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

  /** Sets a specific bit in the 128-bit feature mask. */
  private static void setBit(long[] features, int bitPosition) {
    checkArgument(features.length == BITMASK_SIZE, "Bitmask must be 128 bits (2 longs).");
    checkArgument(
        bitPosition >= 0 && bitPosition < 128, "Bit position must be in the range [0, 127].");
    if (bitPosition < 64) {
      features[LOW_BITS_INDEX] |= (1L << bitPosition);
    } else {
      features[HIGH_BITS_INDEX] |= (1L << (bitPosition - 64));
    }
  }

  /** Clears a specific bit in the 128-bit feature mask. */
  private static void clearBit(long[] features, int bitPosition) {
    checkArgument(features.length == BITMASK_SIZE);
    checkArgument(
        bitPosition >= 0 && bitPosition < 128, "Bit position must be in the range [0, 127].");
    if (bitPosition < 64) {
      features[LOW_BITS_INDEX] &= ~(1L << bitPosition);
    } else {
      features[HIGH_BITS_INDEX] &= ~(1L << (bitPosition - 64));
    }
  }
}
