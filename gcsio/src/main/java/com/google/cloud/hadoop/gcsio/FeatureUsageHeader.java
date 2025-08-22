package com.google.cloud.hadoop.gcsio;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.Fadvise;
import com.google.common.annotations.VisibleForTesting;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Generates the x-goog-storage-hadoop-connector-features header value by combining
 * configuration-derived and request-specific features.
 */
public final class FeatureUsageHeader {

  @VisibleForTesting static final int BITMASK_SIZE = 2;
  @VisibleForTesting static final int HIGH_BITS_INDEX = 0;
  @VisibleForTesting static final int LOW_BITS_INDEX = 1;

  // Cache for the configuration-derived part of the feature bitmask.
  private static final ConcurrentHashMap<GoogleCloudStorageFileSystemOptions, long[]>
      configFeaturesCache = new ConcurrentHashMap<>();
  private static long[] configFeaturesMask = new long[BITMASK_SIZE];
  public static final String NAME = "X-Goog-Storage-Hadoop-Connector-Features";

  private FeatureUsageHeader() {}

  /**
   * Sets the configuration-derived features based on the provided {@link
   * GoogleCloudStorageFileSystemOptions}. This method caches the computed feature mask to avoid
   * redundant calculations for the same options.
   */
  public static void setConfigFeatures(GoogleCloudStorageFileSystemOptions fsOptions) {
    configFeaturesMask =
        configFeaturesCache.computeIfAbsent(
            fsOptions,
            opts -> {
              long[] features = new long[BITMASK_SIZE];
              addConfigurationFeatures(features, opts);
              return features;
            });
  }

  /**
   * Generates the value for the x-goog-storage-hadoop-connector-features header.
   *
   * @return The Base64 encoded string for the header, or {@code null} if no features are set.
   */
  public static String getValue() {
    return encode(configFeaturesMask);
  }

  /**
   * Populates the bitmask with features derived from {@link GoogleCloudStorageFileSystemOptions}.
   */
  private static void addConfigurationFeatures(
      long[] features, GoogleCloudStorageFileSystemOptions fsOptions) {
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

    // Copy with Rewrite
    if (storageOptions.isCopyWithRewriteEnabled()) {
      setBit(features, TrackedFeatures.GCS_COPY_WITH_REWRITE_ENABLED.getBitPosition());
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

    //    // Cloud Logging
    //    if (fsOptions.isCloudLoggingEnabled()) {
    //      setBit(features, TrackedFeatures.CLOUD_LOGGING_ENABLED.getBitPosition());
    //    }

    // GCS Status Parallel
    if (fsOptions.isStatusParallelEnabled()) {
      setBit(features, TrackedFeatures.GCS_STATUS_PARALLEL_ENABLED.getBitPosition());
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
