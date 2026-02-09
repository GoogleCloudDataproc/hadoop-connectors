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

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.Fadvise;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;
import java.util.BitSet;

/**
 * Generates the x-goog-storage-hadoop-connector-features header value by combining
 * configuration-derived and request-specific features.
 */
public class FeatureHeaderGenerator {

  /**
   * The size of the bitmask in bits. This was chosen to allow tracking all current connector
   * features with enough room for future expansion.
   */
  @VisibleForTesting static final int BITMASK_SIZE = 128;

  @VisibleForTesting
  static final InheritableThreadLocal<BitSet> requestFeatures =
      new InheritableThreadLocal<>() {
        @Override
        protected BitSet initialValue() {
          return new BitSet(BITMASK_SIZE);
        }
      };

  public static final String HEADER_NAME = "X-Goog-Storage-Hadoop-Connector-Features";
  private final BitSet configFeatures;

  @FunctionalInterface
  public interface IOCallable<V> {
    V call() throws IOException;
  }

  public FeatureHeaderGenerator(GoogleCloudStorageFileSystemOptions options) {
    this.configFeatures = new BitSet(BITMASK_SIZE);
    populateBitMask(this.configFeatures, options);
  }

  /**
   * Generates the value for the x-goog-storage-hadoop-connector-features header.
   *
   * @return The Base64 encoded string for the header, or {@code null} if no features are set.
   */
  public String getValue() {
    BitSet features = (BitSet) configFeatures.clone();
    features.or(requestFeatures.get());
    return encode(features);
  }

  /**
   * Executes a block of code with a specific feature tracked. The feature bit is set before
   * execution and cleared in a finally block, ensuring it doesn't leak to other requests.
   */
  public static <B> B track(TrackedFeatures feature, IOCallable<B> operation) throws IOException {
    requestFeatures.get().set(feature.getBitPosition());
    try {
      return operation.call();
    } finally {
      requestFeatures.get().clear(feature.getBitPosition());
    }
  }

  /** Populates the bitmask with features derived from connector-level options. */
  private void populateBitMask(BitSet features, GoogleCloudStorageFileSystemOptions fsOptions) {
    GoogleCloudStorageOptions storageOptions = fsOptions.getCloudStorageOptions();
    // Fadvise options
    Fadvise fadvise = storageOptions.getReadChannelOptions().getFadvise();
    if (fadvise != null) {
      switch (fadvise) {
        case AUTO:
          features.set(TrackedFeatures.FADVISE_AUTO.getBitPosition());
          break;
        case RANDOM:
          features.set(TrackedFeatures.FADVISE_RANDOM.getBitPosition());
          break;
        case SEQUENTIAL:
          features.set(TrackedFeatures.FADVISE_SEQUENTIAL.getBitPosition());
          break;
        case AUTO_RANDOM:
          features.set(TrackedFeatures.FADVISE_AUTORANDOM.getBitPosition());
          break;
      }
    }

    // Hierarchical Namespace
    if (storageOptions.isHnBucketRenameEnabled()) {
      features.set(TrackedFeatures.HIERARCHICAL_NAMESPACE_ENABLED.getBitPosition());
    }
    if (storageOptions.isHnOptimizationEnabled()) {
      features.set(TrackedFeatures.HNS_OPTIMIZATIONS_ENABLED.getBitPosition());
    }

    // Trace Logging
    if (storageOptions.isTraceLogEnabled()) {
      features.set(TrackedFeatures.TRACE_LOG_ENABLED.getBitPosition());
    }
    if (storageOptions.isOperationTraceLogEnabled()) {
      features.set(TrackedFeatures.OPERATION_TRACE_LOG_ENABLED.getBitPosition());
    }

    // Direct Upload
    if (storageOptions.getWriteChannelOptions().isDirectUploadEnabled()) {
      features.set(TrackedFeatures.DIRECT_UPLOAD_ENABLED.getBitPosition());
    }

    // Bidirectional Support
    if (storageOptions.isBidiEnabled()) {
      features.set(TrackedFeatures.BIDI_ENABLED.getBitPosition());
    }

    // Performance Cache
    if (fsOptions.isPerformanceCacheEnabled()) {
      features.set(TrackedFeatures.PERFORMANCE_CACHE_ENABLED.getBitPosition());
    }

    // Cloud Logging
    if (fsOptions.isCloudLoggingEnabled()) {
      features.set(TrackedFeatures.CLOUD_LOGGING_ENABLED.getBitPosition());
    }
  }

  /**
   * Base64-encodes the 128-bit feature mask into a string, trimming leading zero bytes. Returns
   * {@code null} if the bitmask is zero.
   */
  @VisibleForTesting
  static String encode(BitSet features) {
    if (features.isEmpty()) {
      return null;
    }
    long[] longArray = features.toLongArray();
    // BitSet.toLongArray() is little-endian (longs[0] has bits 0-63).
    // The original implementation was big-endian (high bits first).
    // So we need to pad and reverse the long array to match the 128-bit big-endian representation.
    long[] bigEndianLongs = new long[BITMASK_SIZE / 64]; // 2 longs
    for (int i = 0; i < longArray.length; i++) {
      bigEndianLongs[bigEndianLongs.length - 1 - i] = longArray[i];
    }
    long highBits = bigEndianLongs[0];
    long lowBits = bigEndianLongs[1];

    if (highBits == 0L && lowBits == 0L) {
      return null;
    }

    ByteBuffer buffer = ByteBuffer.allocate(BITMASK_SIZE / 8); // 16 bytes
    buffer.putLong(highBits);
    buffer.putLong(lowBits);
    byte[] fullArray = buffer.array();

    int firstNonZeroByte = 0;
    while (firstNonZeroByte < fullArray.length && fullArray[firstNonZeroByte] == 0) {
      firstNonZeroByte++;
    }

    byte[] trimmedArray = Arrays.copyOfRange(fullArray, firstNonZeroByte, fullArray.length);
    return Base64.getEncoder().encodeToString(trimmedArray);
  }
}
