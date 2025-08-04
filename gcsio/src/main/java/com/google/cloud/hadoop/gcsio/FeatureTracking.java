package com.google.cloud.hadoop.gcsio;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;

/**
 * A context for tracking features used in a request. This class uses a {@link ThreadLocal} to store
 * a bitmask of features that are active for the current request on the current thread.
 */
public final class FeatureTracking {

  // The bitmask is represented by a long array.
  // Index 0: high 64 bits, Index 1: low 64 bits.
  private static final int BITMASK_SIZE = 2;
  private static final int HIGH_BITS_INDEX = 0;
  private static final int LOW_BITS_INDEX = 1;

  private static final ThreadLocal<long[]> requestFeatures =
      ThreadLocal.withInitial(() -> new long[BITMASK_SIZE]);

  private FeatureTracking() {}

  /** A functional interface for a supplier that can throw an IOException. */
  @FunctionalInterface
  public interface SupplierWithIOException<T> {
    T get() throws IOException;
  }

  /** A functional interface for a runnable that can throw an IOException. */
  @FunctionalInterface
  public interface RunnableWithIOException {
    void run() throws IOException;
  }

  /**
   * Executes a block of code with a specific feature tracked. The feature bit is set before
   * execution and cleared in a finally block, ensuring it doesn't leak to other requests.
   */
  public static <T> T track(TrackedFeatures feature, SupplierWithIOException<T> supplier)
      throws IOException {
    set(feature);
    try {
      return supplier.get();
    } finally {
      clear(feature);
    }
  }

  /**
   * Executes a block of code with a specific feature tracked. The feature bit is set before
   * execution and cleared in a finally block, ensuring it doesn't leak to other requests.
   */
  public static void track(TrackedFeatures feature, RunnableWithIOException runnable)
      throws IOException {
    set(feature);
    try {
      runnable.run();
    } finally {
      clear(feature);
    }
  }

  /** Returns the current feature bitmask for the request-specific context. */
  public static long[] get() {
    return requestFeatures.get();
  }

  /** Sets a feature bit in the thread-local context. */
  private static void set(TrackedFeatures feature) {
    setBit(requestFeatures.get(), feature.getBitPosition());
  }

  /** Clears a feature bit in the thread-local context. */
  private static void clear(TrackedFeatures feature) {
    clearBit(requestFeatures.get(), feature.getBitPosition());
  }

  private static void setBit(long[] features, int bitPosition) {
    checkArgument(features.length == BITMASK_SIZE);
    if (bitPosition < 0 || bitPosition >= 128) {
      return;
    }
    if (bitPosition < 64) {
      features[LOW_BITS_INDEX] |= (1L << bitPosition);
    } else {
      features[HIGH_BITS_INDEX] |= (1L << (bitPosition - 64));
    }
  }

  private static void clearBit(long[] features, int bitPosition) {
    checkArgument(features.length == BITMASK_SIZE);
    if (bitPosition < 0 || bitPosition >= 128) {
      return;
    }
    if (bitPosition < 64) {
      features[LOW_BITS_INDEX] &= ~(1L << bitPosition);
    } else {
      features[HIGH_BITS_INDEX] &= ~(1L << (bitPosition - 64));
    }
  }
}
