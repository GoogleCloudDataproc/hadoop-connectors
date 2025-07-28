package com.google.cloud.hadoop.gcsio;

/**
 * Features that can be tracked in the GCS connector. The integer value represents the bit position
 * in a bitmask.
 */
public enum TrackedFeatures {
  FADVISE_AUTO(1),
  FADVISE_RANDOM(2),
  FADVISE_SEQUENTIAL(3);

  private final int bitPosition;

  TrackedFeatures(int bitPosition) {
    this.bitPosition = bitPosition;
  }

  public int getBitPosition() {
    return bitPosition;
  }
}
