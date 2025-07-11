package com.google.cloud.hadoop.gcsio;

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;

@AutoValue
/** Result of the complete vectoredIO read operation. */
public abstract class VectoredIOMetrics {

  public static Builder builder() {
    return new AutoValue_VectoredIOMetrics.Builder()
        .setReadBytes(null)
        .setReadDuration(null)
        .setClientInitializationDuration(null);
  }

  public abstract VectoredIOMetrics.Builder toBuilder();

  @Nullable
  /** Total bytes read across all ranges. */
  public abstract Integer getReadBytes();

  @Nullable
  /** Time taken to initialize the {@link com.google.cloud.storage.BlobReadSession} client. */
  public abstract Long getClientInitializationDuration();

  @Nullable
  /** Time taken to complete all the reads. */
  public abstract Long getReadDuration();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract VectoredIOMetrics.Builder setReadBytes(Integer readBytes);

    public abstract VectoredIOMetrics.Builder setClientInitializationDuration(
        Long clientInitializationDuration);

    public abstract VectoredIOMetrics.Builder setReadDuration(Long readDuration);

    abstract VectoredIOMetrics autoBuild();

    public VectoredIOMetrics build() {
      VectoredIOMetrics result = autoBuild();
      return result;
    }
  }
}
