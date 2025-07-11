package com.google.cloud.hadoop.gcsio;

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;

@AutoValue
/** Result of the complete vectoredIO read operation. */
public abstract class VectoredIOResult {

  public static Builder builder() {
    return new AutoValue_VectoredIOResult.Builder()
        .setReadBytes(null)
        .setReadDuration(null)
        .setClientInitializationDuration(null);
  }

  public abstract VectoredIOResult.Builder toBuilder();

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

    public abstract VectoredIOResult.Builder setReadBytes(Integer readBytes);

    public abstract VectoredIOResult.Builder setClientInitializationDuration(
        Long clientInitializationDuration);

    public abstract VectoredIOResult.Builder setReadDuration(Long readDuration);

    abstract VectoredIOResult autoBuild();

    public VectoredIOResult build() {
      VectoredIOResult result = autoBuild();
      return result;
    }
  }
}
