package com.google.cloud.hadoop.gcsio;

import com.google.auto.value.AutoValue;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

@AutoValue
/** Range related input for vectored IO read operation. */
public abstract class VectoredIORange {

  public static Builder builder() {
    return new AutoValue_VectoredIORange.Builder().setData(new CompletableFuture<>());
  }

  public abstract VectoredIORange.Builder toBuilder();

  /** Offset from which we need to start reading. */
  public abstract long getOffset();

  /** Length of the range. */
  public abstract int getLength();

  /** Future containing the data for the given range. */
  public abstract CompletableFuture<ByteBuffer> getData();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract VectoredIORange.Builder setOffset(long offset);

    public abstract VectoredIORange.Builder setLength(int range);

    public abstract VectoredIORange.Builder setData(CompletableFuture<ByteBuffer> data);

    abstract VectoredIORange autoBuild();

    public VectoredIORange build() {
      VectoredIORange range = autoBuild();
      return range;
    }
  }
}
