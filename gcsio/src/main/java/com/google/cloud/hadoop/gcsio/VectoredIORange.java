package com.google.cloud.hadoop.gcsio;

import com.google.auto.value.AutoValue;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;

@AutoValue
public abstract class VectoredIORange {

  public static Builder builder() {
    return new AutoValue_VectoredIORange.Builder().setData(null);
  }

  public abstract VectoredIORange.Builder toBuilder();

  public abstract long getOffset();

  public abstract int getLength();

  @Nullable
  public abstract CompletableFuture<ByteBuffer> getData();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract VectoredIORange.Builder setOffset(long offset);

    public abstract VectoredIORange.Builder setLength(int length);

    public abstract VectoredIORange.Builder setData(CompletableFuture<ByteBuffer> data);

    abstract VectoredIORange autoBuild();

    public VectoredIORange build() {
      VectoredIORange range = autoBuild();
      return range;
    }
  }
}
