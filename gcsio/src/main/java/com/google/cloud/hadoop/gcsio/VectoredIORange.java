/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
