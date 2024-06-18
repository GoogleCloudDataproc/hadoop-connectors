/*
 * Copyright 2024 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.fs.gcs;

import com.google.auto.value.AutoValue;

@AutoValue
public abstract class VectoredReadOptions {

  public static VectoredReadOptions DEFAULT = builder().build();

  public static VectoredReadOptions.Builder builder() {
    return new AutoValue_VectoredReadOptions.Builder()
        .setMinSeekVectoredReadSize(4 * 1024) // 4KB
        .setMergeRangeMaxSize(8 * 1024 * 1024) // 8MB
        .setReadThreads(16);
  }

  public abstract int getMinSeekVectoredReadSize();

  public abstract int getMergeRangeMaxSize();

  public abstract int getReadThreads();

  public abstract VectoredReadOptions.Builder toBuilder();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract VectoredReadOptions build();

    public abstract Builder setMinSeekVectoredReadSize(int minSeekVectoredReadSize);

    public abstract Builder setMergeRangeMaxSize(int mergedRangeMazSize);

    public abstract Builder setReadThreads(int readThreads);
  }
}
