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

package com.google.cloud.hadoop.fs.gcs;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.fs.FileRange;

class CustomFileRange implements FileRange {
  long offset;
  int length;

  CompletableFuture<ByteBuffer> data;

  CustomFileRange(long offset, int length) {
    this.offset = offset;
    this.length = length;
  }

  @Override
  public long getOffset() {
    return offset;
  }

  @Override
  public int getLength() {
    return length;
  }

  @Override
  public CompletableFuture<ByteBuffer> getData() {
    return data;
  }

  @Override
  public void setData(CompletableFuture<ByteBuffer> completableFuture) {
    data = completableFuture;
  }

  @Override
  public Object getReference() {
    return null;
  }
}
