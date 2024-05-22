/*
 * Copyright 2024 Google Inc.
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

package com.google.cloud.hadoop.gcsio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.List;
import java.util.function.IntFunction;

public interface GoogleCloudStorageReadableByteChannel extends SeekableByteChannel {

  @Override
  default SeekableByteChannel truncate(long size) throws IOException {
    throw new UnsupportedOperationException("Cannot mutate read-only channel");
  }

  @Override
  default int write(ByteBuffer src) throws IOException {
    throw new UnsupportedOperationException("Cannot mutate read-only channel");
  }

  /**
   * Read fully a list of file ranges asynchronously from this file. As a result of the call, each
   * range will have GoogleCloudStorageFileRange.setData(CompletableFuture) called with a future
   * that when complete will have a ByteBuffer with the data from the file's range.
   *
   * <p>The position returned by getPos() after readVectored() is undefined.
   *
   * <p>If a file is changed while the readVectored() operation is in progress, will fail the
   * request and throw IOException
   *
   * <p>While a readVectored() operation is in progress, normal read api calls may block.
   *
   * @param ranges the byte ranges to read
   * @param allocate the function to allocate ByteBuffer
   * @throws IOException any IOE.
   * @throws IllegalArgumentException if any of ranges are invalid, or they overlap.
   */
  default void readVectored(
      List<GoogleCloudStorageFileRange> ranges, IntFunction<ByteBuffer> allocate)
      throws IOException {
    throw new UnsupportedOperationException("readVectored is not supported yet");
  }
}
