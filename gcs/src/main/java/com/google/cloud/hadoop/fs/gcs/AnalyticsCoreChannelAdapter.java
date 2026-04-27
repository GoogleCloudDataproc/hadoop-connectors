/*
 * Copyright 2026 Google Inc.
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

import com.google.cloud.gcs.analyticscore.client.GcsObjectRange;
import com.google.cloud.gcs.analyticscore.core.GoogleCloudStorageInputStream;
import com.google.cloud.hadoop.util.GoogleCloudStorageEventBus;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntFunction;

/**
 * Adapter to expose {@link GoogleCloudStorageInputStream} from Analytics Core library as {@link
 * SeekableByteChannel} for use in {@link GoogleHadoopFSInputStream}.
 */
class AnalyticsCoreChannelAdapter implements SeekableByteChannel {

  private final long size;
  private final GoogleCloudStorageInputStream inputStream;
  private boolean open;

  AnalyticsCoreChannelAdapter(GoogleCloudStorageInputStream inputStream, long size) {
    this.inputStream = inputStream;
    this.size = size;
    this.open = true;
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    checkOpen();
    // If the buffer has no remaining space, return 0.
    if (!dst.hasRemaining()) {
      return 0;
    }

    // Read from the InputStream directly into the ByteBuffer.
    return inputStream.read(dst);
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    throw new NonWritableChannelException();
  }

  @Override
  public long position() throws IOException {
    checkOpen();
    return inputStream.getPos();
  }

  @Override
  public SeekableByteChannel position(long newPosition) throws IOException {
    checkOpen();
    if (newPosition < 0) {
      GoogleCloudStorageEventBus.postOnException();
      throw new EOFException(
          "Invalid seek offset: position value (" + newPosition + ") must be >= 0");
    }
    if (size >= 0 && newPosition >= size) {
      GoogleCloudStorageEventBus.postOnException();
      throw new EOFException(
          "Invalid seek offset: position value ("
              + newPosition
              + ") must be between 0 and "
              + size);
    }

    inputStream.seek(newPosition);
    return this;
  }

  @Override
  public long size() throws IOException {
    checkOpen();
    return size;
  }

  @Override
  public SeekableByteChannel truncate(long size) throws IOException {
    throw new NonWritableChannelException();
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public void close() throws IOException {
    if (open) {
      inputStream.close();
      open = false;
    }
  }

  public void readVectored(List<GcsObjectRange> ranges, IntFunction<ByteBuffer> allocate)
      throws IOException {
    checkOpen();
    List<GcsObjectRange> validRanges = new ArrayList<>();
    for (GcsObjectRange range : ranges) {
      if (range.getOffset() + range.getLength() > size) {
        range
            .getByteBufferFuture()
            .completeExceptionally(new IOException("Range extends beyond file size: " + range));
      } else {
        validRanges.add(range);
      }
    }
    if (!validRanges.isEmpty()) {
      inputStream.readVectored(validRanges, allocate);
    }
  }

  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    checkOpen();
    inputStream.readFully(position, buffer, offset, length);
  }

  private void checkOpen() throws ClosedChannelException {
    if (!open) {
      throw new ClosedChannelException();
    }
  }
}
