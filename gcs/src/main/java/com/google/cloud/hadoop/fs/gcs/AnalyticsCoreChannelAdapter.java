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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.google.cloud.gcs.analyticscore.client.GcsObjectRange;
import com.google.cloud.gcs.analyticscore.core.GoogleCloudStorageInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.SeekableByteChannel;
import java.util.List;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FileRange;

/**
 * Adapter to expose {@link InputStream} from Analytics Core library as {@link SeekableByteChannel}
 * for use in {@link GoogleHadoopFSInputStream}.
 */
class AnalyticsCoreChannelAdapter implements SeekableByteChannel {

  private final long size;

  private GoogleCloudStorageInputStream inputStream;
  private boolean open;

  AnalyticsCoreChannelAdapter(GoogleCloudStorageInputStream inputStream, long size) {
    this.inputStream = inputStream;
    this.size = size;
    this.open = true;
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    checkState(open, "Channel is closed");
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
    checkState(open, "Channel is closed");
    return inputStream.getPos();
  }

  @Override
  public SeekableByteChannel position(long newPosition) throws IOException {
    checkState(open, "Channel is closed");
    checkArgument(newPosition >= 0 && newPosition < size, "Invalid position: %s", newPosition);

    inputStream.seek(newPosition);
    return this;
  }

  @Override
  public long size() throws IOException {
    checkState(open, "Channel is closed");
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

  public void readVectored(List<? extends FileRange> ranges, IntFunction<ByteBuffer> allocate)
      throws IOException {
    checkState(open, "Channel is closed");
    List<GcsObjectRange> objectRanges =
        ranges.stream()
            .map(
                range ->
                    GcsObjectRange.builder()
                        .setOffset(range.getOffset())
                        .setLength(range.getLength())
                        .setByteBufferFuture(range.getData())
                        .build())
            .collect(Collectors.toList());

    inputStream.readVectored(objectRanges, allocate);
  }

  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    checkState(open, "Channel is closed");
    inputStream.readFully(position, buffer, offset, length);
  }

  public int readTail(byte[] buffer, int offset, int length) throws IOException {
    checkState(open, "Channel is closed");
    return inputStream.readTail(buffer, offset, length);
  }
}
