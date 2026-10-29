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

import static com.google.common.base.Preconditions.checkNotNull;

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
 *
 * <p>TODO(user): Wrap exception-throwing calls to post exceptions to GoogleCloudStorageEventBus on
 * failure when adding comprehensive logging.
 */
class GcsAnalyticsCoreInputStreamWrapper implements SeekableByteChannel {

  @FunctionalInterface
  interface SizeProvider {
    long getSize() throws IOException;
  }

  private static final long UNKNOWN_SIZE = -1;

  private final GoogleCloudStorageInputStream inputStream;
  private final SizeProvider sizeProvider;
  private volatile long size;
  private volatile boolean channelIsOpen;

  GcsAnalyticsCoreInputStreamWrapper(GoogleCloudStorageInputStream inputStream, long size) {
    this(inputStream, size, () -> size);
  }

  /** Size is resolved through {@code sizeProvider} only when {@link #size()} is called. */
  GcsAnalyticsCoreInputStreamWrapper(
      GoogleCloudStorageInputStream inputStream, SizeProvider sizeProvider) {
    this(inputStream, UNKNOWN_SIZE, sizeProvider);
  }

  private GcsAnalyticsCoreInputStreamWrapper(
      GoogleCloudStorageInputStream inputStream, long size, SizeProvider sizeProvider) {
    this.inputStream = checkNotNull(inputStream, "inputStream must not be null");
    this.sizeProvider = checkNotNull(sizeProvider, "sizeProvider must not be null");
    this.size = size;
    this.channelIsOpen = true;
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
    // Throwing EOFException aligns with the Hadoop spec for seek >= len(data).
    // See:
    // https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/filesystem/fsdatainputstream.html#Seekable.seek.28s.29
    // Skipped while the size is unknown: seeking must not trigger a metadata request.
    long knownSize = size;
    if (knownSize >= 0 && newPosition >= knownSize) {
      GoogleCloudStorageEventBus.postOnException();
      throw new EOFException(
          "Invalid seek offset: position value ("
              + newPosition
              + ") must be between 0 and "
              + knownSize);
    }

    inputStream.seek(newPosition);
    return this;
  }

  @Override
  public long size() throws IOException {
    checkOpen();
    long resolvedSize = size;
    if (resolvedSize < 0) {
      synchronized (this) {
        resolvedSize = size;
        if (resolvedSize < 0) {
          resolvedSize = sizeProvider.getSize();
          size = resolvedSize;
        }
      }
    }
    return resolvedSize;
  }

  @Override
  public SeekableByteChannel truncate(long size) throws IOException {
    throw new NonWritableChannelException();
  }

  @Override
  public boolean isOpen() {
    return channelIsOpen;
  }

  @Override
  public void close() throws IOException {
    if (channelIsOpen) {
      inputStream.close();
      channelIsOpen = false;
    }
  }

  public void readVectored(List<GcsObjectRange> ranges, IntFunction<ByteBuffer> allocate)
      throws IOException {
    checkOpen();
    long knownSize = size;
    List<GcsObjectRange> validRanges = new ArrayList<>();
    for (GcsObjectRange range : ranges) {
      if (knownSize >= 0 && range.getOffset() + range.getLength() > knownSize) {
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
    if (!channelIsOpen) {
      throw new ClosedChannelException();
    }
  }
}
