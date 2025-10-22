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

import static com.google.api.client.util.Preconditions.checkArgument;
import static com.google.api.client.util.Preconditions.checkNotNull;
import static com.google.common.base.Strings.nullToEmpty;
import static java.lang.Math.max;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.hadoop.util.GoogleCloudStorageEventBus;
import com.google.cloud.storage.*;
import com.google.cloud.storage.ZeroCopySupport.DisposableByteString;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.flogger.GoogleLogger;
import com.google.protobuf.ByteString;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.IntFunction;
import javax.annotation.Nullable;

public final class GoogleCloudStorageBidiReadChannel implements ReadVectoredSeekableByteChannel {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();
  private static final int EOF = -1;
  private final StorageResourceId resourceId;
  private final BlobId blobId;
  private final ApiFuture<BlobReadSession> sessionFuture;
  private volatile BlobReadSession blobReadSession;
  private final ExecutorService boundedThreadPool;
  private static final String GZIP_ENCODING = "gzip";
  private long objectSize;
  private boolean open = true;
  private boolean gzipEncoded = false;
  private final Duration readTimeout;
  private long position = 0;
  private final GoogleCloudStorageReadOptions readOptions;
  private byte[] footerContent;
  private ByteBuffer internalBuffer;
  private long bufferStartPosition = -1;

  public GoogleCloudStorageBidiReadChannel(
      Storage storage,
      GoogleCloudStorageItemInfo itemInfo,
      GoogleCloudStorageReadOptions readOptions,
      ExecutorService boundedThreadPool)
      throws IOException {
    validate(itemInfo);
    // TODO(dhritichopra) Remove grpcReadTimeout if redundant and rename to bidiReadTimeout.
    this.readTimeout = readOptions.getGrpcReadTimeout();
    this.resourceId =
        new StorageResourceId(
            itemInfo.getBucketName(), itemInfo.getObjectName(), itemInfo.getContentGeneration());
    this.blobId =
        BlobId.of(
            resourceId.getBucketName(), resourceId.getObjectName(), resourceId.getGenerationId());
    this.readOptions = readOptions;
    this.sessionFuture = storage.blobReadSession(blobId);
    this.blobReadSession = null;
    this.boundedThreadPool = boundedThreadPool;
    initMetadata(itemInfo.getContentEncoding(), itemInfo.getSize());
  }

  private synchronized BlobReadSession getBlobReadSession() throws IOException {
    if (blobReadSession == null) {
      BlobReadSession readSession = null;
      try {
        readSession = sessionFuture.get(readOptions.getBidiClientTimeout(), TimeUnit.SECONDS);
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        GoogleCloudStorageEventBus.postOnException();
        throw new IOException("Failed to get BlobReadSession", e);
      }
      this.blobReadSession = readSession;
    }
    return blobReadSession;
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    throwIfNotOpen();
    if (!dst.hasRemaining()) {
      return 0;
    }

    if (position >= objectSize) {
      return EOF;
    }

    // Attempt to service the read from the footer cache logic.
    // This method will handle both reading from an existing cache and populating it if necessary.
    // It returns an Optional<Integer>:
    //  - If present, the read was handled by the cache, and the value is the bytes read.
    //  - If empty, the read is not in the footer range, and a standard read should be performed.
    Optional<Integer> bytesReadFromCache = tryReadFromFooter(dst);
    if (bytesReadFromCache.isPresent()) {
      return bytesReadFromCache.get();
    }

    if (isBufferValid()) return readBytesFromInternalBuffer(dst);

    if (dst.remaining() >= readOptions.getMinRangeRequestSize()) {
      return performStandardRead(dst);
    }

    // For small requests, refill the buffer using the same consistent chunk size.
    refillInternalBuffer();

    // Tried to refill buffer but buffer is still invalid, it means there was no data left to get.
    // Therefore, we have reached the end of the file and must return -1.
    if (!isBufferValid()) return -1;

    return readBytesFromInternalBuffer(dst);
  }

  public long position() throws IOException {
    return position;
  }

  @Override
  public SeekableByteChannel position(long newPosition) throws IOException {
    throwIfNotOpen();
    validatePosition(newPosition);
    if (newPosition == this.position) {
      return this;
    }

    logger.atFinest().log(
        "Seek from %d to %d position for '%s'", this.position, newPosition, resourceId);

    this.position = newPosition;

    if (isBufferValid()) {
      internalBuffer.position((int) (newPosition - bufferStartPosition));
    } else {
      invalidateBuffer();
    }

    return this;
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    GoogleCloudStorageEventBus.postOnException();
    throw new UnsupportedOperationException("Cannot mutate read-only channel");
  }

  @Override
  public long size() throws IOException {
    if (objectSize == -1) {
      GoogleCloudStorageEventBus.postOnException();
      throw new IOException("Size of file is not available");
    }
    return objectSize;
  }

  @Override
  public SeekableByteChannel truncate(long size) throws IOException {
    GoogleCloudStorageEventBus.postOnException();
    throw new UnsupportedOperationException("Cannot truncate a read-only channel");
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public void close() throws IOException {
    if (open) {
      logger.atFinest().log("Closing channel for '%s'", resourceId);
      try {
        if (blobReadSession != null) {
          blobReadSession.close();
        } else if (sessionFuture != null) {
          try (BlobReadSession readSession =
              sessionFuture.get(readOptions.getBidiClientTimeout(), TimeUnit.SECONDS)) {
            // The try-with-resources statement ensures the readSession is automatically closed.
          } catch (InterruptedException
              | ExecutionException
              | TimeoutException
              | java.util.concurrent.CancellationException e) {
            logger.atFine().withCause(e).log(
                "Failed to get/close BlobReadSession during close() for '%s'", resourceId);
          }
        }
      } catch (Exception e) {
        GoogleCloudStorageEventBus.postOnException();
        throw new IOException(
            String.format("Exception occurred while closing channel '%s'", resourceId), e);
      } finally {
        blobReadSession = null;
        open = false;
      }
    }
  }

  @Override
  public void readVectored(List<VectoredIORange> ranges, IntFunction<ByteBuffer> allocate)
      throws IOException {
    logger.atFiner().log("readVectored() called for BlobId=%s", blobId.toString());
    long vectoredReadStartTime = System.currentTimeMillis();
    BlobReadSession session = getBlobReadSession();
    ranges.forEach(
        range -> {
          ApiFuture<DisposableByteString> futureBytes =
              session.readAs(
                  ReadProjectionConfigs.asFutureByteString()
                      .withRangeSpec(RangeSpec.of(range.getOffset(), range.getLength())));
          ApiFutures.addCallback(
              futureBytes,
              new ApiFutureCallback<>() {
                @Override
                public void onFailure(Throwable t) {
                  range.getData().completeExceptionally(t);
                  logger.atFiner().log(
                      "Vectored Read failed for range starting from %d with length %d",
                      range.getOffset(), range.getLength());
                }

                @Override
                public void onSuccess(DisposableByteString disposableByteString) {
                  try {
                    long bytesRead =
                        processBytesAndCompleteRange(disposableByteString, range, allocate);
                    logger.atFiner().log(
                        "Vectored Read successful for range starting from %d with length %d.Total Bytes Read are: %d within %d ms",
                        range.getOffset(),
                        range.getLength(),
                        bytesRead,
                        System.currentTimeMillis() - vectoredReadStartTime);
                  } catch (Throwable t) {
                    range.getData().completeExceptionally(t);
                    logger.atFiner().log(
                        "Vectored Read failed for range starting from %d with length %d",
                        range.getOffset(), range.getLength());
                  }
                }
              },
              boundedThreadPool);
        });
  }

  private long processBytesAndCompleteRange(
      DisposableByteString disposableByteString,
      VectoredIORange range,
      IntFunction<ByteBuffer> allocate)
      throws IOException {
    long bytesRead = 0;
    // try-with-resources ensures the DisposableByteString is closed, releasing its memory
    try (DisposableByteString dbs = disposableByteString) {
      ByteString byteString = dbs.byteString();
      int size = byteString.size();
      bytesRead += size;
      ByteBuffer buf = allocate.apply(size);
      for (ByteBuffer b : byteString.asReadOnlyByteBufferList()) {
        buf.put(b);
      }
      buf.flip();
      range.getData().complete(buf);
    }
    return bytesRead;
  }

  private void throwIfNotOpen() throws IOException {
    if (!isOpen()) {
      GoogleCloudStorageEventBus.postOnException();
      throw new ClosedChannelException();
    }
  }

  protected void initMetadata(@Nullable String encoding, long sizeFromMetadata)
      throws UnsupportedOperationException {
    gzipEncoded = nullToEmpty(encoding).contains(GZIP_ENCODING);
    // TODO(dhritichopra) Add Support for GZIP Encoding
    if (gzipEncoded) {
      GoogleCloudStorageEventBus.postOnException();
      throw new UnsupportedOperationException("Gzip Encoded Files are not supported");
    }
    objectSize = sizeFromMetadata;
  }

  private void validatePosition(long position) throws IOException {
    if (position < 0) {
      GoogleCloudStorageEventBus.postOnException();
      throw new EOFException(
          String.format(
              "Invalid seek offset: position value (%d) must be >= 0 for '%s'",
              position, resourceId));
    }

    if (objectSize >= 0 && position >= objectSize) {
      GoogleCloudStorageEventBus.postOnException();
      throw new EOFException(
          String.format(
              "Invalid seek offset: position value (%d) must be between 0 and %d for '%s'",
              position, objectSize, resourceId));
    }
  }

  // Helper functions related to Footer Caching.

  /**
   * Check if the read is current position of the channel belongs to the last Range Request. If yes
   * that will be cached as the footer and future requests in the footer region will be served from
   * the cache.
   */
  @VisibleForTesting
  public boolean isFooterRead() {
    return objectSize - position <= readOptions.getMinRangeRequestSize();
  }

  /**
   * Function to cache the complete last range request as the footer if the footer is not already
   * cached.
   */
  @VisibleForTesting
  public void cacheFooter()
      throws InterruptedException, ExecutionException, TimeoutException, IOException {
    long footerStartPosition = max(0, objectSize - readOptions.getMinRangeRequestSize());
    long footerSize = objectSize - footerStartPosition;

    if (footerSize <= 0) {
      return;
    }

    logger.atFiner().log(
        "Prefetching footer for '%s'. Position: %d, Size: %d",
        resourceId, footerStartPosition, footerSize);
    try (DisposableByteString dbs = readBytes(footerStartPosition, footerSize)) {
      ByteString byteString = dbs.byteString();
      if (byteString.size() != footerSize) {
        throw new IOException(
            String.format(
                "Failed to read complete footer for '%s'. Expected %d, got %d",
                resourceId, footerSize, byteString.size()));
      }
      this.footerContent = byteString.toByteArray();
    }
    logger.atFiner().log("Prefetched %d bytes footer for '%s'", footerContent.length, resourceId);
  }

  /** Serve the data from the cached footer if the footer Cache is already populated */
  @VisibleForTesting
  public int readFromCache(ByteBuffer dst) {
    // Calculate offset inside the footerContent byte array

    long footerStartPosition = objectSize - footerContent.length;

    // The Offset in the cache array will be the number bytes to start reading from the beginning of
    // the footer.
    int offsetInFooterCache = (int) (position - footerStartPosition);

    // The number of bytes we read is limited by dst buffer remaining and cache remaining
    int bytesToCopy = Math.min(dst.remaining(), footerContent.length - offsetInFooterCache);

    if (bytesToCopy <= 0) {
      return 0;
    }

    dst.put(footerContent, offsetInFooterCache, bytesToCopy);
    position += bytesToCopy;
    return bytesToCopy;
  }

  /**
   * Attempts to fulfill the read request from the footer cache.
   *
   * <p>If the footer is already cached and the read is within its range, it reads from the cache.
   * If the footer is not cached but the read is in the footer region, it fetches the footer, caches
   * it, and then reads from the new cache.
   *
   * @return An {@link Optional} containing the number of bytes read if the request was handled by
   *     the cache, or {@link Optional#empty()} if the request is not a footer request.
   * @throws IOException if fetching the footer fails.
   */
  private Optional<Integer> tryReadFromFooter(ByteBuffer dst) throws IOException {
    // The footer is already cached and our read position is within it.
    if (footerContent != null && position >= objectSize - footerContent.length) {
      int bytesRead = readFromCache(dst);
      return Optional.of(bytesRead);
    }

    // The footer is not cached, but we should fetch it as position is in range.
    boolean shouldPrefetchFooter =
        !gzipEncoded
            && footerContent == null
            && isFooterRead()
            && !readOptions.isReadExactRequestedBytesEnabled();

    if (shouldPrefetchFooter) {
      try {
        cacheFooter();
        // After caching, read the requested part from the new cache.
        int bytesRead = readFromCache(dst);
        return Optional.of(bytesRead);
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        GoogleCloudStorageEventBus.postOnException();
        throw new IOException(String.format("Footer prefetch failed on %s", resourceId), e);
      }
    }

    // If neither case was met, this read is not for the footer.
    return Optional.empty();
  }

  // Helper functions for the Look ahead read buffer.
  public boolean isBufferValid() {
    return internalBuffer != null
        && position >= bufferStartPosition
        && position < bufferStartPosition + internalBuffer.limit();
  }

  private void invalidateBuffer() {
    internalBuffer = null;
    bufferStartPosition = -1;
  }

  @VisibleForTesting
  public int readBytesFromInternalBuffer(ByteBuffer dst) {
    int bytesToRead = Math.min(dst.remaining(), internalBuffer.remaining());
    if (bytesToRead == 0) return 0;

    byte[] tempArray = new byte[bytesToRead];
    internalBuffer.get(tempArray);
    dst.put(tempArray);

    position += bytesToRead;
    return bytesToRead;
  }

  /** Refills the internal buffer by fetching a fixed-size chunk from GCS. */
  @VisibleForTesting
  public void refillInternalBuffer() throws IOException {
    invalidateBuffer();
    if (position >= objectSize) return;

    long bytesToRequest = Math.min(readOptions.getMinRangeRequestSize(), objectSize - position);

    if (bytesToRequest <= 0) return;

    try (DisposableByteString dbs = readBytes(position, bytesToRequest)) {
      ByteString byteString = dbs.byteString();
      if (!byteString.isEmpty()) {
        this.bufferStartPosition = position;

        // TODO(dhritichopra): This is a temporary fix for mmeory leak, better alterantives need to
        // be explored.
        byte[] copiedBytes = byteString.toByteArray();
        this.internalBuffer = ByteBuffer.wrap(copiedBytes);
      }
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new IOException(
          String.format("Look ahead read failed on %s at position %d", resourceId, position), e);
    }
  }

  /**
   * Performs a standard read from the current position.
   *
   * @param dst The destination buffer for the read operation.
   * @return The number of bytes read, or -1 if the end of the stream is reached.
   * @throws IOException if the read operation fails.
   */
  @VisibleForTesting
  public int performStandardRead(ByteBuffer dst) throws IOException {
    logger.atFinest().log(
        "Reading up to %d bytes at position %d from '%s'", dst.remaining(), position, resourceId);

    long bytesToRequest = Math.min(dst.remaining(), objectSize - position);

    int bytesRead;

    try (DisposableByteString dbs = readBytes(position, bytesToRequest)) {
      ByteString byteString = dbs.byteString();
      bytesRead = byteString.size();

      if (bytesRead == 0 && position < objectSize) {
        throw new IOException(
            String.format("Read 0 bytes without blocking from object: '%s'", resourceId));
      }

      if (bytesRead > 0) {
        byteString.copyTo(dst);
        position += bytesRead;
      }
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      GoogleCloudStorageEventBus.postOnException();
      throw new IOException(
          String.format("Read failed on %s at position %d", resourceId, position), e);
    }
    return bytesRead > 0 ? bytesRead : EOF;
  }

  /**
   * Reads a range of bytes from the object and returns the disposable, zero-copy resource. The
   * CALLER of this method is responsible for closing the returned resource, typically with a
   * try-with-resources block.
   */
  private DisposableByteString readBytes(long offset, long length)
      throws InterruptedException, ExecutionException, TimeoutException, IOException {
    ApiFuture<DisposableByteString> futureBytes =
        getBlobReadSession()
            .readAs(
                ReadProjectionConfigs.asFutureByteString()
                    .withRangeSpec(RangeSpec.of(offset, length)));
    return futureBytes.get(readTimeout.toNanos(), TimeUnit.NANOSECONDS);
  }

  private static void validate(GoogleCloudStorageItemInfo itemInfo) throws IOException {
    checkNotNull(itemInfo, "itemInfo cannot be null");
    StorageResourceId resourceId = itemInfo.getResourceId();
    checkArgument(
        resourceId.isStorageObject(), "Can not open a non-file object for read: %s", resourceId);
    if (!itemInfo.exists()) {
      GoogleCloudStorageEventBus.postOnException();
      throw new FileNotFoundException(String.format("Item not found: %s", resourceId));
    }
  }
}
