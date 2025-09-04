package com.google.cloud.hadoop.gcsio;

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

public class GoogleCloudStorageBidiReadChannel implements ReadVectoredSeekableByteChannel {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();
  private final BlobReadSession blobReadSession;
  private final ExecutorService boundedThreadPool;
  private static final String GZIP_ENCODING = "gzip";
  private long objectSize;
  private boolean isOpen = true;
  private boolean gzipEncoded = false;
  private final StorageResourceId resourceId;
  private final Duration readTimeout;
  private long position = 0;
  private final GoogleCloudStorageReadOptions readOptions;
  private byte[] footerContent;

  public GoogleCloudStorageBidiReadChannel(
      Storage storage,
      GoogleCloudStorageItemInfo itemInfo,
      GoogleCloudStorageReadOptions readOptions,
      ExecutorService boundedThreadPool)
      throws IOException {
    this.readTimeout = readOptions.getGrpcReadTimeout();
    this.resourceId =
        new StorageResourceId(
            itemInfo.getBucketName(), itemInfo.getObjectName(), itemInfo.getContentGeneration());
    BlobId blobId =
        BlobId.of(
            resourceId.getBucketName(), resourceId.getObjectName(), resourceId.getGenerationId());
    this.readOptions = readOptions;
    this.blobReadSession =
        initializeBlobReadSession(storage, blobId, readOptions.getBidiClientTimeout());
    this.boundedThreadPool = boundedThreadPool;
    initMetadata(itemInfo.getContentEncoding(), itemInfo.getSize());
  }

  private static BlobReadSession initializeBlobReadSession(
      Storage storage, BlobId blobId, int clientTimeout) throws IOException {
    try {
      return storage.blobReadSession(blobId).get(clientTimeout, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      GoogleCloudStorageEventBus.postOnException();
      throw new IOException(e);
    }
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    throwIfNotOpen();
    if (!dst.hasRemaining()) {
      return 0;
    }
    if (position >= objectSize) {
      return -1;
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

    // If not handled by the cache, perform a standard read.
    return performStandardRead(dst);
  }

  @Override
  public long position() throws IOException {
    throwIfNotOpen();
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
    return this;
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    GoogleCloudStorageEventBus.postOnException();
    throw new UnsupportedOperationException("Cannot mutate read-only channel");
  }

  @Override
  public long size() throws IOException {
    throwIfNotOpen();
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
    return isOpen;
  }

  @Override
  public void close() throws IOException {
    if (isOpen) {
      isOpen = false;
      logger.atFinest().log("Closing channel for '%s'", resourceId);
      if (blobReadSession != null) {
        blobReadSession.close();
      }
    }
  }

  @Override
  public void readVectored(List<VectoredIORange> ranges, IntFunction<ByteBuffer> allocate)
      throws IOException {
    ranges.forEach(
        range -> {
          ApiFuture<DisposableByteString> futureBytes =
              blobReadSession.readAs(
                  ReadProjectionConfigs.asFutureByteString()
                      .withRangeSpec(RangeSpec.of(range.getOffset(), range.getLength())));
          ApiFutures.addCallback(
              futureBytes,
              new ApiFutureCallback<>() {
                @Override
                public void onFailure(Throwable t) {
                  range.getData().completeExceptionally(t);
                }

                @Override
                public void onSuccess(DisposableByteString disposableByteString) {
                  try {
                    processBytesAndCompleteRange(disposableByteString, range, allocate);
                  } catch (Throwable t) {
                    range.getData().completeExceptionally(t);
                  }
                }
              },
              boundedThreadPool);
        });
  }

  private void processBytesAndCompleteRange(
      DisposableByteString disposableByteString,
      VectoredIORange range,
      IntFunction<ByteBuffer> allocate)
      throws IOException {
    try (DisposableByteString dbs = disposableByteString) {
      ByteString byteString = dbs.byteString();
      int size = byteString.size();
      ByteBuffer buf = allocate.apply(size);
      for (ByteBuffer b : byteString.asReadOnlyByteBufferList()) {
        buf.put(b);
      }
      buf.flip();
      range.getData().complete(buf);
    }
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

  /* Check if the read is current position of the channel belongs to the last Range Request. If yes that will be cached
  as the footer and future requests in the footer region will be served from the cache.
   */
  @VisibleForTesting
  public boolean isFooterRead() {
    return objectSize - position <= readOptions.getMinRangeRequestSize();
  }

  /* Function to cache the complete last range request as the footer if the footer is not already cached.
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

    ApiFuture<DisposableByteString> futureBytes =
        blobReadSession.readAs(
            ReadProjectionConfigs.asFutureByteString()
                .withRangeSpec(RangeSpec.of(footerStartPosition, footerSize)));

    try (DisposableByteString dbs = futureBytes.get(readTimeout.toNanos(), TimeUnit.NANOSECONDS)) {
      ByteString byteString = dbs.byteString();
      if (byteString.size() != footerSize) {
        throw new IOException(
            String.format(
                "Failed to read complete footer for '%s'. Expected %d, got %d",
                resourceId, footerSize, byteString.size()));
      }
      this.footerContent = byteString.toByteArray();
      logger.atFiner().log("Prefetched %d bytes footer for '%s'", footerContent.length, resourceId);
    }
  }

  /* Serve the data from the cached footer if the footer Cache is already populated
   */
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

  /**
   * Performs a standard read from the current position.
   *
   * @param dst The destination buffer for the read operation.
   * @return The number of bytes read, or -1 if the end of the stream is reached.
   * @throws IOException if the read operation fails.
   */
  private int performStandardRead(ByteBuffer dst) throws IOException {
    logger.atFinest().log(
        "Reading up to %d bytes at position %d from '%s'", dst.remaining(), position, resourceId);

    long bytesToRequest = Math.min(dst.remaining(), objectSize - position);

    try {
      ApiFuture<DisposableByteString> futureBytes =
          blobReadSession.readAs(
              ReadProjectionConfigs.asFutureByteString()
                  .withRangeSpec(RangeSpec.of(position, bytesToRequest)));

      DisposableByteString disposableByteString =
          futureBytes.get(readTimeout.toNanos(), TimeUnit.NANOSECONDS);

      int bytesRead;
      try (DisposableByteString dbs = disposableByteString) {
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
      }

      return bytesRead > 0 ? bytesRead : -1;

    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      GoogleCloudStorageEventBus.postOnException();
      throw new IOException(
          String.format("Read failed on %s at position %d", resourceId, position), e);
    }
  }
}
