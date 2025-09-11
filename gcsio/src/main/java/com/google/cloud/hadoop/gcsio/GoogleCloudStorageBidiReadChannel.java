package com.google.cloud.hadoop.gcsio;

import static com.google.common.base.Strings.nullToEmpty;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.hadoop.util.GoogleCloudStorageEventBus;
import com.google.cloud.storage.*;
import com.google.cloud.storage.ZeroCopySupport.DisposableByteString;
import com.google.common.flogger.GoogleLogger;
import com.google.protobuf.ByteString;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.IntFunction;
import javax.annotation.Nullable;

public class GoogleCloudStorageBidiReadChannel implements ReadVectoredSeekableByteChannel {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private static final int EOF_RETURN_VALUE = -1;
  private final StorageResourceId resourceId;
  private final BlobId blobId;
  private final BlobReadSession blobReadSession;
  private final ExecutorService boundedThreadPool;
  private static final String GZIP_ENCODING = "gzip";
  private long objectSize;
  private boolean isOpen = true;
  private boolean gzipEncoded = false;
  private final Duration readTimeout;
  private long position = 0;

  public GoogleCloudStorageBidiReadChannel(
      Storage storage,
      GoogleCloudStorageItemInfo itemInfo,
      GoogleCloudStorageReadOptions readOptions,
      ExecutorService boundedThreadPool)
      throws IOException {

    // TODO(dhritichopra) Remove grpcReadTimeout if redundant and rename to bidiReadTimeout.
    this.readTimeout = readOptions.getGrpcReadTimeout();
    this.resourceId =
        new StorageResourceId(
            itemInfo.getBucketName(), itemInfo.getObjectName(), itemInfo.getContentGeneration());
    this.blobId =
        BlobId.of(
            resourceId.getBucketName(), resourceId.getObjectName(), resourceId.getGenerationId());
    this.blobReadSession =
        initializeBlobReadSession(storage, blobId, readOptions.getBidiClientTimeout());
    this.boundedThreadPool = boundedThreadPool;
    initMetadata(itemInfo.getContentEncoding(), itemInfo.getSize());
  }

  private static BlobReadSession initializeBlobReadSession(
      Storage storage, BlobId blobId, int clientTimeout) throws IOException {
    long clientInitializationDurationStartTime = System.currentTimeMillis();
    BlobReadSession readSession = null;
    try {
      readSession = storage.blobReadSession(blobId).get(clientTimeout, TimeUnit.SECONDS);
      logger.atFiner().log(
          "Client Initialization successful in %d ms.",
          System.currentTimeMillis() - clientInitializationDurationStartTime);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      GoogleCloudStorageEventBus.postOnException();
      throw new IOException(e);
    }
    return readSession;
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    throwIfNotOpen();
    if (!dst.hasRemaining()) {
      return 0;
    }

    if (position >= objectSize) {
      return EOF_RETURN_VALUE;
    }

    logger.atFinest().log(
        "Reading up to %d bytes at position %d from '%s'", dst.remaining(), position, resourceId);

    long bytesToRequest = Math.min(dst.remaining(), objectSize - position);

    try {
      ApiFuture<DisposableByteString> futureBytes =
          blobReadSession.readAs(
              ReadProjectionConfigs.asFutureByteString()
                  .withRangeSpec(RangeSpec.of(position, bytesToRequest)));

      int bytesRead;
      try (DisposableByteString dbs =
          futureBytes.get(readTimeout.toNanos(), TimeUnit.NANOSECONDS); ) {
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

      return bytesRead > 0 ? bytesRead : EOF_RETURN_VALUE;

    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      GoogleCloudStorageEventBus.postOnException();
      throw new IOException(
          String.format("Read failed on %s at position %d", resourceId, position), e);
    }
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
    logger.atFiner().log("readVectored() called for BlobId=%s", blobId.toString());
    long vectoredReadStartTime = System.currentTimeMillis();
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
                        "Vectored Read successful for range starting from %d with length %d. Total Bytes Read are: %d within %d ms",
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
}
