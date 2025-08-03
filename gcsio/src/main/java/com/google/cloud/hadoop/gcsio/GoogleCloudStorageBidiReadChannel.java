package com.google.cloud.hadoop.gcsio;

import static com.google.common.base.Strings.nullToEmpty;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.hadoop.util.GoogleCloudStorageEventBus;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobReadSession;
import com.google.cloud.storage.RangeSpec;
import com.google.cloud.storage.ReadProjectionConfigs;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.ZeroCopySupport.DisposableByteString;
import com.google.common.flogger.GoogleLogger;
import com.google.protobuf.ByteString;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;
import java.util.List;
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

  private long position = 0;

  public GoogleCloudStorageBidiReadChannel(
      Storage storage,
      GoogleCloudStorageItemInfo itemInfo,
      GoogleCloudStorageReadOptions readOptions,
      ExecutorService boundedThreadPool)
      throws IOException {
    resourceId =
        new StorageResourceId(
            itemInfo.getBucketName(), itemInfo.getObjectName(), itemInfo.getContentGeneration());
    BlobId blobId =
        BlobId.of(
            resourceId.getBucketName(), resourceId.getObjectName(), resourceId.getGenerationId());
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
      Thread.currentThread().interrupt(); // Preserve interrupted status
      throw new IOException("Failed to initialize BlobReadSession", e);
    }
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    throwIfNotOpen();
    if (!dst.hasRemaining()) {
      return 0;
    }

    if (position >= objectSize) {
      return -1; // End of file
    }

    long bytesToRead = Math.min(dst.remaining(), objectSize - position);

    logger.atFiner().log(
        "Reading %d bytes at %d position from '%s'", bytesToRead, position, resourceId);

    ApiFuture<DisposableByteString> futureBytes =
        blobReadSession.readAs(
            ReadProjectionConfigs.asFutureByteString()
                .withRangeSpec(RangeSpec.of(this.position, bytesToRead)));

    try (DisposableByteString dbs = futureBytes.get()) {
      ByteString byteString = dbs.byteString();
      int bytesRead = byteString.size();

      if (bytesRead > 0) {
        byteString.copyTo(dst);
        this.position += bytesRead;
      }
      return bytesRead;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Read operation was interrupted.", e);
    } catch (ExecutionException e) {
      GoogleCloudStorageEventBus.postOnException();
      throw new IOException("Failed to read data from Google Cloud Storage.", e.getCause());
    }
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    GoogleCloudStorageEventBus.postOnException();
    throw new UnsupportedOperationException("Cannot mutate read-only channel");
  }

  @Override
  public long position() throws IOException {
    throwIfNotOpen();
    // Return our internally managed position.
    return this.position;
  }

  @Override
  public SeekableByteChannel position(long newPosition) throws IOException {
    throwIfNotOpen();
    if (newPosition < 0) {
      GoogleCloudStorageEventBus.postOnException();
      throw new IOException(String.format("Invalid seek position: %d", newPosition));
    }
    if (newPosition > objectSize) {
      GoogleCloudStorageEventBus.postOnException();
      throw new EOFException(
          String.format("Seek position %d is beyond file size %d", newPosition, objectSize));
    }
    if (gzipEncoded) {
      GoogleCloudStorageEventBus.postOnException();
      throw new IOException("Seeking is not supported for Gzip-encoded files.");
    }

    logger.atFiner().log(
        "Seek from %s to %s position for '%s'", this.position, newPosition, resourceId);
    // Simply update our internal position. The next read() will use this new position.
    this.position = newPosition;
    return this;
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
      logger.atFiner().log("Closing channel for '%s'", resourceId);
      // We no longer have a contentReadChannel to close.
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
    // try-with-resources ensures the DisposableByteString is closed, releasing its memory
    try (DisposableByteString dbs = disposableByteString) {
      ByteString byteString = dbs.byteString();
      int size = byteString.size();
      ByteBuffer buf = allocate.apply(size);
      // This loop efficiently copies data without creating intermediate byte arrays on the heap
      for (ByteBuffer b : byteString.asReadOnlyByteBufferList()) {
        buf.put(b);
      }
      buf.flip(); // Prepare buffer for reading
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
}
