package com.google.cloud.hadoop.gcsio;

import static com.google.common.base.Strings.nullToEmpty;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.hadoop.util.GoogleCloudStorageEventBus;
import com.google.cloud.storage.*;
import com.google.cloud.storage.ZeroCopySupport.DisposableByteString;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
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
  private final BlobReadSession blobReadSession;
  private final ExecutorService boundedThreadPool;
  private static final String GZIP_ENCODING = "gzip";
  private long objectSize;
  private boolean isOpen = true;
  private boolean gzipEncoded = false;
  @VisibleForTesting private SeekableByteChannel contentReadChannel;
  @VisibleForTesting private long currentPosition = 0;

  private boolean isProjectionObtained= false;

  public GoogleCloudStorageBidiReadChannel(
      Storage storage,
      GoogleCloudStorageItemInfo itemInfo,
      GoogleCloudStorageReadOptions readOptions,
      ExecutorService boundedThreadPool)
      throws IOException {
    StorageResourceId resourceId =
        new StorageResourceId(
            itemInfo.getBucketName(), itemInfo.getObjectName(), itemInfo.getContentGeneration());
    BlobId blobId =
        BlobId.of(
            resourceId.getBucketName(), resourceId.getObjectName(), resourceId.getGenerationId());
    this.blobReadSession =
        initializeBlobReadSession(storage, blobId, readOptions.getBidiClientTimeout());
    this.boundedThreadPool = boundedThreadPool;
    initMetadata(itemInfo.getContentEncoding());
  }

  private void initializeReadSession() {
    ReadAsSeekableChannel seekableChannelConfig = ReadProjectionConfigs.asSeekableChannel();
    this.contentReadChannel = blobReadSession.readAs(seekableChannelConfig);
    this.currentPosition = 0;
  }

  private static BlobReadSession initializeBlobReadSession(
      Storage storage, BlobId blobId, int clientTimeout) throws IOException {
    try {
      return storage.blobReadSession(blobId).get(clientTimeout, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new IOException(e);
    }
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    if(!isProjectionObtained){
      isProjectionObtained = true;
      initializeReadSession();
    }
    throwIfNotOpen();
    if (!dst.hasRemaining()) {
      return 0;
    }

    if (currentPosition >= objectSize) {
      return -1;
    }
    int bytesRead = contentReadChannel.read(dst);

    if (bytesRead == -1) {
      this.currentPosition = objectSize;
      return -1;
    }

    return bytesRead;
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    throw new UnsupportedOperationException("Cannot mutate read-only channel");
  }

  @Override
  public long position() throws IOException {
    throwIfNotOpen();
    return currentPosition;
  }

  @Override
  public SeekableByteChannel position(long newPosition) throws IOException {
    throwIfNotOpen();
    if (newPosition < 0) {
      throw new IOException(String.format("Invalid seek position: %d", newPosition));
    }
    if (newPosition > objectSize) {
      throw new java.io.EOFException(
          String.format("Seek position %d is beyond file size %d", newPosition, objectSize));
    }
    if (gzipEncoded) {
      throw new IOException("Gzip is not supported");
    }
    if (newPosition == this.currentPosition) {
      return this;
    }
    contentReadChannel.position(newPosition);
    this.currentPosition = newPosition;
    return this;
  }

  @Override
  public long size() throws IOException {
    throwIfNotOpen();
    if (objectSize == -1) {
      throw new IOException("Size of file is not available");
    }
    return objectSize;
  }

  @Override
  public SeekableByteChannel truncate(long size) throws IOException {
    throw new UnsupportedOperationException("Cannot truncate a read-only channel");
  }

  @Override
  public boolean isOpen() {
    return isOpen;
  }

  @Override
  public void close() throws IOException {
    blobReadSession.close();
  }

  @Override
  public void readVectored(List<VectoredIORange> ranges, IntFunction<ByteBuffer> allocate)
      throws IOException {
    isProjectionObtained = true;
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

  protected void initMetadata(@Nullable String encoding) throws UnsupportedOperationException {
    gzipEncoded = nullToEmpty(encoding).contains(GZIP_ENCODING);
    if (gzipEncoded) {
      throw new UnsupportedOperationException("Gzip Encoded Files are not supported");
    }
  }
}
