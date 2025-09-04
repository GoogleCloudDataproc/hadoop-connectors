package com.google.cloud.hadoop.gcsio;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobReadSession;
import com.google.cloud.storage.RangeSpec;
import com.google.cloud.storage.ReadProjectionConfigs;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.ZeroCopySupport.DisposableByteString;
import com.google.common.flogger.GoogleLogger;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.IntFunction;

public class GoogleCloudStorageBidiReadChannel implements ReadVectoredSeekableByteChannel {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();
  private final Storage storage;
  private final StorageResourceId resourceId;
  private final BlobId blobId;
  private final GoogleCloudStorageReadOptions readOptions;
  private final BlobReadSession blobReadSession;
  private ExecutorService boundedThreadPool;

  public GoogleCloudStorageBidiReadChannel(
      Storage storage,
      GoogleCloudStorageItemInfo itemInfo,
      GoogleCloudStorageReadOptions readOptions,
      ExecutorService boundedThreadPool)
      throws IOException {
    this.storage = storage;
    this.resourceId =
        new StorageResourceId(
            itemInfo.getBucketName(), itemInfo.getObjectName(), itemInfo.getContentGeneration());
    this.blobId =
        BlobId.of(
            resourceId.getBucketName(), resourceId.getObjectName(), resourceId.getGenerationId());
    this.readOptions = readOptions;
    this.blobReadSession =
        initializeBlobReadSession(storage, blobId, readOptions.getBidiClientTimeout());
    this.boundedThreadPool = boundedThreadPool;
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
      throw new IOException(e);
    }
    return readSession;
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    // TODO(dhritichopra) Add read flow
    return 0;
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    throw new UnsupportedOperationException("Cannot mutate read-only channel");
  }

  @Override
  public long position() throws IOException {
    // TODO(dhritichopra) Add read flow
    return 0;
  }

  @Override
  public SeekableByteChannel position(long newPosition) throws IOException {
    // TODO(dhritichopra) Add read flow
    return null;
  }

  @Override
  public long size() throws IOException {
    // TODO(dhritichopra) Add read flow
    return 0;
  }

  @Override
  public SeekableByteChannel truncate(long size) throws IOException {
    // TODO(dhritichopra) Add read flow
    return null;
  }

  @Override
  public boolean isOpen() {
    // TODO(dhritichopra) Add read flow
    return false;
  }

  @Override
  public void close() throws IOException {
    blobReadSession.close();
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
      // This loop efficiently copies data without creating intermediate byte arrays on the heap
      for (ByteBuffer b : byteString.asReadOnlyByteBufferList()) {
        buf.put(b);
      }
      buf.flip(); // Prepare buffer for reading
      range.getData().complete(buf);
    }
    return bytesRead;
  }
}
