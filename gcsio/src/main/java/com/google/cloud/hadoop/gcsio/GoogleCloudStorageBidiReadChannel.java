package com.google.cloud.hadoop.gcsio;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobReadSession;
import com.google.cloud.storage.RangeSpec;
import com.google.cloud.storage.ReadProjectionConfigs;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.ZeroCopySupport.DisposableByteString;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.IntFunction;

public class GoogleCloudStorageBidiReadChannel implements SeekableByteChannel, ReadVectored {

  private final Storage storage;
  private final StorageResourceId resourceId;
  private final BlobId blobId;
  private final GoogleCloudStorageReadOptions readOptions;
  private BlobReadSession blobReadSession;

  private ExecutorService boundedThreadPool;

  private final BlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<>();

  public GoogleCloudStorageBidiReadChannel(
      Storage storage,
      GoogleCloudStorageItemInfo itemInfo,
      GoogleCloudStorageReadOptions readOptions)
      throws IOException {
    this.storage = storage;
    this.resourceId =
        new StorageResourceId(
            itemInfo.getBucketName(), itemInfo.getObjectName(), itemInfo.getContentGeneration());
    this.blobId =
        BlobId.of(
            resourceId.getBucketName(), resourceId.getObjectName(), resourceId.getGenerationId());
    this.readOptions = readOptions;
    initializeBlobReadSession();
    // TODO(shreyassinha): Replace hard coded values with flag parameter.
    this.boundedThreadPool =
        new ThreadPoolExecutor(
            readOptions.getBidiThreadCount(),
            readOptions.getBidiThreadCount(),
            0L,
            TimeUnit.MILLISECONDS,
            taskQueue,
            new ThreadFactoryBuilder()
                .setNameFormat("vectoredRead-range-pool-%d")
                .setDaemon(true)
                .build());
  }

  private void initializeBlobReadSession() throws IOException {
    try {
      this.blobReadSession = storage.blobReadSession(blobId).get(30, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new IOException(e);
    }
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
    ranges.forEach(
        range -> {
          ApiFuture<DisposableByteString> futureBytes =
              blobReadSession.readAs(
                  ReadProjectionConfigs.asFutureByteString()
                      .withRangeSpec(RangeSpec.of(range.getOffset(), range.getLength())));
          ApiFutures.transform(
              futureBytes,
              disposableByteString -> {
                try {
                  return processBytesAndCompleteRange(disposableByteString, range, allocate);
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              },
              boundedThreadPool);
        });
  }

  private Void processBytesAndCompleteRange(
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
      return null;
    }
  }
}
