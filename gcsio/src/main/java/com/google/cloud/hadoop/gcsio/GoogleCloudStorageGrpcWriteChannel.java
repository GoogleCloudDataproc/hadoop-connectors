/*
 * Copyright 2019 Google Inc. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.gcsio;

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl.encodeMetadata;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.storage.v2.ServiceConstants.Values.MAX_WRITE_CHUNK_BYTES;
import static java.lang.Math.min;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toMap;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl.BackOffFactory;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.cloud.hadoop.util.ResilientOperation;
import com.google.cloud.hadoop.util.RetryDeterminer;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSet;
import com.google.common.flogger.GoogleLogger;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import com.google.storage.v2.ChecksummedData;
import com.google.storage.v2.CommonRequestParams;
import com.google.storage.v2.CommonRequestParams.Builder;
import com.google.storage.v2.Object;
import com.google.storage.v2.ObjectChecksums;
import com.google.storage.v2.QueryWriteStatusRequest;
import com.google.storage.v2.QueryWriteStatusResponse;
import com.google.storage.v2.StartResumableWriteRequest;
import com.google.storage.v2.StartResumableWriteResponse;
import com.google.storage.v2.StorageGrpc;
import com.google.storage.v2.StorageGrpc.StorageStub;
import com.google.storage.v2.WriteObjectRequest;
import com.google.storage.v2.WriteObjectResponse;
import com.google.storage.v2.WriteObjectSpec;
import io.grpc.ClientCall;
import io.grpc.Status;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import java.time.Duration;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

/** Implements WritableByteChannel to provide write access to GCS via gRPC. */
public final class GoogleCloudStorageGrpcWriteChannel
    implements GoogleCloudStorageItemInfo.Provider, WritableByteChannel {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();
  private static final Duration START_RESUMABLE_WRITE_TIMEOUT = Duration.ofMinutes(1);
  private static final Duration QUERY_WRITE_STATUS_TIMEOUT = Duration.ofMinutes(1);

  // A set that defines all transient errors on which retry can be attempted.
  private static final ImmutableSet<Status.Code> TRANSIENT_ERRORS =
      ImmutableSet.of(
          Status.Code.DEADLINE_EXCEEDED,
          Status.Code.INTERNAL,
          Status.Code.RESOURCE_EXHAUSTED,
          Status.Code.UNAVAILABLE);

  private volatile StorageStub stub;

  private final StorageStubProvider stubProvider;
  private final StorageResourceId resourceId;
  private final CreateObjectOptions createOptions;
  private final ObjectWriteConditions writeConditions;
  private final String requesterPaysProject;
  private final BackOffFactory backOffFactory;
  private final Watchdog watchdog;
  private final int MAX_BYTES_PER_MESSAGE = MAX_WRITE_CHUNK_BYTES.getNumber();

  private final ExecutorService threadPool;

  private final AsyncWriteChannelOptions channelOptions;

  // Upload operation that takes place on a separate thread.
  private Future<WriteObjectResponse> uploadOperation;

  private GoogleCloudStorageItemInfo completedItemInfo = null;
  // writeBuffer stores (buffers) data between the upstream write api and downstream thread
  // pushing the data to GCS
  private LinkedBlockingQueue<ByteString> writeBuffer;
  // lastChunk holds on to the data written by upstream write API which is less than 2MB. We do this
  // to accumulate 2MB ByteString buffers in the writeBuffer.
  private ByteString lastChunk;
  private boolean initialized;

  GoogleCloudStorageGrpcWriteChannel(
      StorageStubProvider stubProvider,
      ExecutorService threadPool,
      AsyncWriteChannelOptions channelOptions,
      StorageResourceId resourceId,
      CreateObjectOptions createOptions,
      Watchdog watchdog,
      ObjectWriteConditions writeConditions,
      String requesterPaysProject,
      BackOffFactory backOffFactory) {
    this.stubProvider = stubProvider;
    this.stub = stubProvider.newAsyncStub();
    this.resourceId = resourceId;
    this.createOptions = createOptions;
    this.writeConditions = writeConditions;
    this.requesterPaysProject = requesterPaysProject;
    this.backOffFactory = backOffFactory;
    this.watchdog = watchdog;
    this.threadPool = threadPool;
    this.channelOptions = channelOptions;
  }

  private String getResourceString() {
    return resourceId.toString();
  }

  public void handleResponse(WriteObjectResponse response) {
    Object resource = response.getResource();
    Map<String, byte[]> metadata =
        resource.getMetadataMap().entrySet().stream()
            .collect(
                toMap(Map.Entry::getKey, entry -> BaseEncoding.base64().decode(entry.getValue())));

    byte[] md5Hash = null;
    byte[] crc32c = null;

    if (resource.hasChecksums()) {
      md5Hash =
          !resource.getChecksums().getMd5Hash().isEmpty()
              ? resource.getChecksums().getMd5Hash().toByteArray()
              : null;

      crc32c =
          resource.getChecksums().hasCrc32C()
              ? ByteBuffer.allocate(4).putInt(resource.getChecksums().getCrc32C()).array()
              : null;
    }

    completedItemInfo =
        GoogleCloudStorageItemInfo.createObject(
            resourceId,
            Timestamps.toMillis(resource.getCreateTime()),
            Timestamps.toMillis(resource.getUpdateTime()),
            resource.getSize(),
            resource.getContentType(),
            resource.getContentEncoding(),
            metadata,
            resource.getGeneration(),
            resource.getMetageneration(),
            new VerificationAttributes(md5Hash, crc32c));
  }

  public boolean isOpen() {
    return (writeBuffer != null);
  }

  public synchronized int write(ByteBuffer buffer) throws IOException {
    checkState(initialized, "initialize() must be invoked before use.");
    int bytesWritten = 0;
    if (!isOpen()) {
      throw new ClosedChannelException();
    }

    // No point in writing further if upload failed on another thread.
    if (uploadOperation.isDone()) {
      waitForCompletionAndThrowIfUploadFailed();
    }

    // create chunks of MAX_BYTES_PER_MESSAGE and push them into writeBuffer. If the loop ends up
    // in a scenario where the lastChunk size is less than MAX_BYTES_PER_MESSAGE, then we hold on
    // to it until next write or close.
    while (buffer.remaining() > 0) {
      int bytesToWrite = 0;
      if (lastChunk != null) {
        bytesToWrite = min(buffer.remaining(), MAX_BYTES_PER_MESSAGE - lastChunk.size());
        lastChunk = lastChunk.concat(ByteString.copyFrom(buffer, bytesToWrite));
      } else {
        bytesToWrite = min(buffer.remaining(), MAX_BYTES_PER_MESSAGE);
        lastChunk = ByteString.copyFrom(buffer, bytesToWrite);
      }
      bytesWritten += bytesToWrite;
      if (lastChunk.size() == MAX_BYTES_PER_MESSAGE) {
        try {
          writeBuffer.put(lastChunk);
          lastChunk = null;
        } catch (InterruptedException e) {
          throw new IOException(
              String.format(
                  "Failed to write %d bytes in '%s'", buffer.remaining(), getResourceString()),
              e);
        }
      }
    }

    return bytesWritten;
  }

  /** Initialize this channel object for writing. */
  public void initialize() throws IOException {
    checkState(!initialized, "initialize() must be invoked only once.");
    writeBuffer =
        new LinkedBlockingQueue<ByteString>(
            Math.toIntExact(channelOptions.getNumberOfBufferedRequests()));
    try {
      uploadOperation = threadPool.submit(new UploadOperation(writeBuffer));
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to start upload for '%s'", resourceId), e);
    }
    initialized = true;
  }

  public void close() throws IOException {
    checkState(initialized, "initialize() must be invoked before use.");
    if (!isOpen()) {
      return;
    }
    // The way we signal close (EOF) to downstream thread is by appending a ByteString of size less
    // than 2MB. If the uploaded file size is multiple of 2MB, then we end up appending an empty
    // ByteString buffer. The lastChunk carries the data written by upstream not amounting to 2MB
    if (lastChunk == null) {
      lastChunk = ByteString.EMPTY;
    }
    try {
      writeBuffer.put(lastChunk);
      lastChunk = null;
      handleResponse(waitForCompletionAndThrowIfUploadFailed());
    } catch (InterruptedException e) {
      throw new IOException(String.format("Interrupted on close for '%s'", resourceId), e);
    } finally {
      writeBuffer = null;
      closeInternal();
    }
  }

  private void closeInternal() {
    if (uploadOperation != null && !uploadOperation.isDone()) {
      uploadOperation.cancel(/* mayInterruptIfRunning= */ true);
    }
    uploadOperation = null;
  }
  /**
   * Throws if upload operation failed. Propagates any errors.
   *
   * @throws IOException on IO error
   */
  private WriteObjectResponse waitForCompletionAndThrowIfUploadFailed() throws IOException {
    try {
      return uploadOperation.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      // If we were interrupted, we need to cancel the upload operation.
      uploadOperation.cancel(true);
      IOException exception = new ClosedByInterruptException();
      exception.addSuppressed(e);
      throw exception;
    } catch (ExecutionException e) {
      if (e.getCause() instanceof Error) {
        throw (Error) e.getCause();
      }
      throw new IOException(
          String.format("Upload failed for '%s'", getResourceString()), e.getCause());
    }
  }

  private class UploadOperation implements Callable<WriteObjectResponse> {

    // Read end of the pipe.
    private final int MAX_BYTES_PER_MESSAGE = MAX_WRITE_CHUNK_BYTES.getNumber();

    private Hasher objectHasher;
    private String uploadId;
    private long writeOffset = 0;
    private LinkedBlockingQueue<ByteString> writeBuffer;
    private SimpleResponseObserver<QueryWriteStatusResponse> writeStatusResponseObserver;
    private Stopwatch writeStatusStopWatch;

    // Holds list of most recent number of NUMBER_OF_REQUESTS_TO_RETAIN requests, so upload can
    // be rewound and re-sent upon transient errors.
    private final TreeMap<Long, WriteObjectRequest> requestChunkMap = new TreeMap<>();

    UploadOperation(LinkedBlockingQueue<ByteString> writeBuffer) {
      this.writeBuffer = writeBuffer;
      if (channelOptions.isGrpcChecksumsEnabled()) {
        objectHasher = Hashing.crc32c().newHasher();
      }
    }

    @Override
    public WriteObjectResponse call() throws IOException {
      // Try-with-resource will close this end of the pipe so that
      // the writer at the other end will not hang indefinitely.
      // Send the initial StartResumableWrite request to get an uploadId.
      try {
        Stopwatch stopwatch = Stopwatch.createStarted();
        uploadId = startResumableUploadWithRetries();
        WriteObjectResponse response =
            ResilientOperation.retry(
                this::doResumableUpload,
                backOffFactory.newBackOff(),
                this::isRetriableError,
                IOException.class);
        logger.atFinest().log(
            "File upload complete: resource:%s, time:%d",
            resourceId, stopwatch.elapsed(MILLISECONDS));
        return response;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(
            String.format("Interrupted resumable upload failed for '%s'", resourceId), e);
      }
    }

    class OutOfBufferedDataException extends IOException {
      public OutOfBufferedDataException(String message) {
        super(message);
      }
    }

    boolean isRetriableError(Throwable throwable) {
      if (throwable instanceof OutOfBufferedDataException) return false;
      Throwable cause = throwable.getCause();
      if (cause == null) return true;
      return isRetriableError(cause);
    }

    private WriteObjectResponse doResumableUpload() throws IOException {
      // Only request committed size for the first insert request.
      if (writeOffset > 0) {
        writeOffset = getCommittedWriteSizeWithRetries(uploadId);
      }
      StorageStub storageStub =
          stub.withDeadlineAfter(channelOptions.getGrpcWriteTimeout(), MILLISECONDS);
      InsertChunkResponseObserver responseObserver =
          new InsertChunkResponseObserver(uploadId, writeOffset);
      ClientCall<WriteObjectRequest, WriteObjectResponse> call =
          storageStub
              .getChannel()
              .newCall(StorageGrpc.getWriteObjectMethod(), stub.getCallOptions());
      StreamObserver<WriteObjectRequest> writeObjectRequestStreamObserver =
          ClientCalls.asyncClientStreamingCall(call, responseObserver);
      StreamObserver<WriteObjectRequest> requestStreamObserver =
          watchdog.watch(
              call,
              writeObjectRequestStreamObserver,
              Duration.ofSeconds(channelOptions.getGrpcWriteMessageTimeoutMillis()));

      // Wait for streaming RPC to become ready for upload.
      try {
        // wait for 1 min for the channel to be ready. Else bail out
        if (!responseObserver.ready.await(60 * 1000, MILLISECONDS)) {
          throw new IOException(
              String.format(
                  "Timed out while awaiting ready on responseObserver for '%s' with UploadID '%s'",
                  resourceId, responseObserver.uploadId));
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(
            String.format(
                "Interrupted while awaiting ready on responseObserver for '%s' with UploadID '%s'",
                resourceId, responseObserver.uploadId));
      }

      boolean objectFinalized = false;
      while (!objectFinalized) {
        WriteObjectRequest insertRequest = null;
        if (requestChunkMap.size() > 0 && requestChunkMap.lastKey() >= writeOffset) {
          insertRequest = getCachedRequest(requestChunkMap, writeOffset);
          writeOffset += insertRequest.getChecksummedData().getContent().size();
        } else if (requestChunkMap.size() >= channelOptions.getNumberOfBufferedRequests()) {
          freeUpCommittedRequests(requestChunkMap, writeOffset);
        } else {
          // Pick up a chunk to write only if dataChunkMap has space. Else continue after looking
          // for errors.
          ByteString data = null;
          try {
            data = (writeBuffer == null) ? ByteString.EMPTY : writeBuffer.take();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(
                String.format(
                    "Interrupted while reading from blocking queue for '%s'", resourceId));
          }
          insertRequest = buildInsertRequest(writeOffset, data, false);
          requestChunkMap.put(writeOffset, insertRequest);
          writeOffset += data.size();
        }
        if (insertRequest != null) {
          requestStreamObserver.onNext(insertRequest);
          objectFinalized = insertRequest.getFinishWrite();
          if (objectFinalized) {
            writeBuffer = null;
          }
        }
        if (responseObserver.hasTransientError() || responseObserver.hasNonTransientError()) {
          requestStreamObserver.onError(
              responseObserver.hasTransientError()
                  ? responseObserver.transientError
                  : responseObserver.nonTransientError);
          break;
        } else if (objectFinalized) {
          requestStreamObserver.onCompleted();
        }
      }

      try {
        responseObserver.done.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(
            String.format(
                "Interrupted while awaiting response during upload of '%s' with UploadID '%s'",
                resourceId, responseObserver.uploadId));
      }
      if (responseObserver.hasTransientError()) {
        throw new IOException(
            String.format("Got transient error for UploadID '%s'", responseObserver.uploadId),
            responseObserver.transientError);
      }

      return responseObserver.getResponseOrThrow();
    }

    private WriteObjectRequest buildInsertRequest(
        long writeOffset, ByteString dataChunk, boolean resumeFromFailedInsert) {
      WriteObjectRequest.Builder requestBuilder =
          WriteObjectRequest.newBuilder().setUploadId(uploadId).setWriteOffset(writeOffset);

      if (dataChunk.size() > 0) {
        ChecksummedData.Builder requestDataBuilder =
            ChecksummedData.newBuilder().setContent(dataChunk);
        if (channelOptions.isGrpcChecksumsEnabled()) {
          if (!resumeFromFailedInsert) {
            updateObjectHash(dataChunk);
          }
          requestDataBuilder.setCrc32C(getChunkHash(dataChunk));
        }
        requestBuilder.setChecksummedData(requestDataBuilder);
      }

      if (dataChunk.size() < MAX_BYTES_PER_MESSAGE) {
        requestBuilder.setFinishWrite(true);
        if (channelOptions.isGrpcChecksumsEnabled()) {
          requestBuilder.setObjectChecksums(
              ObjectChecksums.newBuilder().setCrc32C(objectHasher.hash().asInt()));
        }
      }

      return requestBuilder.build();
    }

    private int getChunkHash(ByteString dataChunk) {
      Hasher chunkHasher = Hashing.crc32c().newHasher();
      for (ByteBuffer buffer : dataChunk.asReadOnlyByteBufferList()) {
        chunkHasher.putBytes(buffer);
      }
      return chunkHasher.hash().asInt();
    }

    private void updateObjectHash(ByteString dataChunk) {
      for (ByteBuffer buffer : dataChunk.asReadOnlyByteBufferList()) {
        objectHasher.putBytes(buffer);
      }
    }

    // Handles the case when a writeOffset of data read previously is being processed.
    // This happens if a transient failure happens while uploading, and can be resumed by
    // querying the writeRequest object at the current committed offset.
    private WriteObjectRequest getCachedRequest(
        TreeMap<Long, WriteObjectRequest> requestChunkMap, long writeOffset) {
      WriteObjectRequest request = null;
      if (requestChunkMap.size() > 0 && requestChunkMap.firstKey() <= writeOffset) {
        for (Map.Entry<Long, WriteObjectRequest> entry : requestChunkMap.entrySet()) {
          if (entry.getKey() + entry.getValue().getChecksummedData().getContent().size()
                  > writeOffset
              || entry.getKey() == writeOffset) {
            request = entry.getValue();
            break;
          }
        }
      }
      return checkNotNull(request, "Request chunk not found for '%s'", resourceId);
    }

    /*
    If dataChunkMap is full, get the committedWriteOffset. This will make a API call to the
    server and add latency in this path/context. Since there are already chunks in flight,
    calling this API will not reduce overall throughput. It will throttle the upstream
    write call now rather than onFinalize, which is fine. This will also increase QPS to
    the GCS backend. The increase will be linear to number of chunks written, so that
    should also be fine.    */
    private void freeUpCommittedRequests(
        TreeMap<Long, WriteObjectRequest> requestChunkMap, long writeOffset) throws IOException {

      long committedWriteOffset = getCommittedWriteSizeWithRetries(uploadId);

      logger.atFinest().log(
          "Fetched committedWriteOffset: size:%d, writeOffset:%d, committedWriteOffset:%d",
          requestChunkMap.size(), writeOffset, committedWriteOffset);

      // check and remove chunks from dataChunkMap
      long buffersFreed = 0;
      while (requestChunkMap.size() > 0 && requestChunkMap.firstKey() < committedWriteOffset) {
        requestChunkMap.remove(requestChunkMap.firstKey());
        buffersFreed++;
      }
      logger.atFinest().log("Freed up %d buffers, resource:%s", buffersFreed, resourceId);
    }

    /** Handler for responses from the Insert streaming RPC. */
    private class InsertChunkResponseObserver
        implements ClientResponseObserver<WriteObjectRequest, WriteObjectResponse> {

      private final long writeOffset;
      private final String uploadId;
      // The response from the server, populated at the end of a successful streaming RPC.
      private WriteObjectResponse response;
      // The last transient error to occur during the streaming RPC.
      public Throwable transientError = null;
      // The last non-transient error to occur during the streaming RPC.
      public Throwable nonTransientError = null;

      // CountDownLatch tracking completion of the streaming RPC. Set on error, or once the
      // request stream is closed.
      final CountDownLatch done = new CountDownLatch(1);
      // CountDownLatch tracking readiness of the streaming RPC.
      final CountDownLatch ready = new CountDownLatch(1);

      InsertChunkResponseObserver(String uploadId, long writeOffset) {
        this.uploadId = uploadId;
        this.writeOffset = writeOffset;
      }

      public WriteObjectResponse getResponseOrThrow() throws IOException {
        if (hasNonTransientError()) {
          throw new IOException(
              String.format(
                  "Resumable upload failed for '%s' , uploadId : %s ", resourceId, uploadId),
              nonTransientError);
        }
        return checkNotNull(response, "Response not present for '%s'", resourceId);
      }

      boolean hasTransientError() {
        return transientError != null;
      }

      boolean hasNonTransientError() {
        return response == null && nonTransientError != null;
      }

      @Override
      public void onNext(WriteObjectResponse response) {
        this.response = response;
      }

      @Override
      public void onError(Throwable t) {
        Status status = Status.fromThrowable(t);
        Status.Code statusCode = status.getCode();
        if (TRANSIENT_ERRORS.contains(statusCode)) {
          transientError = t;
        }
        if (transientError == null) {
          nonTransientError =
              new IOException(
                  String.format(
                      "Caught exception for '%s', while uploading to uploadId %s at writeOffset %d."
                          + " Status: %s",
                      resourceId, uploadId, writeOffset, status.getDescription()),
                  t);
        }
        done.countDown();
      }

      @Override
      public void onCompleted() {
        done.countDown();
      }

      @Override
      public void beforeStart(
          ClientCallStreamObserver<WriteObjectRequest> clientCallStreamObserver) {
        clientCallStreamObserver.setOnReadyHandler(ready::countDown);
      }
    }

    /** Send a StartResumableWriteRequest and return the uploadId of the resumable write. */
    private String startResumableUploadWithRetries() throws IOException {
      try {
        WriteObjectSpec.Builder insertObjectSpecBuilder =
            WriteObjectSpec.newBuilder()
                .setResource(
                    Object.newBuilder()
                        .setBucket(GrpcChannelUtils.toV2BucketName(resourceId.getBucketName()))
                        .setName(resourceId.getObjectName())
                        .setContentType(createOptions.getContentType())
                        .putAllMetadata(encodeMetadata(createOptions.getMetadata()))
                        .build());
        if (writeConditions.hasContentGenerationMatch()) {
          insertObjectSpecBuilder.setIfGenerationMatch(writeConditions.getContentGenerationMatch());
        }
        if (writeConditions.hasMetaGenerationMatch()) {
          insertObjectSpecBuilder.setIfMetagenerationMatch(
              writeConditions.getMetaGenerationMatch());
        }

        Builder commonRequestParamsBuilder = null;
        if (requesterPaysProject != null) {
          commonRequestParamsBuilder =
              CommonRequestParams.newBuilder().setUserProject(requesterPaysProject);
        }

        StartResumableWriteRequest.Builder startResumableWriteRequestBuilder =
            StartResumableWriteRequest.newBuilder().setWriteObjectSpec(insertObjectSpecBuilder);
        if (commonRequestParamsBuilder != null) {
          startResumableWriteRequestBuilder.setCommonRequestParams(commonRequestParamsBuilder);
        }
        StartResumableWriteRequest request = startResumableWriteRequestBuilder.build();
        return ResilientOperation.retry(
            () -> startResumableUpload(request),
            backOffFactory.newBackOff(),
            RetryDeterminer.ALL_ERRORS,
            IOException.class);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(
            String.format("Failed to start resumable upload for '%s'", resourceId), e);
      }
    }

    private String startResumableUpload(StartResumableWriteRequest request) throws IOException {
      // It is essential to re-create the observer on retry, so that the CountDownLatch is not
      // re-used and we wait for the actual response instead of returning the last response/error
      SimpleResponseObserver<StartResumableWriteResponse> responseObserver =
          new SimpleResponseObserver<>();
      stub.withDeadlineAfter(START_RESUMABLE_WRITE_TIMEOUT.toMillis(), MILLISECONDS)
          .startResumableWrite(request, responseObserver);
      try {
        responseObserver.done.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(
            String.format("Interrupted while awaiting response during upload of '%s'", resourceId),
            e);
      }
      if (responseObserver.hasError()) {
        throw new IOException(responseObserver.getError());
      }
      return responseObserver.getResponse().getUploadId();
    }

    // TODO(b/150892988): Call this to find resume point after a transient error.
    private long getCommittedWriteSizeWithRetries(String uploadId) throws IOException {
      QueryWriteStatusRequest request =
          QueryWriteStatusRequest.newBuilder().setUploadId(uploadId).build();
      try {
        return ResilientOperation.retry(
            () -> getCommittedWriteSize(request),
            backOffFactory.newBackOff(),
            RetryDeterminer.ALL_ERRORS,
            IOException.class);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(
            String.format("Failed to get committed write size for '%s'", resourceId), e);
      }
    }

    private long getCommittedWriteSize(QueryWriteStatusRequest request) throws IOException {
      SimpleResponseObserver<QueryWriteStatusResponse> responseObserver =
          new SimpleResponseObserver<>();
      stub.withDeadlineAfter(QUERY_WRITE_STATUS_TIMEOUT.toMillis(), MILLISECONDS)
          .queryWriteStatus(request, responseObserver);
      try {
        responseObserver.done.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(
            String.format("Interrupted while awaiting response during upload of '%s'", resourceId),
            e);
      }
      if (responseObserver.hasError()) {
        throw new IOException(responseObserver.getError());
      }
      return responseObserver.getResponse().getPersistedSize();
    }

    /** Stream observer for single response RPCs. */
    private class SimpleResponseObserver<T> implements StreamObserver<T> {

      // The response from the server, populated at the end of a successful RPC.
      private T response;

      // The last error to occur during the RPC. Present only on error.
      private Throwable error;

      // CountDownLatch tracking completion of the RPC.
      final CountDownLatch done = new CountDownLatch(1);

      public T getResponse() {
        return checkNotNull(response, "Response not present for '%s'", resourceId);
      }

      boolean hasError() {
        return error != null || response == null;
      }

      public Throwable getError() {
        return checkNotNull(error, "Error not present for '%s'", resourceId);
      }

      @Override
      public void onNext(T response) {
        this.response = response;
      }

      @Override
      public void onError(Throwable t) {
        error = new IOException(String.format("Caught exception for '%s'", resourceId), t);
        done.countDown();
      }

      @Override
      public void onCompleted() {
        done.countDown();
      }
    }
  }

  /**
   * Returns non-null only if close() has been called and the underlying object has been
   * successfully committed.
   */
  @Override
  public GoogleCloudStorageItemInfo getItemInfo() {
    return completedItemInfo;
  }
}
