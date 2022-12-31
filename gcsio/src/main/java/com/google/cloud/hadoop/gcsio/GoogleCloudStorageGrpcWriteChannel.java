/*
 * Copyright 2019 Google LLC
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

package com.google.cloud.hadoop.gcsio;

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl.encodeMetadata;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.storage.v2.ServiceConstants.Values.MAX_WRITE_CHUNK_BYTES;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toMap;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl.BackOffFactory;
import com.google.cloud.hadoop.util.AbstractGoogleAsyncWriteChannel;
import com.google.cloud.hadoop.util.ResilientOperation;
import com.google.cloud.hadoop.util.RetryDeterminer;
import com.google.common.collect.ImmutableSet;
import com.google.common.flogger.GoogleLogger;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteStreams;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import com.google.storage.v2.ChecksummedData;
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
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

/** Implements WritableByteChannel to provide write access to GCS via gRPC. */
public final class GoogleCloudStorageGrpcWriteChannel
    extends AbstractGoogleAsyncWriteChannel<WriteObjectResponse>
    implements GoogleCloudStorageItemInfo.Provider {

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

  private final StorageStub stub;

  private final StorageResourceId resourceId;
  private final CreateObjectOptions createOptions;
  private final ObjectWriteConditions writeConditions;
  private final String requesterPaysProject;
  private final BackOffFactory backOffFactory;
  private final Watchdog watchdog;
  private final GoogleCloudStorageOptions storageOptions;

  private GoogleCloudStorageItemInfo completedItemInfo = null;

  GoogleCloudStorageGrpcWriteChannel(
      StorageStubProvider stubProvider,
      ExecutorService threadPool,
      GoogleCloudStorageOptions storageOptions,
      StorageResourceId resourceId,
      CreateObjectOptions createOptions,
      Watchdog watchdog,
      ObjectWriteConditions writeConditions,
      String requesterPaysProject,
      BackOffFactory backOffFactory) {
    super(threadPool, storageOptions.getWriteChannelOptions());
    this.storageOptions = storageOptions;
    this.stub = stubProvider.newAsyncStub(resourceId.getBucketName());
    this.resourceId = resourceId;
    this.createOptions = createOptions;
    this.writeConditions = writeConditions;
    this.requesterPaysProject = requesterPaysProject;
    this.backOffFactory = backOffFactory;
    this.watchdog = watchdog;
  }

  @Override
  protected String getResourceString() {
    return resourceId.toString();
  }

  @Override
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

  @Override
  public void startUpload(InputStream pipeSource) {
    // Given that the two ends of the pipe must operate asynchronous relative
    // to each other, we need to start the upload operation on a separate thread.
    try {
      uploadOperation =
          threadPool.submit(
              new UploadOperation(
                  pipeSource, this.resourceId, this.storageOptions.isTraceLogEnabled()));
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to start upload for '%s'", resourceId), e);
    }
  }

  private class UploadOperation implements Callable<WriteObjectResponse> {

    // Read end of the pipe.
    private final BufferedInputStream pipeSource;
    private final int MAX_BYTES_PER_MESSAGE = MAX_WRITE_CHUNK_BYTES.getNumber();
    private final StorageResourceId resourceId;
    private final boolean tracingEnabled;

    private Hasher objectHasher;
    private String uploadId;
    private long writeOffset = 0;
    // Holds list of most recent number of NUMBER_OF_REQUESTS_TO_RETAIN requests, so upload can
    // be rewound and re-sent upon transient errors.
    private final TreeMap<Long, WriteObjectRequest> requestChunkMap = new TreeMap<>();

    UploadOperation(InputStream pipeSource, StorageResourceId resourceId, boolean tracingEnabled) {
      this.resourceId = resourceId;
      this.tracingEnabled = tracingEnabled;
      this.pipeSource = new BufferedInputStream(pipeSource, MAX_BYTES_PER_MESSAGE);
      if (channelOptions.isGrpcChecksumsEnabled()) {
        objectHasher = Hashing.crc32c().newHasher();
      }
    }

    @Override
    public WriteObjectResponse call() throws IOException {
      // Try-with-resource will close this end of the pipe so that
      // the writer at the other end will not hang indefinitely.
      // Send the initial StartResumableWrite request to get an uploadId.
      try (InputStream ignore = pipeSource) {
        uploadId = startResumableUploadWithRetries();
        return ResilientOperation.retry(
            this::doResumableUpload,
            backOffFactory.newBackOff(),
            this::isRetriableError,
            IOException.class);
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

    private StorageStub getStorageStubWithTracking(long grpcWriteTimeoutMilliSeconds) {
      StorageStub stubWithDeadline =
          stub.withDeadlineAfter(grpcWriteTimeoutMilliSeconds, MILLISECONDS);

      if (!this.tracingEnabled) {
        return stubWithDeadline;
      }

      return stubWithDeadline.withInterceptors(
          new GoogleCloudStorageGrpcTracingInterceptor(
              GrpcRequestTracingInfo.getWriteRequestTraceInfo(this.resourceId.getObjectName())));
    }

    private WriteObjectResponse doResumableUpload() throws IOException {
      // Only request committed size for the first insert request.
      if (writeOffset > 0) {
        writeOffset = getCommittedWriteSizeWithRetries(uploadId);
      }
      StorageStub storageStub = getStorageStubWithTracking(channelOptions.getGrpcWriteTimeout());
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
          ByteString data =
              ByteString.readFrom(
                  ByteStreams.limit(pipeSource, MAX_BYTES_PER_MESSAGE), MAX_BYTES_PER_MESSAGE);
          insertRequest = buildInsertRequest(writeOffset, data, false);
          requestChunkMap.put(writeOffset, insertRequest);
          writeOffset += data.size();
        }
        if (insertRequest != null) {
          requestStreamObserver.onNext(insertRequest);
          objectFinalized = insertRequest.getFinishWrite();
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
            Long writeOffsetToResume = entry.getKey();
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
          "Fetched committedWriteOffset: size:%d, numBuffers:%d, writeOffset:%d, committedWriteOffset:%d",
          requestChunkMap.size(),
          channelOptions.getNumberOfBufferedRequests(),
          writeOffset,
          committedWriteOffset);

      // check and remove chunks from dataChunkMap
      while (requestChunkMap.size() > 0 && requestChunkMap.firstKey() < committedWriteOffset) {
        logger.atFinest().log(
            "clearing dataChunkMap one buffer at a time, size: %d, firstKey:%d, committedwriteOffset:%d",
            requestChunkMap.size(), requestChunkMap.firstKey(), committedWriteOffset);
        requestChunkMap.remove(requestChunkMap.firstKey());
      }
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

        StartResumableWriteRequest.Builder startResumableWriteRequestBuilder =
            StartResumableWriteRequest.newBuilder().setWriteObjectSpec(insertObjectSpecBuilder);
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
      getStorageStubWithTracking(START_RESUMABLE_WRITE_TIMEOUT.toMillis())
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
      getStorageStubWithTracking(QUERY_WRITE_STATUS_TIMEOUT.toMillis())
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
