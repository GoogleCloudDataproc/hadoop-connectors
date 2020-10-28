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
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.google.storage.v1.ServiceConstants.Values.MAX_WRITE_CHUNK_BYTES;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toMap;

import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.cloud.hadoop.util.BaseAbstractGoogleAsyncWriteChannel;
import com.google.common.collect.ImmutableSet;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteStreams;
import com.google.google.storage.v1.ChecksummedData;
import com.google.google.storage.v1.InsertObjectRequest;
import com.google.google.storage.v1.InsertObjectSpec;
import com.google.google.storage.v1.Object;
import com.google.google.storage.v1.ObjectChecksums;
import com.google.google.storage.v1.QueryWriteStatusRequest;
import com.google.google.storage.v1.QueryWriteStatusResponse;
import com.google.google.storage.v1.StartResumableWriteRequest;
import com.google.google.storage.v1.StartResumableWriteResponse;
import com.google.google.storage.v1.StorageGrpc.StorageStub;
import com.google.protobuf.ByteString;
import com.google.protobuf.Int64Value;
import com.google.protobuf.UInt32Value;
import com.google.protobuf.util.Timestamps;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCallStreamObserver;
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
    extends BaseAbstractGoogleAsyncWriteChannel<Object>
    implements GoogleCloudStorageItemInfo.Provider {

  // Default size of the chunks in which to read the input stream.
  static final int MAX_READ_FROM_CHUNK_SIZE = 8196;

  private static final Duration START_RESUMABLE_WRITE_TIMEOUT = Duration.ofMinutes(1);
  private static final Duration QUERY_WRITE_STATUS_TIMEOUT = Duration.ofMinutes(1);
  private static final Duration WRITE_STREAM_TIMEOUT = Duration.ofMinutes(10);
  // Maximum number of automatic retries for each data chunk
  // when writing to underlying channel raises error.
  private static final int UPLOAD_RETRIES = 5;

  // Number of insert requests to retain, in case we need to rewind and resume an upload. Using too
  // small of a number could risk being unable to resume the write if the resume point is an
  // already-discarded buffer; and setting the value too high wastes RAM. Note: We could have a
  // more complex implementation that periodically queries the service to find out the last
  // committed offset, to determine what's safe to discard, but that would also impose a performance
  // penalty.
  private static final int NUMBER_OF_REQUESTS_TO_RETAIN = 5;
  // A set that defines all transient errors on which retry can be attempted.
  private static final ImmutableSet<Code> TRANSIENT_ERRORS =
      ImmutableSet.of(
          Code.DEADLINE_EXCEEDED, Code.RESOURCE_EXHAUSTED, Code.INTERNAL, Code.UNAVAILABLE);

  private final StorageStub stub;
  private final StorageResourceId resourceId;
  private final CreateObjectOptions createOptions;
  private final ObjectWriteConditions writeConditions;
  private final String requesterPaysProject;

  private GoogleCloudStorageItemInfo completedItemInfo = null;

  GoogleCloudStorageGrpcWriteChannel(
      StorageStub stub,
      ExecutorService threadPool,
      AsyncWriteChannelOptions channelOptions,
      StorageResourceId resourceId,
      CreateObjectOptions createOptions,
      ObjectWriteConditions writeConditions,
      String requesterPaysProject) {
    super(threadPool, channelOptions);
    this.stub = stub;
    this.resourceId = resourceId;
    this.createOptions = createOptions;
    this.writeConditions = writeConditions;
    this.requesterPaysProject = requesterPaysProject;
  }

  @Override
  protected String getResourceString() {
    return resourceId.toString();
  }

  @Override
  public void handleResponse(Object response) {
    checkArgument(
        !response.getBucket().isEmpty(),
        "Got response from service with empty/missing bucketName: %s",
        response);
    Map<String, byte[]> metadata =
        response.getMetadataMap().entrySet().stream()
            .collect(
                toMap(Map.Entry::getKey, entry -> BaseEncoding.base64().decode(entry.getValue())));

    byte[] md5Hash =
        response.getMd5Hash().length() > 0
            ? BaseEncoding.base64().decode(response.getMd5Hash())
            : null;
    byte[] crc32c =
        response.hasCrc32C()
            ? ByteBuffer.allocate(4).putInt(response.getCrc32C().getValue()).array()
            : null;

    completedItemInfo =
        new GoogleCloudStorageItemInfo(
            new StorageResourceId(response.getBucket(), response.getName()),
            Timestamps.toMillis(response.getTimeCreated()),
            Timestamps.toMillis(response.getUpdated()),
            response.getSize(),
            /* location= */ null,
            /* storageClass= */ null,
            response.getContentType(),
            response.getContentEncoding(),
            metadata,
            response.getGeneration(),
            response.getMetageneration(),
            new VerificationAttributes(md5Hash, crc32c));
  }

  @Override
  public void startUpload(InputStream pipeSource) {
    // Given that the two ends of the pipe must operate asynchronous relative
    // to each other, we need to start the upload operation on a separate thread.
    try {
      uploadOperation = threadPool.submit(new UploadOperation(pipeSource));
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to start upload for '%s'", resourceId), e);
    }
  }

  private class UploadOperation implements Callable<Object> {

    // Read end of the pipe.
    private final BufferedInputStream pipeSource;
    private final int MAX_BYTES_PER_MESSAGE = MAX_WRITE_CHUNK_BYTES.getNumber();

    private Hasher objectHasher;
    private String uploadId;
    private long writeOffset = 0;
    private InsertChunkResponseObserver responseObserver;
    // Holds list of most recent number of NUMBER_OF_REQUESTS_TO_RETAIN requests, so upload can be
    // rewound and re-sent upon transient errors.
    private final TreeMap<Long, ByteString> dataChunkMap = new TreeMap<>();

    UploadOperation(InputStream pipeSource) {
      this.pipeSource = new BufferedInputStream(pipeSource, MAX_BYTES_PER_MESSAGE);
      if (channelOptions.isGrpcChecksumsEnabled()) {
        objectHasher = Hashing.crc32c().newHasher();
      }
    }

    @Override
    public Object call() throws IOException {
      // Try-with-resource will close this end of the pipe so that
      // the writer at the other end will not hang indefinitely.
      // Send the initial StartResumableWrite request to get an uploadId.
      uploadId = startResumableUpload();
      Retryer<Object> retryer = getRetryer();
      try (InputStream ignore = pipeSource) {
        return retryer.call(this::doResumableUpload);
      } catch (Exception e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        throw new IOException(
            String.format("Resumable upload failed for '%s'", getResourceString()), e);
      }
    }

    private Object doResumableUpload() throws IOException, InterruptedException {
      // Only request committed size for the first insert request.
      if (writeOffset > 0) {
        writeOffset = getCommittedWriteSize(uploadId);
      }
      responseObserver = new InsertChunkResponseObserver(uploadId, writeOffset);
      // TODO(b/151184800): Implement per-message timeout, in addition to stream timeout.
      StreamObserver<InsertObjectRequest> requestStreamObserver =
          stub.withDeadlineAfter(WRITE_STREAM_TIMEOUT.toMillis(), MILLISECONDS)
              .insertObject(responseObserver);

      // Wait for streaming RPC to become ready for upload.
      try {
        responseObserver.ready.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(
            String.format(
                "Streaming RPC failed to become ready for resumable upload for '%s'",
                getResourceString()),
            e);
      }

      boolean objectFinalized = false;
      while (!objectFinalized) {
        InsertObjectRequest insertRequest;
        if (dataChunkMap.size() > 0 && dataChunkMap.lastKey() >= writeOffset) {
          insertRequest = buildRequestFromBufferedDataChunk(dataChunkMap, writeOffset);
          writeOffset += insertRequest.getChecksummedData().getContent().size();
        } else {
          ByteString data =
              ByteString.readFrom(
                  ByteStreams.limit(pipeSource, MAX_BYTES_PER_MESSAGE), MAX_READ_FROM_CHUNK_SIZE);
          dataChunkMap.put(writeOffset, data);
          if (dataChunkMap.size() >= NUMBER_OF_REQUESTS_TO_RETAIN) {
            dataChunkMap.remove(dataChunkMap.firstKey());
          }
          insertRequest = buildInsertRequest(writeOffset, data, false);
          writeOffset += data.size();
        }
        requestStreamObserver.onNext(insertRequest);
        objectFinalized = insertRequest.getFinishWrite();

        if (responseObserver.hasTransientError()
            || responseObserver.hasNonTransientError()) {
          requestStreamObserver.onError(
              responseObserver.hasTransientError()
                  ? responseObserver.transientError
                  : responseObserver.nonTransientError);
          break;
        } else if (objectFinalized) {
          requestStreamObserver.onCompleted();
        }
      }

      responseObserver.done.await();
      if (responseObserver.hasTransientError()) {
        throw new IOException(responseObserver.transientError);
      }

      return responseObserver.getResponseOrThrow();
    }

    private InsertObjectRequest buildInsertRequest(
        long writeOffset, ByteString dataChunk, boolean resumeFromFailedInsert) {
      InsertObjectRequest.Builder requestBuilder =
          InsertObjectRequest.newBuilder().setUploadId(uploadId).setWriteOffset(writeOffset);

      if (dataChunk.size() > 0) {
        ChecksummedData.Builder requestDataBuilder =
            ChecksummedData.newBuilder().setContent(dataChunk);
        if (channelOptions.isGrpcChecksumsEnabled()) {
          if (!resumeFromFailedInsert) {
            updateObjectHash(dataChunk);
          }
          requestDataBuilder.setCrc32C(UInt32Value.newBuilder().setValue(getChunkHash(dataChunk)));
        }
        requestBuilder.setChecksummedData(requestDataBuilder);
      }

      if (dataChunk.size() < MAX_BYTES_PER_MESSAGE) {
        requestBuilder.setFinishWrite(true);
        if (channelOptions.isGrpcChecksumsEnabled()) {
          requestBuilder.setObjectChecksums(
              ObjectChecksums.newBuilder()
                  .setCrc32C(UInt32Value.newBuilder().setValue(objectHasher.hash().asInt())));
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
    // querying the current committed offset.
    private InsertObjectRequest buildRequestFromBufferedDataChunk(
        TreeMap<Long, ByteString> dataChunkMap, long writeOffset) throws IOException {
      // Resume will only work if the first request builder in the cache carries an offset
      // not greater than the current writeOffset.
      InsertObjectRequest request = null;
      if (dataChunkMap.size() > 0 && dataChunkMap.firstKey() <= writeOffset) {
        for (Map.Entry<Long, ByteString> entry : dataChunkMap.entrySet()) {
          if (entry.getKey() + entry.getValue().size() > writeOffset) {
            Long writeOffsetToResume = entry.getKey();
            ByteString chunkData = entry.getValue();
            request = buildInsertRequest(writeOffsetToResume, chunkData, true);
            break;
          }
        }
      }
      if (request == null) {
        throw new IOException(
            String.format(
                "Didn't have enough data buffered for attempt to resume upload for"
                    + " uploadID %s: last committed offset=%s, earliest buffered"
                    + " offset=%s. Upload must be restarted from the beginning.",
                uploadId, writeOffset, dataChunkMap.firstKey()));
      }
      return request;
    }

    /** Handler for responses from the Insert streaming RPC. */
    private class InsertChunkResponseObserver
        implements ClientResponseObserver<InsertObjectRequest, Object> {

      private final long writeOffset;
      private final String uploadId;
      // The response from the server, populated at the end of a successful streaming RPC.
      private Object response;
      // The last transient error to occur during the streaming RPC.
      public Throwable transientError = null;
      // The last non-transient error to occur during the streaming RPC.
      public Throwable nonTransientError = null;

      // CountDownLatch tracking completion of the streaming RPC. Set on error, or once the request
      // stream is closed.
      final CountDownLatch done = new CountDownLatch(1);
      // CountDownLatch tracking readiness of the streaming RPC.
      final CountDownLatch ready = new CountDownLatch(1);

      InsertChunkResponseObserver(String uploadId, long writeOffset) {
        this.uploadId = uploadId;
        this.writeOffset = writeOffset;
      }

      public Object getResponseOrThrow() throws IOException {
        if (hasNonTransientError()) {
          throw new IOException(
              String.format("Resumable upload failed for '%s'", getResourceString()),
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
      public void onNext(Object response) {
        this.response = response;
      }

      @Override
      public void onError(Throwable t) {
        Status s = Status.fromThrowable(t);
        String statusDesc = s == null ? "" : s.getDescription();

        if (t.getClass() == StatusException.class || t.getClass() == StatusRuntimeException.class) {
          Code code =
              t.getClass() == StatusException.class
                  ? ((StatusException) t).getStatus().getCode()
                  : ((StatusRuntimeException) t).getStatus().getCode();
          if (TRANSIENT_ERRORS.contains(code)) {
            transientError = t;
          }
        }
        if (transientError == null) {
          nonTransientError =
              new IOException(
                  String.format(
                      "Caught exception for '%s', while uploading to uploadId %s at writeOffset %d."
                          + " Status: %s",
                      resourceId, uploadId, writeOffset, statusDesc),
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
          ClientCallStreamObserver<InsertObjectRequest> clientCallStreamObserver) {
        clientCallStreamObserver.setOnReadyHandler(() -> ready.countDown());
      }
    }

    /** Send a StartResumableWriteRequest and return the uploadId of the resumable write. */
    private String startResumableUpload() throws IOException {
      InsertObjectSpec.Builder insertObjectSpecBuilder =
          InsertObjectSpec.newBuilder()
              .setResource(
                  Object.newBuilder()
                      .setBucket(resourceId.getBucketName())
                      .setName(resourceId.getObjectName())
                      .setContentType(createOptions.getContentType())
                      .putAllMetadata(encodeMetadata(createOptions.getMetadata()))
                      .build());
      if (writeConditions.hasContentGenerationMatch()) {
        insertObjectSpecBuilder.setIfGenerationMatch(
            Int64Value.newBuilder().setValue(writeConditions.getContentGenerationMatch()));
      }
      if (writeConditions.hasMetaGenerationMatch()) {
        insertObjectSpecBuilder.setIfMetagenerationMatch(
            Int64Value.newBuilder().setValue(writeConditions.getMetaGenerationMatch()));
      }
      if (requesterPaysProject != null) {
        insertObjectSpecBuilder.setUserProject(requesterPaysProject);
      }
      StartResumableWriteRequest request =
          StartResumableWriteRequest.newBuilder()
              .setInsertObjectSpec(insertObjectSpecBuilder)
              .build();

      SimpleResponseObserver<StartResumableWriteResponse> responseObserver =
          new SimpleResponseObserver<>();
      Retryer retryer = getRetryer();
      try {
        retryer.call(
            () -> {
              stub.withDeadlineAfter(START_RESUMABLE_WRITE_TIMEOUT.toMillis(), MILLISECONDS)
                  .startResumableWrite(request, responseObserver);
              responseObserver.done.await();
              if (responseObserver.hasError()) {
                throw new IOException(responseObserver.getError());
              }

              return null;
            });
      } catch (Exception e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        throw new IOException(
            String.format("Failed to start resumable upload for '%s'", getResourceString()), e);
      }

      return responseObserver.getResponse().getUploadId();
    }

    // TODO(b/150892988): Call this to find resume point after a transient error.
    private long getCommittedWriteSize(String uploadId) throws IOException {
      QueryWriteStatusRequest request =
          QueryWriteStatusRequest.newBuilder().setUploadId(uploadId).build();

      SimpleResponseObserver<QueryWriteStatusResponse> responseObserver =
          new SimpleResponseObserver<>();
      Retryer retryer = getRetryer();
      try {
        retryer.call(
            () -> {
              stub.withDeadlineAfter(QUERY_WRITE_STATUS_TIMEOUT.toMillis(), MILLISECONDS)
                  .queryWriteStatus(request, responseObserver);
              responseObserver.done.await();
              if (responseObserver.hasError()) {
                throw new IOException(responseObserver.getError());
              }
              return null;
            });
      } catch (Exception e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        throw new IOException(
            String.format("Failed to get committed write size for '%s'", getResourceString()), e);
      }

      return responseObserver.getResponse().getCommittedSize();
    }

    private Retryer<Object> getRetryer() {
      return RetryerBuilder.<Object>newBuilder()
          .retryIfExceptionOfType(IOException.class)
          .withWaitStrategy(WaitStrategies.exponentialWait(2, 20, SECONDS))
          .withStopStrategy(StopStrategies.stopAfterAttempt(UPLOAD_RETRIES))
          .build();
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
    return this.completedItemInfo;
  }
}
