/*
 * Copyright 2023 Google LLC
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

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageGrpcWriteChannel.TRANSIENT_ERRORS;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.common.annotations.VisibleForTesting;
import com.google.storage.v2.StorageGrpc;
import com.google.storage.v2.StorageGrpc.StorageStub;
import com.google.storage.v2.WriteObjectRequest;
import com.google.storage.v2.WriteObjectResponse;
import io.grpc.ClientCall;
import io.grpc.Status;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;

/**
 * Manages WriteObject rpc stream. Provide operation for opening, writing and closing the stream.
 */
@VisibleForTesting
public class GoogleCloudStorageContentWriteStream {

  private final StorageResourceId resourceId;
  private final StorageStub stub;
  private final GoogleCloudStorageOptions storageOptions;
  private final AsyncWriteChannelOptions channelOptions;
  private final String uploadId;
  private final long writeOffset;
  private final Watchdog watchdog;
  private InsertChunkResponseObserver responseObserver;
  private StreamObserver<WriteObjectRequest> requestStreamObserver;

  // Keeps track of inflight request in a stream
  private int inflightRequests = 0;

  public GoogleCloudStorageContentWriteStream(
      StorageResourceId resourceId,
      GoogleCloudStorageOptions storageOptions,
      StorageStub stub,
      String uploadId,
      long writeOffset,
      Watchdog watchdog) {
    this.resourceId = resourceId;
    this.stub = stub;
    this.uploadId = uploadId;
    this.writeOffset = writeOffset;
    this.responseObserver = new InsertChunkResponseObserver(uploadId, writeOffset);
    this.storageOptions = storageOptions;
    this.channelOptions = storageOptions.getWriteChannelOptions();
    this.watchdog = watchdog;
  }

  public void openStream() throws IOException {
    if (isOpen()) {
      throw new IOException(
          String.format(
              "Stream is already open for resourceId %s with uploadId %s and writeOffset %d",
              resourceId, uploadId, writeOffset));
    }
    try {
      StorageStub storageStub = getStorageStubWithTracking(channelOptions.getGrpcWriteTimeout());
      ClientCall clientCall =
          storageStub
              .getChannel()
              .newCall(StorageGrpc.getWriteObjectMethod(), stub.getCallOptions());
      StreamObserver<WriteObjectRequest> writeObjectRequestStreamObserver =
          ClientCalls.asyncClientStreamingCall(clientCall, responseObserver);

      // Wait for streaming RPC to become ready for upload.
      // wait for 1 min for the channel to be ready. Else bail out
      if (!responseObserver.ready.await(60 * 1000, MILLISECONDS)) {
        throw new IOException(
            String.format(
                "Timed out while awaiting ready on responseObserver for '%s' with UploadID '%s' and writeOffset %d",
                resourceId, responseObserver.uploadId, writeOffset));
      }

      this.requestStreamObserver =
          watchdog.watch(
              clientCall,
              writeObjectRequestStreamObserver,
              channelOptions.getGrpcWriteMessageTimeout());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(
          String.format(
              "Interrupted while awaiting ready on responseObserver for '%s' with UploadID '%s' and writeOffset %d",
              resourceId, responseObserver.uploadId, writeOffset));
    }
  }

  private StorageStub getStorageStubWithTracking(Duration grpcWriteTimeout) {
    StorageStub stubWithDeadline =
        stub.withDeadlineAfter(grpcWriteTimeout.toMillis(), MILLISECONDS);

    if (!this.storageOptions.isTraceLogEnabled()) {
      return stubWithDeadline;
    }

    return stubWithDeadline.withInterceptors(
        new GoogleCloudStorageGrpcTracingInterceptor(
            GrpcRequestTracingInfo.getWriteRequestTraceInfo(this.resourceId.getObjectName())));
  }

  public void writeChunk(WriteObjectRequest request) throws IOException {
    if (!isOpen()) {
      throw new IOException(
          String.format(
              "Can't write without stream being open for '%s' with UploadID '%s' and writeOffset %d",
              resourceId, uploadId, writeOffset));
    }
    requestStreamObserver.onNext(request);
    inflightRequests += 1;
    throwIfResponseObserverErrored();
  }

  public int getInflightRequestCount() {
    return inflightRequests;
  }

  public boolean isOpen() {
    return (responseObserver != null
            && responseObserver.isReady()
            && !responseObserver.isComplete())
        ? true
        : false;
  }

  public WriteObjectResponse closeStream() throws IOException {
    try {
      requestStreamObserver.onCompleted();
      responseObserver.done.await();
      throwIfResponseObserverErrored();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(
          String.format(
              "Interrupted while awaiting response during upload of '%s' with UploadID '%s' and writeOffset %d",
              resourceId, responseObserver.uploadId, writeOffset));
    }
    return responseObserver.getResponseOrThrow();
  }

  private void throwIfResponseObserverErrored() throws IOException {

    if (responseObserver.hasTransientError() || responseObserver.hasNonTransientError()) {
      Throwable error =
          responseObserver.hasTransientError()
              ? responseObserver.transientError
              : responseObserver.nonTransientError;
      requestStreamObserver.onError(error);
      throw new IOException(
          String.format(
              "Got transient error for '%s' with UploadID '%s' and writeOffset %d",
              resourceId, responseObserver.uploadId, writeOffset),
          error);
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
                "Resumable upload failed for '%s' , uploadId : %s, writeOffset : %d ",
                resourceId, uploadId, writeOffset),
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
    public void beforeStart(ClientCallStreamObserver<WriteObjectRequest> clientCallStreamObserver) {
      clientCallStreamObserver.setOnReadyHandler(ready::countDown);
    }

    public boolean isComplete() {
      return done.getCount() == 0 ? true : false;
    }

    public boolean isReady() {
      return ready.getCount() == 0 ? true : false;
    }
  }
}
