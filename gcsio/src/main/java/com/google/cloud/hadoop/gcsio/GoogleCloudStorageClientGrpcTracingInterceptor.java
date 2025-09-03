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

import static com.google.common.base.Preconditions.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.flogger.GoogleLogger;
import com.google.gson.Gson;
import com.google.protobuf.MessageLite;
import com.google.storage.v2.ReadObjectRequest;
import com.google.storage.v2.ReadObjectResponse;
import com.google.storage.v2.StartResumableWriteRequest;
import com.google.storage.v2.StartResumableWriteResponse;
import com.google.storage.v2.WriteObjectRequest;
import com.google.storage.v2.WriteObjectResponse;
import io.grpc.Attributes;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientStreamTracer;
import io.grpc.ClientStreamTracer.StreamInfo;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import javax.annotation.Nonnull;

/** Interceptor to create a trace of the lifecycle of GRPC api calls. */
@VisibleForTesting
public class GoogleCloudStorageClientGrpcTracingInterceptor implements ClientInterceptor {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();
  public static final String IDEMPOTENCY_TOKEN_HEADER = "x-goog-gcs-idempotency-token";
  private static final String DEFAULT_INVOCATION_ID = "NOT-FOUND";
  private static final Metadata.Key<String> idempotencyKey =
      Metadata.Key.of(IDEMPOTENCY_TOKEN_HEADER, Metadata.ASCII_STRING_MARSHALLER);

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
    String rpcMethodName = method.getBareMethodName();

    TrackingStreamTracer streamTracer = getStreamTracer(rpcMethodName);
    return new SimpleForwardingClientCall<ReqT, RespT>(
        next.newCall(
            method,
            callOptions.withStreamTracerFactory(
                new ClientStreamTracer.Factory() {
                  @Override
                  public ClientStreamTracer newClientStreamTracer(
                      StreamInfo info, Metadata headers) {
                    return streamTracer;
                  }
                }))) {
      @Override
      public void sendMessage(ReqT message) {
        try {
          streamTracer.traceRequestMessage((MessageLite) message);
        } finally {
          super.sendMessage(message);
        }
      }

      @Override
      public void start(Listener<RespT> responseListener, Metadata headers) {
        super.start(
            new SimpleForwardingClientCallListener<RespT>(responseListener) {
              @Override
              public void onMessage(RespT message) {
                try {
                  streamTracer.traceResponseMessage((MessageLite) message);
                } finally {
                  super.onMessage(message);
                }
              }

              @Override
              public void onClose(Status status, Metadata trailers) {
                try {
                  streamTracer.statusOnClose(status);
                } finally {
                  super.onClose(status, trailers);
                }
              }
            },
            headers);
      }
    };
  }

  private TrackingStreamTracer getStreamTracer(String rpcMethodName) {
    GrpcStreamType type = GrpcStreamType.getTypeFromName(rpcMethodName);
    /**
     * We are choosing a tracer based on stream type. A designated stream tracer for specific type
     * of stream helps in casting the request/responses to desired types. It also helps in adding
     * custom logic too e.g. WriteObject stream have uploadId common across the stream and need to
     * maintain it in tracers state which is not applicable for ReadObject stream.
     */
    switch (type) {
      case START_RESUMABLE_WRITE:
        return new StartResumableUploadStreamTracer(rpcMethodName);
      case WRITE_OBJECT:
        return new WriteObjectStreamTracer(rpcMethodName);
      case READ_OBJECT:
        return new ReadObjectStreamTracer(rpcMethodName);
      default:
        return new TrackingStreamTracer(rpcMethodName);
    }
  }

  /**
   * ClientStreamTracer support added in grpc helps in tracing the flow of messages over socket and
   * have less control over the actual message. Via this customised Tracer of every stream type we
   * added support to trace the messages sent over stream and also extract and log the meaningful
   * information from it i.e. invocationId header, request parameters. reponse values etc.
   *
   * <p>Via {@link #logRequestMessage(MessageLite)} and {@link #logRequestMessage(MessageLite)}
   * hooks we associate request and response messages to a stream.
   *
   * <p>{@link #statusOnClose(Status)} helps in tracing the closing status of stream.
   */
  private class TrackingStreamTracer extends ClientStreamTracer {

    private final Gson gson = new Gson();
    private final String rpcMethod;
    private Metadata headers;
    protected int requestMessageCounter = 0;
    protected int responseMessageCounter = 0;

    TrackingStreamTracer(String rpcMethod) {
      this.rpcMethod = rpcMethod;
    }

    private void updateRequestCounter() {
      requestMessageCounter++;
    }

    private void updateResponseCounter() {
      responseMessageCounter++;
    }

    /**
     * This helps in tracing the actual message sent over the stream. By adding this hook in {@link
     * ClientCall#sendMessage(Object)} of ClientCall we can associate request to a stream tracer.
     *
     * @param message Message which is supposed to be sent over the wire.
     */
    public void traceRequestMessage(MessageLite message) {
      logRequestMessage(message);
      updateRequestCounter();
    }

    /**
     * This helps in tracing actual message received over the stream by adding a hook in {@link
     * ClientCall.Listener#onMessage(Object)} of ResponseListener. This hook helps in mapping the
     * response message to StreamTracer.
     *
     * @param message Message which was received from server.
     */
    public void traceResponseMessage(MessageLite message) {
      logResponseMessage(message);
      updateResponseCounter();
    }

    public void logRequestMessage(MessageLite message) {}

    public void logResponseMessage(MessageLite message) {}

    public void statusOnClose(Status status) {
      logger.atInfo().log(
          "%s",
          toJson(
              getRequestContext()
                  .put(GoogleCloudStorageTracingFields.STATUS.name, status)
                  .put(GoogleCloudStorageTracingFields.STREAM_OPERATION.name, "onClose")
                  .build()));
    }

    /** The stream is being created on a ready transport. */
    @Override
    public void streamCreated(Attributes transportAttrs, Metadata headers) {
      this.headers = headers;
      super.streamCreated(transportAttrs, headers);
    }

    protected ImmutableMap.Builder<String, Object> getRequestTrackingInfo() {
      return getRequestContext()
          .put(GoogleCloudStorageTracingFields.REQUEST_COUNTER.name, requestMessageCounter)
          .put(GoogleCloudStorageTracingFields.STREAM_OPERATION.name, "request");
    }

    protected ImmutableMap.Builder<String, Object> getResponseTrackingInfo() {
      return getRequestContext()
          .put(GoogleCloudStorageTracingFields.RESPONSE_COUNTER.name, responseMessageCounter)
          .put(GoogleCloudStorageTracingFields.STREAM_OPERATION.name, "response");
    }

    protected String toJson(ImmutableMap<String, Object> eventDetails) {
      return gson.toJson(eventDetails);
    }

    protected String getInvocationId() {
      return headers != null ? headers.get(idempotencyKey) : DEFAULT_INVOCATION_ID;
    }

    private ImmutableMap.Builder<String, Object> getRequestContext() {
      return new ImmutableMap.Builder<String, Object>()
          .put(GoogleCloudStorageTracingFields.RPC_METHOD.name, rpcMethod)
          .put(GoogleCloudStorageTracingFields.IDEMPOTENCY_TOKEN.name, getInvocationId());
    }
  }

  private class StartResumableUploadStreamTracer extends TrackingStreamTracer {
    private StorageResourceId resourceId;

    StartResumableUploadStreamTracer(String rpcMethod) {
      super(rpcMethod);
    }

    @Override
    public void logRequestMessage(MessageLite message) {
      StartResumableWriteRequest request = (StartResumableWriteRequest) message;
      this.resourceId =
          new StorageResourceId(
              request.getWriteObjectSpec().getResource().getBucket(),
              request.getWriteObjectSpec().getResource().getName(),
              request.getWriteObjectSpec().getIfGenerationMatch());
      logger.atInfo().log(
          "%s",
          toJson(
              getRequestTrackingInfo()
                  .put(GoogleCloudStorageTracingFields.RESOURCE.name, resourceId)
                  .build()));
    }

    @Override
    public void logResponseMessage(MessageLite message) {
      StartResumableWriteResponse response = (StartResumableWriteResponse) message;
      logger.atInfo().log(
          "%s",
          toJson(
              getResponseTrackingInfo()
                  .put(GoogleCloudStorageTracingFields.RESOURCE.name, resourceId)
                  .put(GoogleCloudStorageTracingFields.UPLOAD_ID.name, response.getUploadId())
                  .build()));
    }
  }

  private class WriteObjectStreamTracer extends TrackingStreamTracer {

    private static final String STREAM_UPLOAD_ID_NOT_FOUND = "STREAM_UPLOAD_ID_NOT_FOUND";
    private String streamUploadId = STREAM_UPLOAD_ID_NOT_FOUND;

    WriteObjectStreamTracer(String rpcMethod) {
      super(rpcMethod);
    }

    @Override
    public void logRequestMessage(MessageLite message) {
      WriteObjectRequest request = (WriteObjectRequest) message;
      String uploadId = request.getUploadId();
      if (!Strings.isNullOrEmpty(uploadId)) {
        updateUploadId(request.getUploadId());
      }
      logger.atInfo().log(
          "%s",
          toJson(
              getRequestTrackingInfo()
                  .put(GoogleCloudStorageTracingFields.UPLOAD_ID.name, request.getUploadId())
                  .put(GoogleCloudStorageTracingFields.WRITE_OFFSET.name, request.getWriteOffset())
                  .put(
                      GoogleCloudStorageTracingFields.FINALIZE_WRITE.name, request.getFinishWrite())
                  .put(
                      GoogleCloudStorageTracingFields.CONTENT_LENGTH.name,
                      request.getChecksummedData().getContent().size())
                  .build()));
    }

    @Override
    public void logResponseMessage(MessageLite message) {

      WriteObjectResponse response = (WriteObjectResponse) message;
      logger.atInfo().log(
          "%s",
          toJson(
              getResponseTrackingInfo()
                  .put(GoogleCloudStorageTracingFields.UPLOAD_ID.name, this.streamUploadId)
                  .put(
                      GoogleCloudStorageTracingFields.PERSISTED_SIZE.name,
                      response.getPersistedSize())
                  .build()));
    }

    private void updateUploadId(@Nonnull String uploadId) {
      if (streamUploadId.equals(STREAM_UPLOAD_ID_NOT_FOUND)) {
        this.streamUploadId = uploadId;
      }
      checkState(
          uploadId.equals(streamUploadId),
          String.format(
              "Write stream should have unique uploadId associated with each chunk request. Expected was %s got %s",
              streamUploadId, uploadId));
    }
  }

  private class ReadObjectStreamTracer extends TrackingStreamTracer {

    private StorageResourceId resourceId;
    private long readOffset;
    private long readLimit;
    private long totalBytesRead = 0;

    ReadObjectStreamTracer(String rpcMethod) {
      super(rpcMethod);
    }

    private void updateReadRequestContext(ReadObjectRequest request) {
      this.resourceId =
          new StorageResourceId(request.getBucket(), request.getObject(), request.getGeneration());
      this.readOffset = request.getReadOffset();
      this.readLimit = request.getReadLimit();
    }

    @Override
    public void logRequestMessage(MessageLite message) {
      ReadObjectRequest request = (ReadObjectRequest) message;

      updateReadRequestContext(request);
      logger.atInfo().log(
          "%s",
          toJson(
              getRequestTrackingInfo()
                  .put(GoogleCloudStorageTracingFields.RESOURCE.name, resourceId)
                  .put(GoogleCloudStorageTracingFields.READ_OFFSET.name, readOffset)
                  .put(GoogleCloudStorageTracingFields.READ_LIMIT.name, readLimit)
                  .build()));
    }

    @Override
    public void logResponseMessage(MessageLite message) {
      ReadObjectResponse response = (ReadObjectResponse) message;
      int bytesRead = response.getChecksummedData().getContent().size();
      logger.atInfo().log(
          "%s",
          toJson(
              getResponseTrackingInfo()
                  .put(GoogleCloudStorageTracingFields.RESOURCE.name, resourceId)
                  .put(GoogleCloudStorageTracingFields.READ_OFFSET.name, readOffset)
                  .put(GoogleCloudStorageTracingFields.READ_LIMIT.name, readLimit)
                  .put(GoogleCloudStorageTracingFields.REQUEST_START_OFFSET.name, totalBytesRead)
                  .put(GoogleCloudStorageTracingFields.BYTES_READ.name, bytesRead)
                  .build()));
      totalBytesRead += bytesRead;
    }
  }
}
