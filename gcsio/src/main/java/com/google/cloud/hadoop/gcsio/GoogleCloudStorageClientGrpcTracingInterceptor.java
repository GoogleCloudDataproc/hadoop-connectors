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

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
    String rpcMethodName = method.getBareMethodName();
    TrackingStreamTracer streamTracer =
        getStreamTracer(GrpcStreamType.getTypeFromName(rpcMethodName), rpcMethodName);
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
        streamTracer.traceRequestMessage((MessageLite) message);
        super.sendMessage(message);
      }

      @Override
      public void start(Listener<RespT> responseListener, Metadata headers) {
        super.start(
            new SimpleForwardingClientCallListener<RespT>(responseListener) {
              @Override
              public void onMessage(RespT message) {
                streamTracer.traceResponseMessage((MessageLite) message);
                super.onMessage(message);
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

  private TrackingStreamTracer getStreamTracer(GrpcStreamType type, String rpcMethodName) {
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
   */
  private class TrackingStreamTracer extends ClientStreamTracer {

    private final Gson gson = new Gson();
    private final String rpcMethod;

    private Metadata headers;
    protected MessageLite requestMessage;
    protected int requestMessageCounter = 0;
    protected int responseMessageCounter = 0;

    TrackingStreamTracer(String rpcMethod) {
      this.rpcMethod = rpcMethod;
    }

    public void traceRequestMessage(MessageLite message) {
      this.requestMessage = message;
      requestMessageCounter++;
    }

    public void traceResponseMessage(MessageLite message) {
      responseMessageCounter++;
    }

    public void statusOnClose(Status status) {
      logger.atInfo().log(
          "%s",
          toJson(
              getRequestContext()
                  .put(GoogleCloudStorageTracingFields.STATUS.name, status.toString())
                  .build()));
    }

    private ImmutableMap.Builder<String, Object> getRequestContext() {
      return new ImmutableMap.Builder<String, Object>()
          .put(GoogleCloudStorageTracingFields.RPC_METHOD.name, rpcMethod)
          .put(GoogleCloudStorageTracingFields.IDEMPOTENCY_TOKEN.name, getInvocationId());
    }

    protected ImmutableMap.Builder<String, Object> getRequestTrackingInfo() {
      return getRequestContext()
          .put(GoogleCloudStorageTracingFields.REQUEST_COUNTER.name, requestMessageCounter);
    }

    protected ImmutableMap.Builder<String, Object> getResponseTrackingInfo() {
      return getRequestContext()
          .put(GoogleCloudStorageTracingFields.RESPONSE_COUNTER.name, responseMessageCounter);
    }

    protected String toJson(ImmutableMap<String, Object> eventDetails) {
      return gson.toJson(eventDetails);
    }

    protected String getInvocationId() {
      Metadata.Key<String> key =
          Metadata.Key.of(IDEMPOTENCY_TOKEN_HEADER, Metadata.ASCII_STRING_MARSHALLER);
      return headers != null ? headers.get(key) : "";
    }

    /** The stream is being created on a ready transport. */
    @Override
    public void streamCreated(Attributes transportAttrs, Metadata headers) {
      this.headers = headers;
      super.streamCreated(transportAttrs, headers);
    }
  }

  private class StartResumableUploadStreamTracer extends TrackingStreamTracer {
    private StorageResourceId resourceId;

    StartResumableUploadStreamTracer(String rpcMethod) {
      super(rpcMethod);
    }

    /**
     * An outbound message has been passed to the stream. This is called as soon as the stream knows
     * about the message, but doesn't have further guarantee such as whether the message is
     * serialized or not.
     *
     * @param seqNo the sequential number of the message within the stream, starting from 0. It can
     *     be used to correlate with outboundMessageSent(int, long, long) for the same message.
     */
    @Override
    public void outboundMessage(int seqNo) {
      StartResumableWriteRequest request = (StartResumableWriteRequest) requestMessage;

      this.resourceId =
          new StorageResourceId(
              request.getWriteObjectSpec().getResource().getBucket(),
              request.getWriteObjectSpec().getResource().getName(),
              request.getWriteObjectSpec().getIfGenerationMatch());
      logger.atInfo().log(
          "%s",
          toJson(
              getRequestTrackingInfo()
                  .put(GoogleCloudStorageTracingFields.RESOURCE.name, resourceId.toString())
                  .build()));
    }

    @Override
    public void traceResponseMessage(MessageLite message) {
      try {
        StartResumableWriteResponse response = (StartResumableWriteResponse) message;
        logger.atInfo().log(
            "%s",
            toJson(
                getResponseTrackingInfo()
                    .put(GoogleCloudStorageTracingFields.RESOURCE.name, resourceId.toString())
                    .put(GoogleCloudStorageTracingFields.UPLOAD_ID.name, response.getUploadId())
                    .build()));
      } finally {
        super.traceResponseMessage(message);
      }
    }
  }

  private class WriteObjectStreamTracer extends TrackingStreamTracer {

    private String streamUploadId = null;

    WriteObjectStreamTracer(String rpcMethod) {
      super(rpcMethod);
    }

    private void updateUploadId(@Nonnull String uploadId) {
      if (streamUploadId == null) {
        this.streamUploadId = uploadId;
      }
      checkState(
          uploadId.equals(streamUploadId),
          "Write stream should have unique uploadId associated with each chunk request.");
    }

    /**
     * An outbound message has been passed to the stream. This is called as soon as the stream knows
     * about the message, but doesn't have further guarantee such as whether the message is
     * serialized or not.
     *
     * @param seqNo the sequential number of the message within the stream, starting from 0. It can
     *     be used to correlate with outboundMessageSent(int, long, long) for the same message.
     */
    @Override
    public void outboundMessage(int seqNo) {
      WriteObjectRequest request = (WriteObjectRequest) requestMessage;
      updateUploadId(request.getUploadId());
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
    public void traceResponseMessage(MessageLite message) {
      try {
        WriteObjectResponse response = (WriteObjectResponse) message;
        logger.atInfo().log(
            "%s",
            toJson(
                getResponseTrackingInfo()
                    .put(GoogleCloudStorageTracingFields.UPLOAD_ID.name, streamUploadId)
                    .put(
                        GoogleCloudStorageTracingFields.PERSISTED_SIZE.name,
                        response.getPersistedSize())
                    .build()));
      } finally {
        super.traceResponseMessage(message);
      }
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

    /**
     * An outbound message has been passed to the stream. This is called as soon as the stream knows
     * about the message, but doesn't have further guarantee such as whether the message is
     * serialized or not.
     *
     * @param seqNo the sequential number of the message within the stream, starting from 0. It can
     *     be used to correlate with outboundMessageSent(int, long, long) for the same message.
     */
    @Override
    public void outboundMessage(int seqNo) {
      ReadObjectRequest request = (ReadObjectRequest) requestMessage;

      updateReadRequestContext(request);
      logger.atInfo().log(
          "%s",
          toJson(
              getRequestTrackingInfo()
                  .put(GoogleCloudStorageTracingFields.RESOURCE.name, resourceId.toString())
                  .put(GoogleCloudStorageTracingFields.READ_OFFSET.name, readOffset)
                  .put(GoogleCloudStorageTracingFields.READ_LIMIT.name, readLimit)
                  .build()));
    }

    @Override
    public void traceResponseMessage(MessageLite message) {
      try {
        ReadObjectResponse response = (ReadObjectResponse) message;
        int bytesRead = response.getChecksummedData().getContent().size();
        logger.atInfo().log(
            "%s",
            toJson(
                getResponseTrackingInfo()
                    .put(GoogleCloudStorageTracingFields.RESOURCE.name, resourceId.toString())
                    .put(GoogleCloudStorageTracingFields.READ_OFFSET.name, readOffset)
                    .put(GoogleCloudStorageTracingFields.READ_LIMIT.name, readLimit)
                    .put(GoogleCloudStorageTracingFields.REQUEST_START_OFFSET.name, totalBytesRead)
                    .put(GoogleCloudStorageTracingFields.BYTES_READ.name, bytesRead)
                    .build()));
        totalBytesRead += bytesRead;
      } finally {
        super.traceResponseMessage(message);
      }
    }
  }
}
