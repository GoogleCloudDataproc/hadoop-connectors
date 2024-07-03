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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.flogger.GoogleLogger;
import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import com.google.protobuf.MessageLite;
import com.google.protobuf.MessageOrBuilder;
import com.google.storage.v2.ReadObjectResponse;
import com.google.storage.v2.WriteObjectRequest;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import org.checkerframework.checker.nullness.qual.NonNull;

/** Interceptor to create a trace of the lifecycle of GRPC api calls. */
@VisibleForTesting
public class GoogleCloudStorageClientGrpcTracingInterceptor implements ClientInterceptor {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();
  public static final String IDEMPOTENCY_TOKEN_HEADER = "x-goog-gcs-idempotency-token";
  public static final String USER_PROJECT_HEADER = "x-goog-user-project";
  private static final DateTimeFormatter dtf =
      DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss.SSS");
  private static final String DEFAULT_HEADER_VALUE = "NOT-FOUND";
  private static final Metadata.Key<String> idempotencyKey =
      Metadata.Key.of(IDEMPOTENCY_TOKEN_HEADER, Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> userProjectKey =
      Metadata.Key.of(USER_PROJECT_HEADER, Metadata.ASCII_STRING_MARSHALLER);

  @NonNull
  static String fmtProto(@NonNull Object obj) {
    if (obj instanceof WriteObjectRequest) {
      return fmtProto((WriteObjectRequest) obj);
    } else if (obj instanceof ReadObjectResponse) {
      return fmtProto((ReadObjectResponse) obj);
    } else if (obj instanceof MessageOrBuilder) {
      return fmtProto((MessageOrBuilder) obj);
    } else {
      return obj.toString();
    }
  }

  @NonNull
  static String fmtProto(@NonNull final MessageOrBuilder msg) {
    return msg.toString();
  }

  @NonNull
  static String fmtProto(@NonNull WriteObjectRequest msg) {
    if (msg.hasChecksummedData()) {
      ByteString content = msg.getChecksummedData().getContent();
      WriteObjectRequest.Builder b = msg.toBuilder();
      ByteString snip = updatedContent(content.size());
      b.getChecksummedDataBuilder().setContent(snip);
      return b.build().toString();
    }
    return msg.toString();
  }

  @NonNull
  static String fmtProto(@NonNull ReadObjectResponse msg) {
    if (msg.hasChecksummedData()) {
      ByteString content = msg.getChecksummedData().getContent();
      ReadObjectResponse.Builder b = msg.toBuilder();
      ByteString snip = updatedContent(content.size());
      b.getChecksummedDataBuilder().setContent(snip);
      return b.build().toString();
    }
    return msg.toString();
  }

  static ByteString updatedContent(int contentSize) {
    return ByteString.copyFromUtf8(String.format("<size (%d)>", contentSize));
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
    String rpcMethodName = method.getBareMethodName();

    TrackingStreamTracer streamTracer = new TrackingStreamTracer(rpcMethodName);
    return new SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
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
        streamTracer.streamStarted(headers);
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

  private class TrackingStreamTracer {
    private final Gson gson = new Gson();
    private final String rpcMethod;
    private Metadata headers;
    private long streamStartTimeMs;
    private int requestCounter = 0;
    private int responseCounter = 0;

    TrackingStreamTracer(String rpcMethod) {
      this.rpcMethod = rpcMethod;
    }

    public void traceRequestMessage(MessageLite message) {
      logger.atInfo().log(
          "%s",
          toJson(
              getRequestTrackingInfo()
                  .put(
                      GoogleCloudStorageTracingFields.REQUEST_MESSAGE_AS_STRING.name,
                      fmtProto(message))
                  .build()));
    }

    public void traceResponseMessage(MessageLite message) {
      logger.atInfo().log(
          "%s",
          toJson(
              getResponseTrackingInfo()
                  .put(
                      GoogleCloudStorageTracingFields.RESPONSE_MESSAGE_AS_STRING.name,
                      fmtProto(message))
                  .build()));
    }

    public void statusOnClose(Status status) {
      long streamEndTimeMs = System.currentTimeMillis();
      long duration = streamEndTimeMs - streamStartTimeMs;
      logger.atInfo().log(
          "%s",
          toJson(
              getCommonTraceFields()
                  .put(GoogleCloudStorageTracingFields.STATUS.name, status.getCode())
                  .put(GoogleCloudStorageTracingFields.STREAM_OPERATION.name, "onClose")
                  .put(GoogleCloudStorageTracingFields.DURATION_MS.name, duration)
                  .build()));
    }

    /**
     * This will be very first interaction with StreamTracer. It updates the tracer with header of
     * gRPC stream. It also updates the startTime of tracer
     *
     * @param headers grpc stream header
     */
    public void streamStarted(Metadata headers) {
      this.headers = headers;
      this.streamStartTimeMs = System.currentTimeMillis();
    }

    private ImmutableMap.Builder<String, Object> getRequestTrackingInfo() {
      requestCounter++;
      return getCommonTraceFields()
          .put(GoogleCloudStorageTracingFields.REQUEST_COUNTER.name, requestCounter)
          .put(GoogleCloudStorageTracingFields.STREAM_OPERATION.name, "request");
    }

    private ImmutableMap.Builder<String, Object> getResponseTrackingInfo() {
      responseCounter++;
      return getCommonTraceFields()
          .put(GoogleCloudStorageTracingFields.RESPONSE_COUNTER.name, responseCounter)
          .put(GoogleCloudStorageTracingFields.STREAM_OPERATION.name, "response");
    }

    protected String toJson(ImmutableMap<String, Object> eventDetails) {
      return gson.toJson(eventDetails);
    }

    private String getInvocationId() {
      String value = headers.get(idempotencyKey);
      if (Strings.isNullOrEmpty(value)) {
        logger.atWarning().log(
            "Received a null or empty value for invocation Id: %s for rpc method: %s",
            value, rpcMethod);
        value = DEFAULT_HEADER_VALUE;
      }
      return value;
    }

    protected String getRequesterPaysProject() {
      String value = headers.get(userProjectKey);
      return value == null ? DEFAULT_HEADER_VALUE : value;
    }

    private ImmutableMap.Builder<String, Object> getHeaderValues() {
      return new ImmutableMap.Builder<String, Object>()
          .put(
              GoogleCloudStorageTracingFields.REQUESTER_PAYS_PROJECT.name,
              getRequesterPaysProject())
          .put(GoogleCloudStorageTracingFields.IDEMPOTENCY_TOKEN.name, getInvocationId());
    }

    private ImmutableMap.Builder<String, Object> getStreamContext() {
      return getHeaderValues().put(GoogleCloudStorageTracingFields.RPC_METHOD.name, rpcMethod);
    }

    private ImmutableMap.Builder<String, Object> getCommonTraceFields() {
      return getStreamContext()
          .put(GoogleCloudStorageTracingFields.CURRENT_TIME.name, dtf.format(LocalDateTime.now()));
    }
  }
}
