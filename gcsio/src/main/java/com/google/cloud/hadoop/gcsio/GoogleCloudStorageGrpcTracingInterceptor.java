/*
 * Copyright 2022 Google LLC
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
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.flogger.GoogleLogger;
import com.google.gson.Gson;
import io.grpc.Attributes;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientStreamTracer;
import io.grpc.ClientStreamTracer.StreamInfo;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import java.net.SocketAddress;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/** Interceptor to create a trace of the lifecycle of GRPC api calls. */
@VisibleForTesting
public class GoogleCloudStorageGrpcTracingInterceptor implements ClientInterceptor {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();
  private final GrpcRequestTracingInfo requestInfo;

  GoogleCloudStorageGrpcTracingInterceptor(GrpcRequestTracingInfo requestInfo) {
    this.requestInfo = requestInfo;
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions, Channel channel) {
    return channel.newCall(methodDescriptor, callOptions.withStreamTracerFactory(getFactory()));
  }

  private ClientStreamTracer.Factory getFactory() {
    return new ClientStreamTracer.Factory() {
      @Override
      public ClientStreamTracer newClientStreamTracer(StreamInfo info, Metadata headers) {
        return new GrpcClientStreamTracer(info, requestInfo);
      }
    };
  }

  private static class GrpcClientStreamTracer extends ClientStreamTracer {
    private static final DateTimeFormatter DATE_TIME_FORMATTER =
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX").withZone(ZoneOffset.UTC);

    private final StreamInfo streamInfo;
    private final Gson gson = new Gson();
    private final String initiatingThreadName = Thread.currentThread().getName();
    private final GrpcRequestTracingInfo requestInfo;
    private final Stopwatch stopwatch = Stopwatch.createUnstarted();
    private SocketAddress remoteAddress;

    public GrpcClientStreamTracer(StreamInfo info, GrpcRequestTracingInfo requestInfo) {
      this.streamInfo = info;
      this.requestInfo = requestInfo;
    }

    /** The stream is being created on a ready transport. */
    @Override
    public void streamCreated(Attributes transportAttrs, Metadata headers) {
      this.remoteAddress = transportAttrs.get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);

      logger.atInfo().log(
          "%s",
          toJson(
              getRequestTrackingInfo("streamCreated")
                  .put("transportattrs", transportAttrs)
                  .put("headers", headers == null ? "null" : headers.toString())
                  .put("streamInfo", streamInfo == null ? "null" : streamInfo.toString())
                  .build()));

      stopwatch.start();
      super.streamCreated(transportAttrs, headers);
    }

    private String toJson(ImmutableMap<String, Object> eventDetails) {
      return gson.toJson(eventDetails);
    }

    private ImmutableMap.Builder<String, Object> getRequestTrackingInfo(String message) {
      return new ImmutableMap.Builder<String, Object>()
          .put("initiatingthreadname", initiatingThreadName)
          .put("remoteaddress", remoteAddress)
          .put("elapsedmillis", stopwatch.elapsed().toMillis())
          .put("requestinfo", requestInfo)
          .put("eventtime", DATE_TIME_FORMATTER.format(Instant.now()))
          .put("details", message);
    }

    /** Trailing metadata has been received from the server. */
    public void inboundTrailers(Metadata trailers) {
      logger.atInfo().log(
          "%s",
          toJson(
              getRequestTrackingInfo("inboundTrailers()")
                  .put("trailers", trailers == null ? "null" : trailers.toString())
                  .build()));
    }

    /** Stream is closed. This will be called exactly once. */
    public void streamClosed(Status status) {
      logger.atInfo().log(
          "%s", toJson(getRequestTrackingInfo("streamcloses()").put("status", status).build()));
    }

    /**
     * An outbound message has been passed to the stream. This is called as soon as the stream knows
     * about the message, but doesn't have further guarantee such as whether the message is
     * serialized or not.
     *
     * @param seqNo the sequential number of the message within the stream, starting from 0. It can
     *     be used to correlate with outboundMessageSent(int, long, long) for the same message.
     */
    public void outboundMessage(int seqNo) {
      logger.atInfo().log(
          "%s", toJson(getRequestTrackingInfo("outboundMessage()").put("seqno", seqNo).build()));
    }

    /**
     * An inbound message has been received by the stream. This is called as soon as the stream
     * knows about the message, but doesn't have further guarantee such as whether the message is
     * deserialized or not.
     *
     * @param seqNo the sequential number of the message within the stream, starting from 0. It can
     *     be used to correlate with inboundMessageRead(int, long, long) for the same message.
     */
    public void inboundMessage(int seqNo) {
      logger.atInfo().log(
          "%s", toJson(getRequestTrackingInfo("inboundMessage()").put("seqno", seqNo).build()));
    }

    /**
     * An outbound message has been serialized and sent to the transport.
     *
     * @param seqNo the sequential number of the message within the stream, starting from 0. It can
     *     be used to correlate with outboundMessage(int) for the same message.
     * @param optionalUncompressedSize the uncompressed serialized size of the message. -1 if
     *     unknown
     * @param optionalWireSize the wire size of the message. -1 if unknown
     */
    public void outboundMessageSent(
        int seqNo, long optionalWireSize, long optionalUncompressedSize) {
      logger.atInfo().log(
          "%s",
          toJson(
              getRequestTrackingInfo("outboundMessageSent()")
                  .put("seqno", seqNo)
                  .put("optionalWireSize", optionalWireSize)
                  .put("optionalUncompressedSize", optionalUncompressedSize)
                  .build()));
    }

    /**
     * An inbound message has been fully read from the transport.
     *
     * @param seqNo the sequential number of the message within the stream, starting from 0. It can
     *     be used to correlate with inboundMessage(int) for the same message.
     * @param optionalUncompressedSize the wire size of the message. -1 if unknown
     * @param optionalWireSize the uncompressed serialized size of the message. -1 if unknown
     */
    public void inboundMessageRead(
        int seqNo, long optionalWireSize, long optionalUncompressedSize) {
      logger.atInfo().log(
          "%s",
          toJson(
              getRequestTrackingInfo("inboundMessageRead()")
                  .put("seqno", seqNo)
                  .put("optionalWireSize", optionalWireSize)
                  .put("optionalUncompressedSize", optionalUncompressedSize)
                  .build()));
    }
  }
}
