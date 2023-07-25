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

import static com.google.common.collect.ImmutableList.toImmutableList;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.MessageLite;
import com.google.storage.v2.BucketName;
import com.google.storage.v2.StartResumableWriteRequest;
import com.google.storage.v2.WriteObjectRequest;
import io.grpc.Attributes;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientStreamTracer;
import io.grpc.ClientStreamTracer.StreamInfo;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class TrackingGrpcRequestInterceptor implements ClientInterceptor {

  public static final String REQUEST_PREFIX_FORMAT = "rpcMethod:%s";
  private static final String RESUMABLE_UPLOAD_REQUEST_FORMAT =
      "StartResumableUpload:{bucket:%s;object:%s;ifGenerationMatch:generationId_%d;}";

  private static final String RESUMABLE_UPLOAD_CHUNK_REQUEST_FORMAT =
      "WriteObjectRequest:{writeOffset:%d;finalRequest:%b;uploadId:upload_%s;ifGenerationMatch:generationId_%d;contentLength:%d;}";

  private static final String UPLOAD_ID_PARAM_PATTERN = "uploadId:[^};]+";
  private static final String GENERATION_MATCH_TOKEN_PARAM_PATTERN = "ifGenerationMatch:[^};]+";

  public List<TrackingStreamTracer> streamTracerList = new ArrayList<>();

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
    String rpcMethodName = method.getBareMethodName();
    TrackingStreamTracer streamTracer =
        getStreamTracer(GrpcStreamType.getTypeFromName(rpcMethodName), rpcMethodName);
    streamTracerList.add(streamTracer);
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
        streamTracer.traceMessage((MessageLite) message);
        super.sendMessage(message);
      }
    };
  }

  private TrackingStreamTracer getStreamTracer(GrpcStreamType type, String rpcMethodName) {
    switch (type) {
      case START_RESUMABLE_WRITE:
        return new StartResumableUploadStreamTracer(type, rpcMethodName);
      case WRITE_OBJECT:
        return new WriteObjectStreamTracer(type, rpcMethodName);
      default:
        return new TrackingStreamTracer(type, rpcMethodName);
    }
  }

  private class TrackingStreamTracer extends ClientStreamTracer {
    private final GrpcStreamType type;
    private final String rpcMethod;
    protected List<MessageLite> streamMessages = new ArrayList<>();
    private Metadata headers;

    TrackingStreamTracer(GrpcStreamType type, String rpcMethod) {
      this.type = type;
      this.rpcMethod = rpcMethod;
    }

    public void traceMessage(MessageLite message) {
      streamMessages.add(message);
    }

    protected String messageToString(MessageLite message) {
      return String.format(REQUEST_PREFIX_FORMAT, rpcMethod);
    }

    public List<String> requestStringList() {
      List<String> stringList = new ArrayList<>();
      for (MessageLite message : streamMessages) {
        String requestString = messageToString(message);
        stringList.add(requestString);
      }
      return stringList;
    }

    @Override
    public void streamCreated(Attributes transportAttrs, Metadata headers) {
      this.headers = headers;
      super.streamCreated(transportAttrs, headers);
    }
  }

  private class StartResumableUploadStreamTracer extends TrackingStreamTracer {

    StartResumableUploadStreamTracer(GrpcStreamType type, String rpcMethod) {
      super(type, rpcMethod);
    }

    @Override
    protected String messageToString(MessageLite message) {
      StartResumableWriteRequest request = (StartResumableWriteRequest) message;
      return String.format(
          "%s;%s", super.messageToString(message), resumableUploadRequestToString(request));
    }
  }

  private class WriteObjectStreamTracer extends TrackingStreamTracer {
    WriteObjectStreamTracer(GrpcStreamType type, String rpcMethod) {
      super(type, rpcMethod);
    }

    @Override
    protected String messageToString(MessageLite message) {
      WriteObjectRequest request = (WriteObjectRequest) message;
      return String.format(
          "%s;%s", super.messageToString(message), writeObjectRequestToString(request));
    }
  }

  public ImmutableList<String> getAllRequestStrings() {
    AtomicLong resumableUploadId = new AtomicLong();
    AtomicLong generationMatchId = new AtomicLong();
    return streamTracerList.stream()
        .map(r -> r.requestStringList())
        .collect(Collectors.toList())
        .stream()
        .flatMap(Collection::stream)
        .map(r -> replaceResumableUploadIdWithId(r, resumableUploadId))
        .map(r -> replaceGenerationMatchWithId(r, generationMatchId))
        .collect(toImmutableList());
  }

  private static String replaceResumableUploadIdWithId(String request, AtomicLong uploadId) {
    return replaceWithId(request, UPLOAD_ID_PARAM_PATTERN, "uploadId:upload_", uploadId);
  }

  private static String replaceGenerationMatchWithId(String request, AtomicLong generationId) {
    return replaceWithId(
        request,
        GENERATION_MATCH_TOKEN_PARAM_PATTERN,
        "ifGenerationMatch:generationId_",
        generationId);
  }

  private static String replaceWithId(
      String request, String pattern, String idPrefix, AtomicLong id) {
    long nextId = id.get() + 1;
    String replacedRequest = request.replaceAll(pattern, idPrefix + nextId);
    if (!request.equals(replacedRequest)) {
      id.incrementAndGet();
    }
    return replacedRequest;
  }

  private static String resumableUploadRequestToString(StartResumableWriteRequest request) {
    return String.format(
        RESUMABLE_UPLOAD_REQUEST_FORMAT,
        request.getWriteObjectSpec().getResource().getBucket(),
        request.getWriteObjectSpec().getResource().getName(),
        request.getWriteObjectSpec().getIfGenerationMatch());
  }

  private static String writeObjectRequestToString(WriteObjectRequest request) {
    return String.format(
        RESUMABLE_UPLOAD_CHUNK_REQUEST_FORMAT,
        request.getWriteOffset(),
        request.getFinishWrite(),
        request.getUploadId(),
        request.getWriteObjectSpec().getIfGenerationMatch(),
        request.getChecksummedData().getContent().size());
  }

  private static String requestPrefixString(String rpcMethodName) {
    return String.format(REQUEST_PREFIX_FORMAT, rpcMethodName);
  }

  private static String requestString(String requestPrefix, String value) {
    return String.format("%s;%s", requestPrefix, value);
  }

  public static String resumableUploadRequestString(
      String bucketName, String object, Integer generationId) {
    String requestPrefixString = requestPrefixString(GrpcStreamType.START_RESUMABLE_WRITE.name);
    String requestString =
        String.format(
            RESUMABLE_UPLOAD_REQUEST_FORMAT,
            // TODO: for now project field is not populated in bucketname, once it does do update it
            // by extracting project from env variable.
            BucketName.newBuilder().setBucket(bucketName).setProject("_").build().toString(),
            object,
            generationId);
    return requestString(requestPrefixString, requestString);
  }

  public static String resumableUploadChunkRequestString(
      Integer generationId,
      Integer uploadId,
      long contentLength,
      long writeOffset,
      boolean finishWrite) {
    String requestPrefixString = requestPrefixString(GrpcStreamType.WRITE_OBJECT.name);
    String requestString =
        String.format(
            RESUMABLE_UPLOAD_CHUNK_REQUEST_FORMAT,
            writeOffset,
            finishWrite,
            uploadId,
            generationId,
            contentLength);
    return requestString(requestPrefixString, requestString);
  }
}
