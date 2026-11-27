/*
 * Copyright 2026 Google LLC
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

import static com.google.common.truth.Truth.assertThat;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertThrows;

import com.google.cloud.hadoop.util.LoggingUrlRedactor;
import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import com.google.storage.v2.ChecksummedData;
import com.google.storage.v2.StartResumableWriteRequest;
import com.google.storage.v2.StartResumableWriteResponse;
import com.google.storage.v2.StorageGrpc;
import com.google.storage.v2.WriteObjectRequest;
import com.google.storage.v2.WriteObjectResponse;
import com.google.storage.v2.WriteObjectSpec;
import io.grpc.Attributes;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientStreamTracer;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GoogleCloudStorageClientGrpcTracingInterceptorTest {

  private static final Logger LOGGER =
      Logger.getLogger(GoogleCloudStorageClientGrpcTracingInterceptor.class.getName());
  private static final Metadata.Key<String> IDEMPOTENCY_KEY =
      Metadata.Key.of(
          GoogleCloudStorageClientGrpcTracingInterceptor.IDEMPOTENCY_TOKEN_HEADER,
          Metadata.ASCII_STRING_MARSHALLER);

  private GoogleCloudStorageClientGrpcTracingInterceptor interceptor;
  private AssertingLogHandler assertingHandler;
  private Level previousLevel;
  private boolean previousUseParentHandlers;

  @Before
  public void setUp() {
    interceptor = new GoogleCloudStorageClientGrpcTracingInterceptor();
    assertingHandler = new AssertingLogHandler();
    previousLevel = LOGGER.getLevel();
    previousUseParentHandlers = LOGGER.getUseParentHandlers();
    LOGGER.setUseParentHandlers(false);
    LOGGER.addHandler(assertingHandler);
    LOGGER.setLevel(Level.INFO);
  }

  @After
  public void tearDown() {
    LOGGER.removeHandler(assertingHandler);
    LOGGER.setLevel(previousLevel);
    LOGGER.setUseParentHandlers(previousUseParentHandlers);
  }

  @Test
  public void resumableWriteTracing_digestsUploadIdDeterministicallyAcrossRpcs() {
    String rawUploadId = "SECRET_GRPC_UPLOAD_SESSION_ID";
    String expectedDigest = LoggingUrlRedactor.digestToken(rawUploadId);

    // 1. StartResumableWrite RPC
    AtomicReference<ClientCall.Listener<StartResumableWriteResponse>> startListenerRef =
        new AtomicReference<>();
    ClientCall<StartResumableWriteRequest, StartResumableWriteResponse> startCall =
        interceptor.interceptCall(
            StorageGrpc.getStartResumableWriteMethod(),
            CallOptions.DEFAULT,
            fakeChannel(startListenerRef));
    Metadata startHeaders = new Metadata();
    startHeaders.put(IDEMPOTENCY_KEY, "inv-start-1");
    startCall.start(new NoOpListener<>(), startHeaders);
    startCall.sendMessage(
        StartResumableWriteRequest.newBuilder()
            .setWriteObjectSpec(
                WriteObjectSpec.newBuilder()
                    .setResource(
                        com.google.storage.v2.Object.newBuilder()
                            .setBucket("projects/_/buckets/bkt")
                            .setName("obj")))
            .build());
    startListenerRef
        .get()
        .onMessage(StartResumableWriteResponse.newBuilder().setUploadId(rawUploadId).build());
    startListenerRef.get().onClose(Status.OK, new Metadata());

    // 2. WriteObject RPC with two chunks using the same upload ID
    AtomicReference<ClientCall.Listener<WriteObjectResponse>> writeListenerRef =
        new AtomicReference<>();
    ClientCall<WriteObjectRequest, WriteObjectResponse> writeCall =
        interceptor.interceptCall(
            StorageGrpc.getWriteObjectMethod(), CallOptions.DEFAULT, fakeChannel(writeListenerRef));
    Metadata writeHeaders = new Metadata();
    writeHeaders.put(IDEMPOTENCY_KEY, "inv-write-1");
    writeCall.start(new NoOpListener<>(), writeHeaders);
    writeCall.sendMessage(
        WriteObjectRequest.newBuilder()
            .setUploadId(rawUploadId)
            .setWriteOffset(0)
            .setChecksummedData(
                ChecksummedData.newBuilder().setContent(ByteString.copyFromUtf8("abcd")))
            .build());
    writeCall.sendMessage(
        WriteObjectRequest.newBuilder()
            .setUploadId(rawUploadId)
            .setWriteOffset(4)
            .setFinishWrite(true)
            .setChecksummedData(
                ChecksummedData.newBuilder().setContent(ByteString.copyFromUtf8("efgh")))
            .build());
    writeListenerRef.get().onMessage(WriteObjectResponse.newBuilder().setPersistedSize(8).build());
    writeListenerRef.get().onClose(Status.OK, new Metadata());

    assertThat(uploadIds("StartResumableWrite", "response")).containsExactly(expectedDigest);
    assertThat(uploadIds("WriteObject", "request")).containsExactly(expectedDigest, expectedDigest);
    assertThat(uploadIds("WriteObject", "response")).containsExactly(expectedDigest);

    String allLogsJson = new Gson().toJson(assertingHandler.getAllLogRecords());
    assertThat(allLogsJson).doesNotContain(rawUploadId);
  }

  @Test
  public void writeObjectTracing_preservesEmptyAndNotFoundSentinels() {
    AtomicReference<ClientCall.Listener<WriteObjectResponse>> writeListenerRef =
        new AtomicReference<>();
    ClientCall<WriteObjectRequest, WriteObjectResponse> writeCall =
        interceptor.interceptCall(
            StorageGrpc.getWriteObjectMethod(), CallOptions.DEFAULT, fakeChannel(writeListenerRef));
    Metadata headers = new Metadata();
    headers.put(IDEMPOTENCY_KEY, "inv-direct-1");
    writeCall.start(new NoOpListener<>(), headers);
    writeCall.sendMessage(WriteObjectRequest.newBuilder().setFinishWrite(true).build());
    writeListenerRef.get().onMessage(WriteObjectResponse.newBuilder().setPersistedSize(0).build());
    writeListenerRef.get().onClose(Status.OK, new Metadata());

    assertThat(uploadIds("WriteObject", "request")).containsExactly("");
    assertThat(uploadIds("WriteObject", "response"))
        .containsExactly(GoogleCloudStorageClientGrpcTracingInterceptor.STREAM_UPLOAD_ID_NOT_FOUND);
  }

  @Test
  public void writeObjectTracing_mismatchedUploadIdThrowsWithDigestedIdsOnly() {
    String uploadId1 = "SECRET_GRPC_UPLOAD_ID_ONE";
    String uploadId2 = "SECRET_GRPC_UPLOAD_ID_TWO";

    AtomicReference<ClientCall.Listener<WriteObjectResponse>> writeListenerRef =
        new AtomicReference<>();
    ClientCall<WriteObjectRequest, WriteObjectResponse> writeCall =
        interceptor.interceptCall(
            StorageGrpc.getWriteObjectMethod(), CallOptions.DEFAULT, fakeChannel(writeListenerRef));
    Metadata headers = new Metadata();
    headers.put(IDEMPOTENCY_KEY, "inv-mismatch-1");
    writeCall.start(new NoOpListener<>(), headers);
    writeCall.sendMessage(WriteObjectRequest.newBuilder().setUploadId(uploadId1).build());

    WriteObjectRequest mismatchedRequest =
        WriteObjectRequest.newBuilder().setUploadId(uploadId2).build();
    IllegalStateException thrown =
        assertThrows(IllegalStateException.class, () -> writeCall.sendMessage(mismatchedRequest));

    assertThat(thrown.getMessage()).contains(LoggingUrlRedactor.digestToken(uploadId1));
    assertThat(thrown.getMessage()).contains(LoggingUrlRedactor.digestToken(uploadId2));
    assertThat(thrown.getMessage()).doesNotContain(uploadId1);
    assertThat(thrown.getMessage()).doesNotContain(uploadId2);
  }

  @Test
  public void writeObjectTracing_chunkOmittingUploadIdRetainsStreamDigestInResponse() {
    String rawUploadId = "SECRET_MULTI_CHUNK_UPLOAD_ID";
    String expectedDigest = LoggingUrlRedactor.digestToken(rawUploadId);

    AtomicReference<ClientCall.Listener<WriteObjectResponse>> writeListenerRef =
        new AtomicReference<>();
    ClientCall<WriteObjectRequest, WriteObjectResponse> writeCall =
        interceptor.interceptCall(
            StorageGrpc.getWriteObjectMethod(), CallOptions.DEFAULT, fakeChannel(writeListenerRef));
    Metadata headers = new Metadata();
    headers.put(IDEMPOTENCY_KEY, "inv-multichunk-1");
    writeCall.start(new NoOpListener<>(), headers);
    // Chunk 1 carries upload_id; chunk 2 omits it (matching Gapic finishMessage behavior).
    writeCall.sendMessage(
        WriteObjectRequest.newBuilder().setUploadId(rawUploadId).setWriteOffset(0).build());
    writeCall.sendMessage(
        WriteObjectRequest.newBuilder().setWriteOffset(4).setFinishWrite(true).build());
    writeListenerRef.get().onMessage(WriteObjectResponse.newBuilder().setPersistedSize(8).build());
    writeListenerRef.get().onClose(Status.OK, new Metadata());

    assertThat(uploadIds("WriteObject", "request")).containsExactly(expectedDigest, "").inOrder();
    assertThat(uploadIds("WriteObject", "response")).containsExactly(expectedDigest);
    String allLogsJson = new Gson().toJson(assertingHandler.getAllLogRecords());
    assertThat(allLogsJson).doesNotContain(rawUploadId);
  }

  @Test
  public void formatUploadIdForLog_handlesSentinelsAndTokens() {
    assertThat(GoogleCloudStorageClientGrpcTracingInterceptor.formatUploadIdForLog(null))
        .isEqualTo("");
    assertThat(GoogleCloudStorageClientGrpcTracingInterceptor.formatUploadIdForLog(""))
        .isEqualTo("");
    assertThat(
            GoogleCloudStorageClientGrpcTracingInterceptor.formatUploadIdForLog(
                GoogleCloudStorageClientGrpcTracingInterceptor.STREAM_UPLOAD_ID_NOT_FOUND))
        .isEqualTo(GoogleCloudStorageClientGrpcTracingInterceptor.STREAM_UPLOAD_ID_NOT_FOUND);
    assertThat(GoogleCloudStorageClientGrpcTracingInterceptor.formatUploadIdForLog("raw-token"))
        .isEqualTo(LoggingUrlRedactor.digestToken("raw-token"));
  }

  /** Returns the logged upload IDs for one RPC method and stream operation, in log order. */
  private List<Object> uploadIds(String rpcMethod, String streamOperation) {
    return assertingHandler.getAllLogRecords().stream()
        .filter(r -> rpcMethod.equals(r.get(GoogleCloudStorageTracingFields.RPC_METHOD.name)))
        .filter(
            r ->
                streamOperation.equals(
                    r.get(GoogleCloudStorageTracingFields.STREAM_OPERATION.name)))
        .map(r -> r.get(GoogleCloudStorageTracingFields.UPLOAD_ID.name))
        .collect(toList());
  }

  private static <V> Channel fakeChannel(AtomicReference<ClientCall.Listener<V>> capturedListener) {
    return new Channel() {
      @Override
      public <Q, R> ClientCall<Q, R> newCall(
          MethodDescriptor<Q, R> methodDescriptor, CallOptions callOptions) {
        return new ClientCall<Q, R>() {
          @Override
          @SuppressWarnings("unchecked")
          public void start(Listener<R> responseListener, Metadata headers) {
            capturedListener.set((Listener<V>) responseListener);
            for (ClientStreamTracer.Factory factory : callOptions.getStreamTracerFactories()) {
              ClientStreamTracer tracer =
                  factory.newClientStreamTracer(
                      ClientStreamTracer.StreamInfo.newBuilder().build(), headers);
              tracer.streamCreated(Attributes.EMPTY, headers);
            }
          }

          @Override
          public void request(int numMessages) {
            // No-op stub for fake ClientCall.
          }

          @Override
          public void cancel(String message, Throwable cause) {
            // No-op stub for fake ClientCall.
          }

          @Override
          public void halfClose() {
            // No-op stub for fake ClientCall.
          }

          @Override
          public void sendMessage(Q message) {
            // No-op stub for fake ClientCall.
          }
        };
      }

      @Override
      public String authority() {
        return "fake-authority";
      }
    };
  }

  private static class NoOpListener<T> extends ClientCall.Listener<T> {}
}
