/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.gcsio;

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageClientGrpcDownscopingInterceptor.GOOGLE_STORAGE_V_2_STORAGE_COMPOSE_OBJECT;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageClientGrpcDownscopingInterceptor.GOOGLE_STORAGE_V_2_STORAGE_DELETE_OBJECT;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageClientGrpcDownscopingInterceptor.GOOGLE_STORAGE_V_2_STORAGE_READ_OBJECT;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageClientGrpcDownscopingInterceptor.GOOGLE_STORAGE_V_2_STORAGE_START_RESUMABLE_WRITE;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageClientGrpcDownscopingInterceptor.GOOGLE_STORAGE_V_2_STORAGE_WRITE_OBJECT;
import static io.grpc.protobuf.services.BinaryLogProvider.BYTEARRAY_MARSHALLER;
import static org.mockito.Mockito.mock;

import com.google.cloud.hadoop.util.AccessBoundary;
import com.google.cloud.hadoop.util.AccessBoundary.Action;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.SettableFuture;
import com.google.storage.v2.ComposeObjectRequest;
import com.google.storage.v2.ComposeObjectRequest.SourceObject;
import com.google.storage.v2.DeleteObjectRequest;
import com.google.storage.v2.ReadObjectRequest;
import com.google.storage.v2.StartResumableWriteRequest;
import com.google.storage.v2.WriteObjectRequest;
import com.google.storage.v2.WriteObjectSpec;
import io.grpc.Attributes;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientCall.Listener;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.MethodDescriptor.MethodType;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import javax.annotation.Nullable;
import junit.framework.TestCase;

public class GoogleCloudStorageClientGrpcDownscopingInterceptorTest extends TestCase {
  private static final Charset US_ASCII = StandardCharsets.US_ASCII;
  private AtomicReference<ClientCall.Listener<byte[]>> interceptedListener;
  private AtomicReference<Metadata> actualClientInitial;
  private AtomicReference<Object> actualRequest;
  private SettableFuture<Void> halfCloseCalled;
  private SettableFuture<Void> cancelCalled;
  private Channel channel;
  private DownscopeHandler handler;
  private GoogleCloudStorageClientGrpcDownscopingInterceptor interceptor;
  private Metadata clientInitial;
  private Listener<byte[]> listener;

  @Override
  protected void setUp() throws Exception {
    interceptedListener = new AtomicReference<>();
    actualClientInitial = new AtomicReference<>();
    actualRequest = new AtomicReference<>();
    halfCloseCalled = SettableFuture.create();
    cancelCalled = SettableFuture.create();

    this.channel =
        new Channel() {
          @Override
          public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(
              MethodDescriptor<RequestT, ResponseT> methodDescriptor, CallOptions callOptions) {
            return new NoopClientCall<RequestT, ResponseT>() {
              @Override
              @SuppressWarnings("unchecked")
              public void start(Listener<ResponseT> responseListener, Metadata headers) {
                interceptedListener.set((Listener<byte[]>) responseListener);
                actualClientInitial.set(headers);
              }

              @Override
              public void sendMessage(RequestT message) {
                actualRequest.set(message);
              }

              @Override
              public void cancel(String message, Throwable cause) {
                cancelCalled.set(null);
              }

              @Override
              public void halfClose() {
                halfCloseCalled.set(null);
              }

              @Override
              public Attributes getAttributes() {
                return Attributes.newBuilder().set(Grpc.TRANSPORT_ATTR_REMOTE_ADDR, peer).build();
              }
            };
          }

          @Override
          public String authority() {
            return "the-authority";
          }
        };

    this.handler = new DownscopeHandler();
    this.interceptor =
        new GoogleCloudStorageClientGrpcDownscopingInterceptor(handler.getDownscopeHandler());

    this.clientInitial = new Metadata();
    clientInitial.put(Metadata.Key.of("a", Metadata.ASCII_STRING_MARSHALLER), "aaaaaaaaa");

    this.listener =
        new Listener<byte[]>() {
          @Override
          public void onHeaders(Metadata headers) {
            super.onHeaders(headers);
          }
        };
  }

  @SuppressWarnings("AddressSelection") // It will only be one address
  private SocketAddress peer = new InetSocketAddress("127.0.0.1", 1234);

  public void testReadObjectRequest() {
    MethodDescriptor<ReadObjectRequest, byte[]> method =
        getMethodDescriptor(GOOGLE_STORAGE_V_2_STORAGE_READ_OBJECT);

    ReadObjectRequest readObjectRequest =
        ReadObjectRequest.newBuilder()
            .setObject(handler.getObjectName())
            .setBucket(handler.getFormattedBucketName())
            .build();

    ClientCall<ReadObjectRequest, byte[]> interceptedLoggingCall =
        interceptor.interceptCall(method, CallOptions.DEFAULT, channel);

    interceptedLoggingCall.start(listener, clientInitial);
    interceptedLoggingCall.sendMessage(readObjectRequest);

    verify(interceptedLoggingCall, handler.getExpectedReadObjectHeader());
  }

  public void testWriteObjectRequest() {
    MethodDescriptor<WriteObjectRequest, byte[]> method =
        getMethodDescriptor(GOOGLE_STORAGE_V_2_STORAGE_WRITE_OBJECT);

    WriteObjectRequest writeObjectRequest =
        WriteObjectRequest.newBuilder()
            .setWriteObjectSpec(WriteObjectSpec.newBuilder().setResource(getObject()).build())
            .build();

    ClientCall<WriteObjectRequest, byte[]> interceptedLoggingCall =
        interceptor.interceptCall(method, CallOptions.DEFAULT, channel);

    interceptedLoggingCall.start(listener, clientInitial);
    interceptedLoggingCall.sendMessage(writeObjectRequest);

    verify(interceptedLoggingCall, handler.getExpectedWriteObjectHeader());
  }

  public void testDeleteObjectRequest() {
    MethodDescriptor<DeleteObjectRequest, byte[]> method =
        getMethodDescriptor(GOOGLE_STORAGE_V_2_STORAGE_DELETE_OBJECT);

    DeleteObjectRequest deleteObjectRequest =
        DeleteObjectRequest.newBuilder()
            .setBucket(handler.getFormattedBucketName())
            .setObject(handler.getObjectName())
            .build();

    ClientCall<DeleteObjectRequest, byte[]> interceptedLoggingCall =
        interceptor.interceptCall(method, CallOptions.DEFAULT, channel);

    interceptedLoggingCall.start(listener, clientInitial);
    interceptedLoggingCall.sendMessage(deleteObjectRequest);

    verify(interceptedLoggingCall, handler.getExpectedDeleteObjectHeader());
  }

  public void testResumableUploadRequest() {
    MethodDescriptor<StartResumableWriteRequest, byte[]> method =
        getMethodDescriptor(GOOGLE_STORAGE_V_2_STORAGE_START_RESUMABLE_WRITE);

    StartResumableWriteRequest resumableWriteRequest =
        StartResumableWriteRequest.newBuilder()
            .setWriteObjectSpec(WriteObjectSpec.newBuilder().setResource(getObject()).build())
            .build();

    ClientCall<StartResumableWriteRequest, byte[]> interceptedLoggingCall =
        interceptor.interceptCall(method, CallOptions.DEFAULT, channel);

    interceptedLoggingCall.start(listener, clientInitial);
    interceptedLoggingCall.sendMessage(resumableWriteRequest);

    verify(interceptedLoggingCall, handler.getExpectedResumableUploadHeader());
  }

  public void testComposeObjectRequest() {
    MethodDescriptor<ComposeObjectRequest, byte[]> method =
        getMethodDescriptor(GOOGLE_STORAGE_V_2_STORAGE_COMPOSE_OBJECT);

    ImmutableList<SourceObject> sources = ImmutableList.of(getSourceObject(), getSourceObject());

    ComposeObjectRequest composeObjectRequest =
        ComposeObjectRequest.newBuilder()
            .setDestination(getObject())
            .addAllSourceObjects(sources)
            .build();
    ClientCall<ComposeObjectRequest, byte[]> interceptedLoggingCall =
        interceptor.interceptCall(method, CallOptions.DEFAULT, channel);

    interceptedLoggingCall.start(listener, clientInitial);
    interceptedLoggingCall.sendMessage(composeObjectRequest);

    verify(interceptedLoggingCall, handler.getExpectedComposeHeader(sources));
  }

  public void testMultipleAuthInterceptors_shouldNotSendMultipleAuthHeaders() {
    GoogleCloudStorageClientGrpcDownscopingInterceptor interceptor1 =
        new GoogleCloudStorageClientGrpcDownscopingInterceptor(accessBoundaries -> "token1");
    GoogleCloudStorageClientGrpcDownscopingInterceptor interceptor2 =
        new GoogleCloudStorageClientGrpcDownscopingInterceptor(accessBoundaries -> "token2");

    Channel interceptedChannel =
        io.grpc.ClientInterceptors.intercept(channel, interceptor1, interceptor2);

    MethodDescriptor<ReadObjectRequest, byte[]> method =
        getMethodDescriptor(GOOGLE_STORAGE_V_2_STORAGE_READ_OBJECT);

    ClientCall<ReadObjectRequest, byte[]> call =
        interceptedChannel.newCall(method, CallOptions.DEFAULT);

    call.start(listener, clientInitial);
    call.sendMessage(
        ReadObjectRequest.newBuilder()
            .setBucket(handler.getFormattedBucketName())
            .setObject(handler.getObjectName())
            .build());

    List<String> actualAuthHeaders =
        ImmutableList.copyOf(
            actualClientInitial
                .get()
                .getAll(GoogleCloudStorageClientGrpcDownscopingInterceptor.AUTH_KEY));

    // Only the last interceptor's header should be present.
    assertEquals(1, actualAuthHeaders.size());
    assertEquals("Bearer token2", actualAuthHeaders.get(0));
  }

  private static MethodDescriptor getMethodDescriptor(String methodName) {
    return MethodDescriptor.<StartResumableWriteRequest, byte[]>newBuilder()
        .setType(MethodType.UNARY)
        .setFullMethodName(methodName)
        .setRequestMarshaller(mock(MethodDescriptor.Marshaller.class))
        .setResponseMarshaller(BYTEARRAY_MARSHALLER)
        .build();
  }

  private com.google.storage.v2.Object getObject() {
    return com.google.storage.v2.Object.newBuilder()
        .setBucket(handler.getFormattedBucketName())
        .setName(handler.getObjectName())
        .build();
  }

  private SourceObject getSourceObject() {
    return SourceObject.newBuilder()
        .setName(String.format("part-%s.part", ThreadLocalRandom.current().nextInt(1, 1000)))
        .build();
  }

  private <ReqT> void verify(
      ClientCall<ReqT, byte[]> interceptedLoggingCall, String expectedAuthHeader) {
    String actualAuthHeader =
        actualClientInitial.get().get(GoogleCloudStorageClientGrpcDownscopingInterceptor.AUTH_KEY);

    assertEquals(expectedAuthHeader, actualAuthHeader);
    interceptedLoggingCall.halfClose();
    byte[] response = "this is a response".getBytes(US_ASCII);
    interceptedListener.get().onMessage(response);
  }

  private static class DownscopeHandler {
    List<AccessBoundary> accessBoundaries;
    private String bucketName =
        String.format("bucket_%s", ThreadLocalRandom.current().nextInt(1, 1000));
    private String objectName;

    DownscopeHandler() {
      this.objectName =
          String.format(
              "%s/dir/obj%s.text", getBucketName(), ThreadLocalRandom.current().nextInt(0, 100));
    }

    public Function<List<AccessBoundary>, String> getDownscopeHandler() {
      return new Function<List<AccessBoundary>, String>() {
        @Override
        public String apply(List<AccessBoundary> accessBoundaries) {
          DownscopeHandler.this.accessBoundaries = accessBoundaries;
          StringBuilder sb = new StringBuilder();
          for (AccessBoundary ab : accessBoundaries) {
            sb.append(String.format("[%s;%s;%s]", ab.bucketName(), ab.objectName(), ab.action()));
          }

          return sb.toString();
        }
      };
    }

    public String getObjectName() {
      return objectName;
    }

    public String getBucketName() {
      return this.bucketName;
    }

    public String getFormattedBucketName() {
      return String.format("projects/_/buckets/%s", this.bucketName);
    }

    public String getExpectedReadObjectHeader() {
      return getHeader(Action.READ_OBJECTS);
    }

    private String getHeader(Action action) {
      return String.format("Bearer [%s;%s;%s]", bucketName, getObjectName(), action);
    }

    public String getExpectedWriteObjectHeader() {
      return getHeader(Action.WRITE_OBJECTS);
    }

    public String getExpectedDeleteObjectHeader() {
      return getHeader(Action.DELETE_OBJECTS);
    }

    public String getExpectedResumableUploadHeader() {
      return getHeader(Action.WRITE_OBJECTS);
    }

    public String getExpectedComposeHeader(ImmutableList<SourceObject> sources) {
      StringBuilder sb = new StringBuilder();
      sb.append(String.format("[%s;%s;%s]", bucketName, getObjectName(), Action.WRITE_OBJECTS));

      for (SourceObject so : sources) {
        sb.append(String.format("[%s;%s;%s]", bucketName, so.getName(), Action.READ_OBJECTS));
      }

      return String.format("Bearer %s", sb);
    }
  }

  private class NoopClientCall<RequestT, ResponseT> extends ClientCall<RequestT, ResponseT> {
    @Override
    public void start(Listener<ResponseT> listener, Metadata metadata) {}

    @Override
    public void request(int i) {}

    @Override
    public void cancel(@Nullable String s, @Nullable Throwable throwable) {}

    @Override
    public void halfClose() {}

    @Override
    public void sendMessage(RequestT requestT) {}
  }
}
