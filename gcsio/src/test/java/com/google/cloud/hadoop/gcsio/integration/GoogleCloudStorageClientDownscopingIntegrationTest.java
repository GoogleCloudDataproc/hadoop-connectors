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

package com.google.cloud.hadoop.gcsio.integration;

import static com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.getStandardOptionBuilder;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.NoCredentials;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageClientImpl;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.TestBucketHelper;
import com.google.cloud.hadoop.util.AccessBoundary;
import com.google.common.collect.ImmutableList;
import com.google.common.flogger.GoogleLogger;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Integration test for {@code GoogleCloudStorageClientGrpcDownscopingInterceptor} ensuring that
 * chained interceptors do not duplicate Authorization headers.
 */
@RunWith(JUnit4.class)
public class GoogleCloudStorageClientDownscopingIntegrationTest {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();
  private static final TestBucketHelper BUCKET_HELPER =
      new TestBucketHelper("downscoping-it-bucket");
  private static final String TEST_BUCKET = BUCKET_HELPER.getUniqueBucketPrefix();
  private static final Metadata.Key<String> AUTH_KEY =
      Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER);

  @Test
  public void testChainedInterceptorsDoNotDuplicateHeaders() throws Exception {
    verifyInterceptorsDoNotDuplicateHeaders(NoCredentials.getInstance(), "test-object");
  }

  @Test
  public void testChainedInterceptorsDoNotDuplicateHeaders_withCredentials() throws Exception {
    verifyInterceptorsDoNotDuplicateHeaders(
        GoogleCloudStorageTestHelper.getCredentials(), "test-object-with-creds");
  }

  private void verifyInterceptorsDoNotDuplicateHeaders(
      com.google.auth.Credentials credentials, String objectName) throws Exception {
    // Arrange
    GoogleCloudStorageOptions options =
        getStandardOptionBuilder().setGrpcEnabled(true).setGrpcWriteEnabled(true).build();

    String token1 = "token-from-interceptor-1";
    String token2 = "token-from-interceptor-2";

    ClientInterceptor interceptor1 =
        createInterceptor(
            boundaries -> {
              logger.atInfo().log("Token Provider 1 called");
              return token1;
            });
    ClientInterceptor interceptor2 =
        createInterceptor(
            boundaries -> {
              logger.atInfo().log("Token Provider 2 called");
              return token2;
            });

    HeaderCapturingInterceptor capturingInterceptor = new HeaderCapturingInterceptor();

    // Chain them: Interceptor1 -> Interceptor2 -> Capturing -> Transport
    ImmutableList<ClientInterceptor> interceptors =
        ImmutableList.of(interceptor1, interceptor2, capturingInterceptor);

    GoogleCloudStorage gcs =
        GoogleCloudStorageClientImpl.builder()
            .setOptions(options)
            .setCredentials(credentials)
            .setGRPCInterceptors(interceptors)
            .build();

    // Act
    try {
      StorageResourceId resourceId = new StorageResourceId(TEST_BUCKET, objectName);
      try (WritableByteChannel channel = gcs.create(resourceId)) {
        channel.write(ByteBuffer.wrap("test data".getBytes()));
      }
    } catch (IOException | RuntimeException e) {
      // Catching RuntimeException is expected. We use NoCredentials to isolate the
      // HeaderCapturingInterceptor logic, which intentionally causes the underlying library
      // to crash during initialization. A generic catch ensures test robustness.
      logger.atInfo().withCause(e).log("Operation failed with exception");
    }

    // Assert
    Metadata headers = capturingInterceptor.capturedHeaders.get();
    assertThat(headers).isNotNull();

    Iterable<String> authHeaders = headers.getAll(AUTH_KEY);
    assertThat(authHeaders).isNotNull();

    AtomicInteger headerCount = new AtomicInteger(0);
    AtomicReference<String> capturedAuth = new AtomicReference<>();

    authHeaders.forEach(
        header -> {
          capturedAuth.set(header);
          headerCount.incrementAndGet();
          logger.atInfo().log("Captured Auth Header: %s", header);
        });

    // Expect exactly one header
    assertThat(headerCount.get()).isEqualTo(1);
    // Expect it to be from the inner interceptor in this chain setup
    assertThat(capturedAuth.get()).contains(token2);
  }

  private ClientInterceptor createInterceptor(Function<List<AccessBoundary>, String> tokenFn)
      throws Exception {
    Class<?> cls =
        Class.forName(
            "com.google.cloud.hadoop.gcsio.GoogleCloudStorageClientGrpcDownscopingInterceptor");
    Constructor<?> ctor = cls.getDeclaredConstructor(Function.class);
    ctor.setAccessible(true);
    return (ClientInterceptor) ctor.newInstance(tokenFn);
  }

  private static class HeaderCapturingInterceptor implements ClientInterceptor {
    final AtomicReference<Metadata> capturedHeaders = new AtomicReference<>();

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
        MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
      return new SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
        @Override
        public void start(Listener<RespT> responseListener, Metadata headers) {
          logger.atInfo().log("HeaderCapturingInterceptor.start called with headers: %s", headers);
          capturedHeaders.set(headers); // Capture headers
          super.start(responseListener, headers);
        }
      };
    }
  }
}
