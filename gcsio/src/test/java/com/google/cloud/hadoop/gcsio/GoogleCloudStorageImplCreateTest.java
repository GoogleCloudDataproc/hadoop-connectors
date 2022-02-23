/*
 * Copyright 2021 Google Inc. All Rights Reserved.
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

import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.arbitraryInputStreamSupplier;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.jsonErrorResponse;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.mockTransport;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.throwErrorWhenGetContent;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.throwRuntimeExceptionWhenGetContent;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.services.storage.Storage;
import com.google.auth.Credentials;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.cloud.hadoop.util.HttpTransportFactory;
import com.google.cloud.hadoop.util.RetryHttpInitializer;
import com.google.cloud.hadoop.util.RetryHttpInitializerOptions;
import com.google.cloud.hadoop.util.testing.FakeCredentials;
import com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.ErrorResponses;
import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests that require a particular configuration of GoogleCloudStorageImpl. */
@RunWith(JUnit4.class)
public class GoogleCloudStorageImplCreateTest {

  private static final String PROJECT_ID = "google.com:foo-project";
  private static final String BUCKET_NAME = "foo-bucket";
  private static final String OBJECT_NAME = "bar-object";

  @Test
  public void create_grpcAndVmComputeEngineCredentials_useDirectpath() throws IOException {
    GoogleCloudStorageImpl gcs =
        new GoogleCloudStorageImpl(
            GoogleCloudStorageOptions.builder().setAppName("app").setGrpcEnabled(true).build(),
            createStorage());
    assertThat(gcs.getStorageStubProvider().getGrpcDecorator())
        .isInstanceOf(StorageStubProvider.DirectPathGrpcDecorator.class);
  }

  @Test
  public void create_grpcAndDisableDirectPathAndVmComputeEngineCredentials_useCloudpath()
      throws IOException {
    GoogleCloudStorageImpl gcs =
        new GoogleCloudStorageImpl(
            GoogleCloudStorageOptions.builder()
                .setAppName("app")
                .setGrpcEnabled(true)
                .setDirectPathPreferred(false)
                .build(),
            createStorage());
    assertThat(gcs.getStorageStubProvider().getGrpcDecorator())
        .isInstanceOf(StorageStubProvider.CloudPathGrpcDecorator.class);
  }

  @Test
  public void create_grpcAndNonComputeEngineCredentials_useCloudpath() throws IOException {
    GoogleCloudStorageImpl gcs =
        new GoogleCloudStorageImpl(
            GoogleCloudStorageOptions.builder().setAppName("app").setGrpcEnabled(true).build(),
            createStorage(new FakeCredentials()));
    assertThat(gcs.getStorageStubProvider().getGrpcDecorator())
        .isInstanceOf(StorageStubProvider.CloudPathGrpcDecorator.class);
  }

  @Test
  public void create_grpcAndNullCredentials_useCloudpath() throws IOException {
    GoogleCloudStorageImpl gcs =
        new GoogleCloudStorageImpl(
            GoogleCloudStorageOptions.builder().setAppName("app").setGrpcEnabled(true).build(),
            createStorage(/* credentials= */ null));
    assertThat(gcs.getStorageStubProvider().getGrpcDecorator())
        .isInstanceOf(StorageStubProvider.CloudPathGrpcDecorator.class);
  }

  @Test
  public void create_grpcAndTrafficDirector_useTrafficDirector() throws IOException {
    GoogleCloudStorageImpl gcs =
        new GoogleCloudStorageImpl(
            GoogleCloudStorageOptions.builder()
                .setAppName("app")
                .setGrpcEnabled(true)
                .setTrafficDirectorEnabled(true)
                .build(),
            createStorage());
    assertThat(gcs.getStorageStubProvider().getGrpcDecorator())
        .isInstanceOf(StorageStubProvider.TrafficDirectorGrpcDecorator.class);
  }

  /**
   * Test handling of various types of Errors thrown during JSON API call for
   * GoogleCloudStorage.create(2).
   */
  @Test
  public void testCreateObjectApiError() throws IOException {
    // Set up the mock Insert to throw an exception when execute() is called.
    Error fakeError = new Error("Fake error");
    MockHttpTransport transport =
        mockTransport(
            jsonErrorResponse(ErrorResponses.NOT_FOUND), throwErrorWhenGetContent(fakeError));
    GoogleCloudStorageImpl gcs = mockedGcs(transport);

    WritableByteChannel writeChannel = gcs.create(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    assertThat(writeChannel.isOpen()).isTrue();

    Error thrown = assertThrows(Error.class, writeChannel::close);
    assertThat(thrown).isEqualTo(fakeError);
  }

  /**
   * Test handling of various types of exceptions thrown during JSON API call for
   * GoogleCloudStorage.create(2).
   */
  @Test
  public void testCreateObjectApiRuntimeException() throws IOException {
    // Set up the mock Insert to throw an exception when execute() is called.
    RuntimeException fakeException = new RuntimeException("Fake exception");
    MockHttpTransport transport =
        mockTransport(
            jsonErrorResponse(ErrorResponses.NOT_FOUND),
            throwRuntimeExceptionWhenGetContent(fakeException));
    GoogleCloudStorageImpl gcs = (GoogleCloudStorageImpl) mockedGcs(transport);

    WritableByteChannel writeChannel = gcs.create(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    assertThat(writeChannel.isOpen()).isTrue();

    IOException thrown = assertThrows(IOException.class, writeChannel::close);
    assertThat(thrown).hasCauseThat().isEqualTo(fakeException);
  }

  /**
   * Test handling when the parent thread waiting for the write to finish via the close call is
   * interrupted, that the actual write is cancelled and interrupted as well.
   */
  @Test
  public void testCreateObjectApiInterruptedException() throws Exception {
    // Set up the mock Insert to wait forever.
    CountDownLatch waitForEverLatch = new CountDownLatch(1);
    CountDownLatch writeStartedLatch = new CountDownLatch(2);
    CountDownLatch threadsDoneLatch = new CountDownLatch(2);
    MockHttpTransport transport =
        mockTransport(
            jsonErrorResponse(ErrorResponses.NOT_FOUND),
            arbitraryInputStreamSupplier(
                () -> {
                  try {
                    writeStartedLatch.countDown();
                    waitForEverLatch.await();
                    fail("Unexpected to get here.");
                  } catch (InterruptedException e) {
                    // Expected test behavior. Do nothing.
                  } finally {
                    threadsDoneLatch.countDown();
                  }
                  return null;
                }));
    GoogleCloudStorageImpl gcs = mockedGcs(transport);

    WritableByteChannel writeChannel = gcs.create(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    assertThat(writeChannel.isOpen()).isTrue();

    ExecutorService executorService = Executors.newCachedThreadPool();
    Future<?> write =
        executorService.submit(
            () -> {
              writeStartedLatch.countDown();
              try {
                IOException ioe = assertThrows(IOException.class, writeChannel::close);
                assertThat(ioe).isInstanceOf(ClosedByInterruptException.class);
              } finally {
                threadsDoneLatch.countDown();
              }
            });
    // Wait for the insert object to be executed, then cancel the writing thread, and finally wait
    // for the two threads to finish.
    assertWithMessage("Neither thread started.")
        .that(writeStartedLatch.await(5000, TimeUnit.MILLISECONDS))
        .isTrue();
    write.cancel(/* interrupt= */ true);
    assertWithMessage("Failed to wait for tasks to get interrupted.")
        .that(threadsDoneLatch.await(5000, TimeUnit.MILLISECONDS))
        .isTrue();
  }

  private static Storage createStorage() throws IOException {
    return createStorage(ComputeEngineCredentials.create());
  }

  private static Storage createStorage(Credentials credentials) throws IOException {
    return new Storage.Builder(
            HttpTransportFactory.createHttpTransport(),
            GsonFactory.getDefaultInstance(),
            new RetryHttpInitializer(
                credentials,
                RetryHttpInitializerOptions.builder()
                    .setDefaultUserAgent("foo-user-agent")
                    .build()))
        .build();
  }

  private GoogleCloudStorageImpl mockedGcs(HttpTransport transport) {
    Storage storage =
        new Storage(
            transport,
            GsonFactory.getDefaultInstance(),
            new TrackingHttpRequestInitializer(
                new RetryHttpInitializer(
                    new FakeCredentials(),
                    RetryHttpInitializerOptions.builder()
                        .setDefaultUserAgent("gcs-io-unit-test")
                        .build()),
                false));
    return new GoogleCloudStorageImpl(
        GoogleCloudStorageOptions.builder()
            .setAppName("gcsio-unit-test")
            .setProjectId(PROJECT_ID)
            .build(),
        storage,
        null);
  }
}
