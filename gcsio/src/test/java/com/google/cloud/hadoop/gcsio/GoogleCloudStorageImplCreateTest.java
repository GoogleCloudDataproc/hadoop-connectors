/*
 * Copyright 2021 Google LLC
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

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageClientImpl.BLOB_FIELDS;
import static com.google.cloud.hadoop.gcsio.MockGoogleCloudStorageImplFactory.mockedGcsClientImpl;
import static com.google.cloud.hadoop.gcsio.MockGoogleCloudStorageImplFactory.mockedGcsImpl;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.arbitraryInputStreamSupplier;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.inputStreamResponse;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.jsonDataResponse;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.jsonErrorResponse;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.mockTransport;
import static com.google.common.net.HttpHeaders.CONTENT_LENGTH;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.client.http.HttpTransport;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper;
import com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.ErrorResponses;
import com.google.cloud.hadoop.util.testing.ThrowingInputStream;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobWriteSession;
import com.google.cloud.storage.Storage.BlobField;
import com.google.cloud.storage.Storage.BlobGetOption;
import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileAlreadyExistsException;
import java.util.List;
import java.util.concurrent.*;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/** Tests that require a particular configuration of GoogleCloudStorageImpl. */
@RunWith(Parameterized.class)
public class GoogleCloudStorageImplCreateTest {

  private static final String BUCKET_NAME = "foo-bucket";
  private static final String OBJECT_NAME = "bar-object";
  private static final com.google.cloud.storage.Storage mockedJavaClientStorage =
      mock(com.google.cloud.storage.Storage.class);
  private final BlobWriteSession mockBlobWriteSession = mock(BlobWriteSession.class);

  private final boolean testStorageClientImpl;

  private GoogleCloudStorageOptions gcsOptions;

  public GoogleCloudStorageImplCreateTest(boolean tesStorageClientImpl) {
    this.testStorageClientImpl = tesStorageClientImpl;
  }

  @Parameters
  public static Iterable<Boolean> getTesStorageClientImplParameter() {
    return List.of(false, true);
  }

  @Before
  public void setUp() {
    gcsOptions =
        GoogleCloudStorageOptions.builder()
            .setAppName("gcsio-unit-test")
            .setProjectId("google.com:foo-project")
            .setGrpcEnabled(testStorageClientImpl)
            .build();
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
            jsonErrorResponse(ErrorResponses.NOT_FOUND),
            inputStreamResponse(
                CONTENT_LENGTH,
                /* headerValue = */ 1,
                new ThrowingInputStream(/* readException = */ null, fakeError)));
    // Below mocks will be used only when JavaClientStorage is enabled
    when(mockedJavaClientStorage.get(
            BlobId.of(BUCKET_NAME, OBJECT_NAME),
            BlobGetOption.fields(BLOB_FIELDS.toArray(new BlobField[0]))))
        .thenReturn(null);
    when(mockedJavaClientStorage.blobWriteSession(any(), any())).thenReturn(mockBlobWriteSession);
    when(mockBlobWriteSession.open())
        .thenReturn(
            new FakeWriteChannel() {
              @Override
              public void close() {
                throw fakeError;
              }
            });

    GoogleCloudStorage gcs = getCloudStorageImpl(transport, gcsOptions);

    WritableByteChannel writeChannel = gcs.create(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    assertThat(writeChannel.isOpen()).isTrue();

    Error thrown = assertThrows(Error.class, writeChannel::close);
    assertThat(thrown).isEqualTo(fakeError);
  }

  /**
   * Test handling of various types of Errors thrown during JSON API call for
   * GoogleCloudStorage.create(2).
   */
  @Test
  public void testCreateObjectOverwriteFile() throws IOException {
    MockHttpTransport transport =
        mockTransport(
            jsonDataResponse(
                GoogleCloudStorageTestHelper.newStorageObject(BUCKET_NAME, OBJECT_NAME)));
    GoogleCloudStorage gcs = getCloudStorageImpl(transport, gcsOptions);

    WritableByteChannel writeChannel =
        gcs.create(
            new StorageResourceId(BUCKET_NAME, OBJECT_NAME), CreateObjectOptions.DEFAULT_OVERWRITE);
    assertThat(writeChannel.isOpen()).isTrue();
  }

  /**
   * Test handling of various types of Errors thrown during JSON API call for
   * GoogleCloudStorage.create(2).
   */
  @Test
  public void testCreateObjectFileAlreadyPresent() throws IOException {
    MockHttpTransport transport =
        mockTransport(
            jsonDataResponse(
                GoogleCloudStorageTestHelper.newStorageObject(BUCKET_NAME, OBJECT_NAME)));
    GoogleCloudStorage gcs = getCloudStorageImpl(transport, gcsOptions);

    // Below mocks will be used only when JavaClientStorage is enabled.
    Blob mockedBlob = mock(Blob.class);
    when(mockedJavaClientStorage.get(
            BlobId.of(BUCKET_NAME, OBJECT_NAME),
            BlobGetOption.fields(BLOB_FIELDS.toArray(new BlobField[0]))))
        .thenReturn(mockedBlob);
    when(mockedBlob.getBucket()).thenReturn(BUCKET_NAME);
    when(mockedBlob.getName()).thenReturn(OBJECT_NAME);

    FileAlreadyExistsException thrown =
        assertThrows(
            FileAlreadyExistsException.class,
            () ->
                gcs.create(
                    new StorageResourceId(BUCKET_NAME, OBJECT_NAME),
                    CreateObjectOptions.DEFAULT_NO_OVERWRITE));
    assertThat(thrown).isInstanceOf(FileAlreadyExistsException.class);
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
            inputStreamResponse(
                CONTENT_LENGTH,
                /* headerValue = */ 1,
                new ThrowingInputStream(/* readException = */ null, fakeException)));
    // Below mocks will be used only when JavaClientStorage is enabled
    when(mockedJavaClientStorage.blobWriteSession(any(), any())).thenReturn(mockBlobWriteSession);
    when(mockBlobWriteSession.open())
        .thenReturn(
            new FakeWriteChannel() {
              @Override
              public void close() {
                throw fakeException;
              }
            });
    GoogleCloudStorage gcs = getCloudStorageImpl(transport, gcsOptions);

    WritableByteChannel writeChannel = gcs.create(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    assertThat(writeChannel.isOpen()).isTrue();

    IOException thrown = assertThrows(IOException.class, writeChannel::close);
    assertThat(thrown).hasCauseThat().isEqualTo(fakeException);
  }

  /**
   * Test handling when the parent thread waiting for the write operation to finish via the close
   * call is interrupted, that the actual write is cancelled and interrupted as well.
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

    // Below mocks will be used only when JavaClientStorage is enabled
    when(mockedJavaClientStorage.blobWriteSession(any(), any())).thenReturn(mockBlobWriteSession);
    when(mockBlobWriteSession.open())
        .thenReturn(
            new FakeWriteChannel() {
              @Override
              public void close() {
                try {
                  writeStartedLatch.countDown();
                  waitForEverLatch.await();
                } catch (InterruptedException e) {
                  // Expected test behavior. Do nothing.
                } finally {
                  threadsDoneLatch.countDown();
                }
                fail("Unexpected to get here.");
              }
            });

    GoogleCloudStorage gcs = getCloudStorageImpl(transport, gcsOptions);

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
    // callForClose will be waiting on write(waiting forever) to finish. Interrupt it.
    write.cancel(/* interrupt= */ true);
    assertWithMessage("Failed to wait for tasks to get interrupted.")
        .that(threadsDoneLatch.await(5000, TimeUnit.MILLISECONDS))
        .isTrue();
  }

  private GoogleCloudStorage getCloudStorageImpl(
      HttpTransport transport, GoogleCloudStorageOptions options) throws IOException {
    if (options != null) {
      return testStorageClientImpl
          ? mockedGcsClientImpl(options, transport, mockedJavaClientStorage)
          : mockedGcsImpl(options, transport);
    }
    return testStorageClientImpl
        ? mockedGcsClientImpl(transport, mockedJavaClientStorage)
        : mockedGcsImpl(transport);
  }
}
