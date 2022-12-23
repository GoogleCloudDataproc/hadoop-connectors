package com.google.cloud.hadoop.gcsio;

import static com.google.cloud.hadoop.gcsio.testing.MockGoogleCloudStorageImplFactory.mockGcsJavaStorage;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.jsonErrorResponse;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.mockTransport;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.client.testing.http.MockHttpTransport;
import com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.ErrorResponses;
import com.google.cloud.storage.Storage;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class GcsJavaClientImplCreateTest {

  private static final String BUCKET_NAME = "foo-bucket";
  private static final String OBJECT_NAME = "bar-object";
  private static final Storage mockedJavaClientStorage = mock(Storage.class);

  /**
   * Test handling of various types of Errors thrown during JSON API call for
   * GoogleCloudStorage.create(2).
   */
  @Test
  public void testCreateObjectApiError() throws IOException {
    // Set up the mock Insert to throw an exception when execute() is called.
    Error fakeError = new Error("Fake error");
    // Mocking getItemInfo
    MockHttpTransport transport = mockTransport(jsonErrorResponse(ErrorResponses.NOT_FOUND));
    when(mockedJavaClientStorage.writer(any(), any()))
        .thenReturn(
            new FakeWriteChannel() {
              @Override
              public void close() {
                throw fakeError;
              }
            });
    GoogleCloudStorage gcs = mockGcsJavaStorage(transport, mockedJavaClientStorage);
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
    // Mocking getItemInfo
    MockHttpTransport transport = mockTransport(jsonErrorResponse(ErrorResponses.NOT_FOUND));
    when(mockedJavaClientStorage.writer(any(), any()))
        .thenReturn(
            new FakeWriteChannel() {
              @Override
              public void close() {
                throw fakeException;
              }
            });
    GoogleCloudStorage gcs = mockGcsJavaStorage(transport, mockedJavaClientStorage);

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
    when(mockedJavaClientStorage.writer(any(), any()))
        .thenReturn(
            new FakeWriteChannel() {
              @Override
              public int write(ByteBuffer src) {
                try {
                  writeStartedLatch.countDown();
                  waitForEverLatch.await();
                  fail("Unexpected to get here.");
                } catch (InterruptedException e) {
                  // Expected test behavior. Do nothing.
                } finally {
                  threadsDoneLatch.countDown();
                }
                return 0;
              }
            });
    MockHttpTransport transport = mockTransport(jsonErrorResponse(ErrorResponses.NOT_FOUND));
    GoogleCloudStorage gcs = mockGcsJavaStorage(transport, mockedJavaClientStorage);

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
}
