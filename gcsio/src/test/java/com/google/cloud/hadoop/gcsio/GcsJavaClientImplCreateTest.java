package com.google.cloud.hadoop.gcsio;

import static com.google.cloud.hadoop.gcsio.testing.MockGoogleCloudStorageImplFactory.mockGcsJavaStorage;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.jsonDataResponse;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.jsonErrorResponse;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.mockTransport;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.ErrorResponses;
import com.google.cloud.storage.Storage;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileAlreadyExistsException;
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

  @Test
  public void testFileExistException() throws IOException {

    StorageObject storageObject =
        new StorageObject()
            .setBucket(BUCKET_NAME)
            .setName(OBJECT_NAME)
            .setSize(BigInteger.ONE)
            .setStorageClass("standard");
    // Mocking getItemInfo
    MockHttpTransport transport = mockTransport(jsonDataResponse(storageObject));

    GoogleCloudStorage gcs = mockGcsJavaStorage(transport, mockedJavaClientStorage);
    assertThrows(
        FileAlreadyExistsException.class,
        () ->
            gcs.create(
                new StorageResourceId(BUCKET_NAME, OBJECT_NAME),
                CreateObjectOptions.DEFAULT_NO_OVERWRITE));
  }

  @Test
  public void testResourceIllegalStateException() throws IOException {

    StorageObject storageObject =
        new StorageObject()
            .setBucket(BUCKET_NAME)
            .setName(OBJECT_NAME)
            .setSize(BigInteger.ONE)
            .setStorageClass("standard");
    // Mocking getItemInfo
    MockHttpTransport transport = mockTransport(jsonDataResponse(storageObject));

    GoogleCloudStorage gcs = mockGcsJavaStorage(transport, mockedJavaClientStorage);
    assertThrows(
        IllegalStateException.class,
        () -> gcs.create(new StorageResourceId(BUCKET_NAME, OBJECT_NAME)));
  }
}
