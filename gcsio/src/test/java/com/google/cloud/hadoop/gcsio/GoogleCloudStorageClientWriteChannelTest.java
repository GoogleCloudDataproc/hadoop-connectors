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

import static com.google.cloud.hadoop.gcsio.testing.MockGoogleCloudStorageImplFactory.mockedGcsClientImpl;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.jsonErrorResponse;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.mockTransport;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static com.google.storage.v2.ServiceConstants.Values.MAX_WRITE_CHUNK_BYTES;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.client.testing.http.MockHttpTransport;
import com.google.cloud.WriteChannel;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.ErrorResponses;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobWriteOption;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.WritableByteChannel;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;

@RunWith(JUnit4.class)
public class GoogleCloudStorageClientWriteChannelTest {

  private static final String V1_BUCKET_NAME = "bucket-name";
  private static final String BUCKET_NAME = GrpcChannelUtils.toV2BucketName(V1_BUCKET_NAME);
  private static final String OBJECT_NAME = "object-name";
  private static final String CONTENT_TYPE = "image/jpeg";
  private static final String CONTENT_ENCODING = "content-encoding";
  private static final String KMS_KEY = "kms-key";
  private static final long GENERATION_ID = 0L;
  private static final Map<String, String> metadata =
      ImmutableMap.of("metadata-key-1", "dGVzdC1tZXRhZGF0YQ==");
  private static ExecutorService EXECUTOR_SERVICE = Executors.newCachedThreadPool();

  private final StorageResourceId resourceId =
      new StorageResourceId(BUCKET_NAME, OBJECT_NAME, GENERATION_ID);
  private GoogleCloudStorageClientWriteChannel writeChannel;
  // TODO: Instead of using mock shift to using fakes.
  private Storage mockedStorage = mock(Storage.class);
  private WriteChannel fakeWriteChannel;
  ArgumentCaptor<BlobInfo> blobInfoCapture = ArgumentCaptor.forClass(BlobInfo.class);
  ArgumentCaptor<BlobWriteOption> blobWriteOptionsCapture =
      ArgumentCaptor.forClass(BlobWriteOption.class);

  @Before
  public void setUp() throws Exception {
    fakeWriteChannel = spy(new CoopLockIntegrationTest.FakeWriteChannel());
    when(mockedStorage.writer(blobInfoCapture.capture(), blobWriteOptionsCapture.capture()))
        .thenReturn(fakeWriteChannel);
    writeChannel = getJavaStorageChannel();
  }

  @AfterClass
  public static void cleanUp() {
    try {
      EXECUTOR_SERVICE.shutdown();
    } finally {
      EXECUTOR_SERVICE = null;
    }
  }

  @Test
  public void writeMultipleChunksSuccess() throws IOException {
    int numberOfChunks = 10;
    writeChannel.initialize();
    ByteString data =
        GoogleCloudStorageTestHelper.createTestData(
            MAX_WRITE_CHUNK_BYTES.getNumber() * numberOfChunks);
    writeChannel.write(data.asReadOnlyByteBuffer());
    writeChannel.close();
    // Fake writer only writes half the buffer at a time
    verify(fakeWriteChannel, times(numberOfChunks * 2)).write(any());
    verify(fakeWriteChannel, times(1)).close();
    verifyBlobInfoProperties(blobInfoCapture, resourceId);
    verifyBlobWriteOptionProperties(blobWriteOptionsCapture);
    assertThat(writeChannel.isUploadSuccessful()).isTrue();
  }

  @Test
  public void writeSingleChunkSuccess() throws IOException {
    int numberOfChunks = 1;
    writeChannel.initialize();
    // (chunkSize/2) < data.size < chunkSize
    ByteString data =
        GoogleCloudStorageTestHelper.createTestData(
            MAX_WRITE_CHUNK_BYTES.getNumber() * numberOfChunks - 1);
    writeChannel.write(data.asReadOnlyByteBuffer());
    writeChannel.close();
    // Fake writer only writes half the buffer at a time
    verify(fakeWriteChannel, times(numberOfChunks * 2)).write(any());
    verify(fakeWriteChannel, times(1)).close();
    verifyBlobInfoProperties(blobInfoCapture, resourceId);
    verifyBlobWriteOptionProperties(blobWriteOptionsCapture);
    assertThat(writeChannel.isUploadSuccessful()).isTrue();
  }

  @Test
  public void writeMultipleChunksFailure() throws IOException {
    fakeWriteChannel = spy(new CoopLockIntegrationTest.FakeWriteChannel(true));
    ArgumentCaptor<BlobInfo> blobInfoCapture = ArgumentCaptor.forClass(BlobInfo.class);
    ArgumentCaptor<BlobWriteOption> blobWriteOptionsCapture =
        ArgumentCaptor.forClass(BlobWriteOption.class);
    when(mockedStorage.writer(blobInfoCapture.capture(), blobWriteOptionsCapture.capture()))
        .thenReturn(fakeWriteChannel);
    writeChannel = getJavaStorageChannel();
    writeChannel.initialize();
    ByteString data =
        GoogleCloudStorageTestHelper.createTestData(MAX_WRITE_CHUNK_BYTES.getNumber() * 10);
    assertThrows(IOException.class, () -> writeChannel.write(data.asReadOnlyByteBuffer()));
    verify(fakeWriteChannel, times(1)).write(any());
    verifyBlobInfoProperties(blobInfoCapture, resourceId);
    verifyBlobWriteOptionProperties(blobWriteOptionsCapture);
    assertThrows(IOException.class, writeChannel::close);
    assertThat(writeChannel.isUploadSuccessful()).isFalse();
  }

  /**
   * Test handling when the parent thread waiting for the write to finish via the close call is
   * interrupted, that the actual write is cancelled and interrupted as well.
   */
  @Test
  public void testCreateObjectApiInterruptedException() throws Exception {

    Storage mockedJavaClientStorage = mock(Storage.class);
    // Set up the mock Insert to wait forever.
    CountDownLatch waitForEverLatch = new CountDownLatch(1);
    CountDownLatch writeStartedLatch = new CountDownLatch(2);
    CountDownLatch threadsDoneLatch = new CountDownLatch(2);
    // Mock getItemInfo call
    MockHttpTransport transport = mockTransport(jsonErrorResponse(ErrorResponses.NOT_FOUND));

    when(mockedJavaClientStorage.writer(any(), any()))
        .thenReturn(
            new CoopLockIntegrationTest.FakeWriteChannel() {
              @Override
              public int write(ByteBuffer src) {
                try {
                  writeStartedLatch.countDown();
                  waitForEverLatch.await();
                } catch (InterruptedException e) {
                  // Expected test behavior. Do nothing.
                } finally {
                  threadsDoneLatch.countDown();
                }
                fail("Unexpected to get here.");
                return 0;
              }
            });
    GoogleCloudStorage gcs = mockedGcsClientImpl(transport, mockedJavaClientStorage);

    WritableByteChannel writeChannel = gcs.create(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    assertThat(writeChannel.isOpen()).isTrue();

    ExecutorService executorService = Executors.newCachedThreadPool();
    Future<?> callForClose =
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
    callForClose.cancel(/* interrupt= */ true);
    assertWithMessage("Failed to wait for tasks to get interrupted.")
        .that(threadsDoneLatch.await(5000, TimeUnit.MILLISECONDS))
        .isTrue();
  }

  private GoogleCloudStorageClientWriteChannel getJavaStorageChannel() {
    return new GoogleCloudStorageClientWriteChannel(
        mockedStorage,
        GoogleCloudStorageOptions.DEFAULT
            .toBuilder()
            .setWriteChannelOptions(
                AsyncWriteChannelOptions.DEFAULT.toBuilder().setGrpcChecksumsEnabled(true).build())
            .build(),
        resourceId,
        CreateObjectOptions.DEFAULT_NO_OVERWRITE
            .toBuilder()
            .setContentType(CONTENT_TYPE)
            .setContentEncoding(CONTENT_ENCODING)
            .setMetadata(GoogleCloudStorageTestHelper.getDecodedMetadata(metadata))
            .setKmsKeyName(KMS_KEY)
            .build(),
        EXECUTOR_SERVICE);
  }

  private static void verifyBlobInfoProperties(
      ArgumentCaptor<BlobInfo> blobInfoCapture, StorageResourceId resourceId) {
    BlobInfo blobInfo = blobInfoCapture.getValue();
    assertThat(blobInfo.getBucket()).isEqualTo(resourceId.getBucketName());
    assertThat(blobInfo.getName()).isEqualTo(resourceId.getObjectName());
    assertThat(blobInfo.getContentType()).isEqualTo(CONTENT_TYPE);
    assertThat(blobInfo.getContentEncoding()).isEqualTo(CONTENT_ENCODING);
    assertThat(blobInfo.getMetadata()).isEqualTo(metadata);
  }

  private static void verifyBlobWriteOptionProperties(
      ArgumentCaptor<BlobWriteOption> blobWriteOptionsCapture) {
    List<BlobWriteOption> optionsList = blobWriteOptionsCapture.getAllValues();
    assertThat(optionsList)
        .containsExactly(
            BlobWriteOption.kmsKeyName(KMS_KEY),
            BlobWriteOption.generationMatch(),
            BlobWriteOption.disableGzipContent(),
            BlobWriteOption.crc32cMatch());
  }
}
