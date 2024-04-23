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

import static com.google.common.truth.Truth.assertThat;
import static com.google.storage.v2.ServiceConstants.Values.MAX_WRITE_CHUNK_BYTES;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.client.testing.http.MockHttpTransport;
import com.google.auth.Credentials;
import com.google.cloud.WriteChannel;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.cloud.hadoop.util.RetryHttpInitializer;
import com.google.cloud.hadoop.util.RetryHttpInitializerOptions;
import com.google.cloud.hadoop.util.testing.FakeCredentials;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BlobWriteSession;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobWriteOption;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;

@RunWith(JUnit4.class)
public class GoogleCloudStorageClientWriteChannelTest {

  private static final String BUCKET_NAME = "bucket-name";
  private static final String OBJECT_NAME = "object-name";
  private static final String CONTENT_TYPE = "image/jpeg";
  private static final String CONTENT_ENCODING = "content-encoding";
  private static final String KMS_KEY = "kms-key";
  private static final long GENERATION_ID = 0L;
  private static final Map<String, String> metadata =
      Map.of("metadata-key-1", "dGVzdC1tZXRhZGF0YQ==");
  private static ExecutorService EXECUTOR_SERVICE = Executors.newCachedThreadPool();

  private final StorageResourceId resourceId =
      new StorageResourceId(BUCKET_NAME, OBJECT_NAME, GENERATION_ID);
  private GoogleCloudStorageClientWriteChannel writeChannel;
  // TODO: Instead of using mock shift to using fakes.
  private Storage mockedStorage = mock(Storage.class);
  private WriteChannel fakeWriteChannel;

  private final BlobWriteSession mockBlobWriteSession = mock(BlobWriteSession.class);
  private ArgumentCaptor<BlobInfo> blobInfoCapture = ArgumentCaptor.forClass(BlobInfo.class);
  ArgumentCaptor<BlobWriteOption> blobWriteOptionsCapture =
      ArgumentCaptor.forClass(BlobWriteOption.class);

  @Before
  public void setUp() throws Exception {
    fakeWriteChannel = spy(new FakeWriteChannel());
    // when(mockedStorage.writer(blobInfoCapture.capture(), blobWriteOptionsCapture.capture()))
    //     .thenReturn(fakeWriteChannel);
    when(mockedStorage.blobWriteSession(
            blobInfoCapture.capture(), blobWriteOptionsCapture.capture()))
        .thenReturn(mockBlobWriteSession);
    when(mockBlobWriteSession.open()).thenReturn(fakeWriteChannel);
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
    ByteString data =
        GoogleCloudStorageTestHelper.createTestData(
            MAX_WRITE_CHUNK_BYTES.getNumber() * numberOfChunks);
    writeChannel.write(data.asReadOnlyByteBuffer());
    writeChannel.close();
    // Fake writer only writes half the buffer at a time
    verify(fakeWriteChannel, times(1)).close();
    verifyBlobInfoProperties(blobInfoCapture, resourceId);
    verifyBlobWriteOptionProperties(blobWriteOptionsCapture);
  }

  @Test
  public void writeSingleChunkSuccess() throws IOException {
    int numberOfChunks = 1;
    // (chunkSize/2) < data.size < chunkSize
    ByteString data =
        GoogleCloudStorageTestHelper.createTestData(
            MAX_WRITE_CHUNK_BYTES.getNumber() * numberOfChunks - 1);
    writeChannel.write(data.asReadOnlyByteBuffer());
    writeChannel.close();
  }

  private GoogleCloudStorageClientWriteChannel getJavaStorageChannel() throws IOException {
    return new GoogleCloudStorageClientWriteChannel(
        mockedStorage,
        GoogleCloudStorageOptions.DEFAULT.toBuilder()
            .setWriteChannelOptions(
                AsyncWriteChannelOptions.DEFAULT.toBuilder().setGrpcChecksumsEnabled(true).build())
            .build(),
        resourceId,
        CreateObjectOptions.DEFAULT_NO_OVERWRITE.toBuilder()
            .setContentType(CONTENT_TYPE)
            .setContentEncoding(CONTENT_ENCODING)
            .setMetadata(GoogleCloudStorageTestHelper.getDecodedMetadata(metadata))
            .setKmsKeyName(KMS_KEY)
            .build());
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

  private GoogleCloudStorage mockGcsJavaStorage(
      MockHttpTransport transport, Storage clientLibraryStorage) throws IOException {
    Credentials fakeCredentials = new FakeCredentials();
    GoogleCloudStorageOptions options =
        GoogleCloudStorageOptions.builder()
            .setAppName("gcsio-unit-test")
            .setGrpcEnabled(true)
            .build();
    return GoogleCloudStorageClientImpl.builder()
        .setOptions(options)
        .setCredentials(fakeCredentials)
        .setHttpTransport(transport)
        .setHttpRequestInitializer(
            new RetryHttpInitializer(
                fakeCredentials,
                RetryHttpInitializerOptions.builder()
                    .setDefaultUserAgent("gcsio-unit-test")
                    .build()))
        .setClientLibraryStorage(clientLibraryStorage)
        .build();
  }
}
