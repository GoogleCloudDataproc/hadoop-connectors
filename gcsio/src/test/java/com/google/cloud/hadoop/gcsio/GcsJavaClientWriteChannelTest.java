package com.google.cloud.hadoop.gcsio;

import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.jsonErrorResponse;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.mockTransport;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static com.google.storage.v2.ServiceConstants.Values.MAX_WRITE_CHUNK_BYTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.client.json.gson.GsonFactory;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.auth.Credentials;
import com.google.cloud.WriteChannel;
import com.google.cloud.hadoop.gcsio.GcsJavaClientImpl.GcsJavaClientImplBuilder;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.cloud.hadoop.util.RetryHttpInitializer;
import com.google.cloud.hadoop.util.RetryHttpInitializerOptions;
import com.google.cloud.hadoop.util.testing.FakeCredentials;
import com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.ErrorResponses;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobWriteOption;
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
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;

@RunWith(JUnit4.class)
public class GcsJavaClientWriteChannelTest {

  private static final String V1_BUCKET_NAME = "bucket-name";
  private static final String BUCKET_NAME = GrpcChannelUtils.toV2BucketName(V1_BUCKET_NAME);
  private static final String OBJECT_NAME = "object-name";
  private static final String CONTENT_TYPE = "image/jpeg";
  private static final String CONTENT_ENCODING = "content-encoding";
  private static final String KMS_KEY = "kms-key";
  private static final long GENERATION_ID = 0L;
  private static final Map<String, String> metadata =
      Map.of("metadata-key-1", "dGVzdC1tZXRhZGF0YQ==");

  private final StorageResourceId resourceId =
      new StorageResourceId(BUCKET_NAME, OBJECT_NAME, GENERATION_ID);
  private GcsJavaClientWriteChannel writeChannel;
  private Storage mockedStorage = mock(Storage.class);
  private WriteChannel fakeWriteChannel;
  ArgumentCaptor<BlobInfo> blobInfoCapture = ArgumentCaptor.forClass(BlobInfo.class);
  ArgumentCaptor<BlobWriteOption> blobWriteOptionsCapture =
      ArgumentCaptor.forClass(BlobWriteOption.class);

  private final ExecutorService executor = Executors.newCachedThreadPool();

  @Before
  public void setUp() throws Exception {
    fakeWriteChannel = spy(new FakeWriteChannel());
    when(mockedStorage.writer(blobInfoCapture.capture(), blobWriteOptionsCapture.capture()))
        .thenReturn(fakeWriteChannel);
    writeChannel = getJavaStorageChannel();
  }

  @Test
  public void writeMultipleChunksSuccess() throws IOException {
    fakeWriteChannel = spy(new FakeWriteChannel());
    when(mockedStorage.writer(blobInfoCapture.capture(), blobWriteOptionsCapture.capture()))
        .thenReturn(fakeWriteChannel);
    writeChannel = getJavaStorageChannel();

    int numberOfChunks = 10;
    writeChannel.initialize();
    ByteString data = createTestData(MAX_WRITE_CHUNK_BYTES.getNumber() * numberOfChunks);
    writeChannel.write(data.asReadOnlyByteBuffer());
    writeChannel.close();
    // Fake writer only writes half the buffer at a time
    verify(fakeWriteChannel, times(numberOfChunks * 2)).write(any());
    verify(fakeWriteChannel, times(1)).close();
    verifyBlobInfoProperties();
    verifyBlobWriteOptionProperties();
    assertTrue(writeChannel.isUploadSuccessful());
  }

  @Test
  public void writeSingleChunkSuccess() throws IOException {
    int numberOfChunks = 1;
    writeChannel.initialize();
    // (chunkSize/2) < data.size < chunkSize
    ByteString data = createTestData(MAX_WRITE_CHUNK_BYTES.getNumber() * numberOfChunks - 1);
    writeChannel.write(data.asReadOnlyByteBuffer());
    writeChannel.close();
    // Fake writer only writes half the buffer at a time
    verify(fakeWriteChannel, times(numberOfChunks * 2)).write(any());
    verify(fakeWriteChannel, times(1)).close();
    verifyBlobInfoProperties();
    verifyBlobWriteOptionProperties();
    assertTrue(writeChannel.isUploadSuccessful());
  }

  @Test
  public void writeMultipleChunksFailure() throws IOException {
    fakeWriteChannel = spy(new FakeWriteChannel(true));
    when(mockedStorage.writer(blobInfoCapture.capture(), blobWriteOptionsCapture.capture()))
        .thenReturn(fakeWriteChannel);
    writeChannel = getJavaStorageChannel();
    writeChannel.initialize();
    ByteString data = createTestData(MAX_WRITE_CHUNK_BYTES.getNumber() * 10);
    assertThrows(IOException.class, () -> writeChannel.write(data.asReadOnlyByteBuffer()));
    verify(fakeWriteChannel, times(1)).write(any());
    verifyBlobInfoProperties();
    verifyBlobWriteOptionProperties();
    assertThrows(IOException.class, writeChannel::close);
    assertFalse(writeChannel.isUploadSuccessful());
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
            new FakeWriteChannel() {
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
    GoogleCloudStorage gcs = mockGcsJavaStorage(transport, mockedJavaClientStorage);

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

  private ByteString createTestData(int numBytes) {
    byte[] result = new byte[numBytes];
    for (int i = 0; i < numBytes; ++i) {
      // Sequential data makes it easier to compare expected vs. actual in
      // case of error. Since chunk sizes are multiples of 256, the modulo
      // ensures chunks have different data.
      result[i] = (byte) (i % 257);
    }

    return ByteString.copyFrom(result);
  }

  private GcsJavaClientWriteChannel getJavaStorageChannel() {
    return new GcsJavaClientWriteChannel(
        mockedStorage,
        GoogleCloudStorageOptions.DEFAULT.toBuilder()
            .setWriteChannelOptions(
                AsyncWriteChannelOptions.DEFAULT.toBuilder().setGrpcChecksumsEnabled(true).build())
            .build(),
        resourceId,
        CreateObjectOptions.DEFAULT_NO_OVERWRITE.toBuilder()
            .setContentType(CONTENT_TYPE)
            .setContentEncoding(CONTENT_ENCODING)
            .setMetadata(getDecodedMetadata())
            .setKmsKeyName(KMS_KEY)
            .build(),
        executor);
  }

  private void verifyBlobInfoProperties() {
    BlobInfo blobInfo = blobInfoCapture.getValue();
    assertEquals(resourceId.getBucketName(), blobInfo.getBucket());
    assertEquals(resourceId.getObjectName(), blobInfo.getName());
    assertEquals(CONTENT_TYPE, blobInfo.getContentType());
    assertEquals(CONTENT_ENCODING, blobInfo.getContentEncoding());
    assertTrue(metadata.equals(blobInfo.getMetadata()));
  }

  private void verifyBlobWriteOptionProperties() {
    List<BlobWriteOption> optionsList = blobWriteOptionsCapture.getAllValues();
    assertTrue(optionsList.contains(BlobWriteOption.kmsKeyName(KMS_KEY)));
    assertTrue(optionsList.contains(BlobWriteOption.generationMatch()));
    assertTrue(optionsList.contains(BlobWriteOption.disableGzipContent()));
    assertTrue(optionsList.contains(BlobWriteOption.crc32cMatch()));
  }

  private Map<String, byte[]> getDecodedMetadata() {
    return metadata.entrySet().stream()
        .collect(
            Collectors.toMap(
                entity -> entity.getKey(),
                entity -> GoogleCloudStorageImpl.decodeMetadataValues(entity.getValue())));
  }

  private GoogleCloudStorage mockGcsJavaStorage(
      MockHttpTransport transport, Storage javaClientStorage) throws IOException {
    Credentials fakeCredential = new FakeCredentials();
    com.google.api.services.storage.Storage storage =
        new com.google.api.services.storage.Storage(
            transport,
            GsonFactory.getDefaultInstance(),
            new RetryHttpInitializer(
                new FakeCredentials(),
                RetryHttpInitializerOptions.builder()
                    .setDefaultUserAgent("gcs-io-unit-test")
                    .build()));
    GoogleCloudStorageOptions options =
        GoogleCloudStorageOptions.builder()
            .setAppName("gcsio-unit-test")
            .setGrpcEnabled(true)
            .build();
    return new GcsJavaClientImplBuilder(options, fakeCredential, null)
        .withApairyClientStorage(storage)
        .withJavaClientStorage(javaClientStorage)
        .build();
  }
}
