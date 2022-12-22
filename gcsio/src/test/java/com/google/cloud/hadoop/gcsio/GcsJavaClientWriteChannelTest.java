package com.google.cloud.hadoop.gcsio;

import static com.google.storage.v2.ServiceConstants.Values.MAX_WRITE_CHUNK_BYTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.RestorableState;
import com.google.cloud.WriteChannel;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobWriteOption;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;

@RunWith(JUnit4.class)
public class GcsJavaClientWriteChannelTest {

  private static final int GCS_MINIMUM_CHUNK_SIZE = 256 * 1024;
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
  private GCSJavaClientWriteChannel writeChannel;
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

  private GCSJavaClientWriteChannel getJavaStorageChannel() {
    return new GCSJavaClientWriteChannel(
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

  /** FakeWriterChannel which writes only half the passed in byteBuffer capacity at a time. */
  private class FakeWriteChannel implements WriteChannel {

    private boolean isOpen = false;

    private boolean writeException = false;

    public FakeWriteChannel() {
      isOpen = true;
    }

    public FakeWriteChannel(Boolean writeException) {
      this();
      this.writeException = writeException;
    }

    @Override
    public void setChunkSize(int i) {}

    @Override
    public RestorableState<WriteChannel> capture() {
      return null;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
      if (writeException) {
        throw new IOException("Intentionally triggered");
      }
      int bytesWritten = 0;
      // always writes half or lesser from the provided byte buffer capacity
      int capacity = src.capacity();
      if ((src.limit() - src.position()) <= capacity / 2) {
        bytesWritten = src.limit();
        src.position(src.limit());
      } else {
        bytesWritten = capacity / 2;
        src.position(src.position() + capacity / 2);
      }
      return bytesWritten;
    }

    @Override
    public boolean isOpen() {
      return isOpen;
    }

    @Override
    public void close() throws IOException {
      isOpen = false;
    }
  }
}
