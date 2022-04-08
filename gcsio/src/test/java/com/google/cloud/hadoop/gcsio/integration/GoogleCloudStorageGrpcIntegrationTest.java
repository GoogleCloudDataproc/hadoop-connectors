package com.google.cloud.hadoop.gcsio.integration;

import static com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.assertByteArrayEquals;
import static com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.assertObjectContent;
import static com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.writeObject;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageItemInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions.MetricsSink;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.Fadvise;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.TestBucketHelper;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;
import java.util.Arrays;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class GoogleCloudStorageGrpcIntegrationTest {

  // Prefix this name with the prefix used in other gcs io integrate tests once it's whitelisted by
  // GCS to access gRPC API.
  private static final String BUCKET_NAME_PREFIX = "gcs-grpc-team";

  private static final TestBucketHelper BUCKET_HELPER = new TestBucketHelper(BUCKET_NAME_PREFIX);

  private static final String BUCKET_NAME = BUCKET_HELPER.getUniqueBucketName("shared");

  private static GoogleCloudStorage createGoogleCloudStorage() throws IOException {
    return new GoogleCloudStorageImpl(
        GoogleCloudStorageTestHelper.getStandardOptionBuilder().setGrpcEnabled(true).build(),
        GoogleCloudStorageTestHelper.getCredentials());
  }

  private static GoogleCloudStorage createGoogleCloudStorage(
      AsyncWriteChannelOptions asyncWriteChannelOptions) throws IOException {
    return new GoogleCloudStorageImpl(
        GoogleCloudStorageTestHelper.getStandardOptionBuilder()
            .setWriteChannelOptions(asyncWriteChannelOptions)
            .setGrpcEnabled(true)
            .build(),
        GoogleCloudStorageTestHelper.getCredentials());
  }

  @BeforeClass
  public static void createBuckets() throws IOException {
    GoogleCloudStorage rawStorage = createGoogleCloudStorage();
    rawStorage.createBucket(BUCKET_NAME);
  }

  @AfterClass
  public static void cleanupBuckets() throws IOException {
    GoogleCloudStorage rawStorage = createGoogleCloudStorage();
    BUCKET_HELPER.cleanup(rawStorage);
  }

  @Test
  public void testCreateObject() throws IOException {
    GoogleCloudStorage rawStorage = createGoogleCloudStorage();
    StorageResourceId objectToCreate =
        new StorageResourceId(BUCKET_NAME, "testCreateObject_Object");
    byte[] objectBytes = writeObject(rawStorage, objectToCreate, /* objectSize= */ 512);

    assertObjectContent(rawStorage, objectToCreate, objectBytes);
  }

  @Test
  public void testReadWriteObjectWithTDEnabled() throws IOException {
    GoogleCloudStorageOptions storageOptions =
        GoogleCloudStorageTestHelper.getStandardOptionBuilder()
            .setGrpcEnabled(true)
            .setTrafficDirectorEnabled(true)
            .build();
    GoogleCloudStorage rawStorage =
        new GoogleCloudStorageImpl(storageOptions, GoogleCloudStorageTestHelper.getCredentials());
    StorageResourceId objectToCreate =
        new StorageResourceId(BUCKET_NAME, "testCreateObject_Object_TD_Enabled");
    byte[] objectBytes = writeObject(rawStorage, objectToCreate, /* objectSize= */ 512);

    assertObjectContent(rawStorage, objectToCreate, objectBytes);
  }

  @Test
  public void testCreateExistingObject() throws IOException {
    GoogleCloudStorage rawStorage = createGoogleCloudStorage();
    StorageResourceId objectToCreate =
        new StorageResourceId(BUCKET_NAME, "testCreateExistingObject_Object");
    writeObject(rawStorage, objectToCreate, /* objectSize= */ 128);

    GoogleCloudStorageItemInfo createdItemInfo = rawStorage.getItemInfo(objectToCreate);
    assertThat(createdItemInfo.exists()).isTrue();
    assertThat(createdItemInfo.getSize()).isEqualTo(128);

    byte[] overwriteBytesToWrite = writeObject(rawStorage, objectToCreate, /* objectSize= */ 256);

    GoogleCloudStorageItemInfo overwrittenItemInfo = rawStorage.getItemInfo(objectToCreate);
    assertThat(overwrittenItemInfo.exists()).isTrue();
    assertThat(overwrittenItemInfo.getSize()).isEqualTo(256);
    assertObjectContent(rawStorage, objectToCreate, overwriteBytesToWrite);
  }

  @Test
  public void testCreateEmptyObject() throws IOException {
    GoogleCloudStorage rawStorage = createGoogleCloudStorage();
    StorageResourceId objectToCreate =
        new StorageResourceId(BUCKET_NAME, "testCreateEmptyObject_Object");

    rawStorage.createEmptyObject(objectToCreate);

    GoogleCloudStorageItemInfo itemInfo = rawStorage.getItemInfo(objectToCreate);

    assertThat(itemInfo.exists()).isTrue();
    assertThat(itemInfo.getSize()).isEqualTo(0);
  }

  @Test
  public void testCreateInvalidObject() throws IOException {
    GoogleCloudStorage rawStorage = createGoogleCloudStorage();
    StorageResourceId objectToCreate =
        new StorageResourceId(BUCKET_NAME, "testCreateInvalidObject_InvalidObject\n");

    assertThrows(
        IOException.class, () -> writeObject(rawStorage, objectToCreate, /* objectSize= */ 10));
  }

  @Test
  public void testOpen() throws IOException {
    GoogleCloudStorage rawStorage = createGoogleCloudStorage();
    StorageResourceId objectToCreate = new StorageResourceId(BUCKET_NAME, "testOpen_Object");
    byte[] objectBytes = writeObject(rawStorage, objectToCreate, /* objectSize= */ 100);

    assertObjectContent(rawStorage, objectToCreate, objectBytes);
  }

  @Test
  public void testOpenWithMetricsEnabled() throws IOException {
    GoogleCloudStorage rawStorage =
        new GoogleCloudStorageImpl(
            GoogleCloudStorageTestHelper.getStandardOptionBuilder()
                .setMetricsSink(MetricsSink.CLOUD_MONITORING)
                .build(),
            GoogleCloudStorageTestHelper.getCredentials());
    StorageResourceId objectToCreate = new StorageResourceId(BUCKET_NAME, "testOpen_Object");
    byte[] objectBytes = writeObject(rawStorage, objectToCreate, /* objectSize= */ 100);
    assertObjectContent(rawStorage, objectToCreate, objectBytes);
  }

  @Test
  public void testOpenNonExistentItem() throws IOException {
    GoogleCloudStorage rawStorage = createGoogleCloudStorage();
    StorageResourceId resourceId =
        new StorageResourceId(BUCKET_NAME, "testOpenNonExistentItem_Object");
    IOException exception = assertThrows(IOException.class, () -> rawStorage.open(resourceId));
    assertThat(exception).hasMessageThat().contains("Item not found");
  }

  @Test
  public void testOpenEmptyObject() throws IOException {
    GoogleCloudStorage rawStorage = createGoogleCloudStorage();
    StorageResourceId resourceId = new StorageResourceId(BUCKET_NAME, "testOpenEmptyObject_Object");
    rawStorage.createEmptyObject(resourceId);

    assertObjectContent(rawStorage, resourceId, new byte[0]);
  }

  @Test
  public void testOpenLargeObject() throws IOException {
    GoogleCloudStorage rawStorage = createGoogleCloudStorage();
    StorageResourceId resourceId = new StorageResourceId(BUCKET_NAME, "testOpenLargeObject_Object");

    int partitionsCount = 50;
    byte[] partition =
        writeObject(rawStorage, resourceId, /* partitionSize= */ 10 * 1024 * 1024, partitionsCount);

    assertObjectContent(rawStorage, resourceId, partition, partitionsCount);
  }

  @Test
  public void testOpenObjectWithChecksum() throws IOException {
    AsyncWriteChannelOptions asyncWriteChannelOptions =
        AsyncWriteChannelOptions.builder().setGrpcChecksumsEnabled(true).build();
    GoogleCloudStorage rawStorage = createGoogleCloudStorage(asyncWriteChannelOptions);
    StorageResourceId objectToCreate =
        new StorageResourceId(BUCKET_NAME, "testOpenObjectWithChecksum_Object");
    byte[] objectBytes = writeObject(rawStorage, objectToCreate, /* objectSize= */ 100);

    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setGrpcChecksumsEnabled(true).build();
    assertObjectContent(rawStorage, objectToCreate, readOptions, objectBytes);
  }

  @Test
  public void testOpenObjectWithSeek() throws IOException {
    AsyncWriteChannelOptions asyncWriteChannelOptions =
        AsyncWriteChannelOptions.builder().setGrpcChecksumsEnabled(true).build();
    GoogleCloudStorage rawStorage = createGoogleCloudStorage(asyncWriteChannelOptions);
    StorageResourceId objectToCreate =
        new StorageResourceId(BUCKET_NAME, "testOpenObjectWithSeek_Object");
    byte[] objectBytes = writeObject(rawStorage, objectToCreate, /* objectSize= */ 100);
    int offset = 10;
    byte[] trimmedObjectBytes = Arrays.copyOfRange(objectBytes, offset, objectBytes.length);
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setGrpcChecksumsEnabled(true).build();
    assertObjectContent(
        rawStorage,
        objectToCreate,
        readOptions,
        trimmedObjectBytes,
        /* expectedBytesCount= */ 1,
        offset);
  }

  @Test
  public void testOpenObjectWithSeekOverBounds() throws IOException {
    AsyncWriteChannelOptions asyncWriteChannelOptions =
        AsyncWriteChannelOptions.builder().setGrpcChecksumsEnabled(true).build();
    GoogleCloudStorage rawStorage = createGoogleCloudStorage(asyncWriteChannelOptions);
    StorageResourceId objectToCreate =
        new StorageResourceId(BUCKET_NAME, "testOpenObjectWithSeekOverBounds_Object");
    byte[] objectBytes = writeObject(rawStorage, objectToCreate, /* objectSize= */ 4 * 1024 * 1024);
    int offset = 3 * 1024 * 1024;
    byte[] trimmedObjectBytes = Arrays.copyOfRange(objectBytes, offset, objectBytes.length);
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setGrpcChecksumsEnabled(true).build();
    assertObjectContent(
        rawStorage,
        objectToCreate,
        readOptions,
        trimmedObjectBytes,
        /* expectedBytesCount= */ 1,
        offset);
  }

  @Test
  public void testOpenObjectWithSeekLimits() throws IOException {
    AsyncWriteChannelOptions asyncWriteChannelOptions =
        AsyncWriteChannelOptions.builder().setGrpcChecksumsEnabled(true).build();
    GoogleCloudStorage rawStorage = createGoogleCloudStorage(asyncWriteChannelOptions);
    StorageResourceId objectToCreate =
        new StorageResourceId(BUCKET_NAME, "testOpenObjectWithSeekOverBounds_Object");
    byte[] objectBytes = writeObject(rawStorage, objectToCreate, /* objectSize= */ 1024);
    int offset = 100;
    byte[] trimmedObjectBytes = Arrays.copyOfRange(objectBytes, offset, objectBytes.length);
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder()
            .setInplaceSeekLimit(50)
            .setGrpcChecksumsEnabled(true)
            .build();
    assertObjectContent(
        rawStorage,
        objectToCreate,
        readOptions,
        trimmedObjectBytes,
        /* expectedBytesCount= */ 1,
        offset);
  }

  @Test
  public void testReadFooterDataWithGrpcChecksums() throws IOException {
    AsyncWriteChannelOptions asyncWriteChannelOptions =
        AsyncWriteChannelOptions.builder().setGrpcChecksumsEnabled(true).build();
    GoogleCloudStorage rawStorage = createGoogleCloudStorage(asyncWriteChannelOptions);
    StorageResourceId objectToCreate =
        new StorageResourceId(BUCKET_NAME, "testOpenObjectWithSeekToFooter_Object");
    int objectSize = 1024;
    byte[] objectBytes = writeObject(rawStorage, objectToCreate, /* objectSize= */ objectSize);
    int minRangeRequestSize = 200;
    int offset = objectSize - minRangeRequestSize / 2;
    byte[] trimmedObjectBytes = Arrays.copyOfRange(objectBytes, offset, objectBytes.length);
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder()
            .setInplaceSeekLimit(50)
            .setMinRangeRequestSize(minRangeRequestSize)
            .setFadvise(Fadvise.RANDOM)
            .setGrpcChecksumsEnabled(true)
            .build();
    assertObjectContent(
        rawStorage,
        objectToCreate,
        readOptions,
        trimmedObjectBytes,
        /* expectedBytesCount= */ 1,
        offset);
  }

  @Test
  public void testReadCachedFooterData() throws IOException {
    AsyncWriteChannelOptions asyncWriteChannelOptions = AsyncWriteChannelOptions.builder().build();
    GoogleCloudStorage rawStorage = createGoogleCloudStorage(asyncWriteChannelOptions);
    StorageResourceId objectToCreate =
        new StorageResourceId(BUCKET_NAME, "testReadCachedFooterData_Object");
    int objectSize = 10 * 1024 * 1024;
    byte[] objectBytes = writeObject(rawStorage, objectToCreate, /* objectSize= */ objectSize);

    int minRangeRequestSize = 200;
    int footerOffset = objectSize - minRangeRequestSize / 2;
    byte[] trimmedObjectBytes = Arrays.copyOfRange(objectBytes, footerOffset, objectBytes.length);
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setMinRangeRequestSize(minRangeRequestSize).build();
    assertObjectContent(
        rawStorage,
        objectToCreate,
        readOptions,
        trimmedObjectBytes,
        /* expectedBytesCount= */ 1,
        footerOffset);
  }

  @Test
  public void testReadSeekToFooterData() throws IOException {
    AsyncWriteChannelOptions asyncWriteChannelOptions = AsyncWriteChannelOptions.builder().build();
    GoogleCloudStorage rawStorage = createGoogleCloudStorage(asyncWriteChannelOptions);
    StorageResourceId objectToCreate =
        new StorageResourceId(BUCKET_NAME, "testReadSeekToFooterData_Object");
    int objectSize = 1024;
    byte[] objectBytes = writeObject(rawStorage, objectToCreate, /* objectSize= */ objectSize);

    int minRangeRequestSize = 200;
    int offset = objectSize - minRangeRequestSize / 4;
    byte[] trimmedObjectBytes = Arrays.copyOfRange(objectBytes, offset, objectBytes.length);
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setMinRangeRequestSize(minRangeRequestSize).build();
    assertObjectContent(
        rawStorage,
        objectToCreate,
        readOptions,
        trimmedObjectBytes,
        /* expectedBytesCount= */ 1,
        offset);
  }

  @Test
  public void testReadObjectCachedAsFooter() throws IOException {
    AsyncWriteChannelOptions asyncWriteChannelOptions = AsyncWriteChannelOptions.builder().build();
    GoogleCloudStorage rawStorage = createGoogleCloudStorage(asyncWriteChannelOptions);
    StorageResourceId objectToCreate =
        new StorageResourceId(BUCKET_NAME, "testReadSeekToFooterData_Object");
    int objectSize = 1024;
    byte[] objectBytes = writeObject(rawStorage, objectToCreate, /* objectSize= */ objectSize);

    int minRangeRequestSize = 4 * 1024;
    int offset = 0;
    byte[] trimmedObjectBytes = Arrays.copyOfRange(objectBytes, offset, objectBytes.length);
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setMinRangeRequestSize(minRangeRequestSize).build();
    assertObjectContent(
        rawStorage,
        objectToCreate,
        readOptions,
        trimmedObjectBytes,
        /* expectedBytesCount= */ 1,
        offset);
  }

  @Test
  public void testPartialReadFooterDataWithSingleChunk() throws IOException {
    AsyncWriteChannelOptions asyncWriteChannelOptions = AsyncWriteChannelOptions.builder().build();
    GoogleCloudStorage rawStorage = createGoogleCloudStorage(asyncWriteChannelOptions);
    StorageResourceId objectToCreate =
        new StorageResourceId(BUCKET_NAME, "testPartialReadFooterDataWithSingleChunk_Object");
    int objectSize = 2 * 1024 * 1024;
    byte[] objectBytes = writeObject(rawStorage, objectToCreate, /* objectSize= */ objectSize);

    int minRangeRequestSize = 1024; // server responds in 2MB chunks by default
    int readOffset = objectSize / 2;
    byte[] trimmedObjectBytes = Arrays.copyOfRange(objectBytes, readOffset, objectBytes.length);
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setMinRangeRequestSize(minRangeRequestSize).build();
    assertObjectContent(
        rawStorage,
        objectToCreate,
        readOptions,
        trimmedObjectBytes,
        /* expectedBytesCount= */ 1,
        readOffset);
  }

  @Test
  public void testPartialReadFooterDataWithMultipleChunks() throws IOException {
    AsyncWriteChannelOptions asyncWriteChannelOptions = AsyncWriteChannelOptions.builder().build();
    GoogleCloudStorage rawStorage = createGoogleCloudStorage(asyncWriteChannelOptions);
    StorageResourceId objectToCreate =
        new StorageResourceId(BUCKET_NAME, "testPartialReadFooterDataWithMultipleChunks_Object");
    int objectSize = 10 * 1024 * 1024;
    byte[] objectBytes = writeObject(rawStorage, objectToCreate, /* objectSize= */ objectSize);

    int minRangeRequestSize = 4 * 1024; // server responds in 2MB chunks by default
    int readOffset = objectSize / 2;
    byte[] trimmedObjectBytes = Arrays.copyOfRange(objectBytes, readOffset, objectBytes.length);
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setMinRangeRequestSize(minRangeRequestSize).build();
    assertObjectContent(
        rawStorage,
        objectToCreate,
        readOptions,
        trimmedObjectBytes,
        /* expectedBytesCount= */ 1,
        readOffset);
  }

  @Test
  public void testPartialReadFooterDataWithinSegment() throws IOException {
    AsyncWriteChannelOptions asyncWriteChannelOptions = AsyncWriteChannelOptions.builder().build();
    GoogleCloudStorage rawStorage = createGoogleCloudStorage(asyncWriteChannelOptions);
    StorageResourceId objectToCreate =
        new StorageResourceId(BUCKET_NAME, "testPartialReadFooterDataWithinSegment_Object");
    int objectSize = 10 * 1024;
    byte[] objectBytes = writeObject(rawStorage, objectToCreate, /* objectSize= */ objectSize);

    int minRangeRequestSize = 4 * 1024;
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setMinRangeRequestSize(minRangeRequestSize).build();
    int readOffset = 7 * 1024;
    try (SeekableByteChannel readChannel = rawStorage.open(objectToCreate, readOptions)) {
      byte[] segmentBytes = new byte[100];
      ByteBuffer segmentBuffer = ByteBuffer.wrap(segmentBytes);
      readChannel.position(readOffset);
      readChannel.read(segmentBuffer);
      byte[] expectedSegment =
          Arrays.copyOfRange(
              objectBytes, /* from= */ readOffset, /* to= */ readOffset + segmentBytes.length);
      assertWithMessage("Unexpected segment data read.")
          .that(segmentBytes)
          .isEqualTo(expectedSegment);
    }
  }

  @Test
  public void testPartialRead() throws IOException {
    GoogleCloudStorage rawStorage = createGoogleCloudStorage();
    int segmentSize = 553;
    int segmentCount = 5;

    StorageResourceId resourceId =
        new StorageResourceId(BUCKET_NAME, "testReadPartialObjects_Object");
    byte[] data = writeObject(rawStorage, resourceId, /* objectSize= */ segmentCount * segmentSize);

    byte[][] readSegments = new byte[segmentCount][segmentSize];
    try (SeekableByteChannel readChannel = rawStorage.open(resourceId)) {
      for (int i = 0; i < segmentCount; i++) {
        ByteBuffer segmentBuffer = ByteBuffer.wrap(readSegments[i]);
        int bytesRead = readChannel.read(segmentBuffer);
        assertThat(bytesRead).isEqualTo(segmentSize);
        byte[] expectedSegment =
            Arrays.copyOfRange(
                data, /* from= */ i * segmentSize, /* to= */ i * segmentSize + segmentSize);
        assertWithMessage("Unexpected segment data read.")
            .that(readSegments[i])
            .isEqualTo(expectedSegment);
      }
    }
  }

  @Test
  public void testReadSeekToOffsetGreaterThanMinRangeRequestSize() throws IOException {
    AsyncWriteChannelOptions asyncWriteChannelOptions = AsyncWriteChannelOptions.builder().build();
    GoogleCloudStorage rawStorage = createGoogleCloudStorage(asyncWriteChannelOptions);
    StorageResourceId objectToCreate =
        new StorageResourceId(
            BUCKET_NAME, "testReadSeekToOffsetGreaterThanMinRangeRequestSize_Object");
    int objectSize = 20 * 1024;
    int inPlaceSeekLimit = 8 * 1024;
    int minRangeRequestSize = 4 * 1024;
    byte[] objectBytes = writeObject(rawStorage, objectToCreate, /* objectSize= */ objectSize);
    int totalBytes = 1;
    byte[] readArray = new byte[totalBytes];
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder()
            .setInplaceSeekLimit(inPlaceSeekLimit)
            .setMinRangeRequestSize(minRangeRequestSize)
            .setGrpcChecksumsEnabled(false)
            .setFadvise(Fadvise.RANDOM)
            .build();
    SeekableByteChannel readableByteChannel = rawStorage.open(objectToCreate, readOptions);
    int newPosition = 7 * 1024;
    readableByteChannel.position(newPosition);
    ByteBuffer readBuffer = ByteBuffer.wrap(readArray);
    int bytesRead = readableByteChannel.read(readBuffer);
    byte[] trimmedObjectBytes =
        Arrays.copyOfRange(objectBytes, newPosition, newPosition + totalBytes);
    byte[] readBufferByteArray = Arrays.copyOf(readBuffer.array(), readBuffer.limit());

    assertEquals(totalBytes, bytesRead);
    assertByteArrayEquals(trimmedObjectBytes, readBufferByteArray);
  }

  @Test
  public void testReadBeyondRangeWithFadviseRandom() throws IOException {
    AsyncWriteChannelOptions asyncWriteChannelOptions = AsyncWriteChannelOptions.builder().build();
    GoogleCloudStorage rawStorage = createGoogleCloudStorage(asyncWriteChannelOptions);
    StorageResourceId objectToCreate =
        new StorageResourceId(BUCKET_NAME, "testReadBeyondRangeWithFadviseRandom_Object");
    int objectSize = 20 * 1024;
    int inPlaceSeekLimit = 8 * 1024;
    int minRangeRequestSize = 4 * 1024;
    byte[] objectBytes = writeObject(rawStorage, objectToCreate, /* objectSize= */ objectSize);
    int totalBytes = 2 * 1024;
    byte[] readArray = new byte[totalBytes];
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder()
            .setInplaceSeekLimit(inPlaceSeekLimit)
            .setMinRangeRequestSize(minRangeRequestSize)
            .setGrpcChecksumsEnabled(false)
            .setFadvise(Fadvise.RANDOM)
            .build();
    SeekableByteChannel readableByteChannel = rawStorage.open(objectToCreate, readOptions);
    int newPosition = 7 * 1024;
    readableByteChannel.position(newPosition);
    ByteBuffer readBuffer = ByteBuffer.wrap(readArray);
    int bytesRead = readableByteChannel.read(readBuffer);
    byte[] trimmedObjectBytes =
        Arrays.copyOfRange(objectBytes, newPosition, newPosition + totalBytes);
    byte[] readBufferByteArray = Arrays.copyOf(readBuffer.array(), readBuffer.limit());

    assertEquals(totalBytes, bytesRead);
    assertByteArrayEquals(trimmedObjectBytes, readBufferByteArray);
  }

  @Test
  public void testReadBeyondRangeWithFadviseAuto() throws IOException {
    AsyncWriteChannelOptions asyncWriteChannelOptions = AsyncWriteChannelOptions.builder().build();
    GoogleCloudStorage rawStorage = createGoogleCloudStorage(asyncWriteChannelOptions);
    StorageResourceId objectToCreate =
        new StorageResourceId(BUCKET_NAME, "testReadBeyondRangeWithFadviseAuto_Object");
    int objectSize = 20 * 1024;
    int inPlaceSeekLimit = 8 * 1024;
    int minRangeRequestSize = 4 * 1024;
    byte[] objectBytes = writeObject(rawStorage, objectToCreate, /* objectSize= */ objectSize);
    int totalBytes = 2 * 1024;
    byte[] readArray = new byte[totalBytes];
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder()
            .setInplaceSeekLimit(inPlaceSeekLimit)
            .setMinRangeRequestSize(minRangeRequestSize)
            .setGrpcChecksumsEnabled(false)
            .setFadvise(Fadvise.AUTO)
            .build();
    SeekableByteChannel readableByteChannel = rawStorage.open(objectToCreate, readOptions);
    int newPosition = 7 * 1024;
    readableByteChannel.position(newPosition);
    ByteBuffer readBuffer = ByteBuffer.wrap(readArray);
    int bytesRead = readableByteChannel.read(readBuffer);
    byte[] trimmedObjectBytes =
        Arrays.copyOfRange(objectBytes, newPosition, newPosition + totalBytes);
    byte[] readBufferByteArray = Arrays.copyOf(readBuffer.array(), readBuffer.limit());

    assertEquals(totalBytes, bytesRead);
    assertByteArrayEquals(trimmedObjectBytes, readBufferByteArray);
  }

  @Test
  public void testReadBeyondRangeWithFadviseSequential() throws IOException {
    AsyncWriteChannelOptions asyncWriteChannelOptions = AsyncWriteChannelOptions.builder().build();
    GoogleCloudStorage rawStorage = createGoogleCloudStorage(asyncWriteChannelOptions);
    StorageResourceId objectToCreate =
        new StorageResourceId(BUCKET_NAME, "testReadBeyondRangeWithFadviseSequential_Object");
    int objectSize = 20 * 1024;
    int inPlaceSeekLimit = 8 * 1024;
    int minRangeRequestSize = 4 * 1024;
    byte[] objectBytes = writeObject(rawStorage, objectToCreate, /* objectSize= */ objectSize);
    int totalBytes = 2 * 1024;
    byte[] readArray = new byte[totalBytes];
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder()
            .setInplaceSeekLimit(inPlaceSeekLimit)
            .setMinRangeRequestSize(minRangeRequestSize)
            .setGrpcChecksumsEnabled(false)
            .setFadvise(Fadvise.SEQUENTIAL)
            .build();
    SeekableByteChannel readableByteChannel = rawStorage.open(objectToCreate, readOptions);
    int newPosition = 7 * 1024;
    readableByteChannel.position(newPosition);
    ByteBuffer readBuffer = ByteBuffer.wrap(readArray);
    int bytesRead = readableByteChannel.read(readBuffer);
    byte[] trimmedObjectBytes =
        Arrays.copyOfRange(objectBytes, newPosition, newPosition + totalBytes);
    byte[] readBufferByteArray = Arrays.copyOf(readBuffer.array(), readBuffer.limit());

    assertEquals(totalBytes, bytesRead);
    assertByteArrayEquals(trimmedObjectBytes, readBufferByteArray);
  }

  @Test
  public void testChannelClosedException() throws IOException {
    GoogleCloudStorage rawStorage = createGoogleCloudStorage();
    int totalBytes = 1200;

    StorageResourceId resourceId =
        new StorageResourceId(BUCKET_NAME, "testChannelClosedException_Object");
    writeObject(rawStorage, resourceId, /* objectSize= */ totalBytes);

    byte[] readArray = new byte[totalBytes];
    SeekableByteChannel readableByteChannel = rawStorage.open(resourceId);
    ByteBuffer readBuffer = ByteBuffer.wrap(readArray);
    readBuffer.limit(5);
    readableByteChannel.read(readBuffer);
    assertThat(readableByteChannel.position()).isEqualTo(readBuffer.position());

    readableByteChannel.close();
    readBuffer.clear();

    assertThrows(ClosedChannelException.class, () -> readableByteChannel.read(readBuffer));
  }
}
