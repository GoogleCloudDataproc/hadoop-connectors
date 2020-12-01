package com.google.cloud.hadoop.gcsio.integration;

import static com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.assertObjectContent;
import static com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.writeObject;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.Assert.assertThrows;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageItemInfo;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.TestBucketHelper;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;
import java.util.Arrays;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class GoogleCloudStorageGrpcIntegrationTest {

  // Prefix this name with the prefix used in other gcs io integrate tests once it's whitelisted by
  // GCS to access gRPC API.
  private static final String BUCKET_NAME_PREFIX = "gcs-grpc-team-dataproc-it";

  private static final TestBucketHelper BUCKET_HELPER = new TestBucketHelper(BUCKET_NAME_PREFIX);

  private static final String BUCKET_NAME = BUCKET_HELPER.getUniqueBucketName("shared");

  private static GoogleCloudStorage createGoogleCloudStorage() throws IOException {
    return new GoogleCloudStorageImpl(
        GoogleCloudStorageTestHelper.getStandardOptionBuilder().setGrpcEnabled(true).build(),
        GoogleCloudStorageTestHelper.getCredential());
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
  public void testOpenNonExistentItem() throws IOException {
    GoogleCloudStorage rawStorage = createGoogleCloudStorage();
    Throwable throwable =
        assertThrows(
            IOException.class,
            () ->
                rawStorage.open(
                    new StorageResourceId(BUCKET_NAME, "testOpenNonExistentItem_Object")));
    assertThat(throwable).hasCauseThat().hasCauseThat().hasMessageThat().contains("Item not found");
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
                data,
                /* from= */ i * segmentSize,
                /* to= */ i * segmentSize + segmentSize);
        assertWithMessage("Unexpected segment data read.")
            .that(readSegments[i])
            .isEqualTo(expectedSegment);
      }
    }
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
