package com.google.cloud.hadoop.gcsio.integration;

import static com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.assertObjectContent;
import static com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.writeObject;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.Assert.assertThrows;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageItemInfo;
import com.google.cloud.hadoop.gcsio.ListObjectOptions;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.cloud.hadoop.gcsio.ThrottledGoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.ThrottledGoogleCloudStorage.StorageOperation;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.common.flogger.GoogleLogger;
import com.google.common.util.concurrent.RateLimiter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class GoogleCloudStorageGrpcIntegrationTest {

  // Prefix this name with the prefix used in other gcs io integrate tests once it's whitelisted by
  // GCS to access gRPC API.
  private static final String BUCKET_NAME = "gcs-grpc-team-dataproc-it";

  private static final LoadingCache<GoogleCloudStorage, String> SHARED_BUCKETS =
      CacheBuilder.newBuilder()
          .build(
              new CacheLoader<GoogleCloudStorage, String>() {
                @Override
                public String load(GoogleCloudStorage gcs) throws Exception {
                  return BUCKET_NAME;
                }
              });

  private static final GoogleCloudStorage rawStorage = getRawStorage();

  private static GoogleCloudStorage getRawStorage() {
    try {
      return new ThrottledGoogleCloudStorage(
          // Allow 2 create or delete bucket operation every second. This will hit rate limits,
          // but GCS now has back-offs implemented for bucket operations.
          RateLimiter.create(2),
          new GoogleCloudStorageImpl(
              GoogleCloudStorageTestHelper.getStandardOptionBuilder().setGrpcEnabled(true).build(),
              GoogleCloudStorageTestHelper.getCredential()),
          EnumSet.of(StorageOperation.DELETE_BUCKETS, StorageOperation.CREATE_BUCKET));
    } catch (IOException e) {
      GoogleLogger logger = GoogleLogger.forEnclosingClass();
      logger.atWarning().withCause(e).log(
          "Caught exception during GCS (%s) buckets creation", BUCKET_NAME);
      return null;
    }
  }

  @BeforeClass
  public static void createBuckets() throws IOException {
    rawStorage.createBucket(BUCKET_NAME);
  }

  @AfterClass
  public static void cleanupBuckets() throws IOException {
    // randomize buckets order in case concurrent clean ups are running
    List<GoogleCloudStorageItemInfo> objectsToDelete =
        rawStorage.listObjectInfo(
            BUCKET_NAME, /* objectNamePrefix= */ null, ListObjectOptions.DEFAULT_FLAT_LIST);

    rawStorage.deleteObjects(
        Lists.transform(objectsToDelete, GoogleCloudStorageItemInfo::getResourceId));
    rawStorage.deleteBuckets(Arrays.asList(BUCKET_NAME));
  }

  @Test
  public void testCreateObject() throws IOException {
    StorageResourceId objectToCreate =
        new StorageResourceId(BUCKET_NAME, "testCreateObject_Object");
    byte[] objectBytes = writeObject(rawStorage, objectToCreate, /* objectSize= */ 512);

    assertObjectContent(rawStorage, objectToCreate, objectBytes);
  }

  @Test
  public void testCreateExistingObject() throws IOException {
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
    StorageResourceId objectToCreate =
        new StorageResourceId(BUCKET_NAME, "testCreateEmptyObject_Object");

    rawStorage.createEmptyObject(objectToCreate);

    GoogleCloudStorageItemInfo itemInfo = rawStorage.getItemInfo(objectToCreate);

    assertThat(itemInfo.exists()).isTrue();
    assertThat(itemInfo.getSize()).isEqualTo(0);
  }

  @Test
  public void testCreateInvalidObject() {
    StorageResourceId objectToCreate =
        new StorageResourceId(BUCKET_NAME, "testCreateInvalidObject_InvalidObject\n");

    assertThrows(
        IOException.class, () -> writeObject(rawStorage, objectToCreate, /* objectSize= */ 10));
  }

  @Test
  public void testOpen() throws IOException {
    StorageResourceId objectToCreate = new StorageResourceId(BUCKET_NAME, "testOpen_Object");
    byte[] objectBytes = writeObject(rawStorage, objectToCreate, /* objectSize= */ 100);

    assertObjectContent(rawStorage, objectToCreate, objectBytes);
  }

  @Test
  public void testOpenNonExistentItem() {
    assertThrows(
        FileNotFoundException.class,
        () ->
            rawStorage.open(new StorageResourceId(BUCKET_NAME, "testOpenNonExistentItem_Object")));
  }

  @Test
  public void testOpenEmptyObject() throws IOException {
    StorageResourceId resourceId = new StorageResourceId(BUCKET_NAME, "testOpenEmptyObject_Object");
    rawStorage.createEmptyObject(resourceId);

    assertObjectContent(rawStorage, resourceId, new byte[0]);
  }

  @Test
  public void testOpenLargeObject() throws IOException {
    StorageResourceId resourceId = new StorageResourceId(BUCKET_NAME, "testOpenLargeObject_Object");

    int partitionsCount = 50;
    byte[] partition =
        writeObject(rawStorage, resourceId, /* partitionSize= */ 10 * 1024 * 1024, partitionsCount);

    assertObjectContent(rawStorage, resourceId, partition, partitionsCount);
  }

  @Test
  public void testPartialRead() throws IOException {
    final int segmentSize = 553;
    final int segmentCount = 5;

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
                i * segmentSize, /* from index */
                (i * segmentSize) + segmentSize /* to index */);
        assertWithMessage("Unexpected segment data read.")
            .that(readSegments[i])
            .isEqualTo(expectedSegment);
      }
    }
  }

  @Test
  public void testChannelClosedException() throws IOException {
    final int totalBytes = 1200;

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
