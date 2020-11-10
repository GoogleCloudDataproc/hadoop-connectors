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
import com.google.cloud.hadoop.gcsio.ThrottledGoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.ThrottledGoogleCloudStorage.StorageOperation;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.TestBucketHelper;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.RateLimiter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;
import java.util.Arrays;
import java.util.EnumSet;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class GoogleCloudStorageGrpcIntegrationTest {

  // This prefix will be replaced by the prefix used in other gcs io integrate tests once it's
  // whitelisted by GCS to access gRPC API.
  private static final String BUCKET_NAME_PREFIX = "gcs-grpc-team-dataproc-it";

  private static final Supplier<TestBucketHelper> BUCKET_HELPER =
      Suppliers.memoize(() -> new TestBucketHelper(BUCKET_NAME_PREFIX));

  private static final LoadingCache<GoogleCloudStorage, String> SHARED_BUCKETS =
      CacheBuilder.newBuilder()
          .build(
              new CacheLoader<GoogleCloudStorage, String>() {
                @Override
                public String load(GoogleCloudStorage gcs) throws Exception {
                  return BUCKET_HELPER.get().getUniqueBucketName("shared") + "_" + gcs.hashCode();
                }
              });

  private final GoogleCloudStorage rawStorage;

  public GoogleCloudStorageGrpcIntegrationTest() throws IOException {
    GoogleCloudStorage gcs =
        new ThrottledGoogleCloudStorage(
            // Allow 2 create or delete bucket operation every second. This will hit rate limits,
            // but GCS now has back-offs implemented for bucket operations.
            RateLimiter.create(2),
            new GoogleCloudStorageImpl(
                GoogleCloudStorageTestHelper.getStandardOptionBuilder()
                    .setGrpcEnabled(true)
                    .build(),
                GoogleCloudStorageTestHelper.getCredential()),
            EnumSet.of(StorageOperation.DELETE_BUCKETS, StorageOperation.CREATE_BUCKET));
    this.rawStorage = gcs;
  }

  @Before
  public void setUp() throws IOException {
    rawStorage.createBucket(getSharedBucketName());
  }

  @AfterClass
  public static void cleanupBuckets() throws IOException {
    BUCKET_HELPER.get().cleanup(Iterables.getLast(SHARED_BUCKETS.asMap().keySet()));
  }

  private String getSharedBucketName() {
    return SHARED_BUCKETS.getUnchecked(rawStorage);
  }

  @Test
  public void testCreateObject() throws IOException {
    String bucketName = getSharedBucketName();

    StorageResourceId objectToCreate = new StorageResourceId(bucketName, "testCreateObject_Object");
    byte[] objectBytes = writeObject(rawStorage, objectToCreate, /* objectSize= */ 512);

    assertObjectContent(rawStorage, objectToCreate, objectBytes);
  }

  @Test
  public void testCreateExistingObject() throws IOException {
    String bucketName = getSharedBucketName();

    StorageResourceId objectToCreate =
        new StorageResourceId(bucketName, "testCreateExistingObject_Object");
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
    String bucketName = getSharedBucketName();

    StorageResourceId objectToCreate =
        new StorageResourceId(bucketName, "testCreateEmptyObject_Object");

    rawStorage.createEmptyObject(objectToCreate);

    GoogleCloudStorageItemInfo itemInfo = rawStorage.getItemInfo(objectToCreate);

    assertThat(itemInfo.exists()).isTrue();
    assertThat(itemInfo.getSize()).isEqualTo(0);
  }

  @Test
  public void testCreateInvalidObject() {
    String bucketName = getSharedBucketName();

    StorageResourceId objectToCreate =
        new StorageResourceId(bucketName, "testCreateInvalidObject_InvalidObject\n");

    assertThrows(
        IOException.class, () -> writeObject(rawStorage, objectToCreate, /* objectSize= */ 10));
  }

  @Test
  public void testOpen() throws IOException {
    String bucketName = getSharedBucketName();

    StorageResourceId objectToCreate = new StorageResourceId(bucketName, "testOpen_Object");
    byte[] objectBytes = writeObject(rawStorage, objectToCreate, /* objectSize= */ 100);

    assertObjectContent(rawStorage, objectToCreate, objectBytes);
  }

  @Test
  public void testOpenNonExistentItem() {
    String bucketName = getSharedBucketName();

    assertThrows(
        FileNotFoundException.class,
        () -> rawStorage.open(new StorageResourceId(bucketName, "testOpenNonExistentItem_Object")));
  }

  @Test
  public void testOpenEmptyObject() throws IOException {
    String bucketName = getSharedBucketName();

    StorageResourceId resourceId = new StorageResourceId(bucketName, "testOpenEmptyObject_Object");
    rawStorage.createEmptyObject(resourceId);

    assertObjectContent(rawStorage, resourceId, new byte[0]);
  }

  @Test
  public void testOpenLargeObject() throws IOException {
    String bucketName = getSharedBucketName();
    StorageResourceId resourceId = new StorageResourceId(bucketName, "testOpenLargeObject_Object");

    int partitionsCount = 50;
    byte[] partition =
        writeObject(rawStorage, resourceId, /* partitionSize= */ 10 * 1024 * 1024, partitionsCount);

    assertObjectContent(rawStorage, resourceId, partition, partitionsCount);
  }

  @Test
  public void testPartialRead() throws IOException {
    final int segmentSize = 553;
    final int segmentCount = 5;

    String bucketName = getSharedBucketName();

    StorageResourceId resourceId =
        new StorageResourceId(bucketName, "testReadPartialObjects_Object");
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
    String bucketName = getSharedBucketName();

    StorageResourceId resourceId =
        new StorageResourceId(bucketName, "testChannelClosedException_Object");
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
