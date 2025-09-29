/*
 * Copyright 2014 Google Inc.
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

package com.google.cloud.hadoop.gcsio.integration;

import static com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.assertObjectContent;
import static com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.writeObject;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

import com.google.cloud.hadoop.gcsio.CreateBucketOptions;
import com.google.cloud.hadoop.gcsio.CreateObjectOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorage.ListPage;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageItemInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions;
import com.google.cloud.hadoop.gcsio.ListObjectOptions;
import com.google.cloud.hadoop.gcsio.PerformanceCachingGoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.PerformanceCachingGoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.cloud.hadoop.gcsio.StringPaths;
import com.google.cloud.hadoop.gcsio.UpdatableItemInfo;
import com.google.cloud.hadoop.gcsio.VerificationAttributes;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.TestBucketHelper;
import com.google.cloud.hadoop.gcsio.testing.InMemoryGoogleCloudStorage;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.common.base.Equivalence;
import com.google.common.base.Suppliers;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class GoogleCloudStorageTest {

  // This string is used to prefix all bucket names that are created for GCS IO integration testing
  private static final String BUCKET_NAME_PREFIX = "gcsio-it";

  private static final ListObjectOptions INCLUDE_PREFIX_LIST_OPTIONS =
      ListObjectOptions.DEFAULT.toBuilder().setIncludePrefix(true).build();

  private static final Supplier<TestBucketHelper> BUCKET_HELPER =
      Suppliers.memoize(() -> new TestBucketHelper(BUCKET_NAME_PREFIX));

  private static final LoadingCache<GoogleCloudStorage, String> SHARED_BUCKETS =
      CacheBuilder.newBuilder()
          .build(
              new CacheLoader<>() {
                @Override
                public String load(GoogleCloudStorage gcs) throws Exception {
                  return createUniqueBucket(gcs, "shared");
                }
              });

  private static final LoadingCache<GoogleCloudStorage, String> SHARED_HNS_BUCKETS =
      CacheBuilder.newBuilder()
          .build(
              new CacheLoader<>() {
                @Override
                public String load(GoogleCloudStorage gcs) throws Exception {
                  // Note the call to createHnBucket and the new suffix
                  return createHnBucket(gcs, "shared-hns");
                }
              });

  private static String createUniqueBucket(GoogleCloudStorage gcs, String suffix)
      throws IOException {
    String bucketName = getUniqueBucketName(suffix) + "_" + gcs.hashCode();
    gcs.createBucket(bucketName);
    return bucketName;
  }

  private static String createHnBucket(GoogleCloudStorage gcs, String suffix) throws IOException {
    String bucketName = getUniqueBucketName(suffix) + "_" + gcs.hashCode();
    gcs.createBucket(
        bucketName, CreateBucketOptions.builder().setHierarchicalNamespaceEnabled(true).build());
    return bucketName;
  }

  private String getSharedHnsBucketName() {
    return SHARED_HNS_BUCKETS.getUnchecked(rawStorage);
  }

  private String createUniqueBucket(String suffix) throws IOException {
    return createUniqueBucket(rawStorage, suffix);
  }

  private static String getUniqueBucketName(String suffix) {
    return BUCKET_HELPER.get().getUniqueBucketName(suffix);
  }

  /** An Equivalence for byte arrays. */
  public static final Equivalence<byte[]> BYTE_ARRAY_EQUIVALENCE =
      new Equivalence<>() {
        @Override
        protected boolean doEquivalent(byte[] bytes, byte[] bytes2) {
          return Arrays.equals(bytes, bytes2);
        }

        @Override
        protected int doHash(byte[] bytes) {
          return Arrays.hashCode(bytes);
        }
      };

  // Test classes using JUnit4 runner must have only a single constructor. Since we
  // want to be able to pass in dependencies, we'll maintain this base class as
  // @Parameterized with @Parameters.
  @Parameterized.Parameters
  public static Collection<Object[]> getConstructorArguments() throws IOException {
    return Arrays.asList(
        new Object[] {new InMemoryGoogleCloudStorage()},
        new Object[] {
          new PerformanceCachingGoogleCloudStorage(
              new InMemoryGoogleCloudStorage(), PerformanceCachingGoogleCloudStorageOptions.DEFAULT)
        });
  }

  private final GoogleCloudStorage rawStorage;

  public GoogleCloudStorageTest(GoogleCloudStorage rawStorage) {
    this.rawStorage = rawStorage;
  }

  @Before
  public void setUp() {
    if (rawStorage instanceof PerformanceCachingGoogleCloudStorage) {
      ((PerformanceCachingGoogleCloudStorage) rawStorage).invalidateCache();
    }
  }

  @AfterClass
  public static void cleanupBuckets() throws IOException {
    // Use any GCS object (from tested ones) for clean up
    BUCKET_HELPER.get().cleanup(Iterables.getLast(SHARED_BUCKETS.asMap().keySet()));
  }

  private String getSharedBucketName() {
    return SHARED_BUCKETS.getUnchecked(rawStorage);
  }

  @Test
  public void testCreateSuccessfulBucket() throws IOException {
    String bucketName = createUniqueBucket("create-successful");

    // Verify the bucket exist by creating an object
    StorageResourceId objectToCreate = new StorageResourceId(bucketName, "CreateTestObject");
    rawStorage.createEmptyObject(objectToCreate);
  }

  @Test
  public void testCreateExistingBucket() throws IOException {
    String bucketName = getSharedBucketName();

    assertThrows(IOException.class, () -> rawStorage.createBucket(bucketName));
  }

  @Test
  public void testCreateInvalidBucket() throws IOException {
    // Buckets must start with a letter or number
    String bucketName = "--" + getUniqueBucketName("create-invalid");

    assertThrows(IOException.class, () -> rawStorage.createBucket(bucketName));
  }

  @Test
  public void testCreateObject() throws IOException {
    String bucketName = getSharedBucketName();

    // Verify the bucket exist by creating an object
    StorageResourceId objectToCreate = new StorageResourceId(bucketName, "testCreateObject_Object");
    byte[] objectBytes = writeObject(rawStorage, objectToCreate, /* objectSize= */ 512);

    assertObjectContent(rawStorage, objectToCreate, objectBytes);
  }

  @Test
  public void testCreateInvalidObject() throws IOException {
    String bucketName = getSharedBucketName();

    StorageResourceId objectToCreate =
        new StorageResourceId(bucketName, "testCreateInvalidObject_InvalidObject\n");

    assertThrows(
        IOException.class, () -> writeObject(rawStorage, objectToCreate, /* objectSize= */ 10));
  }

  @Test
  public void testCreateZeroLengthObjectUsingCreate() throws IOException {
    String bucketName = getSharedBucketName();

    StorageResourceId objectToCreate =
        new StorageResourceId(bucketName, "testCreateZeroLengthObjectUsingCreate_Object");
    byte[] objectBytes = writeObject(rawStorage, objectToCreate, /* objectSize= */ 0);

    assertObjectContent(rawStorage, objectToCreate, objectBytes);
  }

  @Test
  public void testCreate1PageLengthObjectUsingCreate() throws IOException {
    String bucketName = getSharedBucketName();

    int objectSize = AsyncWriteChannelOptions.DEFAULT.getPipeBufferSize();
    StorageResourceId objectToCreate =
        new StorageResourceId(bucketName, "testCreate1PageLengthObjectUsingCreate_Object");
    byte[] objectBytes = writeObject(rawStorage, objectToCreate, objectSize);

    assertObjectContent(rawStorage, objectToCreate, objectBytes);
  }

  @Test
  public void testCreate1PageLengthPlus1byteObjectUsingCreate() throws IOException {
    String bucketName = getSharedBucketName();

    int objectSize = AsyncWriteChannelOptions.DEFAULT.getPipeBufferSize() + 1;
    StorageResourceId objectToCreate =
        new StorageResourceId(bucketName, "testCreate1PageLengthPlus1byteObjectUsingCreate_Object");
    byte[] objectBytes = writeObject(rawStorage, objectToCreate, objectSize);

    assertObjectContent(rawStorage, objectToCreate, objectBytes);
  }

  @Test
  public void testCreateExistingObject() throws IOException {
    String bucketName = getSharedBucketName();

    StorageResourceId objectToCreate =
        new StorageResourceId(bucketName, "testCreateExistingObject_Object");
    writeObject(rawStorage, objectToCreate, /* objectSize= */ 128);

    byte[] overwriteBytesToWrite = writeObject(rawStorage, objectToCreate, /* objectSize= */ 256);

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
  public void testCreateEmptyObjects() throws IOException {
    String bucketName = getSharedBucketName();

    List<StorageResourceId> storageResourceIds =
        Lists.newArrayList(
            new StorageResourceId(bucketName, "testCreateEmptyObjects_Object1"),
            new StorageResourceId(bucketName, "testCreateEmptyObjects_Object2"));

    rawStorage.createEmptyObjects(storageResourceIds);

    rawStorage
        .getItemInfos(storageResourceIds)
        .forEach(
            itemInfo -> {
              assertWithMessage("%s should be empty", itemInfo).that(itemInfo.exists()).isTrue();
              assertWithMessage("%s should be empty", itemInfo)
                  .that(itemInfo.getSize())
                  .isEqualTo(0);
            });
  }

  @Test
  public void testCreateEmptyObjectsWithOptions() throws IOException {
    String bucketName = getSharedBucketName();

    List<StorageResourceId> storageResourceIds =
        Lists.newArrayList(
            new StorageResourceId(bucketName, "testCreateEmptyObjectsWithOptions_Object1"),
            new StorageResourceId(bucketName, "testCreateEmptyObjectsWithOptions_Object2"));

    rawStorage.createEmptyObjects(storageResourceIds, CreateObjectOptions.DEFAULT_OVERWRITE);

    rawStorage
        .getItemInfos(storageResourceIds)
        .forEach(
            itemInfo -> {
              assertWithMessage("%s should be empty", itemInfo).that(itemInfo.exists()).isTrue();
              assertWithMessage("%s should be empty", itemInfo)
                  .that(itemInfo.getSize())
                  .isEqualTo(0);
            });
  }

  @Test
  public void testGetOptions() {
    GoogleCloudStorageOptions options = rawStorage.getOptions();

    assertThat(options.getAppName()).startsWith("GHFS/");
  }

  @Test
  public void testOpenFileWithMatchingSize() throws IOException {
    String bucketName = getSharedBucketName();

    StorageResourceId objectToCreate =
        new StorageResourceId(bucketName, "testOpenFileWithMatchingSize_Object");
    byte[] objectBytes = writeObject(rawStorage, objectToCreate, /* objectSize= */ 512);

    try (SeekableByteChannel channel = rawStorage.open(objectToCreate)) {
      assertThat(channel.size()).isEqualTo(objectBytes.length);
    }
  }

  @Test
  public void testOpenFileWithMatchingSizeAndSpecifiedReadOptions() throws IOException {
    String bucketName = getSharedBucketName();

    StorageResourceId objectToCreate =
        new StorageResourceId(
            bucketName, "testOpenFileWithMatchingSizeAndSpecifiedReadOptions_Object");
    byte[] objectBytes = writeObject(rawStorage, objectToCreate, /* objectSize= */ 512);

    try (SeekableByteChannel channel =
        rawStorage.open(objectToCreate, GoogleCloudStorageReadOptions.DEFAULT)) {
      assertThat(channel.size()).isEqualTo(objectBytes.length);
    }
  }

  @Test
  public void listObjectInfo() throws IOException {
    String bucketName = getSharedBucketName();

    StorageResourceId objectToCreate =
        new StorageResourceId(bucketName, "listObjectInfo_test_object");

    writeObject(rawStorage, objectToCreate, /* objectSize= */ 512);

    List<GoogleCloudStorageItemInfo> listedObjects =
        rawStorage.listObjectInfo(bucketName, "listObjectInfo_test_");

    assertThat(
            listedObjects.stream().map(GoogleCloudStorageItemInfo::getObjectName).collect(toList()))
        .containsExactly(objectToCreate.getObjectName());
  }

  @Test
  public void listObjectInfo_includePrefix() throws IOException {
    String bucketName = getSharedBucketName();

    String prefix = "listObjectInfo_includePrefix_test_";

    StorageResourceId objectToCreate = new StorageResourceId(bucketName, prefix + "object");

    writeObject(rawStorage, objectToCreate, /* objectSize= */ 512);

    List<GoogleCloudStorageItemInfo> listedObjects =
        rawStorage.listObjectInfo(bucketName, prefix, INCLUDE_PREFIX_LIST_OPTIONS);

    assertThat(
            listedObjects.stream().map(GoogleCloudStorageItemInfo::getObjectName).collect(toList()))
        .containsExactly(prefix, objectToCreate.getObjectName())
        .inOrder();
  }

  @Test
  public void listObjectInfoPage() throws IOException {
    String bucketName = getSharedBucketName();

    StorageResourceId objectToCreate =
        new StorageResourceId(bucketName, "listObjectInfoPage_test_object");

    writeObject(rawStorage, objectToCreate, /* objectSize= */ 512);

    ListPage<GoogleCloudStorageItemInfo> listedObjectsPage =
        rawStorage.listObjectInfoPage(bucketName, "listObjectInfoPage_test_", /* pageToken= */ "");

    assertThat(listedObjectsPage.getNextPageToken()).isNull();
    assertThat(
            listedObjectsPage.getItems().stream()
                .map(GoogleCloudStorageItemInfo::getObjectName)
                .collect(toList()))
        .containsExactly(objectToCreate.getObjectName());
  }

  @Test
  public void listObjectInfoPage_includePrefix() throws IOException {
    String bucketName = getSharedBucketName();

    String prefix = "listObjectInfoPage_includePrefix_test_";

    StorageResourceId objectToCreate = new StorageResourceId(bucketName, prefix + "object");

    writeObject(rawStorage, objectToCreate, /* objectSize= */ 512);

    ListPage<GoogleCloudStorageItemInfo> listedObjectsPage =
        rawStorage.listObjectInfoPage(
            bucketName, prefix, INCLUDE_PREFIX_LIST_OPTIONS, /* pageToken= */ "");

    assertThat(listedObjectsPage.getNextPageToken()).isNull();
    assertThat(
            listedObjectsPage.getItems().stream()
                .map(GoogleCloudStorageItemInfo::getObjectName)
                .collect(toList()))
        .containsExactly(prefix, objectToCreate.getObjectName())
        .inOrder();
  }

  @Test
  public void testComposeObjectsMovesObjectToAnother() throws IOException {
    String bucketName = getSharedBucketName();

    StorageResourceId srcObject =
        new StorageResourceId(bucketName, "testListObjectMovesObjectToAnother_srcObject");
    StorageResourceId dstObject =
        new StorageResourceId(bucketName, "testListObjectMovesObjectToAnother_dstObject");

    writeObject(rawStorage, srcObject, /* objectSize= */ 512);

    GoogleCloudStorageItemInfo composedObject =
        rawStorage.composeObjects(
            ImmutableList.of(srcObject), dstObject, CreateObjectOptions.DEFAULT_OVERWRITE);
    assertThat(composedObject.exists()).isTrue();
    assertThat(composedObject.getObjectName()).isEqualTo(dstObject.getObjectName());
  }

  @Test
  public void testCreateWithOptions() throws IOException {
    String bucketName = getSharedBucketName();

    StorageResourceId objectToCreate =
        new StorageResourceId(bucketName, "testCreateWithOptions_Object");

    rawStorage.create(objectToCreate, CreateObjectOptions.DEFAULT_OVERWRITE).close();

    GoogleCloudStorageItemInfo itemInfo = rawStorage.getItemInfo(objectToCreate);
    assertThat(itemInfo.exists()).isTrue();
    assertThat(itemInfo.getObjectName()).isEqualTo(objectToCreate.getObjectName());
    assertThat(itemInfo.getSize()).isEqualTo(0);
  }

  @Test
  public void testCreateEmptyExistingObject() throws IOException {
    String bucketName = getSharedBucketName();

    StorageResourceId objectToCreate =
        new StorageResourceId(bucketName, "testCreateEmptyExistingObject_Object");

    rawStorage.createEmptyObject(objectToCreate);

    GoogleCloudStorageItemInfo itemInfo = rawStorage.getItemInfo(objectToCreate);

    assertThat(itemInfo.exists()).isTrue();
    assertThat(itemInfo.getSize()).isEqualTo(0);

    rawStorage.createEmptyObject(objectToCreate);

    GoogleCloudStorageItemInfo secondItemInfo = rawStorage.getItemInfo(objectToCreate);

    assertThat(secondItemInfo.exists()).isTrue();
    assertThat(secondItemInfo.getSize()).isEqualTo(0);
    assertThat(secondItemInfo.getCreationTime()).isNotSameInstanceAs(itemInfo.getCreationTime());
  }

  @Test
  public void testGetSingleItemInfo() throws IOException {
    String bucketName = getSharedBucketName();

    StorageResourceId objectToCreate =
        new StorageResourceId(bucketName, "testGetSingleItemInfo_Object1");

    rawStorage.createEmptyObject(objectToCreate);

    GoogleCloudStorageItemInfo itemInfo = rawStorage.getItemInfo(objectToCreate);

    assertThat(itemInfo.exists()).isTrue();
    assertThat(itemInfo.getSize()).isEqualTo(0);

    StorageResourceId secondObjectToCreate =
        new StorageResourceId(bucketName, "testGetSingleItemInfo_Object2");
    writeObject(rawStorage, secondObjectToCreate, /* objectSize= */ 100);

    GoogleCloudStorageItemInfo secondItemInfo = rawStorage.getItemInfo(secondObjectToCreate);

    assertThat(secondItemInfo.exists()).isTrue();
    assertThat(secondItemInfo.getSize()).isEqualTo(100);
    assertThat(secondItemInfo.isBucket()).isFalse();
    assertThat(secondItemInfo.isRoot()).isFalse();

    GoogleCloudStorageItemInfo nonExistentItemInfo =
        rawStorage.getItemInfo(
            new StorageResourceId(bucketName, "testGetSingleItemInfo_IDontExist"));

    assertThat(nonExistentItemInfo.exists()).isFalse();
    assertThat(nonExistentItemInfo.isBucket()).isFalse();
    assertThat(nonExistentItemInfo.isRoot()).isFalse();

    // Test bucket get item info
    GoogleCloudStorageItemInfo bucketInfo =
        rawStorage.getItemInfo(new StorageResourceId(bucketName));
    assertThat(bucketInfo.exists()).isTrue();
    assertThat(bucketInfo.isBucket()).isTrue();

    GoogleCloudStorageItemInfo rootInfo = rawStorage.getItemInfo(StorageResourceId.ROOT);
    assertThat(rootInfo.exists()).isTrue();
    assertThat(rootInfo.isRoot()).isTrue();
  }

  @Test
  public void testGetMultipleItemInfo() throws IOException {
    String bucketName = getSharedBucketName();

    List<StorageResourceId> objectsCreated = new ArrayList<>();

    for (int i = 0; i < 3; i++) {
      StorageResourceId objectToCreate =
          new StorageResourceId(bucketName, "testGetMultipleItemInfo_Object" + i);
      rawStorage.createEmptyObject(objectToCreate);
      objectsCreated.add(objectToCreate);
    }

    StorageResourceId bucketResourceId = new StorageResourceId(bucketName);
    StorageResourceId nonExistentResourceId =
        new StorageResourceId(bucketName, "testGetMultipleItemInfo_IDontExist");

    List<StorageResourceId> allResources = Lists.newArrayList();
    allResources.addAll(objectsCreated);
    allResources.add(nonExistentResourceId);
    allResources.add(bucketResourceId);

    List<GoogleCloudStorageItemInfo> allInfo = rawStorage.getItemInfos(allResources);

    for (int i = 0; i < objectsCreated.size(); i++) {
      StorageResourceId resourceId = objectsCreated.get(i);
      GoogleCloudStorageItemInfo info = allInfo.get(i);

      assertThat(info.getResourceId()).isEqualTo(resourceId);
      assertThat(info.getSize()).isEqualTo(0);
      assertWithMessage("Item should exist").that(info.exists()).isTrue();
      assertThat(info.getCreationTime()).isNotEqualTo(0);
      assertThat(info.isBucket()).isFalse();
    }

    GoogleCloudStorageItemInfo nonExistentItemInfo = allInfo.get(allInfo.size() - 2);
    assertThat(nonExistentItemInfo.exists()).isFalse();

    GoogleCloudStorageItemInfo bucketInfo = allInfo.get(allInfo.size() - 1);
    assertThat(bucketInfo.exists()).isTrue();
    assertThat(bucketInfo.isBucket()).isTrue();
  }

  // TODO(user): Re-enable once a new method of inducing errors is devised.
  @Test
  @Ignore
  public void testGetMultipleItemInfoWithSomeInvalid() throws IOException {
    String bucketName = getSharedBucketName();

    List<StorageResourceId> resourceIdList = new ArrayList<>();
    StorageResourceId newObject =
        new StorageResourceId(bucketName, "testGetMultipleItemInfoWithSomeInvalid_Object");
    resourceIdList.add(newObject);
    rawStorage.createEmptyObject(newObject);

    StorageResourceId invalidObject =
        new StorageResourceId(bucketName, "testGetMultipleItemInfoWithSomeInvalid_InvalidObject\n");
    resourceIdList.add(invalidObject);

    IOException e = assertThrows(IOException.class, () -> rawStorage.getItemInfos(resourceIdList));
    assertThat(e).hasMessageThat().isEqualTo("Error getting StorageObject");
  }

  // TODO(user): Re-enable once a new method of inducing errors is devised.
  @Test
  @Ignore
  public void testOneInvalidGetItemInfo() throws IOException {
    String bucketName = getSharedBucketName();

    IOException e =
        assertThrows(
            IOException.class,
            () ->
                rawStorage.getItemInfo(
                    new StorageResourceId(
                        bucketName, "testOneInvalidGetItemInfo_InvalidObject\n")));
    assertThat(e).hasMessageThat().isEqualTo("Error accessing");
  }

  @Test
  public void testSingleObjectDelete() throws IOException {
    String bucketName = getSharedBucketName();

    StorageResourceId resource = new StorageResourceId(bucketName, "testSingleObjectDelete_Object");
    rawStorage.createEmptyObject(resource);

    GoogleCloudStorageItemInfo info = rawStorage.getItemInfo(resource);
    assertThat(info.exists()).isTrue();

    rawStorage.deleteObjects(ImmutableList.of(resource));

    GoogleCloudStorageItemInfo deletedInfo = rawStorage.getItemInfo(resource);
    assertThat(deletedInfo.exists()).isFalse();
  }

  @Test
  public void testMultipleObjectDelete() throws IOException {
    String bucketName = getSharedBucketName();

    StorageResourceId resource =
        new StorageResourceId(bucketName, "testMultipleObjectDelete_Object1");
    rawStorage.createEmptyObject(resource);

    StorageResourceId secondResource =
        new StorageResourceId(bucketName, "testMultipleObjectDelete_Object2");
    rawStorage.createEmptyObject(secondResource);

    assertThat(rawStorage.getItemInfo(resource).exists()).isTrue();
    assertThat(rawStorage.getItemInfo(secondResource).exists()).isTrue();

    rawStorage.deleteObjects(ImmutableList.of(resource, secondResource));

    assertThat(rawStorage.getItemInfo(resource).exists()).isFalse();
    assertThat(rawStorage.getItemInfo(secondResource).exists()).isFalse();
  }

  // TODO(user): Re-enable once a new method of inducing errors is devised.
  @Test
  @Ignore
  public void testSomeInvalidObjectsDelete() throws IOException {
    String bucketName = getSharedBucketName();

    StorageResourceId resource =
        new StorageResourceId(bucketName, "testSomeInvalidObjectsDelete_Object");
    rawStorage.createEmptyObject(resource);

    // Don't actually create a GCS object for this resource.
    StorageResourceId secondResource =
        new StorageResourceId(bucketName, "testSomeInvalidObjectsDelete_IDontExist");
    StorageResourceId invalidName =
        new StorageResourceId(bucketName, "testSomeInvalidObjectsDelete_InvalidObject\n");

    assertThat(rawStorage.getItemInfo(resource).exists()).isTrue();
    assertThat(rawStorage.getItemInfo(secondResource).exists()).isFalse();

    IOException e =
        assertThrows(
            IOException.class,
            () ->
                rawStorage.deleteObjects(ImmutableList.of(resource, secondResource, invalidName)));
    assertThat(e).hasMessageThat().isEqualTo("Error deleting");
  }

  @Test
  public void testDeleteNonExistingObject() throws IOException {
    String bucketName = getSharedBucketName();

    StorageResourceId resource =
        new StorageResourceId(bucketName, "testDeleteNonExistingObject_IDontExist");

    rawStorage.deleteObjects(ImmutableList.of(resource));
  }

  @Test
  public void testDeleteNonExistingBucket() throws IOException {
    // Composite exception thrown, not a FileNotFoundException.
    String bucketName = getUniqueBucketName("delete_ne_bucket");

    assertThrows(IOException.class, () -> rawStorage.deleteBuckets(ImmutableList.of(bucketName)));
  }

  @Test
  public void testSingleDeleteBucket() throws IOException {
    String bucketName = createUniqueBucket("delete-single");

    rawStorage.deleteBuckets(ImmutableList.of(bucketName));

    GoogleCloudStorageItemInfo info = rawStorage.getItemInfo(new StorageResourceId(bucketName));
    assertThat(info.exists()).isFalse();

    // Create the bucket again to assure that the previous one was deleted...
    rawStorage.createBucket(bucketName);
  }

  @Test
  public void testMultipleDeleteBucket() throws IOException {
    StorageResourceId bucket1 = new StorageResourceId(createUniqueBucket("delete-multi-1"));
    StorageResourceId bucket2 = new StorageResourceId(createUniqueBucket("delete-multi-2"));
    rawStorage
        .getItemInfos(ImmutableList.of(bucket1, bucket2))
        .forEach(i -> assertWithMessage("Expected to exist:%n%s", i).that(i.exists()).isTrue());

    rawStorage.deleteBuckets(ImmutableList.of(bucket1.getBucketName(), bucket2.getBucketName()));

    rawStorage
        .getItemInfos(ImmutableList.of(bucket1, bucket2))
        .forEach(
            i -> assertWithMessage("Expected to not exist:%n%s", i).that(i.exists()).isFalse());
  }

  @Test
  public void testSomeInvalidDeleteBucket() throws IOException {
    String bucketName1 = createUniqueBucket("delete-multi-valid-1");
    String bucketName2 = createUniqueBucket("delete-multi-valid-2");
    String invalidBucketName = "--" + getUniqueBucketName("delete-multi-invalid");

    assertThrows(
        IOException.class,
        () ->
            rawStorage.deleteBuckets(
                ImmutableList.of(bucketName1, bucketName2, invalidBucketName)));

    List<GoogleCloudStorageItemInfo> infoList =
        rawStorage.getItemInfos(
            ImmutableList.of(
                new StorageResourceId(bucketName1), new StorageResourceId(bucketName2)));

    for (GoogleCloudStorageItemInfo info : infoList) {
      assertThat(info.exists()).isFalse();
    }
  }

  @Test
  public void testListBucketInfo() throws IOException {
    String bucketName = getSharedBucketName();

    // This has potential to become flaky...
    List<GoogleCloudStorageItemInfo> infoList = rawStorage.listBucketInfo();

    assertWithMessage("At least one bucket should exist").that(infoList).isNotEmpty();
    boolean bucketListed = false;
    for (GoogleCloudStorageItemInfo info : infoList) {
      assertThat(info.exists()).isTrue();
      assertThat(info.isBucket()).isTrue();
      assertThat(info.isRoot()).isFalse();
      bucketListed |= info.getBucketName().equals(bucketName);
    }
    assertThat(bucketListed).isTrue();
  }

  @Test
  public void testListBucketNames() throws IOException {
    String bucketName = getSharedBucketName();

    // This has potential to become flaky...
    List<String> bucketNames = rawStorage.listBucketNames();

    assertWithMessage("Bucket names should not be empty").that(bucketNames).isNotEmpty();
    assertThat(bucketNames).contains(bucketName);
  }

  @Test
  public void listObjectInfo_limited() throws IOException {
    String bucketName = getSharedBucketName();

    String[] names = {"x", "y", "z"};
    for (String name : names) {
      StorageResourceId id =
          new StorageResourceId(bucketName, "listObjectInfo_limited_test_" + name);
      rawStorage.createEmptyObject(id);
    }

    List<GoogleCloudStorageItemInfo> info =
        rawStorage.listObjectInfo(
            bucketName,
            "listObjectInfo_limited_",
            ListObjectOptions.DEFAULT.toBuilder().setMaxResults(2).build());

    assertThat(info.stream().map(GoogleCloudStorageItemInfo::getObjectName).collect(toList()))
        .containsExactly("listObjectInfo_limited_test_x", "listObjectInfo_limited_test_y")
        .inOrder();
  }

  @Test
  public void listObjectInfo_includePrefix_limited() throws IOException {
    String bucketName = getSharedBucketName();

    rawStorage.createEmptyObject(
        new StorageResourceId(bucketName, "listObjectInfo_includePrefix_limited_test_"));

    String[] names = {"x", "y", "z"};
    for (String name : names) {
      StorageResourceId id =
          new StorageResourceId(bucketName, "listObjectInfo_includePrefix_limited_test_" + name);
      rawStorage.createEmptyObject(id);
    }

    List<GoogleCloudStorageItemInfo> info =
        rawStorage.listObjectInfo(
            bucketName,
            "listObjectInfo_includePrefix_limited_test_",
            ListObjectOptions.DEFAULT.toBuilder().setMaxResults(2).build());

    assertThat(info.stream().map(GoogleCloudStorageItemInfo::getObjectName).collect(toList()))
        .containsExactly(
            "listObjectInfo_includePrefix_limited_test_",
            "listObjectInfo_includePrefix_limited_test_x")
        .inOrder();
  }

  @Test
  public void listObjectInfo_subdirectories() throws IOException {
    String bucketName = getSharedBucketName();

    String[] names = {
      "listObjectInfo_subdirectories_test_",
      "listObjectInfo_subdirectories_test_d1/",
      "listObjectInfo_subdirectories_test_d1/o1",
      "listObjectInfo_subdirectories_test_d2/d3/",
      "listObjectInfo_subdirectories_test_d2/d3/o2"
    };
    for (String name : names) {
      StorageResourceId id = new StorageResourceId(bucketName, name);
      rawStorage.createEmptyObject(id);
    }

    GoogleCloudStorageItemInfo itemInfo =
        rawStorage.getItemInfo(
            new StorageResourceId(bucketName, "listObjectInfo_subdirectories_test_d2/"));
    assertThat(itemInfo.exists()).isFalse();

    List<GoogleCloudStorageItemInfo> rootInfo =
        rawStorage.listObjectInfo(bucketName, "listObjectInfo_subdirectories_test_");
    assertWithMessage("Infos not expected to be empty")
        .that(rootInfo.stream().map(GoogleCloudStorageItemInfo::getObjectName).collect(toList()))
        .containsExactly(
            "listObjectInfo_subdirectories_test_",
            "listObjectInfo_subdirectories_test_d1/",
            "listObjectInfo_subdirectories_test_d2/")
        .inOrder();

    GoogleCloudStorageItemInfo d2Info =
        rawStorage.getItemInfo(
            new StorageResourceId(bucketName, "listObjectInfo_subdirectories_test_d2/"));
    assertThat(d2Info.exists())
        .isEqualTo(rawStorage instanceof PerformanceCachingGoogleCloudStorage);

    List<GoogleCloudStorageItemInfo> d2ItemInfos =
        rawStorage.listObjectInfo(bucketName, "listObjectInfo_subdirectories_test_d2/");
    assertWithMessage("D2 item infos expected to be not empty")
        .that(d2ItemInfos.stream().map(GoogleCloudStorageItemInfo::getObjectName).collect(toList()))
        .containsExactly("listObjectInfo_subdirectories_test_d2/d3/");

    List<GoogleCloudStorageItemInfo> d3ItemInfos =
        rawStorage.listObjectInfo(bucketName, "listObjectInfo_subdirectories_test_d2/d3/");
    assertWithMessage("D3 item infos expected to be not empty")
        .that(d3ItemInfos.stream().map(GoogleCloudStorageItemInfo::getObjectName).collect(toList()))
        .containsExactly("listObjectInfo_subdirectories_test_d2/d3/o2");

    // Testing GCS treating object names as opaque blobs
    List<GoogleCloudStorageItemInfo> blobNamesInfo =
        rawStorage.listObjectInfo(
            bucketName, "listObjectInfo_subdirectories_test_", ListObjectOptions.DEFAULT_FLAT_LIST);

    assertWithMessage("blobNamesInfo not expected to be empty")
        .that(
            blobNamesInfo.stream().map(GoogleCloudStorageItemInfo::getObjectName).collect(toList()))
        .containsExactly(
            "listObjectInfo_subdirectories_test_",
            "listObjectInfo_subdirectories_test_d1/",
            "listObjectInfo_subdirectories_test_d1/o1",
            "listObjectInfo_subdirectories_test_d2/d3/",
            "listObjectInfo_subdirectories_test_d2/d3/o2")
        .inOrder();
  }

  @Test
  public void listObjectInfo_includePrefix_subdirectories() throws IOException {
    String bucketName = getSharedBucketName();

    String[] names = {
      "listObjectInfo_includePrefix_subdirectories_test_",
      "listObjectInfo_includePrefix_subdirectories_test_d1/",
      "listObjectInfo_includePrefix_subdirectories_test_d1/o1",
      "listObjectInfo_includePrefix_subdirectories_test_d2/d3/",
      "listObjectInfo_includePrefix_subdirectories_test_d2/d3/o2"
    };
    for (String name : names) {
      StorageResourceId id = new StorageResourceId(bucketName, name);
      rawStorage.createEmptyObject(id);
    }

    GoogleCloudStorageItemInfo itemInfo =
        rawStorage.getItemInfo(
            new StorageResourceId(
                bucketName, "listObjectInfo_includePrefix_subdirectories_test_d2/"));
    assertThat(itemInfo.exists()).isFalse();

    List<GoogleCloudStorageItemInfo> rootInfo =
        rawStorage.listObjectInfo(
            bucketName,
            "listObjectInfo_includePrefix_subdirectories_test_",
            INCLUDE_PREFIX_LIST_OPTIONS);
    assertWithMessage("Infos not expected to be empty")
        .that(rootInfo.stream().map(GoogleCloudStorageItemInfo::getObjectName).collect(toList()))
        .containsExactly(
            "listObjectInfo_includePrefix_subdirectories_test_",
            "listObjectInfo_includePrefix_subdirectories_test_d1/",
            "listObjectInfo_includePrefix_subdirectories_test_d2/")
        .inOrder();

    GoogleCloudStorageItemInfo d2Info =
        rawStorage.getItemInfo(
            new StorageResourceId(
                bucketName, "listObjectInfo_includePrefix_subdirectories_test_d2/"));
    assertThat(d2Info.exists())
        .isEqualTo(rawStorage instanceof PerformanceCachingGoogleCloudStorage);

    List<GoogleCloudStorageItemInfo> d2ItemInfos =
        rawStorage.listObjectInfo(
            bucketName,
            "listObjectInfo_includePrefix_subdirectories_test_d2/",
            INCLUDE_PREFIX_LIST_OPTIONS);
    assertWithMessage("D2 item infos expected to be not empty")
        .that(d2ItemInfos.stream().map(GoogleCloudStorageItemInfo::getObjectName).collect(toList()))
        .containsExactly(
            "listObjectInfo_includePrefix_subdirectories_test_d2/",
            "listObjectInfo_includePrefix_subdirectories_test_d2/d3/")
        .inOrder();

    List<GoogleCloudStorageItemInfo> d3ItemInfos =
        rawStorage.listObjectInfo(
            bucketName,
            "listObjectInfo_includePrefix_subdirectories_test_d2/d3/",
            INCLUDE_PREFIX_LIST_OPTIONS);
    assertWithMessage("D3 item infos expected to be not empty")
        .that(d3ItemInfos.stream().map(GoogleCloudStorageItemInfo::getObjectName).collect(toList()))
        .containsExactly(
            "listObjectInfo_includePrefix_subdirectories_test_d2/d3/",
            "listObjectInfo_includePrefix_subdirectories_test_d2/d3/o2")
        .inOrder();

    // Testing GCS treating object names as opaque blobs
    List<GoogleCloudStorageItemInfo> blobNamesInfo =
        rawStorage.listObjectInfo(
            bucketName,
            "listObjectInfo_includePrefix_subdirectories_test_",
            ListObjectOptions.DEFAULT_FLAT_LIST.toBuilder().setIncludePrefix(true).build());

    assertWithMessage("blobNamesInfo not expected to be empty")
        .that(
            blobNamesInfo.stream().map(GoogleCloudStorageItemInfo::getObjectName).collect(toList()))
        .containsExactly(
            "listObjectInfo_includePrefix_subdirectories_test_",
            "listObjectInfo_includePrefix_subdirectories_test_d1/",
            "listObjectInfo_includePrefix_subdirectories_test_d1/o1",
            "listObjectInfo_includePrefix_subdirectories_test_d2/d3/",
            "listObjectInfo_includePrefix_subdirectories_test_d2/d3/o2")
        .inOrder();
  }

  @Test
  public void testCopySingleItem() throws IOException {
    String bucketName = getSharedBucketName();

    StorageResourceId objectToCreate =
        new StorageResourceId(bucketName, "testCopySingleItem_SourceObject");
    byte[] objectBytes = writeObject(rawStorage, objectToCreate, /* objectSize= */ 4096);

    StorageResourceId copiedResourceId =
        new StorageResourceId(bucketName, "testCopySingleItem_DestinationObject");
    rawStorage.copy(
        bucketName, ImmutableList.of(objectToCreate.getObjectName()),
        bucketName, ImmutableList.of(copiedResourceId.getObjectName()));

    assertObjectContent(rawStorage, copiedResourceId, objectBytes);
  }

  @Test
  public void testCopyToDifferentBucket() throws IOException {
    String sourceBucketName = getSharedBucketName();
    String destinationBucketName = createUniqueBucket("copy-destination");

    StorageResourceId objectToCreate =
        new StorageResourceId(sourceBucketName, "testCopyToDifferentBucket_SourceObject");
    byte[] objectBytes = writeObject(rawStorage, objectToCreate, /* objectSize= */ 4096);

    StorageResourceId copiedResourceId =
        new StorageResourceId(destinationBucketName, "testCopyToDifferentBucket_DestinationObject");
    rawStorage.copy(
        sourceBucketName, ImmutableList.of(objectToCreate.getObjectName()),
        destinationBucketName, ImmutableList.of(copiedResourceId.getObjectName()));

    assertObjectContent(rawStorage, copiedResourceId, objectBytes);
  }

  @Test
  public void testCopySingleItemOverExistingItem() throws IOException {
    String bucketName = getSharedBucketName();

    StorageResourceId objectToCopy =
        new StorageResourceId(bucketName, "testCopySingleItemOverExistingItem_Object1");
    byte[] objectBytes = writeObject(rawStorage, objectToCopy, /* objectSize= */ 4096);
    assertObjectContent(rawStorage, objectToCopy, objectBytes);

    StorageResourceId secondObject =
        new StorageResourceId(bucketName, "testCopySingleItemOverExistingItem_Object2");
    byte[] secondObjectBytes = writeObject(rawStorage, secondObject, /* objectSize= */ 2046);
    assertObjectContent(rawStorage, secondObject, secondObjectBytes);

    rawStorage.copy(
        bucketName, ImmutableList.of(objectToCopy.getObjectName()),
        bucketName, ImmutableList.of(secondObject.getObjectName()));

    // Second object should now have the bytes of the first.
    assertObjectContent(rawStorage, secondObject, objectBytes);
  }

  @Test
  public void testCopySingleItemOverItself() throws IOException {
    String bucketName = getSharedBucketName();

    StorageResourceId objectToCopy =
        new StorageResourceId(bucketName, "testCopySingleItemOverItself_Object");
    writeObject(rawStorage, objectToCopy, /* objectSize= */ 1024);

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                rawStorage.copy(
                    bucketName, ImmutableList.of(objectToCopy.getObjectName()),
                    bucketName, ImmutableList.of(objectToCopy.getObjectName())));

    assertThat(e).hasMessageThat().startsWith("Copy destination must be different");
  }

  static class CopyObjectData {
    public final StorageResourceId sourceResourceId;
    public final StorageResourceId destinationResourceId;
    public final byte[] objectBytes;

    CopyObjectData(
        StorageResourceId sourceResourceId,
        StorageResourceId destinationResourceId,
        byte[] objectBytes) {
      this.sourceResourceId = sourceResourceId;
      this.destinationResourceId = destinationResourceId;
      this.objectBytes = objectBytes;
    }
  }

  @Test
  public void testCopyMultipleItems() throws IOException {
    String bucketName = getSharedBucketName();

    final int copyObjectCount = 3;

    List<CopyObjectData> objectsToCopy = new ArrayList<>();
    for (int i = 0; i < copyObjectCount; i++) {
      String sourceObjectName = "testCopyMultipleItems_SourceObject" + i;
      String destinationObjectName = "testCopyMultipleItems_DestinationObject" + i;

      StorageResourceId sourceId = new StorageResourceId(bucketName, sourceObjectName);
      byte[] objectBytes = writeObject(rawStorage, sourceId, 1024 * i);

      StorageResourceId destinationId = new StorageResourceId(bucketName, destinationObjectName);
      objectsToCopy.add(new CopyObjectData(sourceId, destinationId, objectBytes));
    }

    List<String> sourceObjects =
        objectsToCopy.stream()
            .map(copyObjectData -> copyObjectData.sourceResourceId.getObjectName())
            .collect(toList());

    List<String> destinationObjects =
        objectsToCopy.stream()
            .map(copyObjectData -> copyObjectData.destinationResourceId.getObjectName())
            .collect(toList());

    rawStorage.copy(bucketName, sourceObjects, bucketName, destinationObjects);

    for (CopyObjectData copyObjectData : objectsToCopy) {
      assertObjectContent(rawStorage, copyObjectData.sourceResourceId, copyObjectData.objectBytes);
      assertObjectContent(
          rawStorage, copyObjectData.destinationResourceId, copyObjectData.objectBytes);
    }
  }

  @Test
  public void testCopyNonExistentItem() throws IOException {
    String bucketName = getSharedBucketName();
    String notExistentName = "testCopyNonExistentItem_IDontExist";

    assertThrows(
        FileNotFoundException.class,
        () ->
            rawStorage.copy(
                bucketName, ImmutableList.of(notExistentName),
                bucketName, ImmutableList.of("testCopyNonExistentItem_DestinationObject")));
  }

  @Test
  public void testCopyMultipleItemsToSingleDestination() throws IOException {
    String bucketName = getSharedBucketName();

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                rawStorage.copy(
                    bucketName,
                    ImmutableList.of(
                        "testCopyMultipleItemsToSingleDestination_SourceObject1",
                        "testCopyMultipleItemsToSingleDestination_SourceObject2"),
                    bucketName,
                    ImmutableList.of(
                        "testCopyMultipleItemsToSingleDestination_DestinationObject")));
    assertThat(e).hasMessageThat().startsWith("Must supply same number of elements");
  }

  @Test
  public void testMoveSingleItem_withMove() throws IOException {
    String bucketName = getSharedBucketName();

    StorageResourceId srcResourceId =
        new StorageResourceId(bucketName, "testMoveSingleItem_srcFile.txt");
    StorageResourceId dstResourceId =
        new StorageResourceId(bucketName, "testMoveSingleItem_dstFile.txt");

    rawStorage.deleteObjects(ImmutableList.of(srcResourceId, dstResourceId));

    assertThat(rawStorage.getItemInfo(srcResourceId).exists()).isFalse();
    assertThat(rawStorage.getItemInfo(dstResourceId).exists()).isFalse();

    byte[] objectContent = writeObject(rawStorage, srcResourceId, /* objectSize= */ 4096);
    assertThat(rawStorage.getItemInfo(srcResourceId).exists()).isTrue();

    rawStorage.move(ImmutableMap.of(srcResourceId, dstResourceId));

    // Verify source no longer exists
    GoogleCloudStorageItemInfo srcInfo = rawStorage.getItemInfo(srcResourceId);
    assertWithMessage("Source object %s should not exist after move", srcResourceId)
        .that(srcInfo.exists())
        .isFalse();

    // Verify destination exists and has the correct content
    GoogleCloudStorageItemInfo dstInfo = rawStorage.getItemInfo(dstResourceId);
    assertWithMessage("Destination object %s should exist after move", dstResourceId)
        .that(dstInfo.exists())
        .isTrue();
    assertObjectContent(rawStorage, dstResourceId, objectContent);
  }

  @Test
  public void testMoveMultipleItems() throws IOException {
    String bucketName = getSharedBucketName();

    StorageResourceId src1 = new StorageResourceId(bucketName, "testMoveMultipleItems_src1.txt");
    StorageResourceId dst1 = new StorageResourceId(bucketName, "testMoveMultipleItems_dst1.txt");
    StorageResourceId src2 = new StorageResourceId(bucketName, "testMoveMultipleItems_src2.dat");
    StorageResourceId dst2 = new StorageResourceId(bucketName, "testMoveMultipleItems_dst2.dat");
    StorageResourceId src3 =
        new StorageResourceId(bucketName, "testMoveMultipleItems_src3/sub.txt");
    StorageResourceId dst3 =
        new StorageResourceId(bucketName, "testMoveMultipleItems_dst3/sub_moved.txt");

    rawStorage.deleteObjects(ImmutableList.of(src1, dst1, src2, dst2, src3, dst3));

    byte[] content1 = writeObject(rawStorage, src1, 100);
    byte[] content2 = writeObject(rawStorage, src2, 200);
    byte[] content3 = writeObject(rawStorage, src3, 300);

    Map<StorageResourceId, StorageResourceId> moveMap =
        ImmutableMap.of(
            src1, dst1,
            src2, dst2,
            src3, dst3);

    rawStorage.move(moveMap);

    // Verify sources are deleted and destinations exist with correct content
    assertThat(rawStorage.getItemInfo(src1).exists()).isFalse();
    assertObjectContent(rawStorage, dst1, content1);
    assertThat(rawStorage.getItemInfo(dst1).getSize()).isEqualTo(content1.length);

    assertThat(rawStorage.getItemInfo(src2).exists()).isFalse();
    assertObjectContent(rawStorage, dst2, content2);
    assertThat(rawStorage.getItemInfo(dst2).getSize()).isEqualTo(content2.length);

    assertThat(rawStorage.getItemInfo(src3).exists()).isFalse();
    assertObjectContent(rawStorage, dst3, content3);
    assertThat(rawStorage.getItemInfo(dst3).getSize()).isEqualTo(content3.length);
  }

  @Test
  public void testMove_sourceNotExists_throwsFileNotFound() throws IOException {
    String bucketName = getSharedBucketName();
    StorageResourceId nonExistentSource =
        new StorageResourceId(bucketName, "nonExistentSourceForMove.txt");
    StorageResourceId destination =
        new StorageResourceId(bucketName, "destinationForNonExistentMove.txt");

    // Verify source doesn't exist
    rawStorage.deleteObjects(ImmutableList.of(nonExistentSource, destination));
    assertThat(rawStorage.getItemInfo(nonExistentSource).exists()).isFalse();

    Map<StorageResourceId, StorageResourceId> moveMap =
        ImmutableMap.of(nonExistentSource, destination);

    // Verify FileNotFound Exception thrown
    assertThrows(FileNotFoundException.class, () -> rawStorage.move(moveMap));
  }

  @Test
  public void testOpen() throws IOException {
    String bucketName = getSharedBucketName();

    StorageResourceId objectToCreate = new StorageResourceId(bucketName, "testOpen_Object");
    byte[] objectBytes = writeObject(rawStorage, objectToCreate, /* objectSize= */ 100);

    assertObjectContent(rawStorage, objectToCreate, objectBytes);
  }

  @Test
  public void testOpenNonExistentItem() throws IOException {
    String bucketName = getSharedBucketName();

    assertThrows(
        FileNotFoundException.class,
        () -> rawStorage.open(new StorageResourceId(bucketName, "testOpenNonExistentItem_Object")));
  }

  @Test
  public void testOpenItemInfoNonExistentItem() throws IOException {
    String bucketName = getSharedBucketName();
    StorageResourceId nonExistent =
        new StorageResourceId(bucketName, "testOpenItemInfoNonExistentItem_Object");
    GoogleCloudStorageItemInfo itemInfo = GoogleCloudStorageItemInfo.createNotFound(nonExistent);
    assertThrows(FileNotFoundException.class, () -> rawStorage.open(itemInfo));
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
  public void testPlusInObjectNames() throws IOException {
    String bucketName = getSharedBucketName();

    StorageResourceId resourceId =
        new StorageResourceId(bucketName, "testPlusInObjectNames_An+Object");
    rawStorage.createEmptyObject(resourceId);

    assertObjectContent(rawStorage, resourceId, new byte[0]);
  }

  @Test
  public void testObjectPosition() throws IOException {
    final int totalBytes = 1200;
    String bucketName = getSharedBucketName();

    StorageResourceId resourceId = new StorageResourceId(bucketName, "testObjectPosition_Object");
    byte[] data = writeObject(rawStorage, resourceId, /* objectSize= */ totalBytes);

    byte[] readBackingArray = new byte[totalBytes];
    ByteBuffer readBuffer = ByteBuffer.wrap(readBackingArray);
    try (SeekableByteChannel readChannel = rawStorage.open(resourceId)) {
      assertWithMessage("Expected new file to open at position 0")
          .that(readChannel.position())
          .isEqualTo(0);
      assertWithMessage("Unexpected readChannel.size()")
          .that(readChannel.size())
          .isEqualTo(totalBytes);

      readBuffer.limit(4);
      int bytesRead = readChannel.read(readBuffer);
      assertWithMessage("Unexpected number of bytes read").that(bytesRead).isEqualTo(4);
      assertWithMessage("Unexpected position after read()")
          .that(readChannel.position())
          .isEqualTo(4);
      assertWithMessage("Unexpected readChannel.size()")
          .that(readChannel.size())
          .isEqualTo(totalBytes);

      readChannel.position(4);
      assertWithMessage("Unexpected position after no-op")
          .that(readChannel.position())
          .isEqualTo(4);

      readChannel.position(6);
      assertWithMessage("Unexpected position after explicit position(6)")
          .that(readChannel.position())
          .isEqualTo(6);

      readChannel.position(data.length - 1);
      assertWithMessage("Unexpected position after seek to EOF - 1")
          .that(readChannel.position())
          .isEqualTo(data.length - 1);
      readBuffer.clear();
      bytesRead = readChannel.read(readBuffer);
      assertWithMessage("Expected to read 1 byte").that(bytesRead).isEqualTo(1);
      assertWithMessage("Unexpected data read for last byte")
          .that(readBackingArray[0])
          .isEqualTo(data[data.length - 1]);

      bytesRead = readChannel.read(readBuffer);
      assertWithMessage("Expected to read -1 bytes for EOF marker").that(bytesRead).isEqualTo(-1);

      readChannel.position(0);
      assertWithMessage("Unexpected position after reset to 0")
          .that(readChannel.position())
          .isEqualTo(0);

      assertThrows(EOFException.class, () -> readChannel.position(-1));
      assertThrows(EOFException.class, () -> readChannel.position(totalBytes));
    }
  }

  @Test
  public void testReadPartialObjects() throws IOException {
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
  public void testSpecialResourceIds() throws IOException {
    assertWithMessage("Unexpected ROOT item info returned")
        .that(rawStorage.getItemInfo(StorageResourceId.ROOT))
        .isEqualTo(GoogleCloudStorageItemInfo.ROOT_INFO);

    assertThrows(
        IllegalArgumentException.class, () -> StringPaths.fromComponents(null, "objectName"));
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

  @Test
  @Ignore("Not implemented")
  public void testOperationsAfterCloseFail() {}

  @Test
  public void testMetadataIsWrittenWhenCreatingObjects() throws IOException {
    String bucketName = getSharedBucketName();

    byte[] bytesToWrite = new byte[100];
    GoogleCloudStorageTestHelper.fillBytes(bytesToWrite);

    Map<String, byte[]> metadata =
        ImmutableMap.of(
            "key1", "value1".getBytes(StandardCharsets.UTF_8),
            "key2", "value2".getBytes(StandardCharsets.UTF_8));

    // Verify the bucket exist by creating an object
    StorageResourceId objectToCreate =
        new StorageResourceId(bucketName, "testUpdateItemInfoUpdatesMetadata_Object");
    try (WritableByteChannel channel =
        rawStorage.create(
            objectToCreate, CreateObjectOptions.builder().setMetadata(metadata).build())) {
      channel.write(ByteBuffer.wrap(bytesToWrite));
    }

    // Verify metadata was set on create.
    GoogleCloudStorageItemInfo itemInfo = rawStorage.getItemInfo(objectToCreate);
    assertMapsEqual(metadata, itemInfo.getMetadata(), BYTE_ARRAY_EQUIVALENCE);
  }

  @Test
  public void testMetadataIsWrittenWhenCreatingEmptyObjects() throws IOException {
    String bucketName = getSharedBucketName();

    Map<String, byte[]> metadata =
        ImmutableMap.of(
            "key1", "value1".getBytes(StandardCharsets.UTF_8),
            "key2", "value2".getBytes(StandardCharsets.UTF_8));

    // Verify the bucket exist by creating an object
    StorageResourceId objectToCreate =
        new StorageResourceId(bucketName, "testMetadataIsWrittenWhenCreatingEmptyObjects_Object");
    rawStorage.createEmptyObject(
        objectToCreate, CreateObjectOptions.builder().setMetadata(metadata).build());

    // Verify we get metadata from getItemInfo
    GoogleCloudStorageItemInfo itemInfo = rawStorage.getItemInfo(objectToCreate);
    assertMapsEqual(metadata, itemInfo.getMetadata(), BYTE_ARRAY_EQUIVALENCE);
  }

  @Test
  public void testUpdateItemInfoUpdatesMetadata() throws IOException {
    String bucketName = getSharedBucketName();

    Map<String, byte[]> metadata =
        ImmutableMap.of(
            "key1", "value1".getBytes(StandardCharsets.UTF_8),
            "key2", "value2".getBytes(StandardCharsets.UTF_8));

    StorageResourceId objectToCreate =
        new StorageResourceId(bucketName, "testUpdateItemInfoUpdatesMetadata_Object");
    writeObject(rawStorage, objectToCreate, /* objectSize= */ 100);

    GoogleCloudStorageItemInfo itemInfo = rawStorage.getItemInfo(objectToCreate);
    assertWithMessage("initial metadata should be empty").that(itemInfo.getMetadata()).isEmpty();

    // Verify we can update metadata:
    List<GoogleCloudStorageItemInfo> results =
        rawStorage.updateItems(ImmutableList.of(new UpdatableItemInfo(objectToCreate, metadata)));

    assertThat(results).hasSize(1);
    assertMapsEqual(metadata, results.get(0).getMetadata(), BYTE_ARRAY_EQUIVALENCE);

    // Verify we get metadata from getItemInfo
    itemInfo = rawStorage.getItemInfo(objectToCreate);
    assertMapsEqual(metadata, itemInfo.getMetadata(), BYTE_ARRAY_EQUIVALENCE);

    // Delete key1 from metadata:
    Map<String, byte[]> deletionMap = new HashMap<>();
    deletionMap.put("key1", null);
    rawStorage.updateItems(ImmutableList.of(new UpdatableItemInfo(objectToCreate, deletionMap)));

    itemInfo = rawStorage.getItemInfo(objectToCreate);
    // Ensure that only key2:value2 still exists:
    assertMapsEqual(
        ImmutableMap.of("key2", "value2".getBytes(StandardCharsets.UTF_8)),
        itemInfo.getMetadata(),
        BYTE_ARRAY_EQUIVALENCE);
  }

  @Test
  public void testGetBeforeAndAfterCreateObject() throws IOException {
    StorageResourceId resourceId =
        new StorageResourceId(getSharedBucketName(), "testGetBeforeAndAfterCreateObject_Object");

    assertThat(rawStorage.getItemInfo(resourceId).exists()).isFalse();

    byte[] objectContent = writeObject(rawStorage, resourceId, /* objectSize= */ 512);
    assertObjectContent(rawStorage, resourceId, objectContent);

    GoogleCloudStorageItemInfo itemInfo = rawStorage.getItemInfo(resourceId);
    assertThat(itemInfo.exists()).isTrue();
    assertThat(itemInfo.getSize()).isEqualTo(512);
  }

  @Test
  public void testGetMultipleBeforeAndAfterCreateObject() throws IOException {
    String bucketName = getSharedBucketName();

    List<StorageResourceId> resourceIds =
        ImmutableList.of(
            new StorageResourceId(bucketName, "testGetMultipleBeforeAndAfterCreateObject_Object1"),
            new StorageResourceId(bucketName, "testGetMultipleBeforeAndAfterCreateObject_Object2"),
            new StorageResourceId(bucketName, "testGetMultipleBeforeAndAfterCreateObject_Object3"));

    for (GoogleCloudStorageItemInfo itemInfo : rawStorage.getItemInfos(resourceIds)) {
      assertThat(itemInfo.exists()).isFalse();
    }

    rawStorage.createEmptyObjects(resourceIds);

    List<GoogleCloudStorageItemInfo> itemInfos = rawStorage.getItemInfos(resourceIds);
    for (int i = 0; i < resourceIds.size(); i++) {
      StorageResourceId resourceId = resourceIds.get(i);
      GoogleCloudStorageItemInfo itemInfo = itemInfos.get(i);

      assertThat(itemInfo.getResourceId()).isEqualTo(resourceId);
      assertThat(itemInfo.getSize()).isEqualTo(0);
      assertThat(itemInfo.exists()).isTrue();
      assertThat(itemInfo.getCreationTime()).isNotEqualTo(0);
      assertThat(itemInfo.isBucket()).isFalse();
      assertThat(itemInfo.isDirectory()).isFalse();
    }
  }

  @Test
  public void testListObjectInfoBeforeAndAfterCreate() throws IOException {
    String bucketName = getSharedBucketName();
    String objectName = "testListObjectInfoBeforeAndAfterCreate_";

    List<StorageResourceId> resourceIds =
        ImmutableList.of(
            new StorageResourceId(bucketName, objectName + "x"),
            new StorageResourceId(bucketName, objectName + "y"),
            new StorageResourceId(bucketName, objectName + "z"));

    assertThat(rawStorage.listObjectInfo(bucketName, objectName)).isEmpty();

    rawStorage.createEmptyObjects(resourceIds);

    assertThat(rawStorage.listObjectInfo(bucketName, objectName)).hasSize(3);
  }

  @Test
  public void testlistObjectInfoPageBeforeAndAfterCreate() throws IOException {
    String bucketName = getSharedBucketName();
    String objectName = "testlistObjectInfoPageBeforeAndAfterCreate_Object";

    StorageResourceId resourceId = new StorageResourceId(bucketName, objectName);

    ListPage<GoogleCloudStorageItemInfo> itemInfosPage =
        rawStorage.listObjectInfoPage(bucketName, objectName, /* pageToken= */ null);

    assertThat(itemInfosPage.getNextPageToken()).isNull();
    assertThat(itemInfosPage.getItems()).isEmpty();

    writeObject(rawStorage, resourceId, /* objectSize= */ 512);

    itemInfosPage = rawStorage.listObjectInfoPage(bucketName, objectName, /* pageToken= */ null);

    assertThat(itemInfosPage.getNextPageToken()).isNull();
    assertThat(itemInfosPage.getItems()).hasSize(1);
    assertThat(itemInfosPage.getItems().get(0).getResourceId()).isEqualTo(resourceId);
  }

  @Test
  public void testOverwriteExistingObject() throws IOException {
    String bucketName = getSharedBucketName();

    StorageResourceId resourceId =
        new StorageResourceId(bucketName, "testOverwriteExistingObject_Object");

    assertThat(rawStorage.getItemInfo(resourceId).exists()).isFalse();

    writeObject(rawStorage, resourceId, /* objectSize= */ 128);

    GoogleCloudStorageItemInfo createdItemInfo = rawStorage.getItemInfo(resourceId);
    assertThat(createdItemInfo.exists()).isTrue();
    assertThat(createdItemInfo.getSize()).isEqualTo(128);

    byte[] objectContent = writeObject(rawStorage, resourceId, /* objectSize= */ 256);

    GoogleCloudStorageItemInfo overwrittenItemInfo = rawStorage.getItemInfo(resourceId);
    assertThat(overwrittenItemInfo.exists()).isTrue();
    assertThat(overwrittenItemInfo.getSize()).isEqualTo(256);
    assertObjectContent(rawStorage, resourceId, objectContent);
  }

  @Test
  public void testMoveSingleItem_withCopyDelete() throws IOException {
    String bucketName = getSharedBucketName();

    StorageResourceId srcResourceId = new StorageResourceId(bucketName, "testMoveSingleItem_src");
    StorageResourceId dstResourceId = new StorageResourceId(bucketName, "testMoveSingleItem_dst");

    assertThat(rawStorage.getItemInfo(srcResourceId).exists()).isFalse();
    assertThat(rawStorage.getItemInfo(dstResourceId).exists()).isFalse();

    byte[] objectContent = writeObject(rawStorage, srcResourceId, /* objectSize= */ 4096);

    rawStorage.copy(
        bucketName, ImmutableList.of(srcResourceId.getObjectName()),
        bucketName, ImmutableList.of(dstResourceId.getObjectName()));

    assertThat(rawStorage.getItemInfo(srcResourceId).exists()).isTrue();
    assertThat(rawStorage.getItemInfo(dstResourceId).exists()).isTrue();
    assertObjectContent(rawStorage, dstResourceId, objectContent);

    rawStorage.deleteObjects(ImmutableList.of(srcResourceId));

    assertThat(rawStorage.getItemInfo(srcResourceId).exists()).isFalse();
    assertThat(rawStorage.getItemInfo(dstResourceId).exists()).isTrue();
  }

  @Test
  public void testCompose() throws Exception {
    String bucketName = getSharedBucketName();

    StorageResourceId destinationObject =
        new StorageResourceId(bucketName, "testCompose_DestinationObject");

    // Create source objects
    StorageResourceId sourceObject1 =
        new StorageResourceId(bucketName, "testCompose_SourceObject1");
    byte[] content1 = writeObject(rawStorage, sourceObject1, /* objectSize= */ 50);

    StorageResourceId sourceObject2 =
        new StorageResourceId(bucketName, "testCompose_SourceObject2");
    byte[] content2 = writeObject(rawStorage, sourceObject2, /* objectSize= */ 150);

    // Do the compose
    rawStorage.compose(
        bucketName,
        ImmutableList.of("testCompose_SourceObject1", "testCompose_SourceObject2"),
        destinationObject.getObjectName(),
        "application/octet-stream");

    assertObjectContent(rawStorage, destinationObject, Bytes.concat(content1, content2));
  }

  @Test
  public void testObjectVerificationAttributes() throws IOException {
    String bucketName = getSharedBucketName();

    StorageResourceId testObject =
        new StorageResourceId(bucketName, "testObjectValidationAttributes_Object");
    // Don't use hashes in object creation, just validate the round trip. This of course
    // could lead to flaky looking tests due to bit flip errors.
    byte[] objectBytes = writeObject(rawStorage, testObject, /* objectSize= */ 1024);
    GoogleCloudStorageItemInfo itemInfo = rawStorage.getItemInfo(testObject);

    HashCode originalMd5 = Hashing.md5().hashBytes(objectBytes);
    HashCode originalCrc32c = Hashing.crc32c().hashBytes(objectBytes);
    // Note that HashCode#asBytes returns a little-endian encoded array while
    // GCS uses big-endian. We avoid that by grabbing the int value of the CRC32c
    // and running it through Ints.toByteArray which encodes using big-endian.
    byte[] bigEndianCrc32c = Ints.toByteArray(originalCrc32c.asInt());

    GoogleCloudStorageTestHelper.assertByteArrayEquals(
        originalMd5.asBytes(), itemInfo.getVerificationAttributes().getMd5hash());
    // These string versions are slightly easier to debug (used when trying to
    // replicate GCS crc32c values in InMemoryGoogleCloudStorage).
    String originalCrc32cString = Integer.toHexString(Ints.fromByteArray(bigEndianCrc32c));
    String newCrc32cString =
        Integer.toHexString(Ints.fromByteArray(itemInfo.getVerificationAttributes().getCrc32c()));
    assertThat(newCrc32cString).isEqualTo(originalCrc32cString);
    GoogleCloudStorageTestHelper.assertByteArrayEquals(
        bigEndianCrc32c, itemInfo.getVerificationAttributes().getCrc32c());

    VerificationAttributes expectedAttributes =
        new VerificationAttributes(originalMd5.asBytes(), bigEndianCrc32c);

    assertThat(itemInfo.getVerificationAttributes()).isEqualTo(expectedAttributes);
  }

  @Test
  public void testCreateAndGetNativeFolder() throws IOException {
    String bucketName = getSharedHnsBucketName();
    StorageResourceId folderId = new StorageResourceId(bucketName, "testCreateAndGetNativeFolder/");

    // Ensure the resource doesn't exist before we start.
    // Use deleteObjects because a folder might be represented by a placeholder object in some
    // states.
    rawStorage.deleteObjects(ImmutableList.of(folderId));
    assertThat(rawStorage.getItemInfo(folderId).exists()).isFalse();

    rawStorage.createFolder(folderId, false);

    GoogleCloudStorageItemInfo folderInfo = rawStorage.getFolderInfo(folderId);

    assertThat(folderInfo).isNotNull();
    assertWithMessage("Folder '%s' should exist", folderId).that(folderInfo.exists()).isTrue();
    assertThat(folderInfo.getResourceId()).isEqualTo(folderId);
    assertThat(folderInfo.isDirectory()).isTrue();
    assertThat(folderInfo.isNativeHNSFolder()).isTrue(); // This is the key check
    assertThat(folderInfo.isInferredDirectory()).isFalse();
    assertThat(folderInfo.getSize()).isEqualTo(0);
    assertThat(folderInfo.getMetaGeneration()).isGreaterThan(0L);
  }

  @Test
  public void testCreateFolder_alreadyExists_throwsException() throws IOException {
    String bucketName = getSharedHnsBucketName();
    StorageResourceId folderId =
        new StorageResourceId(bucketName, "testCreateFolder_alreadyExists/");

    rawStorage.createFolder(folderId, false);
    assertThat(rawStorage.getFolderInfo(folderId).exists()).isTrue();

    assertThrows(
        java.nio.file.FileAlreadyExistsException.class,
        () -> rawStorage.createFolder(folderId, false));
  }

  @Test
  public void testCreateFolder_conflictingFileExists_throwsException() throws IOException {
    String bucketName = getSharedHnsBucketName();
    StorageResourceId resourceId =
        new StorageResourceId(bucketName, "testCreateFolder_conflicts_with_file/");

    rawStorage.createEmptyObject(resourceId);
    assertThat(rawStorage.getItemInfo(resourceId).exists()).isTrue();

    assertThrows(
        java.nio.file.FileAlreadyExistsException.class,
        () -> rawStorage.createFolder(resourceId, false));
  }

  @Test
  public void testGetFolderInfo_nonExistent() throws IOException {
    String bucketName = getSharedHnsBucketName();
    StorageResourceId nonExistentFolderId =
        new StorageResourceId(bucketName, "this-folder-does-not-exist/");

    GoogleCloudStorageItemInfo folderInfo = rawStorage.getFolderInfo(nonExistentFolderId);

    assertThat(folderInfo.exists()).isFalse();
  }

  @Test
  public void testGetFolderInfo_forObject_isNotFound() throws IOException {
    String bucketName = getSharedHnsBucketName();
    StorageResourceId object1 = new StorageResourceId(bucketName, "this-is-a-non-empty-object");
    ;

    writeObject(rawStorage, object1, /* objectSize= */ 1024);

    GoogleCloudStorageItemInfo genericInfo = rawStorage.getItemInfo(object1);
    assertThat(genericInfo.exists()).isTrue();
    assertThat(genericInfo.isNativeHNSFolder()).isFalse();

    GoogleCloudStorageItemInfo nativeFolderInfo = rawStorage.getFolderInfo(object1);
    assertThat(nativeFolderInfo.exists()).isFalse();
  }

  @Test
  public void testGetFolderInfo_forPlaceholderObject_isNotFound() throws IOException {
    String bucketName = getSharedBucketName();
    StorageResourceId placeholderId =
        new StorageResourceId(bucketName, "this-is-a-placeholder-folder/");

    rawStorage.createEmptyObject(placeholderId);

    GoogleCloudStorageItemInfo genericInfo = rawStorage.getItemInfo(placeholderId);
    assertThat(genericInfo.exists()).isTrue();
    assertThat(genericInfo.isNativeHNSFolder()).isFalse();

    GoogleCloudStorageItemInfo nativeFolderInfo = rawStorage.getFolderInfo(placeholderId);

    assertThat(nativeFolderInfo.exists()).isFalse();
  }

  @Test
  public void googleCloudStorageItemInfo_equals() throws IOException {
    String bucketName = getSharedBucketName();

    StorageResourceId object1 = new StorageResourceId(bucketName, "testEquals_Object_1");
    StorageResourceId object2 = new StorageResourceId(bucketName, "testEquals_Object_2");

    writeObject(rawStorage, object1, /* objectSize= */ 1024);
    writeObject(rawStorage, object2, /* objectSize= */ 1024);

    GoogleCloudStorageItemInfo itemInfo1 = rawStorage.getItemInfo(object1);
    GoogleCloudStorageItemInfo itemInfo2 = rawStorage.getItemInfo(object2);

    assertThat(itemInfo1.equals(itemInfo1)).isTrue();
    assertThat(itemInfo1.equals(itemInfo2)).isFalse();
  }

  @Test
  public void googleCloudStorageItemInfo_toString() throws IOException {
    String bucketName = getSharedBucketName();

    StorageResourceId object1 = new StorageResourceId(bucketName, "testToString_Object_1");
    StorageResourceId object2 = new StorageResourceId(bucketName, "testToString_Object_2");

    writeObject(rawStorage, object1, /* objectSize= */ 1024);

    GoogleCloudStorageItemInfo itemInfo1 = rawStorage.getItemInfo(object1);
    GoogleCloudStorageItemInfo itemInfo2 = rawStorage.getItemInfo(object2);

    assertThat(itemInfo1.exists()).isTrue();
    assertThat(itemInfo1.toString()).contains("created on:");

    assertThat(itemInfo2.exists()).isFalse();
    assertThat(itemInfo2.toString()).contains("exists: no");
  }

  public static <K, V> void assertMapsEqual(
      Map<K, V> expected, Map<K, V> result, Equivalence<V> valueEquivalence) {
    MapDifference<K, V> diff = Maps.difference(expected, result, valueEquivalence);
    if (!diff.areEqual()) {
      fail(
          String.format(
              "Maps differ. Entries differing: %s%nMissing entries: %s%nExtra entries: %s%n",
              diff.entriesDiffering(), diff.entriesOnlyOnLeft(), diff.entriesOnlyOnRight()));
    }
  }
}
