/*
 * Copyright 2013 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.hadoop.gcsio;

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorage.MAX_RESULTS_UNLIMITED;
import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.api.client.util.Clock;
import com.google.cloud.hadoop.gcsio.testing.GcsItemInfoTestBuilder;
import com.google.cloud.hadoop.gcsio.testing.InMemoryGoogleCloudStorage;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentMatchers;
import org.mockito.MockitoAnnotations;

@RunWith(JUnit4.class)
public class PerformanceCachingGoogleCloudStorageTest {

  private static final HashCode EMPTY_OBJECT_MD5 = Hashing.md5().hashBytes(new byte[0]);
  private static final HashCode EMPTY_OBJECT_CRC32C = Hashing.crc32c().hashBytes(new byte[0]);

  // Sample empty metadata.
  private static final ImmutableMap<String, byte[]> TEST_METADATA =
      ImmutableMap.of("test_key", new byte[] {2});

  private static final CreateBucketOptions CREATE_BUCKET_OPTIONS =
      CreateBucketOptions.builder()
          .setLocation("test_location")
          .setStorageClass("test_storage_class")
          .build();

  private static final CreateObjectOptions CREATE_OBJECT_OPTIONS =
      new CreateObjectOptions(/* overwriteExisting= */ true, "test_content_type", TEST_METADATA);

  // Sample bucket names.
  private static final String BUCKET_A = "alpha";
  private static final String BUCKET_B = "alph";

  // Sample object names.
  private static final String PREFIX_A = "bar";
  private static final String PREFIX_AA = "bar/apple";
  private static final String PREFIX_ABA = "bar/berry/foo";
  private static final String PREFIX_B = "baz";

  /* Sample bucket item info. */
  private static final GoogleCloudStorageItemInfo ITEM_A = createBucketItemInfo(BUCKET_A);
  private static final GoogleCloudStorageItemInfo ITEM_B = createBucketItemInfo(BUCKET_B);

  /* Sample item info. */
  private static final GoogleCloudStorageItemInfo ITEM_A_A =
      createObjectItemInfo(BUCKET_A, PREFIX_A);
  private static final GoogleCloudStorageItemInfo ITEM_A_AA =
      createObjectItemInfo(BUCKET_A, PREFIX_AA);
  private static final GoogleCloudStorageItemInfo ITEM_A_ABA =
      createObjectItemInfo(BUCKET_A, PREFIX_ABA);
  private static final GoogleCloudStorageItemInfo ITEM_A_B =
      createObjectItemInfo(BUCKET_A, PREFIX_B);
  private static final GoogleCloudStorageItemInfo ITEM_B_A =
      createObjectItemInfo(BUCKET_B, PREFIX_A);
  private static final GoogleCloudStorageItemInfo ITEM_B_B =
      createObjectItemInfo(BUCKET_B, PREFIX_B);

  /** Clock implementation for testing the GCS delegate. */
  private TestClock clock;
  /** {@link PerformanceCachingGoogleCloudStorage} instance being tested. */
  private PerformanceCachingGoogleCloudStorage gcs;
  /** Cache implementation to back the GCS instance being tested. */
  private PrefixMappedItemCache cache;
  /** GoogleCloudStorage implementation to back the GCS instance being tested. */
  private GoogleCloudStorage gcsDelegate;

  @Before
  public void setUp() throws IOException {
    // Setup mocks.
    MockitoAnnotations.initMocks(this);

    // Create the cache configuration.
    cache = new PrefixMappedItemCache(new TestTicker(), Duration.ofMillis(10));

    // Setup the delegate
    clock = new TestClock();
    GoogleCloudStorage gcsImpl =
        new InMemoryGoogleCloudStorage(GoogleCloudStorageOptions.DEFAULT, clock);
    gcsDelegate = spy(gcsImpl);

    gcs = new PerformanceCachingGoogleCloudStorage(gcsDelegate, cache);

    // Prepare the delegate.
    gcsDelegate.create(BUCKET_A, CREATE_BUCKET_OPTIONS);
    gcsDelegate.create(BUCKET_B, CREATE_BUCKET_OPTIONS);
    gcsDelegate.createEmptyObject(ITEM_A_A.getResourceId(), CREATE_OBJECT_OPTIONS);
    gcsDelegate.createEmptyObject(ITEM_A_AA.getResourceId(), CREATE_OBJECT_OPTIONS);
    gcsDelegate.createEmptyObject(ITEM_A_ABA.getResourceId(), CREATE_OBJECT_OPTIONS);
    gcsDelegate.createEmptyObject(ITEM_A_B.getResourceId(), CREATE_OBJECT_OPTIONS);
    gcsDelegate.createEmptyObject(ITEM_B_A.getResourceId(), CREATE_OBJECT_OPTIONS);
    gcsDelegate.createEmptyObject(ITEM_B_B.getResourceId(), CREATE_OBJECT_OPTIONS);
  }

  @Test
  public void testDeleteBuckets() throws IOException {
    List<String> buckets = Lists.newArrayList(BUCKET_A);

    // Prepare the cache.
    cache.putItem(ITEM_A_A); // Deleted.
    cache.putItem(ITEM_B_A); // Not deleted.

    gcs.deleteBuckets(buckets);

    // Verify the delegate call.
    verify(gcsDelegate).deleteBuckets(eq(buckets));
    // Verify the state of the cache.
    assertThat(cache.getAllItemsRaw()).containsExactly(ITEM_B_A);
  }

  @Test
  public void testDeleteObjects() throws IOException {
    List<StorageResourceId> ids =
        Lists.newArrayList(ITEM_A_A.getResourceId(), ITEM_B_A.getResourceId());

    // Prepare the cache.
    cache.putItem(ITEM_A_A); // Deleted.
    cache.putItem(ITEM_B_A); // Deleted.
    cache.putItem(ITEM_B_B); // Not deleted.

    gcs.deleteObjects(ids);

    // Verify the delegate call.
    verify(gcsDelegate).deleteObjects(eq(ids));
    // Verify the state of the cache.
    assertThat(cache.getAllItemsRaw()).containsExactly(ITEM_B_B);
  }

  @Test
  public void testListBucketInfo() throws IOException {
    List<GoogleCloudStorageItemInfo> expected = Lists.newArrayList(ITEM_A, ITEM_B);

    List<GoogleCloudStorageItemInfo> result = gcs.listBucketInfo();

    // Verify the delegate call.
    verify(gcsDelegate).listBucketInfo();
    assertThat(result).containsExactlyElementsIn(expected);
    // Verify the state of the cache.
    assertThat(cache.getAllItemsRaw()).containsExactlyElementsIn(expected);
  }

  @Test
  public void testListObjectInfo() throws IOException {
    List<GoogleCloudStorageItemInfo> expected = Lists.newArrayList(ITEM_A_A, ITEM_A_AA, ITEM_A_ABA);

    List<GoogleCloudStorageItemInfo> result =
        gcs.listObjectInfo(BUCKET_A, PREFIX_A, null, MAX_RESULTS_UNLIMITED);

    // Verify the delegate call.
    verify(gcsDelegate)
        .listObjectInfo(eq(BUCKET_A), eq(PREFIX_A), eq(null), eq(MAX_RESULTS_UNLIMITED));
    assertThat(result).containsExactlyElementsIn(expected);
    // Verify the state of the cache.
    assertThat(cache.getAllItemsRaw()).containsExactlyElementsIn(expected);
  }

  @Test
  public void testListBucketInfoAndObjectInfo() throws IOException {
    List<GoogleCloudStorageItemInfo> expectedBuckets = ImmutableList.of(ITEM_A, ITEM_B);
    List<GoogleCloudStorageItemInfo> expectedObjectsInBucketA =
        ImmutableList.of(ITEM_A_A, ITEM_A_AA, ITEM_A_ABA, ITEM_A_B);
    Iterable<GoogleCloudStorageItemInfo> expectedAllCachedItems =
        Iterables.concat(expectedBuckets, expectedObjectsInBucketA);

    List<GoogleCloudStorageItemInfo> objects1 =
        gcs.listObjectInfo(BUCKET_A, /* objectNamePrefix= */ null, /* delimiter= */ null);
    assertThat(objects1).containsExactlyElementsIn(expectedObjectsInBucketA);
    assertThat(cache.getAllItemsRaw()).containsExactlyElementsIn(expectedObjectsInBucketA);

    List<GoogleCloudStorageItemInfo> buckets = gcs.listBucketInfo();
    assertThat(buckets).containsExactlyElementsIn(expectedBuckets);
    assertThat(cache.getAllItemsRaw()).containsExactlyElementsIn(expectedAllCachedItems);

    List<GoogleCloudStorageItemInfo> objects2 =
        gcs.listObjectInfo(BUCKET_A, /* objectNamePrefix= */ null, /* delimiter= */ null);
    assertThat(objects2).containsExactlyElementsIn(expectedObjectsInBucketA);
    assertThat(cache.getAllItemsRaw()).containsExactlyElementsIn(expectedAllCachedItems);

    verify(gcsDelegate).listBucketInfo();
    verify(gcsDelegate, times(2))
        .listObjectInfo(eq(BUCKET_A), eq(null), eq(null), eq(MAX_RESULTS_UNLIMITED));
  }

  @Test
  public void testListObjectInfo_delimiter() throws IOException {
    GoogleCloudStorageItemInfo itemAAPrefix = createInferredDirectory(BUCKET_A, PREFIX_A + "/");

    List<GoogleCloudStorageItemInfo> expectedResult = Lists.newArrayList(ITEM_A_A, itemAAPrefix);
    List<GoogleCloudStorageItemInfo> expectedCached = Lists.newArrayList(ITEM_A_A, itemAAPrefix);

    List<GoogleCloudStorageItemInfo> result =
        gcs.listObjectInfo(BUCKET_A, PREFIX_A, "/", MAX_RESULTS_UNLIMITED);

    // Verify the delegate call.
    verify(gcsDelegate)
        .listObjectInfo(eq(BUCKET_A), eq(PREFIX_A), eq("/"), eq(MAX_RESULTS_UNLIMITED));

    // Verify the result.
    assertThat(result).containsExactlyElementsIn(expectedResult);

    // Verify the state of the cache.
    assertThat(cache.getAllItemsRaw()).containsExactlyElementsIn(expectedCached);
  }

  @Test
  public void testListObjectInfo_prefixDir_delimiter() throws IOException {
    String prefixADir = PREFIX_A + "/";

    String prefixABADir = PREFIX_ABA.substring(0, PREFIX_ABA.lastIndexOf('/') + 1);
    GoogleCloudStorageItemInfo itemABAPrefix = createInferredDirectory(BUCKET_A, prefixABADir);

    List<GoogleCloudStorageItemInfo> expectedResult = Lists.newArrayList(ITEM_A_AA, itemABAPrefix);
    List<GoogleCloudStorageItemInfo> expectedCached = Lists.newArrayList(ITEM_A_AA, itemABAPrefix);

    List<GoogleCloudStorageItemInfo> result =
        gcs.listObjectInfo(BUCKET_A, prefixADir, "/", MAX_RESULTS_UNLIMITED);

    // Verify the delegate call.
    verify(gcsDelegate)
        .listObjectInfo(eq(BUCKET_A), eq(prefixADir), eq("/"), eq(MAX_RESULTS_UNLIMITED));

    // Verify the result.
    assertThat(result).containsExactlyElementsIn(expectedResult);

    // Verify the state of the cache.
    assertThat(cache.getAllItemsRaw()).containsExactlyElementsIn(expectedCached);
  }

  @Test
  public void testListObjectInfoAlt() throws IOException {
    List<GoogleCloudStorageItemInfo> expected = Lists.newArrayList(ITEM_B_A, ITEM_B_B);

    List<GoogleCloudStorageItemInfo> result = gcs.listObjectInfo(BUCKET_B, null, null);

    // Verify the delegate call.
    verify(gcsDelegate)
        .listObjectInfo(
            eq(BUCKET_B),
            ArgumentMatchers.eq(null),
            ArgumentMatchers.eq(null),
            eq(MAX_RESULTS_UNLIMITED));

    assertThat(result).containsExactlyElementsIn(expected);
    // Verify the state of the cache.
    assertThat(cache.getAllItemsRaw()).containsExactlyElementsIn(expected);
  }

  @Test
  public void testListObjectInfoCached() throws IOException {
    List<GoogleCloudStorageItemInfo> expected =
        Lists.newArrayList(ITEM_A_A, ITEM_A_AA, ITEM_A_ABA, ITEM_A_B);

    // First call to get the values in cache.
    gcs.listObjectInfo(BUCKET_A, null, null);
    // Second call to ensure the values are being served from cache.
    List<GoogleCloudStorageItemInfo> result = gcs.listObjectInfo(BUCKET_A, null, null);

    // Verify the delegate call once.
    verify(gcsDelegate, times(2))
        .listObjectInfo(
            eq(BUCKET_A),
            ArgumentMatchers.eq(null),
            ArgumentMatchers.eq(null),
            eq(MAX_RESULTS_UNLIMITED));
    assertThat(result).containsExactlyElementsIn(expected);
    // Verify the state of the cache.
    assertThat(cache.getAllItemsRaw()).containsExactlyElementsIn(expected);
  }

  @Test
  public void testGetItemInfo() throws IOException {
    // Prepare the cache.
    cache.putItem(ITEM_A_A);

    GoogleCloudStorageItemInfo result = gcs.getItemInfo(ITEM_A_A.getResourceId());

    // Verify the cached item was returned.
    assertThat(result).isEqualTo(ITEM_A_A);
    // Verify the state of the cache.
    assertThat(cache.getAllItemsRaw()).containsExactly(ITEM_A_A);
  }

  @Test
  public void testGetItemInfoMissing() throws IOException {
    GoogleCloudStorageItemInfo result = gcs.getItemInfo(ITEM_A_A.getResourceId());

    // Verify the delegate call.
    verify(gcsDelegate).getItemInfo(eq(ITEM_A_A.getResourceId()));
    assertThat(result).isEqualTo(ITEM_A_A);
    // Verify the cache was updated.
    assertThat(cache.getItem(ITEM_A_A.getResourceId())).isEqualTo(ITEM_A_A);
  }

  @Test
  public void testGetItemInfosAllCached() throws IOException {
    List<StorageResourceId> requestedIds =
        Lists.newArrayList(ITEM_A_A.getResourceId(), ITEM_A_B.getResourceId());
    List<GoogleCloudStorageItemInfo> expected = Lists.newArrayList(ITEM_A_A, ITEM_A_B);

    // Prepare the cache.
    cache.putItem(ITEM_A_A);
    cache.putItem(ITEM_A_B);

    List<GoogleCloudStorageItemInfo> result = gcs.getItemInfos(requestedIds);

    // Verify the result is exactly what the delegate returns in order.
    assertThat(result).containsExactlyElementsIn(expected).inOrder();
    // Verify the state of the cache.
    assertThat(cache.getAllItemsRaw()).containsExactlyElementsIn(expected);
  }

  @Test
  public void testGetItemInfosSomeCached() throws IOException {
    List<StorageResourceId> requestedIds =
        Lists.newArrayList(
            ITEM_A_A.getResourceId(), // Not cached
            ITEM_A_B.getResourceId(), // Cached
            ITEM_B_A.getResourceId(), // Not cached
            ITEM_B_B.getResourceId()); // Cached
    List<StorageResourceId> uncachedIds =
        Lists.newArrayList(ITEM_A_A.getResourceId(), ITEM_B_A.getResourceId());
    List<GoogleCloudStorageItemInfo> expected =
        Lists.newArrayList(ITEM_A_A, ITEM_A_B, ITEM_B_A, ITEM_B_B);

    // Prepare the cache.
    cache.putItem(ITEM_A_B);
    cache.putItem(ITEM_B_B);

    List<GoogleCloudStorageItemInfo> result = gcs.getItemInfos(requestedIds);

    // Verify the delegate call.
    verify(gcsDelegate).getItemInfos(eq(uncachedIds));
    // Verify the result and its ordering.
    assertThat(result).containsExactlyElementsIn(expected).inOrder();
    // Verify the state of the cache.
    assertThat(cache.getAllItemsRaw()).containsExactlyElementsIn(expected);
  }

  @Test
  public void testGetItemInfosNoneCached() throws IOException {
    List<StorageResourceId> requestedIds =
        Lists.newArrayList(ITEM_A_A.getResourceId(), ITEM_A_B.getResourceId());
    List<GoogleCloudStorageItemInfo> expected = Lists.newArrayList(ITEM_A_A, ITEM_A_B);

    List<GoogleCloudStorageItemInfo> result = gcs.getItemInfos(requestedIds);

    // Verify the delegate call.
    verify(gcsDelegate).getItemInfos(eq(requestedIds));
    // Verify the result and its ordering.
    assertThat(result).containsExactlyElementsIn(expected).inOrder();
    // Verify the state of the cache.
    assertThat(cache.getAllItemsRaw()).containsExactlyElementsIn(expected);
  }

  @Test
  public void testUpdateItems() throws IOException {
    List<UpdatableItemInfo> updateItems =
        Lists.newArrayList(new UpdatableItemInfo(ITEM_A_A.getResourceId(), TEST_METADATA));
    GoogleCloudStorageItemInfo itemAAUpdated =
        updateObjectItemInfo(ITEM_A_A, ITEM_A_A.getMetaGeneration() + 1);

    List<GoogleCloudStorageItemInfo> result = gcs.updateItems(updateItems);

    // Verify the delegate call.
    verify(gcsDelegate).updateItems(eq(updateItems));
    assertThat(result).containsExactly(itemAAUpdated);
    // Verify the state of the cache.
    assertThat(cache.getAllItemsRaw()).containsExactly(itemAAUpdated);
  }

  @Test
  public void testClose() {
    // Prepare the cache.
    cache.putItem(ITEM_A_A);

    gcs.close();

    // Verify the delegate call was made.
    verify(gcsDelegate).close();
    // Verify the state of the cache.
    assertThat(cache.getAllItemsRaw()).isEmpty();
  }

  @Test
  public void testComposeObjects() throws IOException {
    List<StorageResourceId> ids =
        Lists.newArrayList(ITEM_A_A.getResourceId(), ITEM_A_B.getResourceId());

    GoogleCloudStorageItemInfo result =
        gcs.composeObjects(ids, ITEM_A_AA.getResourceId(), CREATE_OBJECT_OPTIONS);

    // Verify the delegate call.
    verify(gcsDelegate)
        .composeObjects(eq(ids), eq(ITEM_A_AA.getResourceId()), eq(CREATE_OBJECT_OPTIONS));
    assertThat(result).isEqualTo(ITEM_A_AA);
    // Verify the state of the cache.
    assertThat(cache.getAllItemsRaw()).containsExactly(ITEM_A_AA);
  }

  /**
   * Helper to generate GoogleCloudStorageItemInfo for a bucket entry.
   *
   * @param bucketName the name of the bucket.
   * @return the generated item.
   */
  public static GoogleCloudStorageItemInfo createBucketItemInfo(String bucketName) {
    return new GoogleCloudStorageItemInfo(
        new StorageResourceId(bucketName),
        /* creationTime= */ 0,
        /* modificationTime= */ 0,
        /* size= */ 0,
        CREATE_BUCKET_OPTIONS.getLocation(),
        CREATE_BUCKET_OPTIONS.getStorageClass());
  }

  /**
   * Helper to generate a GoogleCloudStorageItemInfo for an object entry.
   *
   * @param bucketName the name of the bucket for the generated item.
   * @param objectName the object name of the generated item.
   * @return the generated item.
   */
  public static GoogleCloudStorageItemInfo createObjectItemInfo(
      String bucketName, String objectName) {
    return createObjectItemInfo(bucketName, objectName, CREATE_OBJECT_OPTIONS);
  }

  /**
   * Helper to generate a GoogleCloudStorageItemInfo for an object entry.
   *
   * @param bucketName the name of the bucket for the generated item.
   * @param objectName the object name of the generated item.
   * @param createObjectOptions the {@link CreateObjectOptions} to use to generate item.
   * @return the generated item.
   */
  public static GoogleCloudStorageItemInfo createObjectItemInfo(
      String bucketName, String objectName, CreateObjectOptions createObjectOptions) {
    return GcsItemInfoTestBuilder.create()
        .setStorageResourceId(new StorageResourceId(bucketName, objectName))
        .setCreationTime(0)
        .setModificationTime(0)
        .setSize(0)
        .setLocation(null)
        .setStorageClass(null)
        .setContentType(createObjectOptions.getContentType())
        .setContentEncoding(createObjectOptions.getContentEncoding())
        .setMetadata(createObjectOptions.getMetadata())
        .setContentGeneration(1)
        .setMetaGeneration(1)
        .setVerificationAttributes(
            new VerificationAttributes(EMPTY_OBJECT_MD5.asBytes(), EMPTY_OBJECT_CRC32C.asBytes()))
        .build();
  }

  public static GoogleCloudStorageItemInfo createInferredDirectory(
      String bucketName, String objectName) {
    return GoogleCloudStorageItemInfo.createInferredDirectory(
        new StorageResourceId(bucketName, objectName));
  }

  private static GoogleCloudStorageItemInfo updateObjectItemInfo(
      GoogleCloudStorageItemInfo object, long metaGeneration) {
    return new GoogleCloudStorageItemInfo(
        object.getResourceId(),
        object.getCreationTime(),
        object.getModificationTime(),
        object.getSize(),
        object.getLocation(),
        object.getStorageClass(),
        object.getContentType(),
        object.getContentEncoding(),
        object.getMetadata(),
        object.getContentGeneration(),
        metaGeneration,
        object.getVerificationAttributes());
  }

  /** Ticker with a manual time value used for testing the cache. */
  private static class TestTicker extends Ticker {

    @Override
    public long read() {
      return 0L;
    }
  }

  /** Clock with a manual time value used for testing the GCS delegate. */
  private static class TestClock implements Clock {

    @Override
    public long currentTimeMillis() {
      return 0L;
    }
  }
}
