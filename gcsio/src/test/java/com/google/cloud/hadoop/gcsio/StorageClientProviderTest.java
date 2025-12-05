/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.gcsio;

import static org.junit.Assert.*;

import com.google.auth.Credentials;
import com.google.cloud.hadoop.util.AccessBoundary;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class StorageClientProviderTest {

  private static final Credentials TEST_CREDENTIALS = null;
  private static final FeatureHeaderGenerator TEST_FEATURE_HEADER_GENERATOR =
      new FeatureHeaderGenerator(GoogleCloudStorageFileSystemOptions.DEFAULT);

  private static final GoogleCloudStorageOptions TEST_STORAGE_OPTIONS =
      GoogleCloudStorageOptions.builder()
          .setHttpRequestHeaders((ImmutableMap.of("header-key", "header-value")))
          .setTraceLogEnabled(true)
          .setDirectPathPreferred(true)
          .setWriteChannelOptions(AsyncWriteChannelOptions.DEFAULT)
          .setStorageClientCachingEnabled(true)
          .build();

  private static final Function<List<AccessBoundary>, String> TEST_DOWNSCOPED_ACCESS_TOKEN_FUNC =
      (accessBoundaries) -> "token";

  private static final String TEST_PROJECT_PREFIX = "test-project";

  private static final String PROJECT_ENV_VARIABLE = "GOOGLE_CLOUD_PROJECT";

  private StorageClientProvider storageClientProvider;

  private static String testProject = null;

  /** Returns number of times a storage object in cache is referenced among different clients. */
  private int getReferences(StorageClientWrapper storage) {
    return storageClientProvider.storageClientToReferenceMap.getOrDefault(storage, 0);
  }

  @BeforeClass
  public static void beforeClass() {
    if (System.getProperty(PROJECT_ENV_VARIABLE) == null) {
      // Project id is required for storage instantiation.
      testProject = TEST_PROJECT_PREFIX + UUID.randomUUID();
      System.setProperty(PROJECT_ENV_VARIABLE, testProject);
    }
  }

  @AfterClass
  public static void afterCLass() {
    if (Objects.equals(System.getProperty(PROJECT_ENV_VARIABLE), testProject)) {
      System.clearProperty(PROJECT_ENV_VARIABLE);
    }
  }

  @Before
  public void setUp() {
    storageClientProvider = new StorageClientProvider();
  }

  @Test
  public void getStorage_returnsAndCachesNewStorageObject() throws IOException {
    StorageClientWrapper storage =
        storageClientProvider.getStorage(
            TEST_CREDENTIALS,
            TEST_STORAGE_OPTIONS,
            null,
            null,
            TEST_DOWNSCOPED_ACCESS_TOKEN_FUNC,
            TEST_FEATURE_HEADER_GENERATOR);

    assertNotNull(storage);
    assertEquals(storageClientProvider.cache.size(), 1);

    StorageClientWrapper cachedStorage =
        storageClientProvider.cache.getIfPresent(
            storageClientProvider.computeCacheKey(
                TEST_CREDENTIALS, TEST_STORAGE_OPTIONS, TEST_DOWNSCOPED_ACCESS_TOKEN_FUNC));
    assertNotNull(cachedStorage);
    assertEquals(storage, cachedStorage);
  }

  @Test
  public void getStorage_cacheDisabled_doesNotCache() throws IOException {
    GoogleCloudStorageOptions disableCacheOptions =
        TEST_STORAGE_OPTIONS.toBuilder().setStorageClientCachingEnabled(false).build();
    StorageClientWrapper storage =
        storageClientProvider.getStorage(
            TEST_CREDENTIALS,
            disableCacheOptions,
            null,
            null,
            TEST_DOWNSCOPED_ACCESS_TOKEN_FUNC,
            TEST_FEATURE_HEADER_GENERATOR);

    assertNotNull(storage);
    assertEquals(storageClientProvider.cache.size(), 0);
  }

  @Test
  public void getStorage_returnsCachedStorageObject() throws IOException {
    StorageClientWrapper storage1 =
        storageClientProvider.getStorage(
            TEST_CREDENTIALS,
            TEST_STORAGE_OPTIONS,
            null,
            null,
            TEST_DOWNSCOPED_ACCESS_TOKEN_FUNC,
            TEST_FEATURE_HEADER_GENERATOR);
    StorageClientWrapper storage2 =
        storageClientProvider.getStorage(
            TEST_CREDENTIALS,
            TEST_STORAGE_OPTIONS,
            null,
            null,
            TEST_DOWNSCOPED_ACCESS_TOKEN_FUNC,
            TEST_FEATURE_HEADER_GENERATOR);

    assertEquals(storageClientProvider.cache.size(), 1);
    // A single storage object should be shared across both the references.
    assertEquals(getReferences(storage1), 2);
    assertEquals(storage1, storage2);
  }

  @Test
  public void getStorage_cacheMiss_returnsNewObject() throws Exception {
    GoogleCloudStorageOptions testOptions =
        TEST_STORAGE_OPTIONS.toBuilder()
            .setTraceLogEnabled(false)
            .setDirectPathPreferred(false)
            .build();

    StorageClientWrapper storage1 =
        storageClientProvider.getStorage(
            TEST_CREDENTIALS,
            TEST_STORAGE_OPTIONS,
            null,
            null,
            TEST_DOWNSCOPED_ACCESS_TOKEN_FUNC,
            TEST_FEATURE_HEADER_GENERATOR);
    StorageClientWrapper storage2 =
        storageClientProvider.getStorage(
            TEST_CREDENTIALS,
            testOptions,
            null,
            null,
            TEST_DOWNSCOPED_ACCESS_TOKEN_FUNC,
            TEST_FEATURE_HEADER_GENERATOR);

    assertNotEquals(storage1, storage2);
    assertEquals(getReferences(storage1), 1);
    assertEquals(getReferences(storage2), 1);
  }

  @Test
  public void close_nonZeroReference_doesNotCloseObject() throws Exception {
    StorageClientWrapper storage1 =
        storageClientProvider.getStorage(
            TEST_CREDENTIALS,
            TEST_STORAGE_OPTIONS,
            null,
            null,
            TEST_DOWNSCOPED_ACCESS_TOKEN_FUNC,
            TEST_FEATURE_HEADER_GENERATOR);
    StorageClientWrapper storage2 =
        storageClientProvider.getStorage(
            TEST_CREDENTIALS,
            TEST_STORAGE_OPTIONS,
            null,
            null,
            TEST_DOWNSCOPED_ACCESS_TOKEN_FUNC,
            TEST_FEATURE_HEADER_GENERATOR);

    assertEquals(storageClientProvider.cache.size(), 1);
    // A single storage object should be shared across both the references.
    assertEquals(getReferences(storage1), 2);
    assertEquals(storage1, storage2);

    storageClientProvider.close(storage1);
    // Item is not removed from the cache but the reference is reduced.
    assertEquals(storageClientProvider.cache.size(), 1);
    assertEquals(getReferences(storage1), 1);
  }

  @Test
  public void close_zeroReference_closesStorageObject() throws Exception {
    StorageClientWrapper storage1 =
        storageClientProvider.getStorage(
            TEST_CREDENTIALS,
            TEST_STORAGE_OPTIONS,
            null,
            null,
            TEST_DOWNSCOPED_ACCESS_TOKEN_FUNC,
            TEST_FEATURE_HEADER_GENERATOR);

    assertEquals(storageClientProvider.cache.size(), 1);
    // A single storage object should be shared across both the references.
    assertEquals(getReferences(storage1), 1);

    storageClientProvider.close(storage1);
    // Item is removed from the cache.
    assertEquals(storageClientProvider.cache.size(), 0);
    assertEquals(getReferences(storage1), 0);
    assertFalse(storageClientProvider.storageClientToReferenceMap.containsKey(storage1));
  }

  @Test
  public void getStorageConcurrent_returnsStorage() throws Exception {
    int numThreads = 100;
    ExecutorService executorService = Executors.newFixedThreadPool(100);
    final CountDownLatch latch = new CountDownLatch(numThreads);

    AtomicInteger failures = new AtomicInteger();
    for (int i = 0; i < numThreads; i++) {
      executorService.submit(
          () -> {
            try {
              storageClientProvider.getStorage(
                  TEST_CREDENTIALS,
                  TEST_STORAGE_OPTIONS,
                  null,
                  null,
                  TEST_DOWNSCOPED_ACCESS_TOKEN_FUNC,
                  TEST_FEATURE_HEADER_GENERATOR);
            } catch (IOException e) {
              failures.getAndIncrement();
            } finally {
              latch.countDown();
            }
          });
    }

    boolean isComplete = latch.await(5000, TimeUnit.MILLISECONDS);
    executorService.shutdown();
    if (failures.get() > 0 || !isComplete) {
      fail("Storage creation failed");
    }
    assertEquals(storageClientProvider.cache.size(), 1);
  }
}
