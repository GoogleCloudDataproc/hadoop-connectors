package com.google.cloud.hadoop.gcsio;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

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
public class StorageProviderTest {

  private static final Credentials TEST_CREDENTIALS = null;

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

  private StorageProvider storageProvider;

  private static String testProject = null;

  /** Returns number of times a storage object in cache is referenced among different clients. */
  private int getReferences(StorageWrapper storage) {
    return storageProvider.storageClientToReferenceMap.getOrDefault(storage, 0);
  }

  @BeforeClass
  public static void beforeClass() {
    if (System.getProperty(PROJECT_ENV_VARIABLE) == null) {
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
    storageProvider = new StorageProvider();
  }

  @Test
  public void getStorage_returnsAndCachesNewStorageObject() throws IOException {
    StorageWrapper storage =
        storageProvider.getStorage(
            TEST_CREDENTIALS, TEST_STORAGE_OPTIONS, null, null, TEST_DOWNSCOPED_ACCESS_TOKEN_FUNC);

    assertNotNull(storage);
    assertEquals(1, storageProvider.cache.size());

    StorageWrapper cachedStorage =
        storageProvider.cache.getIfPresent(
            storageProvider.computeCacheKey(
                TEST_CREDENTIALS, TEST_STORAGE_OPTIONS, TEST_DOWNSCOPED_ACCESS_TOKEN_FUNC));
    assertNotNull(cachedStorage);
    assertEquals(storage, cachedStorage);
  }

  @Test
  public void getStorage_cachingDisabled_doesNotCache() throws IOException {
    GoogleCloudStorageOptions optionsWithCachingDisabled =
        TEST_STORAGE_OPTIONS.toBuilder().setStorageClientCachingEnabled(false).build();
    StorageWrapper storage1 =
        storageProvider.getStorage(
            TEST_CREDENTIALS,
            optionsWithCachingDisabled,
            null,
            null,
            TEST_DOWNSCOPED_ACCESS_TOKEN_FUNC);

    assertNotNull(storage1);
    // Cache should not be initialized when caching is disabled.
    assertNull(storageProvider.cache);

    // Calling a second time should return a new instance.
    StorageWrapper storage2 =
        storageProvider.getStorage(
            TEST_CREDENTIALS,
            optionsWithCachingDisabled,
            null,
            null,
            TEST_DOWNSCOPED_ACCESS_TOKEN_FUNC);
    assertNotNull(storage2);
    assertNotEquals(storage1, storage2);
  }

  @Test
  public void getStorage_returnsCachedStorageObject() throws IOException {
    StorageWrapper storage1 =
        storageProvider.getStorage(
            TEST_CREDENTIALS, TEST_STORAGE_OPTIONS, null, null, TEST_DOWNSCOPED_ACCESS_TOKEN_FUNC);
    StorageWrapper storage2 =
        storageProvider.getStorage(
            TEST_CREDENTIALS, TEST_STORAGE_OPTIONS, null, null, TEST_DOWNSCOPED_ACCESS_TOKEN_FUNC);

    assertEquals(1, storageProvider.cache.size());
    // A single storage object should be shared across both the references.
    assertEquals(2, getReferences(storage1));
    assertEquals(storage1, storage2);
  }

  @Test
  public void getStorage_cacheMiss_returnsNewObject() throws Exception {
    GoogleCloudStorageOptions testOptions =
        TEST_STORAGE_OPTIONS.toBuilder()
            .setTraceLogEnabled(false)
            .setDirectPathPreferred(false)
            .build();

    StorageWrapper storage1 =
        storageProvider.getStorage(
            TEST_CREDENTIALS, TEST_STORAGE_OPTIONS, null, null, TEST_DOWNSCOPED_ACCESS_TOKEN_FUNC);
    StorageWrapper storage2 =
        storageProvider.getStorage(
            TEST_CREDENTIALS, testOptions, null, null, TEST_DOWNSCOPED_ACCESS_TOKEN_FUNC);

    assertNotEquals(storage1, storage2);
    assertEquals(1, getReferences(storage1));
    assertEquals(1, getReferences(storage2));
  }

  @Test
  public void close_nonZeroReference_doesNotCloseObject() throws Exception {
    StorageWrapper storage1 =
        storageProvider.getStorage(
            TEST_CREDENTIALS, TEST_STORAGE_OPTIONS, null, null, TEST_DOWNSCOPED_ACCESS_TOKEN_FUNC);
    StorageWrapper storage2 =
        storageProvider.getStorage(
            TEST_CREDENTIALS, TEST_STORAGE_OPTIONS, null, null, TEST_DOWNSCOPED_ACCESS_TOKEN_FUNC);

    assertEquals(1, storageProvider.cache.size());
    // A single storage object should be shared across both the references.
    assertEquals(2, getReferences(storage1));
    assertEquals(storage1, storage2);

    storage1.close();
    // Item is not removed from the cache but the reference is reduced.
    assertEquals(1, storageProvider.cache.size());
    assertEquals(1, getReferences(storage1));
  }

  @Test
  public void close_zeroReference_closesStorageObject() throws Exception {
    StorageWrapper storage1 =
        storageProvider.getStorage(
            TEST_CREDENTIALS, TEST_STORAGE_OPTIONS, null, null, TEST_DOWNSCOPED_ACCESS_TOKEN_FUNC);

    assertEquals(1, storageProvider.cache.size());
    // A single storage object should be shared across both the references.
    assertEquals(1, getReferences(storage1));

    storage1.close();
    // Item is removed from the cache.
    assertEquals(0, storageProvider.cache.size());
    assertEquals(0, getReferences(storage1));
    assertFalse(storageProvider.storageClientToReferenceMap.containsKey(storage1));
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
              storageProvider.getStorage(
                  TEST_CREDENTIALS,
                  TEST_STORAGE_OPTIONS,
                  null,
                  null,
                  TEST_DOWNSCOPED_ACCESS_TOKEN_FUNC);
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
    assertEquals(1, storageProvider.cache.size());
  }
}
