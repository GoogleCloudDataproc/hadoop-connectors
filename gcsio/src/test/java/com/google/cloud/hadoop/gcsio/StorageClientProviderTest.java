package com.google.cloud.hadoop.gcsio;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import com.google.auth.Credentials;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.auth.oauth2.UserCredentials;
import com.google.cloud.hadoop.util.AccessBoundary;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.cloud.storage.Storage;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.grpc.ClientInterceptor;
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
  private int getReferences(StorageWrapper storage) {
    return storageClientProvider.storageClientToReferenceMap.getOrDefault(storage, 0);
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
    storageClientProvider = spy(new StorageClientProvider());
  }

  @Test
  public void getStorage_returnsAndCachesNewStorageObject() throws IOException {
    Storage mockStorage = mock(Storage.class);
    doReturn(mockStorage)
        .when(storageClientProvider)
        .createStorage(any(), any(), any(), any(), any());

    StorageWrapper storage =
        storageClientProvider.getStorage(TEST_CREDENTIALS, TEST_STORAGE_OPTIONS, null, null, null);

    assertNotNull(storage);
    assertEquals(1, storageClientProvider.cache.estimatedSize());

    StorageWrapper cachedStorage =
        storageClientProvider.cache.getIfPresent(
            storageClientProvider.computeCacheKey(TEST_CREDENTIALS, TEST_STORAGE_OPTIONS));
    assertNotNull(cachedStorage);
    assertEquals(storage, cachedStorage);
  }

  @Test
  public void getStorage_cachingDisabled_doesNotCache() throws IOException {
    Storage mockStorage1 = mock(Storage.class, "storage1");
    Storage mockStorage2 = mock(Storage.class, "storage2");
    doReturn(mockStorage1, mockStorage2)
        .when(storageClientProvider)
        .createStorage(any(), any(), any(), any(), any());

    GoogleCloudStorageOptions optionsWithCachingDisabled =
        TEST_STORAGE_OPTIONS.toBuilder().setStorageClientCachingEnabled(false).build();
    StorageWrapper storage1 =
        storageClientProvider.getStorage(
            TEST_CREDENTIALS,
            optionsWithCachingDisabled,
            null,
            null,
            TEST_DOWNSCOPED_ACCESS_TOKEN_FUNC);

    assertNotNull(storage1);
    // Cache should not be initialized when caching is disabled.
    assertNull(storageClientProvider.cache);

    // Calling a second time should return a new instance.
    StorageWrapper storage2 =
        storageClientProvider.getStorage(
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
    Storage mockStorage = mock(Storage.class);
    doReturn(mockStorage)
        .when(storageClientProvider)
        .createStorage(any(), any(), any(), any(), any());

    StorageWrapper storage1 =
        storageClientProvider.getStorage(TEST_CREDENTIALS, TEST_STORAGE_OPTIONS, null, null, null);
    StorageWrapper storage2 =
        storageClientProvider.getStorage(TEST_CREDENTIALS, TEST_STORAGE_OPTIONS, null, null, null);

    assertEquals(1, storageClientProvider.cache.estimatedSize());
    // A single storage object should be shared across both the references.
    assertEquals(2, getReferences(storage1));
    assertEquals(storage1, storage2);
  }

  @Test
  public void getStorage_cacheMiss_returnsNewObject() throws Exception {
    Storage mockStorage1 = mock(Storage.class, "storage1");
    Storage mockStorage2 = mock(Storage.class, "storage2");
    doReturn(mockStorage1, mockStorage2)
        .when(storageClientProvider)
        .createStorage(any(), any(), any(), any(), any());

    GoogleCloudStorageOptions testOptions =
        TEST_STORAGE_OPTIONS.toBuilder()
            .setTraceLogEnabled(false)
            .setDirectPathPreferred(false)
            .build();

    StorageWrapper storage1 =
        storageClientProvider.getStorage(TEST_CREDENTIALS, TEST_STORAGE_OPTIONS, null, null, null);
    StorageWrapper storage2 =
        storageClientProvider.getStorage(TEST_CREDENTIALS, testOptions, null, null, null);

    assertNotEquals(storage1, storage2);
    assertEquals(1, getReferences(storage1));
    assertEquals(1, getReferences(storage2));
  }

  @Test
  public void close_nonZeroReference_doesNotCloseObject() throws Exception {
    Storage mockStorage = mock(Storage.class);
    doReturn(mockStorage)
        .when(storageClientProvider)
        .createStorage(any(), any(), any(), any(), any());

    StorageWrapper storage1 =
        storageClientProvider.getStorage(TEST_CREDENTIALS, TEST_STORAGE_OPTIONS, null, null, null);
    StorageWrapper storage2 =
        storageClientProvider.getStorage(TEST_CREDENTIALS, TEST_STORAGE_OPTIONS, null, null, null);

    assertEquals(1, storageClientProvider.cache.estimatedSize());
    // A single storage object should be shared across both the references.
    assertEquals(2, getReferences(storage1));
    assertEquals(storage1, storage2);

    storage1.close();
    // Item is not removed from the cache but the reference is reduced.
    assertEquals(1, storageClientProvider.cache.estimatedSize());
    assertEquals(1, getReferences(storage1));
  }

  @Test
  public void close_zeroReference_closesStorageObject() throws Exception {
    Storage mockStorage = mock(Storage.class);
    doReturn(mockStorage)
        .when(storageClientProvider)
        .createStorage(any(), any(), any(), any(), any());

    StorageWrapper storage1 =
        storageClientProvider.getStorage(TEST_CREDENTIALS, TEST_STORAGE_OPTIONS, null, null, null);

    assertEquals(1, storageClientProvider.cache.estimatedSize());
    // A single storage object should be shared across both the references.
    assertEquals(1, getReferences(storage1));

    storage1.close();
    // Item is removed from the cache.
    assertEquals(0, storageClientProvider.cache.estimatedSize());
    assertEquals(0, getReferences(storage1));
    assertFalse(storageClientProvider.storageClientToReferenceMap.containsKey(storage1));
  }

  @Test
  public void getStorageConcurrent_returnsStorage() throws Exception {
    Storage mockStorage = mock(Storage.class);
    doReturn(mockStorage)
        .when(storageClientProvider)
        .createStorage(any(), any(), any(), any(), any());

    int numThreads = 100;
    ExecutorService executorService = Executors.newFixedThreadPool(100);
    final CountDownLatch latch = new CountDownLatch(numThreads);

    AtomicInteger failures = new AtomicInteger();
    for (int i = 0; i < numThreads; i++) {
      executorService.submit(
          () -> {
            try {
              storageClientProvider.getStorage(
                  TEST_CREDENTIALS, TEST_STORAGE_OPTIONS, null, null, null);
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
    assertEquals(1, storageClientProvider.cache.estimatedSize());
  }

  @Test
  public void getStorage_withInterceptors_doesNotCache() throws IOException {
    Storage mockStorage = mock(Storage.class);
    doReturn(mockStorage)
        .when(storageClientProvider)
        .createStorage(any(), any(), any(), any(), any());

    ClientInterceptor mockInterceptor = mock(ClientInterceptor.class);
    StorageWrapper storage1 =
        storageClientProvider.getStorage(
            TEST_CREDENTIALS,
            TEST_STORAGE_OPTIONS,
            ImmutableList.of(mockInterceptor),
            null,
            TEST_DOWNSCOPED_ACCESS_TOKEN_FUNC);
    assertNotNull(storage1);
    assertNull(storageClientProvider.cache);
    assertTrue(storageClientProvider.storageClientToReferenceMap.isEmpty());
  }

  @Test
  public void getStorage_withExecutor_doesNotCache() throws IOException {
    Storage mockStorage1 = mock(Storage.class, "storage1");
    Storage mockStorage2 = mock(Storage.class, "storage2");
    doReturn(mockStorage1, mockStorage2)
        .when(storageClientProvider)
        .createStorage(any(), any(), any(), any(), any());

    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      StorageWrapper storage1 =
          storageClientProvider.getStorage(
              TEST_CREDENTIALS,
              TEST_STORAGE_OPTIONS,
              null,
              executor,
              TEST_DOWNSCOPED_ACCESS_TOKEN_FUNC);
      assertNotNull(storage1);
      assertNull(storageClientProvider.cache);
      StorageWrapper storage2 =
          storageClientProvider.getStorage(
              TEST_CREDENTIALS,
              TEST_STORAGE_OPTIONS,
              null,
              executor,
              TEST_DOWNSCOPED_ACCESS_TOKEN_FUNC);
      assertNotNull(storage2);
      assertNotEquals(storage1, storage2);
      assertTrue(storageClientProvider.storageClientToReferenceMap.isEmpty());
    } finally {
      executor.shutdown();
    }
  }

  @Test
  public void close_uncachedObject_doesNotAffectCache() throws IOException {
    Storage mockStorage = mock(Storage.class);
    doReturn(mockStorage)
        .when(storageClientProvider)
        .createStorage(any(), any(), any(), any(), any());

    // Get an uncached storage object by passing an interceptor
    ClientInterceptor mockInterceptor = mock(ClientInterceptor.class);
    StorageWrapper uncachedStorage =
        storageClientProvider.getStorage(
            TEST_CREDENTIALS,
            TEST_STORAGE_OPTIONS,
            ImmutableList.of(mockInterceptor),
            null,
            TEST_DOWNSCOPED_ACCESS_TOKEN_FUNC);

    // Verify it's not in the reference map
    assertFalse(storageClientProvider.storageClientToReferenceMap.containsKey(uncachedStorage));

    // Closing an uncached object should not throw an exception and should not affect the maps.
    uncachedStorage.close();

    // Verify maps are still empty
    assertTrue(storageClientProvider.storageClientToReferenceMap.isEmpty());
    assertTrue(storageClientProvider.storageToCacheKeyMap.isEmpty());
  }

  @Test
  public void computeCacheKey_withUserCredentials_returnsKey() {
    // Use the builder to create a real UserCredentials object instead of mocking a final class.
    UserCredentials credentials =
        UserCredentials.newBuilder()
            .setClientId("test-client-id")
            .setClientSecret("dummy-secret")
            .setRefreshToken("dummy-refresh-token")
            .build();

    StorageClientProviderCacheKey key =
        storageClientProvider.computeCacheKey(credentials, TEST_STORAGE_OPTIONS);

    assertNotNull(key);
    assertEquals(ImmutableList.of("test-client-id"), key.getCredentialsKey());
  }

  @Test
  public void computeCacheKey_withComputeEngineCredentials_returnsKey() {
    ComputeEngineCredentials credentials = mock(ComputeEngineCredentials.class);
    StorageClientProviderCacheKey key =
        storageClientProvider.computeCacheKey(credentials, TEST_STORAGE_OPTIONS);
    assertNotNull(key);
    assertEquals(ImmutableList.of(ComputeEngineCredentials.class), key.getCredentialsKey());
  }

  @Test
  public void onRemoval_evictionBySize_cleansUpResources() throws IOException {
    Storage mockStorage1 = mock(Storage.class, "storage1");
    Storage mockStorage2 = mock(Storage.class, "storage2");
    doReturn(mockStorage1, mockStorage2)
        .when(storageClientProvider)
        .createStorage(any(), any(), any(), any(), any());

    // Configure a cache of size 1
    GoogleCloudStorageOptions options =
        TEST_STORAGE_OPTIONS.toBuilder().setStorageClientCacheMaxSize(1).build();
    // Create two different options objects to generate two different cache keys.
    // We vary the trace log setting, which is part of the cache key.
    GoogleCloudStorageOptions options1 = options.toBuilder().setTraceLogEnabled(true).build();
    GoogleCloudStorageOptions options2 = options.toBuilder().setTraceLogEnabled(false).build();
    // Use null credentials, which is a safe and common case for testing.
    // The cache key will be differentiated by the options objects.
    Credentials credentials = null;
    // Get the first storage object, which will be cached.
    StorageWrapper storage1 =
        storageClientProvider.getStorage(credentials, options1, null, null, null);
    assertEquals(1, storageClientProvider.cache.estimatedSize());
    assertTrue(storageClientProvider.storageToCacheKeyMap.containsKey(storage1));
    assertTrue(storageClientProvider.storageClientToReferenceMap.containsKey(storage1));
    // Get a second, different storage object. This should cause the first one to be evicted
    // because the cache size is 1.
    StorageWrapper storage2 =
        storageClientProvider.getStorage(credentials, options2, null, null, null);
    assertNotEquals(storage1, storage2);

    // Manually trigger the cache's cleanup to run the removal listener synchronously.
    storageClientProvider.cache.cleanUp();

    // The first storage object is no longer in the cache, but because its reference count is still
    // >0, it MUST remain in both the key map and the reference map.
    assertTrue(storageClientProvider.storageToCacheKeyMap.containsKey(storage1));
    assertTrue(storageClientProvider.storageClientToReferenceMap.containsKey(storage1));

    storage1.close();

    // it should be removed from ALL maps, and the underlying storage should be closed.
    assertFalse(storageClientProvider.storageToCacheKeyMap.containsKey(storage1));
    assertFalse(storageClientProvider.storageClientToReferenceMap.containsKey(storage1));
  }
}
