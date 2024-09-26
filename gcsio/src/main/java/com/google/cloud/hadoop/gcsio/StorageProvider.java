package com.google.cloud.hadoop.gcsio;

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageClientImpl.*;

import com.google.auth.Credentials;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.hadoop.util.AccessBoundary;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.flogger.GoogleLogger;
import io.grpc.ClientInterceptor;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Provides GCS SDK Storage object which is used to access the GCS. Also Caches the storage objects,
 * so they can be used re-used.
 */
public class StorageProvider {

  // TODO: Replace Storage with a StorageWrapper which does not expose the close method. The
  //  dependants of this provider should not be able to close the storage accidentally. It should
  //  always be managed through this provider.
  @VisibleForTesting
  final Cache<StorageProviderCacheKey, Storage> cache =
      CacheBuilder.newBuilder().recordStats().build();

  /**
   * Tracks the number of times a storage client is used. Used to determine when a storage can be
   * closed.
   */
  @VisibleForTesting
  final Map<Storage, Integer> storageClientToReferenceMap = new ConcurrentHashMap<>();

  /** Reverse map for storage reference to cache keys. */
  final Map<Storage, StorageProviderCacheKey> storageToCacheKeyMap = new ConcurrentHashMap<>();

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  @VisibleForTesting
  StorageProviderCacheKey computeCacheKey(
      Credentials credentials,
      GoogleCloudStorageOptions storageOptions,
      Function<List<AccessBoundary>, String> downscopedAccessTokenFn) {
    return StorageProviderCacheKey.builder()
        .setCredentials(credentials)
        .setHttpHeaders(storageOptions.getHttpRequestHeaders())
        .setIsDirectPathPreferred(storageOptions.isDirectPathPreferred())
        .setIsDownScopingEnabled(downscopedAccessTokenFn != null)
        .setIsTracingEnabled(storageOptions.isTraceLogEnabled())
        .setWriteChannelOptions(storageOptions.getWriteChannelOptions())
        .setProjectId(storageOptions.getProjectId())
        .build();
  }

  /** Determines if the storage instance can be served from the cache. */
  private static boolean canCache(
      GoogleCloudStorageOptions options,
      List<ClientInterceptor> interceptors,
      ExecutorService pCUExecutorService) {

    return options.isStorageClientCachingExperimentEnabled()
        // These values are currently passed while creating the storage client, but they are
        // currently not configurable by the FS options so skipping caching.
        && pCUExecutorService == null
        && (interceptors == null || interceptors.isEmpty());
  }

  Storage getStorage(
      Credentials credentials,
      GoogleCloudStorageOptions storageOptions,
      List<ClientInterceptor> interceptors,
      ExecutorService pCUExecutorService,
      Function<List<AccessBoundary>, String> downscopedAccessTokenFn)
      throws IOException {
    if (!canCache(storageOptions, interceptors, pCUExecutorService)) {
      return createStorage(
          credentials, storageOptions, interceptors, pCUExecutorService, downscopedAccessTokenFn);
    }
    StorageProviderCacheKey key =
        computeCacheKey(credentials, storageOptions, downscopedAccessTokenFn);
    Storage storage = cache.getIfPresent(key);
    if (storage == null) {
      storage = createStorage(credentials, storageOptions, null, null, downscopedAccessTokenFn);
      cache.put(key, storage);
      storageToCacheKeyMap.put(storage, key);
      logger.atFinest().log(
          "Cache miss, created new storage client. Cache hit count : %d, Cache hit rate : %d",
          cache.stats().hitCount(), cache.stats().hitRate());
    } else {
      logger.atFinest().log(
          "Cache hit, reusing the storage client. Cache hit count : %d, Cache hit rate : %d",
          cache.stats().hitCount(), cache.stats().hitRate());
    }
    // Increment the reference count of the storage object.
    storageClientToReferenceMap.put(
        storage, storageClientToReferenceMap.getOrDefault(storage, 0) + 1);
    return storage;
  }

  private static Storage createStorage(
      Credentials credentials,
      GoogleCloudStorageOptions storageOptions,
      List<ClientInterceptor> interceptors,
      ExecutorService pCUExecutorService,
      Function<List<AccessBoundary>, String> downscopedAccessTokenFn)
      throws IOException {
    final ImmutableMap<String, String> headers = getUpdatedHeadersWithUserAgent(storageOptions);

    return StorageOptions.grpc()
        .setAttemptDirectPath(storageOptions.isDirectPathPreferred())
        .setHeaderProvider(() -> headers)
        .setGrpcInterceptorProvider(
            () -> {
              List<ClientInterceptor> clientInterceptorList = new ArrayList<>();
              if (interceptors != null && !interceptors.isEmpty()) {
                clientInterceptorList.addAll(
                    interceptors.stream().filter(x -> x != null).collect(Collectors.toList()));
              }
              if (storageOptions.isTraceLogEnabled()) {
                clientInterceptorList.add(new GoogleCloudStorageClientGrpcTracingInterceptor());
              }

              if (downscopedAccessTokenFn != null) {
                // When downscoping is enabled, we need to set the downscoped token for each
                // request. In the case of gRPC, the downscoped token will be set from the
                // Interceptor.
                clientInterceptorList.add(
                    new GoogleCloudStorageClientGrpcDownscopingInterceptor(
                        downscopedAccessTokenFn));
              }

              clientInterceptorList.add(new GoogleCloudStorageClientGrpcStatisticsInterceptor());
              return ImmutableList.copyOf(clientInterceptorList);
            })
        .setCredentials(
            credentials != null ? credentials : getNoCredentials(downscopedAccessTokenFn))
        .setBlobWriteSessionConfig(
            getSessionConfig(storageOptions.getWriteChannelOptions(), pCUExecutorService))
        .setProjectId(storageOptions.getProjectId())
        .build()
        .getService();
  }

  private static Credentials getNoCredentials(
      Function<List<AccessBoundary>, String> downscopedAccessTokenFn) {
    if (downscopedAccessTokenFn == null) {
      return null;
    }

    // Workaround for https://github.com/googleapis/sdk-platform-java/issues/2356. Once this is
    // fixed, change this to return NoCredentials.getInstance();
    return GoogleCredentials.create(new AccessToken("", null));
  }

  private void closeStorage(Storage storage) {
    try {
      storage.close();
    } catch (Exception e) {
      logger.atWarning().withCause(e).log("Error occurred while closing the storage client");
    }
  }

  /**
   * Signal the storage object to be closed. The resources held by the storage object will be freed
   * only if the instance is closed by all the objects sharing this storage reference.
   */
  void close(Storage storage) {
    if (!storageClientToReferenceMap.containsKey(storage)) {
      closeStorage(storage);
      return;
    }
    // Decrement the reference count of the object.
    storageClientToReferenceMap.put(storage, storageClientToReferenceMap.get(storage) - 1);
    if (storageClientToReferenceMap.get(storage) == 0) {
      StorageProviderCacheKey key = storageToCacheKeyMap.get(storage);
      cache.invalidate(key);
      storageToCacheKeyMap.remove(storage);
      storageClientToReferenceMap.remove(storage);
      closeStorage(storage);
    }
  }
}
