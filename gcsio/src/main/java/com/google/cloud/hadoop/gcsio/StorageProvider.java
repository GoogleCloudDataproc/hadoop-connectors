package com.google.cloud.hadoop.gcsio;

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageClientImpl.*;

import com.google.auth.Credentials;
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
  @VisibleForTesting
  final Cache<StorageProviderCacheKey, Storage> cache =
      CacheBuilder.newBuilder().recordStats().build();

  /** Tracks the number of times a storage client is used. */
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
    if (canCache(storageOptions, interceptors, pCUExecutorService)) {
      StorageProviderCacheKey key =
          computeCacheKey(credentials, storageOptions, downscopedAccessTokenFn);
      Storage storage = cache.getIfPresent(key);
      if (storage == null) {
        storage = createStorage(credentials, storageOptions, null, null, downscopedAccessTokenFn);
        cache.put(key, storage);
        storageToCacheKeyMap.put(storage, key);
      }
      // Increment the reference count of the storage object.
      storageClientToReferenceMap.put(
          storage, storageClientToReferenceMap.getOrDefault(storage, 0) + 1);
      return storage;
    }
    return createStorage(
        credentials, storageOptions, interceptors, pCUExecutorService, downscopedAccessTokenFn);
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
              List<ClientInterceptor> list = new ArrayList<>();
              if (interceptors != null && !interceptors.isEmpty()) {
                list.addAll(
                    interceptors.stream().filter(x -> x != null).collect(Collectors.toList()));
              }
              if (storageOptions.isTraceLogEnabled()) {
                list.add(new GoogleCloudStorageClientGrpcTracingInterceptor());
              }

              if (downscopedAccessTokenFn != null) {
                // When downscoping is enabled, we need to set the downscoped token for each
                // request. In the case of gRPC, the downscoped token will be set from the
                // Interceptor.
                list.add(
                    new GoogleCloudStorageClientGrpcDownscopingInterceptor(
                        downscopedAccessTokenFn));
              }

              list.add(new GoogleCloudStorageClientGrpcStatisticsInterceptor());
              return ImmutableList.copyOf(list);
            })
        .setCredentials(
            credentials != null ? credentials : getNoCredentials(downscopedAccessTokenFn))
        .setBlobWriteSessionConfig(
            getSessionConfig(storageOptions.getWriteChannelOptions(), pCUExecutorService))
        .setProjectId(storageOptions.getProjectId())
        .build()
        .getService();
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
    if (storageClientToReferenceMap.containsKey(storage)) {
      int referenceCount = storageClientToReferenceMap.get(storage);
      // Decrement the reference count of the object.
      storageClientToReferenceMap.put(storage, referenceCount - 1);
      if (referenceCount - 1 == 0) {
        StorageProviderCacheKey key = storageToCacheKeyMap.get(storage);
        cache.invalidate(key);
        storageToCacheKeyMap.remove(storage);
        closeStorage(storage);
      }
    } else {
      closeStorage(storage);
    }
  }
}
