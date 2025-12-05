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
 * for downscoped token so they can be re-used.
 */
public class StorageClientProvider {
  @VisibleForTesting
  final Cache<StorageClientProviderCacheKey, StorageClientWrapper> cache =
      CacheBuilder.newBuilder().recordStats().build();

  /**
   * Tracks the number of times a storage client is used. Used to determine when a storage can be
   * closed.
   */
  @VisibleForTesting
  final Map<StorageClientWrapper, Integer> storageClientToReferenceMap = new ConcurrentHashMap<>();

  /** Reverse map for storage reference to cache keys. */
  final Map<StorageClientWrapper, StorageClientProviderCacheKey> storageToCacheKeyMap =
      new ConcurrentHashMap<>();

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  synchronized StorageClientWrapper getStorage(
      Credentials credentials,
      GoogleCloudStorageOptions storageOptions,
      List<ClientInterceptor> interceptors,
      ExecutorService pCUExecutorService,
      Function<List<AccessBoundary>, String> downscopedAccessTokenFn,
      FeatureHeaderGenerator featureHeaderGenerator)
      throws IOException {
    if (!canCache(storageOptions, interceptors, pCUExecutorService)) {
      logger.atInfo().log("Ignoring storage object cache.");
      return new StorageClientWrapper(
          createStorage(
              credentials,
              storageOptions,
              interceptors,
              pCUExecutorService,
              downscopedAccessTokenFn,
              featureHeaderGenerator),
          this);
    }
    StorageClientProviderCacheKey key =
        computeCacheKey(credentials, storageOptions, downscopedAccessTokenFn);
    StorageClientWrapper storage = cache.getIfPresent(key);
    if (storage == null) {
      storage =
          new StorageClientWrapper(
              createStorage(
                  credentials,
                  storageOptions,
                  null,
                  null,
                  downscopedAccessTokenFn,
                  featureHeaderGenerator),
              this);
      cache.put(key, storage);
      storageToCacheKeyMap.put(storage, key);
      logger.atInfo().log(
          "Cache miss for %d, created new storage client. Cache hit count : %d, Cache hit rate : %.2f",
          key.hashCode(), cache.stats().hitCount(), cache.stats().hitRate());
    } else {
      logger.atFine().log(
          "Cache hit for %d, reusing the storage client. Cache hit count : %d, Cache hit rate : %.2f",
          key.hashCode(), cache.stats().hitCount(), cache.stats().hitRate());
    }
    logger.atFine().log("Cache stats. size: %d", cache.size());
    // Increment the reference count of the storage object.
    storageClientToReferenceMap.put(
        storage, storageClientToReferenceMap.getOrDefault(storage, 0) + 1);
    return storage;
  }

  /**
   * Signal the storage object to be closed. The resources held by the storage object will be freed
   * only if the instance is closed by all the objects sharing this storage reference.
   */
  synchronized void close(StorageClientWrapper storage) {
    if (!storageClientToReferenceMap.containsKey(storage)) {
      closeStorage(storage.getStorage());
      logger.atInfo().log("close() called on storage object outside cache.");
      return;
    }
    // Decrement the reference count of the object.
    storageClientToReferenceMap.put(storage, storageClientToReferenceMap.get(storage) - 1);
    if (storageClientToReferenceMap.get(storage) == 0) {
      logger.atInfo().log("close() called on storage object inside cache.");
      StorageClientProviderCacheKey key = storageToCacheKeyMap.get(storage);
      cache.invalidate(key);
      storageToCacheKeyMap.remove(storage);
      storageClientToReferenceMap.remove(storage);
      closeStorage(storage.getStorage());
    }
  }

  @VisibleForTesting
  StorageClientProviderCacheKey computeCacheKey(
      Credentials credentials,
      GoogleCloudStorageOptions storageOptions,
      Function<List<AccessBoundary>, String> downscopedAccessTokenFn) {
    return StorageClientProviderCacheKey.builder()
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

    return options.isStorageClientCachingEnabled()
        // These values are currently passed while creating the storage client, but they are
        // currently not configurable by the FS options so skipping caching.
        && pCUExecutorService == null
        && (interceptors == null || interceptors.isEmpty());
  }

  private static Storage createStorage(
      Credentials credentials,
      GoogleCloudStorageOptions storageOptions,
      List<ClientInterceptor> interceptors,
      ExecutorService pCUExecutorService,
      Function<List<AccessBoundary>, String> downscopedAccessTokenFn,
      FeatureHeaderGenerator featureHeaderGenerator)
      throws IOException {
    final ImmutableMap<String, String> headers =
        getUpdatedHeaders(storageOptions, featureHeaderGenerator);

    return StorageOptions.grpc()
        .setAttemptDirectPath(storageOptions.isDirectPathPreferred())
        .setHeaderProvider(() -> headers)
        .setGrpcInterceptorProvider(
            () -> getInterceptors(interceptors, storageOptions, downscopedAccessTokenFn))
        .setCredentials(
            credentials != null ? credentials : getNoCredentials(downscopedAccessTokenFn))
        .setBlobWriteSessionConfig(
            getSessionConfig(storageOptions.getWriteChannelOptions(), pCUExecutorService))
        .setProjectId(storageOptions.getProjectId())
        .build()
        .getService();
  }

  private static ImmutableList<ClientInterceptor> getInterceptors(
      List<ClientInterceptor> interceptors,
      GoogleCloudStorageOptions storageOptions,
      Function<List<AccessBoundary>, String> downscopedAccessTokenFn) {
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
          new GoogleCloudStorageClientGrpcDownscopingInterceptor(downscopedAccessTokenFn));
    }

    clientInterceptorList.add(new GoogleCloudStorageClientGrpcStatisticsInterceptor());
    return ImmutableList.copyOf(clientInterceptorList);
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
}
