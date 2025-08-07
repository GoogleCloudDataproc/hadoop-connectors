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
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.flogger.GoogleLogger;
import io.grpc.ClientInterceptor;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Provides GCS SDK Storage object which is used to access the GCS. Also Caches the storage objects,
 * so they can be used re-used.
 */
public class StorageProvider {

  @VisibleForTesting volatile Cache<StorageProviderCacheKey, StorageWrapper> cache;

  /**
   * Tracks the number of times a storage client is used. Used to determine when a storage can be
   * closed.
   */
  @VisibleForTesting
  final Map<StorageWrapper, Integer> storageClientToReferenceMap = new ConcurrentHashMap<>();

  /** Reverse map for storage reference to cache keys. */
  final Map<StorageWrapper, StorageProviderCacheKey> storageToCacheKeyMap =
      new ConcurrentHashMap<>();

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  StorageWrapper getStorage(
      Credentials credentials,
      GoogleCloudStorageOptions storageOptions,
      List<ClientInterceptor> interceptors,
      ExecutorService pCUExecutorService,
      Function<List<AccessBoundary>, String> downscopedAccessTokenFn)
      throws IOException {
    if (!canCache(storageOptions, interceptors, pCUExecutorService, downscopedAccessTokenFn)) {
      logger.atFiner().log(
          "Ignoring storage object cache because caching is disabled or custom components are"
              + " used.");
      return new StorageWrapper(
          createStorage(
              credentials,
              storageOptions,
              interceptors,
              pCUExecutorService,
              downscopedAccessTokenFn),
          this);
    }
    StorageProviderCacheKey key =
        computeCacheKey(credentials, storageOptions, downscopedAccessTokenFn);
    if (key == null) {
      // When a stable key cannot be generated for the credentials, do not cache.
      return new StorageWrapper(
          createStorage(credentials, storageOptions, null, null, downscopedAccessTokenFn), this);
    }

    if (cache == null) {
      synchronized (this) {
        if (cache == null) {
          cache =
              CacheBuilder.newBuilder()
                  .maximumSize(storageOptions.getStorageClientCacheMaxSize())
                  .expireAfterWrite(storageOptions.getStorageClientCacheExpiryTime())
                  .removalListener(this::onRemoval)
                  .recordStats()
                  .build();
        }
      }
    }
    logger.atFinest().log("Key: %s", key);
    StorageWrapper storage = cache.getIfPresent(key);
    if (storage == null) {
      synchronized (this) {
        storage = cache.getIfPresent(key);
        if (storage == null) {
          storage =
              new StorageWrapper(
                  createStorage(credentials, storageOptions, null, null, downscopedAccessTokenFn),
                  this);
          cache.put(key, storage);
          storageToCacheKeyMap.put(storage, key);
          logger.atFinest().log(
              "Cache miss for %d, created new storage client. Cache hit count : %d, Cache hit rate"
                  + ": %.2f",
              key.hashCode(), cache.stats().hitCount(), cache.stats().hitRate());
        } else {
          logger.atInfo().log(
              "Cache hit for %d, reusing the storage client. Cache hit count : %d, Cache hit rate"
                  + ": %.2f",
              key.hashCode(), cache.stats().hitCount(), cache.stats().hitRate());
        }
      }
    } else {
      logger.atInfo().log(
          "Cache hit for %d, reusing the storage client. Cache hit count : %d, Cache hit rate:"
              + " %.2f",
          key.hashCode(), cache.stats().hitCount(), cache.stats().hitRate());
    }
    // Increment the reference count of the storage object.
    storageClientToReferenceMap.compute(storage, (k, v) -> v == null ? 1 : v + 1);
    return storage;
  }

  /**
   * Signal the storage object to be closed. The resources held by the storage object will be freed
   * only if the instance is closed by all the objects sharing this storage reference.
   */
  synchronized void close(StorageWrapper storage) {
    if (!storageClientToReferenceMap.containsKey(storage)) {
      closeStorage(storage.getStorage());
      logger.atFinest().log("close() called on storage object outside cache.");
      return;
    }
    // Atomically decrement the reference count. If it drops to zero, the remapping function
    // returns null, which removes the entry from the map.
    Integer newCount =
        storageClientToReferenceMap.computeIfPresent(storage, (k, v) -> (v > 1) ? v - 1 : null);
    if (newCount == null) {
      // The reference count has reached zero. Invalidate the entry from the cache.
      // The removal listener will handle the actual closing of the storage object
      // and cleanup of the other map.
      StorageProviderCacheKey key = storageToCacheKeyMap.get(storage);
      if (key != null && cache != null) {
        logger.atFiner().log(
            "Reference count is zero; invalidating storage client from cache for key: %s", key);
        cache.invalidate(key);
      } else {
        // This case should ideally not be reached if the object was in the reference map.
        // It indicates a potential state inconsistency or the cache was never initialized.
        logger.atWarning().log(
            "Could not invalidate from cache for a storage object whose reference count reached zero."
                + " The object will be closed directly, but this may indicate a bug.");
        storageToCacheKeyMap.remove(storage); // Clean up the other map.
        closeStorage(storage.getStorage());
      }
    }
  }

  @VisibleForTesting
  StorageProviderCacheKey computeCacheKey(
      Credentials credentials,
      GoogleCloudStorageOptions storageOptions,
      Function<List<AccessBoundary>, String> downscopedAccessTokenFn) {
    List<Object> credentialsKey = getCredentialsCacheKey(credentials);
    if (credentialsKey == null) {
      return null;
    }
    return StorageProviderCacheKey.builder()
        .setCredentialsKey(credentialsKey)
        .setHttpHeaders(storageOptions.getHttpRequestHeaders())
        .setIsDirectPathPreferred(storageOptions.isDirectPathPreferred())
        .setIsDownScopingEnabled(downscopedAccessTokenFn != null)
        .setIsTracingEnabled(storageOptions.isTraceLogEnabled())
        .setWriteChannelOptions(storageOptions.getWriteChannelOptions())
        .setProjectId(storageOptions.getProjectId())
        .build();
  }

  /**
   * Creates a stable cache key for a given Credentials object. This is necessary because many
   * Credentials implementations do not have a stable equals/hashCode implementation, which makes
   * them unsuitable for use as cache keys. This method extracts stable identifiers (like client
   * email) where possible.
   *
   * @return A list of objects forming the cache key, or {@code null} if a stable key cannot be
   *     created.
   */
  private static List<Object> getCredentialsCacheKey(Credentials credentials) {

    if (credentials == null) {
      return ImmutableList.of("null_credentials");
    }
    List<Object> keyParts = new ArrayList<>();
    Credentials current = credentials;
    while (current != null) {
      if (current instanceof com.google.auth.oauth2.ServiceAccountCredentials) {
        keyParts.add(((com.google.auth.oauth2.ServiceAccountCredentials) current).getClientEmail());
        break;
      }
      if (current instanceof com.google.auth.oauth2.UserCredentials) {
        keyParts.add(((com.google.auth.oauth2.UserCredentials) current).getClientId());
        break;
      }
      if (current instanceof com.google.auth.oauth2.ComputeEngineCredentials) {
        keyParts.add(com.google.auth.oauth2.ComputeEngineCredentials.class);
        break;
      }
      if (current instanceof com.google.auth.oauth2.ImpersonatedCredentials) {
        Credentials wrapped =
            ((com.google.auth.oauth2.ImpersonatedCredentials) current).getSourceCredentials();
        current = (wrapped == current) ? null : wrapped;
      } else {
        // This is an unknown credential type.
        // Return null to indicate that a stable key cannot be created, and this
        // storage client should not be cached.
        logger.atWarning().log(
            "Unable to generate a stable cache key for credentials of type %s."
                + " A new storage client will be created and not cached.",
            current.getClass().getName());
        return null;
      }
    }
    return keyParts;
  }

  /** Determines if the storage instance can be served from the cache. */
  private static boolean canCache(
      GoogleCloudStorageOptions options,
      List<ClientInterceptor> interceptors,
      ExecutorService pCUExecutorService,
      Function<List<AccessBoundary>, String> downscopedAccessTokenFn) {
    return options.isStorageClientCachingEnabled()
        // These values are currently passed while creating the storage client, but they are
        // currently not configurable by the FS options so skipping caching.
        && pCUExecutorService == null
        && (interceptors == null || interceptors.isEmpty())
        // Downscoped access token function makes the client's configuration unique and
        // non-shareable.
        && downscopedAccessTokenFn == null;
  }

  /**
   * A listener that is called by the cache whenever an item is removed. This is the centralized
   * location for cleaning up all resources associated with a cached Storage client.
   */
  private void onRemoval(
      RemovalNotification<StorageProviderCacheKey, StorageWrapper> notification) {
    StorageWrapper storageWrapper = notification.getValue();
    // The value can be null if it was garbage-collected (with weak/soft values).
    if (storageWrapper == null) {
      return;
    }
    logger.atFiner().log(
        "Removing storage client for key %s from cache. Cause: %s.",
        notification.getKey(), notification.getCause());
    storageToCacheKeyMap.remove(storageWrapper);
    storageClientToReferenceMap.remove(storageWrapper);
    closeStorage(storageWrapper.getStorage());
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
    List<ClientInterceptor> list = new ArrayList<>();
    if (interceptors != null && !interceptors.isEmpty()) {
      list.addAll(interceptors.stream().filter(Objects::nonNull).collect(Collectors.toList()));
    }
    if (storageOptions.isTraceLogEnabled()) {
      list.add(new GoogleCloudStorageClientGrpcTracingInterceptor());
    }

    if (downscopedAccessTokenFn != null) {
      // When downscoping is enabled, we need to set the downscoped token for each
      // request. In the case of gRPC, the downscoped token will be set from the
      // Interceptor.
      list.add(new GoogleCloudStorageClientGrpcDownscopingInterceptor(downscopedAccessTokenFn));
    }

    list.add(new GoogleCloudStorageClientGrpcStatisticsInterceptor());
    return ImmutableList.copyOf(list);
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
