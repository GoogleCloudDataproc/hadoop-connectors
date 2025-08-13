package com.google.cloud.hadoop.gcsio;

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageClientImpl.*;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.auth.Credentials;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.hadoop.util.AccessBoundary;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.annotations.VisibleForTesting;
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
public class StorageClientProvider {

  @VisibleForTesting volatile Cache<StorageClientProviderCacheKey, StorageWrapper> cache;

  /**
   * Tracks the number of times a storage client is used. Used to determine when a storage can be
   * closed.
   */
  @VisibleForTesting
  final Map<StorageWrapper, Integer> storageClientToReferenceMap = new ConcurrentHashMap<>();

  /** Reverse map for storage reference to cache keys. */
  final Map<StorageWrapper, StorageClientProviderCacheKey> storageToCacheKeyMap =
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
    StorageClientProviderCacheKey key = computeCacheKey(credentials, storageOptions);
    if (key == null) {
      // When a stable key cannot be generated for the credentials, do not cache.
      return new StorageWrapper(
          createStorage(
              credentials,
              storageOptions,
              interceptors,
              pCUExecutorService,
              downscopedAccessTokenFn),
          this);
    }

    if (cache == null) {
      synchronized (this) {
        if (cache == null) {
          cache =
              Caffeine.newBuilder()
                  .maximumSize(storageOptions.getStorageClientCacheMaxSize())
                  .expireAfterWrite(storageOptions.getStorageClientCacheExpiryTime())
                  .removalListener(this::onRemoval)
                  .recordStats()
                  .build();
        }
      }
    }
    logger.atFinest().log("Key: %s", key);
    StorageWrapper storage;
    try {
      synchronized (this) {
        storage =
            cache.get(
                key,
                k -> {
                  try {
                    StorageWrapper newStorage =
                        new StorageWrapper(
                            createStorage(
                                credentials,
                                storageOptions,
                                interceptors,
                                pCUExecutorService,
                                downscopedAccessTokenFn),
                            this);
                    storageToCacheKeyMap.put(newStorage, k);
                    logger.atFinest().log(
                        "Cache miss. Created new storage client. Cache hit count : %d, Cache hit rate"
                            + ": %.2f",
                        cache.stats().hitCount(), cache.stats().hitRate());
                    return newStorage;
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                });
        storageClientToReferenceMap.compute(storage, (k, v) -> v == null ? 1 : v + 1);
      }
    } catch (RuntimeException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      }
      throw e;
    }
    logger.atInfo().log(
        "Using storage client. Reference count: %d. Cache stats: %s",
        storageClientToReferenceMap.get(storage), cache.stats());
    return storage;
  }

  /**
   * Signal the storage object to be closed. The resources held by the storage object will be freed
   * only if the instance is closed by all the objects sharing this storage reference.
   */
  synchronized void close(StorageWrapper storage) {
    if (!storageClientToReferenceMap.containsKey(storage)) {
      closeStorage(storage.getStorage());
      logger.atFinest().log("close() called on storage object outside reference tracking.");
      return;
    }
    // Atomically decrement the reference count. If it drops to zero, the remapping function
    // returns null, which removes the entry from the map.
    Integer newCount =
        storageClientToReferenceMap.computeIfPresent(storage, (k, v) -> (v > 1) ? v - 1 : null);
    if (newCount != null) {
      return; // Still in use.
    }

    // If newCount is null, meaning the reference count dropped to zero and the entry
    // was removed from storageClientToReferenceMap, clean up everything else.
    logger.atFiner().log("Reference count is zero; cleaning up all resources for storage object.");

    StorageClientProviderCacheKey key = storageToCacheKeyMap.get(storage);
    if (key != null && cache != null) {
      cache.invalidate(key);
    }
    closeStorage(storage.getStorage());
  }

  /**
   * Listener for when an item is removed from the cache (e.g., expired, evicted). We must clean up
   * our tracking map to prevent memory leaks.
   */
  private void onRemoval(
      StorageClientProviderCacheKey key, StorageWrapper storageWrapper, RemovalCause cause) {
    // The value can be null if it was garbage-collected.
    if (storageWrapper == null) {
      return;
    }
    logger.atFiner().log("Removing storage client for key %s from cache. Cause: %s.", key, cause);
    // Only remove the entry from the reverse map.
    // The storage object itself will be closed via the close() method
    // when its reference count drops to zero.
    storageToCacheKeyMap.remove(storageWrapper);
  }

  @VisibleForTesting
  StorageClientProviderCacheKey computeCacheKey(
      Credentials credentials, GoogleCloudStorageOptions storageOptions) {
    List<Object> credentialsKey = getCredentialsCacheKey(credentials);
    if (credentialsKey == null) {
      return null;
    }
    return StorageClientProviderCacheKey.builder()
        .setCredentialsKey(credentialsKey)
        .setHttpHeaders(storageOptions.getHttpRequestHeaders())
        .setIsDirectPathPreferred(storageOptions.isDirectPathPreferred())
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

  @VisibleForTesting
  Storage createStorage(
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
