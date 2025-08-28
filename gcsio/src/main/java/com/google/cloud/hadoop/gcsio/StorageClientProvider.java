package com.google.cloud.hadoop.gcsio;

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageClientImpl.*;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.auth.Credentials;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.auth.oauth2.ExternalAccountCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.auth.oauth2.UserCredentials;
import com.google.cloud.hadoop.util.AccessBoundary;
import com.google.cloud.hadoop.util.AccessTokenProvider;
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
    StorageClientProviderCacheKey key =
        computeCacheKey(credentials, storageOptions, downscopedAccessTokenFn);
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
      Credentials credentials,
      GoogleCloudStorageOptions storageOptions,
      Function<List<AccessBoundary>, String> downscopedAccessTokenFn) {
    ImmutableList<Object> credentialsKey = getCredentialsCacheKey(credentials);
    if (credentialsKey.isEmpty()) {
      return null;
    }
    return StorageClientProviderCacheKey.builder()
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
  private static ImmutableList<Object> getCredentialsCacheKey(Credentials credentials) {

    if (credentials == null) {
      return ImmutableList.of("UNAUTHENTICATED");
    }
    if (credentials instanceof ServiceAccountCredentials) {
      ServiceAccountCredentials sac = (ServiceAccountCredentials) credentials;
      return ImmutableList.of("ServiceAccountCredentials", sac.getClientEmail(), sac.getClientId());
    }
    if (credentials instanceof UserCredentials) {
      UserCredentials uc = (UserCredentials) credentials;
      return ImmutableList.of("UserCredentials",
          uc.getClientId(),
          uc.getRefreshToken(),
          uc.getQuotaProjectId());
    }
    if (credentials instanceof ComputeEngineCredentials) {
      ComputeEngineCredentials cec = (ComputeEngineCredentials) credentials;
      return ImmutableList.of(
          "ComputeEngineCredentials",
          cec.getQuotaProjectId());
    }
    if (credentials instanceof ImpersonatedCredentials) {
      ImpersonatedCredentials ic = (ImpersonatedCredentials) credentials;
      // The key must be unique for the combination of the account being impersonated
      // and the underlying credential used for impersonation.
      ImmutableList<Object> sourceKey = getCredentialsCacheKey(ic.getSourceCredentials());
      if (sourceKey.isEmpty()) {
        // If we can't get a stable key for the source, we can't cache this.
        logger.atWarning().log(
            "Could not generate cache key for source credentials of ImpersonatedCredentials."
                + " Caching will be disabled for this client.");
        return ImmutableList.of();
      }
      return ImmutableList.builder()
          .add("ImpersonatedCredentials")
          .add(ic.getAccount())
          .add(ic.getQuotaProjectId())
          .addAll(sourceKey) // Add the entire source key to the list
          .build();
    }
    if (credentials instanceof ExternalAccountCredentials) {
      ExternalAccountCredentials eac = (ExternalAccountCredentials) credentials;
      // Key is based on the unique properties of the Workload Identity Federation config.
      return ImmutableList.of(
          "ExternalAccountCredentials",
          eac.getClass().getName(), //Get type of external source
          eac.getAudience(),
          eac.getSubjectTokenType(),
          eac.getTokenUrl(),
          eac.getQuotaProjectId(),
          ofNullable(eac.getClientId()),
          ofNullable(eac.getWorkforcePoolUserProject()),
          ofNullable(eac.getServiceAccountImpersonationUrl()));
    }

    if (credentials instanceof AccessTokenProvider) {
      AccessTokenProvider provider = (AccessTokenProvider) credentials;
      // Delegation tokens require special handling, as multiple provider instances
      // can represent the same underlying token.
      if (provider.getDelegationTokenIdentifier().isPresent()) {
        // Use the stable delegation token identifier for the key.
        // Wrap the byte[] in a ByteBuffer for a stable equals/hashCode implementation.
        return ImmutableList.of(
            "AccessTokenProvider",
            "DelegationToken",
            java.nio.ByteBuffer.wrap(provider.getDelegationTokenIdentifier().get()));
      }
      // For other custom token providers, use the class name as an identifier.
      return ImmutableList.of("AccessTokenProvider", provider.getClass().getName());
    }
    // For any other unknown credential type, we cannot generate a stable key.
    // Return an empty list to indicate that this storage client should not be cached.
    logger.atWarning().log(
        "Unable to generate a stable cache key for credentials of type %s."
            + " A new storage client will be created and not cached.",
        credentials.getClass().getName());
    return ImmutableList.of();
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
        && (interceptors == null || interceptors.isEmpty());
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
