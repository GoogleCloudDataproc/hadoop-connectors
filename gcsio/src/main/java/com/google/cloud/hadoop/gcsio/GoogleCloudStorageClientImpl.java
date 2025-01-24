/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.gcsio;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.auth.Credentials;
import com.google.auto.value.AutoBuilder;
import com.google.cloud.hadoop.util.AccessBoundary;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions.PartFileCleanupType;
import com.google.cloud.hadoop.util.ErrorTypeExtractor;
import com.google.cloud.hadoop.util.GoogleCloudStorageEventBus;
import com.google.cloud.hadoop.util.GrpcErrorTypeExtractor;
import com.google.cloud.storage.BlobWriteSessionConfig;
import com.google.cloud.storage.BlobWriteSessionConfigs;
import com.google.cloud.storage.ParallelCompositeUploadBlobWriteSessionConfig.BufferAllocationStrategy;
import com.google.cloud.storage.ParallelCompositeUploadBlobWriteSessionConfig.ExecutorSupplier;
import com.google.cloud.storage.ParallelCompositeUploadBlobWriteSessionConfig.PartCleanupStrategy;
import com.google.cloud.storage.ParallelCompositeUploadBlobWriteSessionConfig.PartNamingStrategy;
import com.google.cloud.storage.Storage;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.flogger.GoogleLogger;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.grpc.ClientInterceptor;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import javax.annotation.Nullable;

/**
 * Provides read/write access to Google Cloud Storage (GCS), using Java nio channel semantics. This
 * is a basic implementation of the GoogleCloudStorage interface that mostly delegates through to
 * the appropriate API call(s) google-cloud-storage client.
 */
@VisibleForTesting
public class GoogleCloudStorageClientImpl extends ForwardingGoogleCloudStorage {
  private static final String USER_AGENT = "user-agent";
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private final GoogleCloudStorageOptions storageOptions;

  @VisibleForTesting final Storage storage;

  private static final StorageProvider storageProvider = new StorageProvider();

  // Error extractor to map APi exception to meaningful ErrorTypes.
  private static final ErrorTypeExtractor errorExtractor = GrpcErrorTypeExtractor.INSTANCE;

  // Thread-pool used for background tasks.
  private ExecutorService backgroundTasksThreadPool =
      Executors.newCachedThreadPool(
          new ThreadFactoryBuilder()
              .setNameFormat("gcsio-storage-client-write-channel-pool-%d")
              .setDaemon(true)
              .build());
  /**
   * Having an instance of gscImpl to redirect calls to Json client while new client implementation
   * is in WIP.
   */
  GoogleCloudStorageClientImpl(
      GoogleCloudStorageOptions options,
      @Nullable Storage clientLibraryStorage,
      @Nullable Credentials credentials,
      @Nullable Credential credential,
      @Nullable com.google.api.services.storage.Storage apiaryClientStorage,
      @Nullable HttpRequestInitializer httpRequestInitializer,
      @Nullable ImmutableList<ClientInterceptor> gRPCInterceptors,
      @Nullable Function<List<AccessBoundary>, String> downscopedAccessTokenFn,
      @Nullable ExecutorService pCUExecutorService)
      throws IOException {
    super(
        getDelegate(
            httpRequestInitializer,
            apiaryClientStorage,
            options,
            credentials,
            credential,
            downscopedAccessTokenFn));
    this.storageOptions = options;
    this.storage =
        clientLibraryStorage == null
            ? storageProvider.getStorage(
                credentials,
                storageOptions,
                gRPCInterceptors,
                pCUExecutorService,
                downscopedAccessTokenFn)
            : clientLibraryStorage;
  }

  @Override
  public WritableByteChannel create(StorageResourceId resourceId, CreateObjectOptions options)
      throws IOException {
    if (!storageOptions.isGrpcWriteEnabled()) {
      return super.create(resourceId, options);
    }

    logger.atFiner().log("create(%s)", resourceId);
    checkArgument(
        resourceId.isStorageObject(), "Expected full StorageObject id, got %s", resourceId);
    // Update resourceId if generationId is missing
    StorageResourceId resourceIdWithGeneration = resourceId;
    if (!resourceId.hasGenerationId()) {
      resourceIdWithGeneration =
          new StorageResourceId(
              resourceId.getBucketName(),
              resourceId.getObjectName(),
              getWriteGeneration(resourceId, options.isOverwriteExisting()));
    }

    return new GoogleCloudStorageClientWriteChannel(
        storage, storageOptions, resourceIdWithGeneration, options);
  }

  @Override
  public SeekableByteChannel open(
      StorageResourceId resourceId, GoogleCloudStorageReadOptions readOptions) throws IOException {
    logger.atFiner().log("open(%s, %s)", resourceId, readOptions);
    return open(resourceId, /* itemInfo= */ null, readOptions);
  }

  private SeekableByteChannel open(
      StorageResourceId resourceId,
      GoogleCloudStorageItemInfo itemInfo,
      GoogleCloudStorageReadOptions readOptions)
      throws IOException {
    return new GoogleCloudStorageClientReadChannel(
        storage,
        itemInfo == null ? getItemInfo(resourceId) : itemInfo,
        readOptions,
        errorExtractor,
        storageOptions);
  }

  @Override
  public void close() {
    try {
      try {
        storageProvider.close(storage);
      } catch (Exception e) {
        logger.atWarning().withCause(e).log("Error occurred while closing the storage client");
      }
      try {
        super.close();
      } finally {
        backgroundTasksThreadPool.shutdown();
      }
    } finally {
      backgroundTasksThreadPool = null;
    }
  }

  public void renameHnFolder(URI src, URI dst) throws IOException {
    super.renameHnFolder(src, dst);
  }

  /**
   * Gets the object generation for a write operation
   *
   * <p>making getItemInfo call even if overwrite is disabled to fail fast in case file is existing.
   *
   * @param resourceId object for which generation info is requested
   * @param overwrite whether existing object should be overwritten
   * @return the generation of the object
   * @throws IOException if the object already exists and cannot be overwritten
   */
  private long getWriteGeneration(StorageResourceId resourceId, boolean overwrite)
      throws IOException {
    logger.atFiner().log("getWriteGeneration(%s, %s)", resourceId, overwrite);
    GoogleCloudStorageItemInfo info = getItemInfo(resourceId);
    if (!info.exists()) {
      return 0L;
    }
    if (info.exists() && overwrite) {
      long generation = info.getContentGeneration();
      checkState(generation != 0, "Generation should not be 0 for an existing item");
      return generation;
    }
    GoogleCloudStorageEventBus.postOnException();
    throw new FileAlreadyExistsException(String.format("Object %s already exists.", resourceId));
  }

  private static GoogleCloudStorage getDelegate(
      HttpRequestInitializer httpRequestInitializer,
      com.google.api.services.storage.Storage storage,
      GoogleCloudStorageOptions storageOptions,
      Credentials credentials,
      Credential credential,
      Function<List<AccessBoundary>, String> downscopedAccessTokenFn)
      throws IOException {
    if (httpRequestInitializer != null) {
      logger.atWarning().log("Overriding httpRequestInitializer. ALERT: Use this only for testing");
      return new GoogleCloudStorageImpl(
          storageOptions, httpRequestInitializer, downscopedAccessTokenFn);
    } else if (storage != null) {
      logger.atWarning().log("Overriding storage. ALERT: Use this only for testing");
      return new GoogleCloudStorageImpl(
          storageOptions, storage, credentials, downscopedAccessTokenFn);
    }
    return new GoogleCloudStorageImpl(storageOptions, credential, downscopedAccessTokenFn);
  }

  static ImmutableMap<String, String> getUpdatedHeadersWithUserAgent(
      GoogleCloudStorageOptions storageOptions) {
    ImmutableMap<String, String> httpRequestHeaders =
        MoreObjects.firstNonNull(storageOptions.getHttpRequestHeaders(), ImmutableMap.of());
    String appName = storageOptions.getAppName();
    if (!httpRequestHeaders.containsKey(USER_AGENT) && !Strings.isNullOrEmpty(appName)) {
      logger.atFiner().log("Setting useragent %s", appName);
      return ImmutableMap.<String, String>builder()
          .putAll(httpRequestHeaders)
          .put(USER_AGENT, appName)
          .build();
    }

    return httpRequestHeaders;
  }

  static BlobWriteSessionConfig getSessionConfig(
      AsyncWriteChannelOptions writeOptions, ExecutorService pCUExecutorService)
      throws IOException {
    logger.atFiner().log("Upload strategy in use: %s", writeOptions.getUploadType());
    switch (writeOptions.getUploadType()) {
      case CHUNK_UPLOAD:
        return BlobWriteSessionConfigs.getDefault()
            .withChunkSize(writeOptions.getUploadChunkSize());
      case WRITE_TO_DISK_THEN_UPLOAD:
        if (writeOptions.getTemporaryPaths() == null
            || writeOptions.getTemporaryPaths().isEmpty()) {
          return BlobWriteSessionConfigs.bufferToTempDirThenUpload();
        }
        return BlobWriteSessionConfigs.bufferToDiskThenUpload(
            writeOptions.getTemporaryPaths().stream()
                .map(x -> Paths.get(x))
                .collect(ImmutableSet.toImmutableSet()));
      case JOURNALING:
        if (writeOptions.getTemporaryPaths() == null
            || writeOptions.getTemporaryPaths().isEmpty()) {
          GoogleCloudStorageEventBus.postOnException();
          throw new IllegalArgumentException(
              "Upload using `Journaling` requires the property:fs.gs.write.temporary.dirs to be set.");
        }
        return BlobWriteSessionConfigs.journaling(
            writeOptions.getTemporaryPaths().stream()
                .map(x -> Paths.get(x))
                .collect(ImmutableSet.toImmutableSet()));
      case PARALLEL_COMPOSITE_UPLOAD:
        return BlobWriteSessionConfigs.parallelCompositeUpload()
            .withBufferAllocationStrategy(
                BufferAllocationStrategy.fixedPool(
                    writeOptions.getPCUBufferCount(), writeOptions.getPCUBufferCapacity()))
            .withPartCleanupStrategy(getPartCleanupStrategy(writeOptions.getPartFileCleanupType()))
            .withExecutorSupplier(getPCUExecutorSupplier(pCUExecutorService))
            .withPartNamingStrategy(getPartNamingStrategy(writeOptions.getPartFileNamePrefix()));
      default:
        GoogleCloudStorageEventBus.postOnException();
        throw new IllegalArgumentException(
            String.format("Upload type:%s is not supported.", writeOptions.getUploadType()));
    }
  }

  private static PartCleanupStrategy getPartCleanupStrategy(PartFileCleanupType cleanupType) {
    switch (cleanupType) {
      case NEVER:
        return PartCleanupStrategy.never();
      case ON_SUCCESS:
        return PartCleanupStrategy.onlyOnSuccess();
      case ALWAYS:
        return PartCleanupStrategy.always();
      default:
        GoogleCloudStorageEventBus.postOnException();
        throw new IllegalArgumentException(
            String.format("Cleanup type:%s is not handled.", cleanupType));
    }
  }

  private static PartNamingStrategy getPartNamingStrategy(String partFilePrefix) {
    if (Strings.isNullOrEmpty(partFilePrefix)) {
      return PartNamingStrategy.useObjectNameAsPrefix();
    }
    return PartNamingStrategy.prefix(partFilePrefix);
  }

  private static ExecutorSupplier getPCUExecutorSupplier(ExecutorService pCUExecutorService) {
    return pCUExecutorService == null
        ? ExecutorSupplier.cachedPool()
        : ExecutorSupplier.useExecutor(pCUExecutorService);
  }

  public static Builder builder() {
    return new AutoBuilder_GoogleCloudStorageClientImpl_Builder();
  }

  @AutoBuilder(ofClass = GoogleCloudStorageClientImpl.class)
  public abstract static class Builder {

    public abstract Builder setOptions(GoogleCloudStorageOptions options);

    public abstract Builder setCredentials(@Nullable Credentials credentials);

    public abstract Builder setCredential(@Nullable Credential credential);

    public abstract Builder setApiaryClientStorage(
        @Nullable com.google.api.services.storage.Storage apiaryClientStorage);

    @VisibleForTesting
    public abstract Builder setHttpRequestInitializer(
        @Nullable HttpRequestInitializer httpRequestInitializer);

    public abstract Builder setDownscopedAccessTokenFn(
        @Nullable Function<List<AccessBoundary>, String> downscopedAccessTokenFn);

    public abstract Builder setGRPCInterceptors(
        @Nullable ImmutableList<ClientInterceptor> gRPCInterceptors);

    @VisibleForTesting
    public abstract Builder setClientLibraryStorage(@Nullable Storage clientLibraryStorage);

    @VisibleForTesting
    public abstract Builder setPCUExecutorService(@Nullable ExecutorService pCUExecutorService);

    public abstract GoogleCloudStorageClientImpl build() throws IOException;
  }
}
