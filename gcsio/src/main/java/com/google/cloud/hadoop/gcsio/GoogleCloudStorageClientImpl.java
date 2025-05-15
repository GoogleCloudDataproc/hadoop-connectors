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

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageExceptions.createFileNotFoundException;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl.validateMoveArguments;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.common.util.concurrent.FutureCallback;
import com.google.auth.Credentials;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auto.value.AutoBuilder;
import com.google.cloud.hadoop.util.AccessBoundary;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions.PartFileCleanupType;
import com.google.cloud.hadoop.util.ErrorTypeExtractor;
import com.google.cloud.hadoop.util.ErrorTypeExtractor.ErrorType;
import com.google.cloud.hadoop.util.GoogleCloudStorageEventBus;
import com.google.cloud.hadoop.util.GrpcErrorTypeExtractor;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobWriteSessionConfig;
import com.google.cloud.storage.BlobWriteSessionConfigs;
import com.google.cloud.storage.ParallelCompositeUploadBlobWriteSessionConfig.BufferAllocationStrategy;
import com.google.cloud.storage.ParallelCompositeUploadBlobWriteSessionConfig.ExecutorSupplier;
import com.google.cloud.storage.ParallelCompositeUploadBlobWriteSessionConfig.PartCleanupStrategy;
import com.google.cloud.storage.ParallelCompositeUploadBlobWriteSessionConfig.PartNamingStrategy;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobSourceOption;
import com.google.cloud.storage.Storage.BlobTargetOption;
import com.google.cloud.storage.Storage.MoveBlobRequest;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentHashMap.KeySetView;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;
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
  private final Storage storage;

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
      @Nullable HttpTransport httpTransport,
      @Nullable HttpRequestInitializer httpRequestInitializer,
      @Nullable ImmutableList<ClientInterceptor> gRPCInterceptors,
      @Nullable Function<List<AccessBoundary>, String> downscopedAccessTokenFn,
      @Nullable ExecutorService pCUExecutorService)
      throws IOException {
    super(
        GoogleCloudStorageImpl.builder()
            .setOptions(options)
            .setCredentials(credentials)
            .setHttpTransport(httpTransport)
            .setHttpRequestInitializer(httpRequestInitializer)
            .setDownscopedAccessTokenFn(downscopedAccessTokenFn)
            .build());
    this.storageOptions = options;
    this.storage =
        clientLibraryStorage == null
            ? createStorage(
                credentials, options, gRPCInterceptors, downscopedAccessTokenFn, pCUExecutorService)
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

  /**
   * See {@link GoogleCloudStorage#move(Map<StorageResourceId, StorageResourceId>)} for details
   * about expected behavior.
   */
  @Override
  public void move(Map<StorageResourceId, StorageResourceId> sourceToDestinationObjectsMap)
      throws IOException {

    validateMoveArguments(sourceToDestinationObjectsMap);

    if (sourceToDestinationObjectsMap.isEmpty()) {
      return;
    }

    // Gather FileNotFoundExceptions for individual objects,
    // but only throw a single combined exception at the end.
    ConcurrentHashMap.KeySetView<IOException, Boolean> innerExceptions =
        ConcurrentHashMap.newKeySet();

    BatchExecutor executor = new BatchExecutor(storageOptions.getBatchThreads());

    try {
      for (Map.Entry<StorageResourceId, StorageResourceId> entry :
          sourceToDestinationObjectsMap.entrySet()) {
        StorageResourceId srcObject = entry.getKey();
        StorageResourceId dstObject = entry.getValue();
        moveInternal(
            executor,
            innerExceptions,
            srcObject.getBucketName(),
            srcObject.getGenerationId(),
            srcObject.getObjectName(),
            dstObject.getGenerationId(),
            dstObject.getObjectName());
      }
    } finally {
      executor.shutdown();
    }

    if (!innerExceptions.isEmpty()) {
      GoogleCloudStorageEventBus.postOnException();
      throw GoogleCloudStorageExceptions.createCompositeException(innerExceptions);
    }
  }

  private void moveInternal(
      BatchExecutor executor,
      KeySetView<IOException, Boolean> innerExceptions,
      String srcBucketName,
      long srcContentGeneration,
      String srcObjectName,
      long dstContentGeneration,
      String dstObjectName) {
    MoveBlobRequest.Builder moveRequestBuilder =
        createMoveRequestBuilder(
            srcBucketName,
            srcObjectName,
            dstObjectName,
            srcContentGeneration,
            dstContentGeneration);

    executor.queue(
        () -> {
          try {
            String srcString = StringPaths.fromComponents(srcBucketName, srcObjectName);
            String dstString = StringPaths.fromComponents(srcBucketName, dstObjectName);

            Blob movedBlob = storage.moveBlob(moveRequestBuilder.build());
            if (movedBlob != null) {
              logger.atFiner().log("Successfully moved %s to %s", srcString, dstString);
            }
          } catch (StorageException e) {
            GoogleCloudStorageEventBus.postOnException();
            if (errorExtractor.getErrorType(e) == ErrorType.NOT_FOUND) {
              innerExceptions.add(
                  createFileNotFoundException(srcBucketName, srcObjectName, new IOException(e)));
            } else {
              innerExceptions.add(
                  new IOException(
                      String.format(
                          "Error moving '%s'",
                          StringPaths.fromComponents(srcBucketName, srcObjectName)),
                      e));
            }
          }
          return null;
        },
        new FutureCallback<Blob>() {
          @Override
          public void onSuccess(@Nullable Blob result) {
            logger.atFiner().log("Move operation succeeded via callback for %s.", StringPaths.fromComponents(srcBucketName, srcObjectName));
          }

          @Override
          public void onFailure(Throwable t) {
            logger.atWarning().withCause(t).log("Move operation failed via callback for %s.", StringPaths.fromComponents(srcBucketName, srcObjectName));
          }
        });
  }

  /** Creates a builder for a blob move request. */
  private MoveBlobRequest.Builder createMoveRequestBuilder(
      String srcBucketName,
      String srcObjectName,
      String dstObjectName,
      long srcContentGeneration,
      long dstContentGeneration) {

    MoveBlobRequest.Builder moveRequestBuilder =
        MoveBlobRequest.newBuilder().setSource(BlobId.of(srcBucketName, srcObjectName));
    moveRequestBuilder.setTarget(BlobId.of(srcBucketName, dstObjectName));

    List<BlobTargetOption> blobTargetOptions = new ArrayList<>();
    List<BlobSourceOption> blobSourceOptions = new ArrayList<>();

    if (srcContentGeneration != StorageResourceId.UNKNOWN_GENERATION_ID) {
      blobSourceOptions.add(BlobSourceOption.generationMatch(srcContentGeneration));
    }

    if (dstContentGeneration != StorageResourceId.UNKNOWN_GENERATION_ID) {
      blobTargetOptions.add(BlobTargetOption.generationMatch(dstContentGeneration));
    }

    if (storageOptions.getEncryptionKey() != null) {
      blobSourceOptions.add(
          BlobSourceOption.decryptionKey(storageOptions.getEncryptionKey().value()));
      blobTargetOptions.add(
          BlobTargetOption.encryptionKey(storageOptions.getEncryptionKey().value()));
    }

    moveRequestBuilder.setSourceOptions(blobSourceOptions);
    moveRequestBuilder.setTargetOptions(blobTargetOptions);

    return moveRequestBuilder;
  }

  @Override
  public SeekableByteChannel open(
      StorageResourceId resourceId, GoogleCloudStorageReadOptions readOptions) throws IOException {
    logger.atFiner().log("open(%s, %s)", resourceId, readOptions);
    return open(resourceId, /* itemInfo= */ null, readOptions);
  }

  @Override
  public SeekableByteChannel open(
      GoogleCloudStorageItemInfo itemInfo, GoogleCloudStorageReadOptions readOptions)
      throws IOException {
    logger.atFiner().log("open(%s, %s)", itemInfo, readOptions);
    checkNotNull(itemInfo, "itemInfo should not be null");

    StorageResourceId resourceId = itemInfo.getResourceId();
    checkArgument(
        resourceId.isStorageObject(), "Expected full StorageObject id, got %s", resourceId);

    return open(resourceId, itemInfo, readOptions);
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
        storage.close();
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

  private static Storage createStorage(
      Credentials credentials,
      GoogleCloudStorageOptions storageOptions,
      List<ClientInterceptor> interceptors,
      Function<List<AccessBoundary>, String> downscopedAccessTokenFn,
      ExecutorService pCUExecutorService)
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

  private static ImmutableMap<String, String> getUpdatedHeadersWithUserAgent(
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

  private static BlobWriteSessionConfig getSessionConfig(
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

    public abstract Builder setHttpTransport(@Nullable HttpTransport httpTransport);

    public abstract Builder setCredentials(@Nullable Credentials credentials);

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
