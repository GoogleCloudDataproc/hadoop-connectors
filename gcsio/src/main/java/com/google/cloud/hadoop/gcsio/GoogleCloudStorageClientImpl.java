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
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl.decodeMetadata;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl.validateCopyArguments;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static java.lang.Math.toIntExact;

import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.gax.paging.Page;
import com.google.auth.Credentials;
import com.google.auto.value.AutoBuilder;
import com.google.cloud.NoCredentials;
import com.google.cloud.hadoop.util.AccessBoundary;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions.PartFileCleanupType;
import com.google.cloud.hadoop.util.ErrorTypeExtractor;
import com.google.cloud.hadoop.util.ErrorTypeExtractor.ErrorType;
import com.google.cloud.hadoop.util.GcsClientStatisticInterface;
import com.google.cloud.hadoop.util.GrpcErrorTypeExtractor;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BlobWriteSessionConfig;
import com.google.cloud.storage.BlobWriteSessionConfigs;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.BucketInfo.LifecycleRule.LifecycleAction;
import com.google.cloud.storage.BucketInfo.LifecycleRule.LifecycleCondition;
import com.google.cloud.storage.CopyWriter;
import com.google.cloud.storage.ParallelCompositeUploadBlobWriteSessionConfig.BufferAllocationStrategy;
import com.google.cloud.storage.ParallelCompositeUploadBlobWriteSessionConfig.ExecutorSupplier;
import com.google.cloud.storage.ParallelCompositeUploadBlobWriteSessionConfig.PartCleanupStrategy;
import com.google.cloud.storage.ParallelCompositeUploadBlobWriteSessionConfig.PartNamingStrategy;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobField;
import com.google.cloud.storage.Storage.BlobGetOption;
import com.google.cloud.storage.Storage.BlobSourceOption;
import com.google.cloud.storage.Storage.BlobTargetOption;
import com.google.cloud.storage.Storage.BucketField;
import com.google.cloud.storage.Storage.BucketListOption;
import com.google.cloud.storage.Storage.ComposeRequest;
import com.google.cloud.storage.Storage.CopyRequest;
import com.google.cloud.storage.StorageClass;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.flogger.GoogleLogger;
import com.google.common.io.BaseEncoding;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.grpc.ClientInterceptor;
import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  // Maximum number of times to retry deletes in the case of precondition failures.
  private static final int MAXIMUM_PRECONDITION_FAILURES_IN_DELETE = 4;

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

  private static String encodeMetadataValues(byte[] bytes) {
    return bytes == null ? null : BaseEncoding.base64().encode(bytes);
  }

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
      @Nullable ExecutorService pCUExecutorService,
      @Nullable GcsClientStatisticInterface gcsClientStatisticInterface)
      throws IOException {
    super(
        GoogleCloudStorageImpl.builder()
            .setOptions(options)
            .setCredentials(credentials)
            .setHttpTransport(httpTransport)
            .setHttpRequestInitializer(httpRequestInitializer)
            .setDownscopedAccessTokenFn(downscopedAccessTokenFn)
            .setGcsClientStatisticInterface(gcsClientStatisticInterface)
            .build());

    this.storageOptions = options;
    this.storage =
        clientLibraryStorage == null
            ? createStorage(credentials, options, gRPCInterceptors, pCUExecutorService)
            : clientLibraryStorage;
  }

  @Override
  public WritableByteChannel create(StorageResourceId resourceId, CreateObjectOptions options)
      throws IOException {
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

    GoogleCloudStorageClientWriteChannel channel =
        new GoogleCloudStorageClientWriteChannel(
            storage, storageOptions, resourceIdWithGeneration, options, backgroundTasksThreadPool);
    channel.initialize();
    return channel;
  }

  /**
   * See {@link GoogleCloudStorage#createBucket(String, CreateBucketOptions)} for details about
   * expected behavior.
   */
  @Override
  public void createBucket(String bucketName, CreateBucketOptions options) throws IOException {
    logger.atFiner().log("createBucket(%s)", bucketName);
    checkArgument(!isNullOrEmpty(bucketName), "bucketName must not be null or empty");
    checkNotNull(options, "options must not be null");
    checkNotNull(storageOptions.getProjectId(), "projectId must not be null");

    BucketInfo.Builder bucketInfoBuilder =
        BucketInfo.newBuilder(bucketName).setLocation(options.getLocation());

    if (options.getStorageClass() != null) {
      bucketInfoBuilder.setStorageClass(
          StorageClass.valueOfStrict(options.getStorageClass().toUpperCase()));
    }
    if (options.getTtl() != null) {
      bucketInfoBuilder.setLifecycleRules(
          Collections.singletonList(
              new BucketInfo.LifecycleRule(
                  LifecycleAction.newDeleteAction(),
                  LifecycleCondition.newBuilder()
                      .setAge(toIntExact(options.getTtl().toDays()))
                      .build())));
    }
    try {
      storage.create(bucketInfoBuilder.build());
    } catch (StorageException e) {
      if (errorExtractor.bucketAlreadyExists(e)) {
        throw (FileAlreadyExistsException)
            new FileAlreadyExistsException(String.format("Bucket '%s' already exists.", bucketName))
                .initCause(e);
      }
      throw new IOException(e);
    }
  }

  /**
   * See {@link GoogleCloudStorage#copy(String, List, String, List)} for details about expected
   * behavior.
   */
  @Override
  public void copy(
      String srcBucketName,
      List<String> srcObjectNames,
      String dstBucketName,
      List<String> dstObjectNames)
      throws IOException {
    checkArgument(srcObjectNames != null, "srcObjectNames must not be null");
    checkArgument(dstObjectNames != null, "dstObjectNames must not be null");
    checkArgument(
        srcObjectNames.size() == dstObjectNames.size(),
        "Must supply same number of elements in srcObjects and dstObjects");

    Map<StorageResourceId, StorageResourceId> sourceToDestinationObjectsMap =
        new HashMap<>(srcObjectNames.size());
    for (int i = 0; i < srcObjectNames.size(); i++) {
      sourceToDestinationObjectsMap.put(
          new StorageResourceId(srcBucketName, srcObjectNames.get(i)),
          new StorageResourceId(dstBucketName, dstObjectNames.get(i)));
    }
    copy(sourceToDestinationObjectsMap);
  }

  /**
   * See {@link GoogleCloudStorage#copy(String, List, String, List)} for details about expected
   * behavior.
   */
  @Override
  public void copy(Map<StorageResourceId, StorageResourceId> sourceToDestinationObjectsMap)
      throws IOException {

    validateCopyArguments(sourceToDestinationObjectsMap, this);

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
        copyInternal(
            executor,
            innerExceptions,
            srcObject.getBucketName(),
            srcObject.getObjectName(),
            dstObject.getGenerationId(),
            dstObject.getBucketName(),
            dstObject.getObjectName());
      }
    } finally {
      executor.shutdown();
    }

    if (!innerExceptions.isEmpty()) {
      throw GoogleCloudStorageExceptions.createCompositeException(innerExceptions);
    }
  }

  private void copyInternal(
      BatchExecutor executor,
      ConcurrentHashMap.KeySetView<IOException, Boolean> innerExceptions,
      String srcBucketName,
      String srcObjectName,
      long dstContentGeneration,
      String dstBucketName,
      String dstObjectName) {
    CopyRequest.Builder copyRequestBuilder =
        CopyRequest.newBuilder().setSource(BlobId.of(srcBucketName, srcObjectName));
    if (dstContentGeneration != StorageResourceId.UNKNOWN_GENERATION_ID) {
      copyRequestBuilder.setTarget(
          BlobId.of(dstBucketName, dstObjectName),
          BlobTargetOption.generationMatch(dstContentGeneration));
    } else {
      copyRequestBuilder.setTarget(BlobId.of(dstBucketName, dstObjectName));
    }

    if (storageOptions.getMaxRewriteChunkSize() > 0) {
      copyRequestBuilder.setMegabytesCopiedPerChunk(
          // Convert raw byte size into Mib.
          storageOptions.getMaxRewriteChunkSize() / (1024 * 1024));
    }
    executor.queue(
        () -> {
          try {
            String srcString = StringPaths.fromComponents(srcBucketName, srcObjectName);
            String dstString = StringPaths.fromComponents(dstBucketName, dstObjectName);

            CopyWriter copyWriter = storage.copy(copyRequestBuilder.build());
            while (!copyWriter.isDone()) {
              copyWriter.copyChunk();
              logger.atFinest().log(
                  "Copy (%s to %s) did not complete. Resuming...", srcString, dstString);
            }
            logger.atFiner().log("Successfully copied %s to %s", srcString, dstString);
          } catch (StorageException e) {
            if (errorExtractor.getErrorType(e) == ErrorType.NOT_FOUND) {
              innerExceptions.add(
                  createFileNotFoundException(srcBucketName, srcObjectName, new IOException(e)));
            } else {
              innerExceptions.add(
                  new IOException(
                      String.format(
                          "Error copying '%s'",
                          StringPaths.fromComponents(srcBucketName, srcObjectName)),
                      e));
            }
          }
          return null;
        },
        null);
  }

  /** See {@link GoogleCloudStorage#listBucketNames()} for details about expected behavior. */
  @Override
  public List<String> listBucketNames() throws IOException {
    logger.atFiner().log("listBucketNames()");
    List<Bucket> allBuckets = listBucketsInternal();
    List<String> bucketNames = new ArrayList<>(allBuckets.size());
    for (Bucket bucket : allBuckets) {
      bucketNames.add(bucket.getName());
    }
    return bucketNames;
  }

  /** See {@link GoogleCloudStorage#listBucketInfo()} for details about expected behavior. */
  @Override
  public List<GoogleCloudStorageItemInfo> listBucketInfo() throws IOException {
    logger.atFiner().log("listBucketInfo()");
    List<Bucket> allBuckets = listBucketsInternal();
    List<GoogleCloudStorageItemInfo> bucketInfos = new ArrayList<>(allBuckets.size());
    for (Bucket bucket : allBuckets) {
      bucketInfos.add(createItemInfoForBucket(new StorageResourceId(bucket.getName()), bucket));
    }
    return bucketInfos;
  }

  /**
   * Shared helper for actually dispatching buckets list API calls and accumulating paginated
   * results; these can then be used to either extract just their names, or to parse into full
   * GoogleCloudStorageItemInfos.
   */
  private List<Bucket> listBucketsInternal() throws IOException {
    logger.atFiner().log("listBucketsInternal()");
    checkNotNull(storageOptions.getProjectId(), "projectId must not be null");
    List<Bucket> allBuckets = new ArrayList<>();
    try {
      Page<Bucket> buckets =
          storage.list(
              BucketListOption.pageSize(storageOptions.getMaxListItemsPerCall()),
              BucketListOption.fields(
                  BucketField.LOCATION,
                  BucketField.STORAGE_CLASS,
                  BucketField.TIME_CREATED,
                  BucketField.UPDATED));

      // Loop to fetch all the items.
      for (Bucket bucket : buckets.iterateAll()) {
        allBuckets.add(bucket);
      }
    } catch (StorageException e) {
      throw new IOException(e);
    }
    return allBuckets;
  }

  /** Helper for converting a StorageResourceId + Bucket into a GoogleCloudStorageItemInfo. */
  private static GoogleCloudStorageItemInfo createItemInfoForBucket(
      StorageResourceId resourceId, Bucket bucket) {
    checkArgument(resourceId != null, "resourceId must not be null");
    checkArgument(bucket != null, "bucket must not be null");
    checkArgument(resourceId.isBucket(), "resourceId must be a Bucket. resourceId: %s", resourceId);
    checkArgument(
        resourceId.getBucketName().equals(bucket.getName()),
        "resourceId.getBucketName() must equal bucket.getName(): '%s' vs '%s'",
        resourceId.getBucketName(),
        bucket.getName());

    return GoogleCloudStorageItemInfo.createBucket(
        resourceId,
        bucket.asBucketInfo().getCreateTimeOffsetDateTime().toInstant().toEpochMilli(),
        bucket.asBucketInfo().getUpdateTimeOffsetDateTime().toInstant().toEpochMilli(),
        bucket.getLocation(),
        bucket.getStorageClass() == null ? null : bucket.getStorageClass().name());
  }

  /** See {@link GoogleCloudStorage#deleteObjects(List)} for details about the expected behavior. */
  @Override
  public void deleteObjects(List<StorageResourceId> fullObjectNames) throws IOException {
    logger.atFiner().log("deleteObjects(%s)", fullObjectNames);

    if (fullObjectNames.isEmpty()) {
      return;
    }

    // Validate that all the elements represent StorageObjects.
    for (StorageResourceId fullObjectName : fullObjectNames) {
      checkArgument(
          fullObjectName.isStorageObject(),
          "Expected full StorageObject names only, got: %s",
          fullObjectName);
    }

    // Gather exceptions to wrap in a composite exception at the end.
    ConcurrentHashMap.KeySetView<IOException, Boolean> innerExceptions =
        ConcurrentHashMap.newKeySet();

    BatchExecutor executor = new BatchExecutor(storageOptions.getBatchThreads());

    try {
      for (StorageResourceId object : fullObjectNames) {
        queueSingleObjectDelete(object, innerExceptions, executor, 0);
      }
    } finally {
      executor.shutdown();
    }

    if (!innerExceptions.isEmpty()) {
      throw GoogleCloudStorageExceptions.createCompositeException(innerExceptions);
    }
  }

  private void queueSingleObjectDelete(
      StorageResourceId resourceId,
      KeySetView<IOException, Boolean> innerExceptions,
      BatchExecutor batchExecutor,
      int attempt) {
    String bucketName = resourceId.getBucketName();
    String objectName = resourceId.getObjectName();
    if (resourceId.hasGenerationId()) {
      batchExecutor.queue(
          () ->
              storage.delete(
                  BlobId.of(bucketName, objectName),
                  BlobSourceOption.generationMatch(resourceId.getGenerationId())),
          getObjectDeletionCallback(
              resourceId, innerExceptions, batchExecutor, attempt, resourceId.getGenerationId()));

    } else {
      // We first need to get the current object version to issue a safe delete for only the latest
      // version of the object.
      batchExecutor.queue(
          () ->
              storage.get(
                  BlobId.of(bucketName, objectName), BlobGetOption.fields(BlobField.GENERATION)),
          new FutureCallback<>() {
            @Override
            public void onSuccess(Blob blob) {
              if (blob == null) {
                // Denotes that the item cannot be found.
                // If the item isn't found, treat it the same as if it's not found
                // in the delete case: assume the user wanted the object gone, and now it is.
                logger.atFiner().log("deleteObjects(%s): get not found.", resourceId);
                return;
              }
              long generation = checkNotNull(blob.getGeneration(), "generation can not be null");
              batchExecutor.queue(
                  () ->
                      storage.delete(
                          BlobId.of(bucketName, objectName),
                          BlobSourceOption.generationMatch(generation)),
                  getObjectDeletionCallback(
                      resourceId, innerExceptions, batchExecutor, attempt, generation));
            }

            @Override
            public void onFailure(Throwable throwable) {
              innerExceptions.add(
                  new IOException(
                      String.format("Error deleting %s, stage 1", resourceId), throwable));
            }
          });
    }
  }

  private FutureCallback<Boolean> getObjectDeletionCallback(
      StorageResourceId resourceId,
      ConcurrentHashMap.KeySetView<IOException, Boolean> innerExceptions,
      BatchExecutor batchExecutor,
      int attempt,
      long generation) {
    return new FutureCallback<>() {
      @Override
      public void onSuccess(Boolean result) {
        if (!result) {
          // Ignore item-not-found scenario. We do not have to delete what we cannot find.
          // This situation typically shows up when we make a request to delete something and the
          // server receives the request, but we get a retry-able error before we get a response.
          // During a retry, we no longer find the item because the server had deleted it already.
          logger.atFiner().log("Delete object %s not found.", resourceId);
        } else {
          logger.atFiner().log("Successfully deleted %s at generation %s", resourceId, generation);
        }
      }

      @Override
      public void onFailure(Throwable throwable) {
        if (throwable instanceof Exception
            && errorExtractor.getErrorType((Exception) throwable) == ErrorType.FAILED_PRECONDITION
            && attempt <= MAXIMUM_PRECONDITION_FAILURES_IN_DELETE) {
          logger.atInfo().log(
              "Precondition not met while deleting '%s' at generation %s. Attempt %s."
                  + " Retrying:%s",
              resourceId, generation, attempt, throwable);
          queueSingleObjectDelete(resourceId, innerExceptions, batchExecutor, attempt + 1);
        } else {
          innerExceptions.add(
              new IOException(
                  String.format(
                      "Error deleting '%s', stage 2 with generation %s", resourceId, generation),
                  throwable));
        }
      }
    };
  }

  /** See {@link GoogleCloudStorage#deleteBuckets(List)} for details about expected behavior. */
  @Override
  public void deleteBuckets(List<String> bucketNames) throws IOException {
    logger.atFiner().log("deleteBuckets(%s)", bucketNames);

    // Validate all the inputs first.
    for (String bucketName : bucketNames) {
      checkArgument(!isNullOrEmpty(bucketName), "bucketName must not be null or empty");
    }

    // Gather exceptions to wrap in a composite exception at the end.
    List<IOException> innerExceptions = new ArrayList<>();

    for (String bucketName : bucketNames) {
      try {
        boolean isDeleted = storage.delete(bucketName);
        if (!isDeleted) {
          innerExceptions.add(createFileNotFoundException(bucketName, null, null));
        }
      } catch (StorageException e) {
        innerExceptions.add(
            new IOException(String.format("Error deleting '%s' bucket", bucketName), e));
      }
    }

    if (!innerExceptions.isEmpty()) {
      throw GoogleCloudStorageExceptions.createCompositeException(innerExceptions);
    }
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

  @Override
  public List<GoogleCloudStorageItemInfo> updateItems(List<UpdatableItemInfo> itemInfoList)
      throws IOException {
    logger.atFiner().log("updateItems(%s)", itemInfoList);

    if (itemInfoList.isEmpty()) {
      return new ArrayList<>();
    }

    for (UpdatableItemInfo itemInfo : itemInfoList) {
      checkArgument(
          !itemInfo.getStorageResourceId().isBucket() && !itemInfo.getStorageResourceId().isRoot(),
          "Buckets and GCS Root resources are not supported for updateItems");
    }

    Map<StorageResourceId, GoogleCloudStorageItemInfo> resultItemInfos = new ConcurrentHashMap<>();
    Set<IOException> innerExceptions = newConcurrentHashSet();
    BatchExecutor executor = new BatchExecutor(storageOptions.getBatchThreads());

    try {
      for (UpdatableItemInfo itemInfo : itemInfoList) {
        StorageResourceId resourceId = itemInfo.getStorageResourceId();
        String bucketName = resourceId.getBucketName();
        String blobName = resourceId.getObjectName();

        Map<String, byte[]> originalMetadata = itemInfo.getMetadata();
        Map<String, String> rewrittenMetadata = encodeMetadata(originalMetadata);

        BlobInfo blobUpdate =
            BlobInfo.newBuilder(bucketName, blobName).setMetadata(rewrittenMetadata).build();

        executor.queue(
            () -> storage.update(blobUpdate),
            new FutureCallback<>() {
              @Override
              public void onSuccess(Blob blob) {
                if (blob == null) {
                  // Indicated that the blob was not found.
                  logger.atFiner().log("updateItems: object not found %s", resourceId);
                  resultItemInfos.put(
                      resourceId, GoogleCloudStorageItemInfo.createNotFound(resourceId));
                } else {
                  logger.atFiner().log(
                      "updateItems: Successfully updated object '%s' for resourceId '%s'",
                      blob, resourceId);
                  resultItemInfos.put(resourceId, createItemInfoForBlob(resourceId, blob));
                }
              }

              @Override
              public void onFailure(Throwable throwable) {
                innerExceptions.add(
                    new IOException(
                        String.format("Error updating '%s' object", resourceId), throwable));
              }
            });
      }
    } finally {
      executor.shutdown();
    }

    if (!innerExceptions.isEmpty()) {
      throw GoogleCloudStorageExceptions.createCompositeException(innerExceptions);
    }

    // Assemble the return list in the same order as the input arguments.
    List<GoogleCloudStorageItemInfo> sortedItemInfos = new ArrayList<>();
    for (UpdatableItemInfo itemInfo : itemInfoList) {
      checkState(
          resultItemInfos.containsKey(itemInfo.getStorageResourceId()),
          "Missing resourceId '%s' from map: %s",
          itemInfo.getStorageResourceId(),
          resultItemInfos);
      sortedItemInfos.add(resultItemInfos.get(itemInfo.getStorageResourceId()));
    }

    // We expect the return list to be the same size, even if some entries were "not found".
    checkState(
        sortedItemInfos.size() == itemInfoList.size(),
        "sortedItemInfos.size() (%s) != resourceIds.size() (%s). infos: %s, updateItemInfos: %s",
        sortedItemInfos.size(),
        itemInfoList.size(),
        sortedItemInfos,
        itemInfoList);
    return sortedItemInfos;
  }

  private static Map<String, String> encodeMetadata(Map<String, byte[]> metadata) {
    return Maps.transformValues(metadata, GoogleCloudStorageClientImpl::encodeMetadataValues);
  }

  @Override
  public void compose(
      String bucketName, List<String> sources, String destination, String contentType)
      throws IOException {
    logger.atFiner().log("compose(%s, %s, %s, %s)", bucketName, sources, destination, contentType);
    List<StorageResourceId> sourceIds =
        sources.stream()
            .map(objectName -> new StorageResourceId(bucketName, objectName))
            .collect(Collectors.toList());
    StorageResourceId destinationId = new StorageResourceId(bucketName, destination);
    CreateObjectOptions options =
        CreateObjectOptions.DEFAULT_OVERWRITE.toBuilder()
            .setContentType(contentType)
            .setEnsureEmptyObjectsMetadataMatch(false)
            .build();
    composeObjects(sourceIds, destinationId, options);
  }

  /**
   * See {@link GoogleCloudStorage#composeObjects(List, StorageResourceId, CreateObjectOptions)}}
   * for details about expected behavior.
   */
  @Override
  public GoogleCloudStorageItemInfo composeObjects(
      List<StorageResourceId> sources, StorageResourceId destination, CreateObjectOptions options)
      throws IOException {
    logger.atFiner().log("composeObjects(%s, %s, %s)", sources, destination, options);
    for (StorageResourceId inputId : sources) {
      if (!destination.getBucketName().equals(inputId.getBucketName())) {
        throw new IOException(
            String.format(
                "Bucket doesn't match for source '%s' and destination '%s'!",
                inputId, destination));
      }
    }
    ComposeRequest request =
        ComposeRequest.newBuilder()
            .addSource(
                sources.stream().map(StorageResourceId::getObjectName).collect(Collectors.toList()))
            .setTarget(
                BlobInfo.newBuilder(destination.getBucketName(), destination.getObjectName())
                    .setContentType(options.getContentType())
                    .setContentEncoding(options.getContentEncoding())
                    .setMetadata(encodeMetadata(options.getMetadata()))
                    .build())
            .setTargetOptions(
                BlobTargetOption.generationMatch(
                    destination.hasGenerationId()
                        ? destination.getGenerationId()
                        : getWriteGeneration(destination, true)))
            .build();

    Blob composedBlob;
    try {
      composedBlob = storage.compose(request);
    } catch (StorageException e) {
      throw new IOException(e);
    }
    GoogleCloudStorageItemInfo compositeInfo = createItemInfoForBlob(destination, composedBlob);
    logger.atFiner().log("composeObjects() done, returning: %s", compositeInfo);
    return compositeInfo;
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
    throw new FileAlreadyExistsException(String.format("Object %s already exists.", resourceId));
  }

  private static Storage createStorage(
      Credentials credentials,
      GoogleCloudStorageOptions storageOptions,
      List<ClientInterceptor> interceptors,
      ExecutorService pCUExecutorService)
      throws IOException {
    return StorageOptions.grpc()
        .setAttemptDirectPath(storageOptions.isDirectPathPreferred())
        .setHeaderProvider(() -> storageOptions.getHttpRequestHeaders())
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
              return ImmutableList.copyOf(list);
            })
        .setCredentials(credentials != null ? credentials : NoCredentials.getInstance())
        .setBlobWriteSessionConfig(
            getSessionConfig(storageOptions.getWriteChannelOptions(), pCUExecutorService))
        .setProjectId(storageOptions.getProjectId())
        .build()
        .getService();
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
      return PartNamingStrategy.noPrefix();
    }
    return PartNamingStrategy.prefix(partFilePrefix);
  }

  private static ExecutorSupplier getPCUExecutorSupplier(ExecutorService pCUExecutorService) {
    return pCUExecutorService == null
        ? ExecutorSupplier.cachedPool()
        : ExecutorSupplier.useExecutor(pCUExecutorService);
  }

  /** Helper for converting a StorageResourceId + Blob into a GoogleCloudStorageItemInfo. */
  private static GoogleCloudStorageItemInfo createItemInfoForBlob(
      StorageResourceId resourceId, Blob blob) {
    checkArgument(resourceId != null, "resourceId must not be null");
    checkArgument(blob != null, "object must not be null");
    checkArgument(
        resourceId.isStorageObject(),
        "resourceId must be a StorageObject. resourceId: %s",
        resourceId);
    checkArgument(
        resourceId.getBucketName().equals(blob.getBucket()),
        "resourceId.getBucketName() must equal object.getBucket(): '%s' vs '%s'",
        resourceId.getBucketName(),
        blob.getBucket());
    checkArgument(
        resourceId.getObjectName().equals(blob.getName()),
        "resourceId.getObjectName() must equal object.getName(): '%s' vs '%s'",
        resourceId.getObjectName(),
        blob.getName());

    Map<String, byte[]> decodedMetadata =
        blob.getMetadata() == null ? null : decodeMetadata(blob.getMetadata());

    byte[] md5Hash = null;
    byte[] crc32c = null;

    if (!isNullOrEmpty(blob.getCrc32c())) {
      crc32c = BaseEncoding.base64().decode(blob.getCrc32c());
    }

    if (!isNullOrEmpty(blob.getMd5())) {
      md5Hash = BaseEncoding.base64().decode(blob.getMd5());
    }

    return GoogleCloudStorageItemInfo.createObject(
        resourceId,
        blob.getCreateTimeOffsetDateTime() == null
            ? 0
            : blob.getCreateTimeOffsetDateTime().toInstant().toEpochMilli(),
        blob.getUpdateTimeOffsetDateTime() == null
            ? 0
            : blob.getUpdateTimeOffsetDateTime().toInstant().toEpochMilli(),
        blob.getSize() == null ? 0 : blob.getSize(),
        blob.getContentType(),
        blob.getContentEncoding(),
        decodedMetadata,
        blob.getGeneration() == null ? 0 : blob.getGeneration(),
        blob.getMetageneration() == null ? 0 : blob.getMetageneration(),
        new VerificationAttributes(md5Hash, crc32c));
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

    public abstract Builder setGcsClientStatisticInterface(
        @Nullable GcsClientStatisticInterface gcsClientStatisticInterface);

    @VisibleForTesting
    public abstract Builder setClientLibraryStorage(@Nullable Storage clientLibraryStorage);

    @VisibleForTesting
    public abstract Builder setPCUExecutorService(@Nullable ExecutorService pCUExecutorService);

    public abstract GoogleCloudStorageClientImpl build() throws IOException;
  }
}
