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
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl.EMPTY_OBJECT_CREATE_OPTIONS;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl.decodeMetadata;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl.sleeper;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl.validateCopyArguments;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl.validateMoveArguments;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static java.lang.Math.toIntExact;

import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.gax.paging.Page;
import com.google.auth.Credentials;
import com.google.auto.value.AutoBuilder;
import com.google.cloud.hadoop.util.AccessBoundary;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions.PartFileCleanupType;
import com.google.cloud.hadoop.util.ErrorTypeExtractor;
import com.google.cloud.hadoop.util.ErrorTypeExtractor.ErrorType;
import com.google.cloud.hadoop.util.GoogleCloudStorageEventBus;
import com.google.cloud.hadoop.util.GrpcErrorTypeExtractor;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobAppendableUpload;
import com.google.cloud.storage.BlobAppendableUpload.AppendableUploadWriteableByteChannel;
import com.google.cloud.storage.BlobAppendableUploadConfig;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BlobWriteSessionConfig;
import com.google.cloud.storage.BlobWriteSessionConfigs;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.BucketInfo.HierarchicalNamespace;
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
import com.google.cloud.storage.Storage.MoveBlobRequest;
import com.google.cloud.storage.StorageClass;
import com.google.cloud.storage.StorageException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentHashMap.KeySetView;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
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
  private static final String RAPID = "RAPID";
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  // Maximum number of times to retry deletes in the case of precondition
  // failures.
  private static final int MAXIMUM_PRECONDITION_FAILURES_IN_DELETE = 4;

  private final GoogleCloudStorageOptions storageOptions;
  @VisibleForTesting final StorageClientWrapper storageWrapper;

  private static final StorageClientProvider storageClientProvider = new StorageClientProvider();

  // Error extractor to map APi exception to meaningful ErrorTypes.
  private static final ErrorTypeExtractor errorExtractor = GrpcErrorTypeExtractor.INSTANCE;

  // Blob field that are used in GoogleCloudStorageItemInfo.
  static final List<BlobField> BLOB_FIELDS =
      ImmutableList.of(
          BlobField.BUCKET,
          BlobField.CONTENT_ENCODING,
          BlobField.CONTENT_TYPE,
          BlobField.CRC32C,
          BlobField.GENERATION,
          BlobField.METADATA,
          BlobField.MD5HASH,
          BlobField.METAGENERATION,
          BlobField.NAME,
          BlobField.SIZE,
          BlobField.TIME_CREATED,
          BlobField.UPDATED);

  // Thread-pool used for background tasks.
  private ExecutorService backgroundTasksThreadPool =
      Executors.newCachedThreadPool(
          new ThreadFactoryBuilder()
              .setNameFormat("gcsio-storage-client-write-channel-pool-%d")
              .setDaemon(true)
              .build());

  private ExecutorService boundedThreadPool;

  private final BlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<>();

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
      @Nullable FeatureHeaderGenerator featureHeaderGenerator)
      throws IOException {
    super(
        GoogleCloudStorageImpl.builder()
            .setOptions(options)
            .setCredentials(credentials)
            .setHttpTransport(httpTransport)
            .setHttpRequestInitializer(httpRequestInitializer)
            .setDownscopedAccessTokenFn(downscopedAccessTokenFn)
            .setFeatureHeaderGenerator(featureHeaderGenerator)
            .build());

    this.storageOptions = options;
    this.storageWrapper =
        clientLibraryStorage == null
            ? storageClientProvider.getStorage(
                credentials,
                storageOptions,
                gRPCInterceptors,
                pCUExecutorService,
                downscopedAccessTokenFn,
                featureHeaderGenerator)
            : new StorageClientWrapper(clientLibraryStorage, storageClientProvider);
    this.boundedThreadPool = null;
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

    if (storageOptions.isBidiEnabled()) {
      return new GoogleCloudStorageBidiWriteChannel(
          storageWrapper.getStorage(), storageOptions, resourceIdWithGeneration, options);
    } else {
      return new GoogleCloudStorageClientWriteChannel(
          storageWrapper.getStorage(), storageOptions, resourceIdWithGeneration, options);
    }
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

    if (options.getZonalPlacement() != null) {
      if (!options.getHierarchicalNamespaceEnabled()) {
        throw new UnsupportedOperationException("Zonal buckets must have HNS Enabled.");
      }
      // Note: Currently StorageClass does not have RAPID as an enum value. Since at the time of
      // writing zonal buckets only support RAPID storage class we default the storage class to
      // RAPID whenever a zonal placement is set and the storageClass is unset or null.
      // Having storage class set as any other value will lead to an Exception thrown.
      // Added Check for storage class equal to RAPID set by the user for when StorageClass enum
      // adds RAPID as a value to make sure this does not start breaking.
      if (options.getStorageClass() != null && !RAPID.equalsIgnoreCase(options.getStorageClass())) {
        throw new UnsupportedOperationException("Zonal bucket storage class must be RAPID");
      }
      // Lifecycle Configs are currently not supported by zonal buckets
      if (options.getTtl() != null) {
        throw new UnsupportedOperationException("Zonal buckets do not support TTL");
      }

      bucketInfoBuilder
          .setCustomPlacementConfig(
              BucketInfo.CustomPlacementConfig.newBuilder()
                  .setDataLocations(ImmutableList.of(options.getZonalPlacement()))
                  .build())
          .setStorageClass(StorageClass.valueOf("RAPID"));

      // A zonal bucket must be an HNS Bucket
      enableHns(bucketInfoBuilder);
    } else {
      if (options.getStorageClass() != null) {
        bucketInfoBuilder.setStorageClass(
            StorageClass.valueOfStrict(options.getStorageClass().toUpperCase()));
      }
      if (options.getHierarchicalNamespaceEnabled()) {
        enableHns(bucketInfoBuilder);
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
    }

    try {
      storageWrapper.create(bucketInfoBuilder.build());
    } catch (StorageException e) {
      GoogleCloudStorageEventBus.postOnException();
      if (errorExtractor.bucketAlreadyExists(e)) {
        throw (FileAlreadyExistsException)
            new FileAlreadyExistsException(String.format("Bucket '%s' already exists.", bucketName))
                .initCause(e);
      }
      throw new IOException(e);
    }
  }

  private void enableHns(BucketInfo.Builder bucketInfoBuilder) {
    bucketInfoBuilder
        .setIamConfiguration(
            BucketInfo.IamConfiguration.newBuilder()
                .setIsUniformBucketLevelAccessEnabled(true)
                .build())
        .setHierarchicalNamespace(HierarchicalNamespace.newBuilder().setEnabled(true).build());
  }

  /**
   * See {@link GoogleCloudStorage#createEmptyObject(StorageResourceId)} for details about expected
   * behavior.
   */
  @Override
  public void createEmptyObject(StorageResourceId resourceId) throws IOException {
    logger.atFiner().log("createEmptyObject(%s)", resourceId);
    checkArgument(
        resourceId.isStorageObject(), "Expected full StorageObject id, got %s", resourceId);
    createEmptyObject(resourceId, EMPTY_OBJECT_CREATE_OPTIONS);
  }

  /**
   * See {@link GoogleCloudStorage#createEmptyObject(StorageResourceId, CreateObjectOptions)} for
   * details about expected behavior.
   */
  @Override
  public void createEmptyObject(StorageResourceId resourceId, CreateObjectOptions options)
      throws IOException {
    checkArgument(
        resourceId.isStorageObject(), "Expected full StorageObject id, got %s", resourceId);

    try {
      createEmptyObjectInternal(resourceId, options);
    } catch (StorageException e) {
      if (canIgnoreExceptionForEmptyObject(e, resourceId, options)) {
        logger.atInfo().log(
            "Ignoring exception of type %s; verified object already exists with desired state.",
            e.getClass().getSimpleName());
        logger.atFine().withCause(e).log("Ignored exception while creating empty object");
      } else {
        if (errorExtractor.getErrorType(e) == ErrorType.ALREADY_EXISTS) {
          GoogleCloudStorageEventBus.postOnException();
          throw (FileAlreadyExistsException)
              new FileAlreadyExistsException(
                      String.format("Object '%s' already exists.", resourceId))
                  .initCause(e);
        }
        throw new IOException(e);
      }
    }
  }

  /**
   * See {@link GoogleCloudStorage#createEmptyObjects(List)} for details about expected behavior.
   */
  @Override
  public void createEmptyObjects(List<StorageResourceId> resourceIds) throws IOException {
    createEmptyObjects(resourceIds, EMPTY_OBJECT_CREATE_OPTIONS);
  }

  /**
   * See {@link GoogleCloudStorage#createEmptyObjects(List, CreateObjectOptions)} for details about
   * expected behavior.
   */
  @Override
  public void createEmptyObjects(List<StorageResourceId> resourceIds, CreateObjectOptions options)
      throws IOException {
    logger.atFiner().log("createEmptyObjects(%s)", resourceIds);
    if (resourceIds.isEmpty()) {
      return;
    }

    // Don't go through batch interface for a single-item case to avoid batching
    // overhead.
    if (resourceIds.size() == 1) {
      createEmptyObject(Iterables.getOnlyElement(resourceIds), options);
      return;
    }

    // Validate that all the elements represent StorageObjects.
    for (StorageResourceId resourceId : resourceIds) {
      checkArgument(
          resourceId.isStorageObject(),
          "Expected full StorageObject names only, got: '%s'",
          resourceId);
    }

    // Gather exceptions to wrap in a composite exception at the end.
    Set<IOException> innerExceptions = newConcurrentHashSet();
    BatchExecutor executor = new BatchExecutor(storageOptions.getBatchThreads());

    try {
      for (StorageResourceId resourceId : resourceIds) {
        executor.queue(
            () -> {
              try {
                createEmptyObjectInternal(resourceId, options);
                logger.atFiner().log("Successfully inserted %s", resourceId);
              } catch (StorageException se) {
                boolean canIgnoreException = false;
                try {
                  canIgnoreException = canIgnoreExceptionForEmptyObject(se, resourceId, options);
                } catch (Exception e) {
                  // Make sure to catch Exception instead of only StorageException so that we can
                  // correctly wrap other such exceptions and propagate them out cleanly inside
                  // innerExceptions.
                  innerExceptions.add(
                      new IOException(
                          "Error re-fetching after rate-limit error: " + resourceId, e));
                }
                if (canIgnoreException) {
                  logger.atInfo().log(
                      "Ignoring exception of type %s; verified object already exists with desired"
                          + " state.",
                      se.getClass().getSimpleName());
                  logger.atFine().withCause(se).log(
                      "Ignored exception while creating empty object");
                } else {
                  innerExceptions.add(new IOException("Error inserting " + resourceId, se));
                }
              } catch (Exception e) {
                innerExceptions.add(new IOException("Error inserting " + resourceId, e));
              }
              return null;
            },
            null);
      }
    } finally {
      executor.shutdown();
    }

    if (!innerExceptions.isEmpty()) {
      GoogleCloudStorageEventBus.postOnException();
      throw GoogleCloudStorageExceptions.createCompositeException(innerExceptions);
    }
  }

  private void createEmptyObjectInternal(
      StorageResourceId resourceId, CreateObjectOptions createObjectOptions) throws IOException {
    Map<String, String> rewrittenMetadata = encodeMetadata(createObjectOptions.getMetadata());

    List<BlobTargetOption> blobTargetOptions = new ArrayList<>();
    blobTargetOptions.add(BlobTargetOption.disableGzipContent());
    if (resourceId.hasGenerationId()) {
      blobTargetOptions.add(BlobTargetOption.generationMatch(resourceId.getGenerationId()));
    } else if (resourceId.isDirectory() || !createObjectOptions.isOverwriteExisting()) {
      blobTargetOptions.add(BlobTargetOption.doesNotExist());
    }

    if (storageOptions.getEncryptionKey() != null) {
      blobTargetOptions.add(
          BlobTargetOption.encryptionKey(storageOptions.getEncryptionKey().value()));
    }

    Bucket bucket = getBucket(resourceId.getBucketName());
    boolean isRapid =
        bucket != null
            && bucket.getStorageClass() != null
            && "RAPID".equalsIgnoreCase(bucket.getStorageClass().toString());
    if (isRapid) {
      createAppendableEmptyObject(resourceId, createObjectOptions, rewrittenMetadata);
    } else {
      storageWrapper.create(
          BlobInfo.newBuilder(BlobId.of(resourceId.getBucketName(), resourceId.getObjectName()))
              .setMetadata(rewrittenMetadata)
              .setContentEncoding(createObjectOptions.getContentEncoding())
              .setContentType(createObjectOptions.getContentType())
              .build(),
          blobTargetOptions.toArray(BlobTargetOption[]::new));
    }
  }

  private void createAppendableEmptyObject(
      StorageResourceId resourceId,
      CreateObjectOptions createObjectOptions,
      Map<String, String> rewrittenMetadata)
      throws IOException {
    try {
      BlobAppendableUpload upload =
          storageWrapper.blobAppendableUpload(
              BlobInfo.newBuilder(BlobId.of(resourceId.getBucketName(), resourceId.getObjectName()))
                  .setMetadata(rewrittenMetadata)
                  .setContentEncoding(createObjectOptions.getContentEncoding())
                  .setContentType(createObjectOptions.getContentType())
                  .build(),
              BlobAppendableUploadConfig.of(),
              Storage.BlobWriteOption.doesNotExist());
      try (AppendableUploadWriteableByteChannel channel = upload.open(); ) {
        channel.write(java.nio.ByteBuffer.wrap(new byte[0]));
        channel.finalizeAndClose();
      }
    } catch (IOException e) {
      // Check if the cause is a StorageException.
      if (e.getCause() instanceof StorageException) {
        StorageException storageException = (StorageException) e.getCause();
        throwIfFileAlreadyExistsError(storageException, resourceId);
        throw storageException;
      } else {
        throw e;
      }
    }
  }

  private void throwIfFileAlreadyExistsError(
      StorageException storageException, StorageResourceId resourceId)
      throws FileAlreadyExistsException {
    if (errorExtractor.getErrorType(storageException) == ErrorType.FAILED_PRECONDITION) {
      // This is the expected error when the object already exists. Translate it to the
      // exception that the calling method expects, similar to storage.create() exceptions.
      throw new FileAlreadyExistsException(String.format("Object %s already exists.", resourceId));
    }
  }

  /**
   * Helper to check whether an empty object already exists with the expected metadata specified in
   * {@code options}, to be used to determine whether it's safe to ignore an exception that was
   * thrown when trying to create the object, {@code exceptionOnCreate}.
   */
  private boolean canIgnoreExceptionForEmptyObject(
      StorageException exceptionOnCreate, StorageResourceId resourceId, CreateObjectOptions options)
      throws IOException {
    ErrorType errorType = errorExtractor.getErrorType(exceptionOnCreate);
    if (errorType == ErrorType.RESOURCE_EXHAUSTED
        || errorType == ErrorType.INTERNAL
        || (resourceId.isDirectory() && errorType == ErrorType.FAILED_PRECONDITION)) {
      GoogleCloudStorageItemInfo existingInfo;
      Duration maxWaitTime = storageOptions.getMaxWaitTimeForEmptyObjectCreation();

      BackOff backOff =
          !maxWaitTime.isZero() && !maxWaitTime.isNegative()
              ? new ExponentialBackOff.Builder()
                  .setMaxElapsedTimeMillis(toIntExact(maxWaitTime.toMillis()))
                  .setMaxIntervalMillis(500)
                  .setInitialIntervalMillis(100)
                  .setMultiplier(1.5)
                  .setRandomizationFactor(0.15)
                  .build()
              : BackOff.STOP_BACKOFF;
      long nextSleep = 0L;
      do {
        if (nextSleep > 0) {
          try {
            sleeper.sleep(nextSleep);
          } catch (InterruptedException e) {
            // We caught an InterruptedException, we should set the interrupted bit on this
            // thread.
            Thread.currentThread().interrupt();
            nextSleep = BackOff.STOP;
          }
        }
        existingInfo = getItemInfo(resourceId);
        nextSleep = nextSleep == BackOff.STOP ? BackOff.STOP : backOff.nextBackOffMillis();
      } while (!existingInfo.exists() && nextSleep != BackOff.STOP);

      // Compare existence, size, and metadata; for 429 errors creating an empty
      // object,
      // we don't care about metaGeneration/contentGeneration as long as the metadata
      // matches, since we don't know for sure whether our low-level request succeeded
      // first or some other client succeeded first.
      if (existingInfo.exists() && existingInfo.getSize() == 0) {
        if (options.isEnsureEmptyObjectsMetadataMatch()) {
          return existingInfo.metadataEquals(options.getMetadata());
        }
        return true;
      }
    }
    return false;
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

            Blob movedBlob = storageWrapper.moveBlob(moveRequestBuilder.build());
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
        null);
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
      GoogleCloudStorageEventBus.postOnException();
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

    if (storageOptions.getEncryptionKey() != null) {
      copyRequestBuilder.setSourceOptions(
          BlobSourceOption.decryptionKey(storageOptions.getEncryptionKey().value()));
      copyRequestBuilder.setTarget(
          copyRequestBuilder.build().getTarget().getBlobId(),
          BlobTargetOption.encryptionKey(storageOptions.getEncryptionKey().value()));
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

            CopyWriter copyWriter = storageWrapper.copy(copyRequestBuilder.build());
            while (!copyWriter.isDone()) {
              copyWriter.copyChunk();
              logger.atFinest().log(
                  "Copy (%s to %s) did not complete. Resuming...", srcString, dstString);
            }
            logger.atFiner().log("Successfully copied %s to %s", srcString, dstString);
          } catch (StorageException e) {
            GoogleCloudStorageEventBus.postOnException();
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
          storageWrapper.list(
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
      GoogleCloudStorageEventBus.postOnException();
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
      GoogleCloudStorageEventBus.postOnException();
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
              storageWrapper.delete(
                  BlobId.of(bucketName, objectName),
                  BlobSourceOption.generationMatch(resourceId.getGenerationId())),
          getObjectDeletionCallback(
              resourceId, innerExceptions, batchExecutor, attempt, resourceId.getGenerationId()));

    } else {
      // We first need to get the current object version to issue a safe delete for
      // only the latest
      // version of the object.
      batchExecutor.queue(
          () ->
              storageWrapper.get(
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
                      storageWrapper.delete(
                          BlobId.of(bucketName, objectName),
                          BlobSourceOption.generationMatch(generation)),
                  getObjectDeletionCallback(
                      resourceId, innerExceptions, batchExecutor, attempt, generation));
            }

            @Override
            public void onFailure(Throwable throwable) {
              GoogleCloudStorageEventBus.postOnException();
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
          // This situation typically shows up when we make a request to delete something
          // and the
          // server receives the request, but we get a retry-able error before we get a
          // response.
          // During a retry, we no longer find the item because the server had deleted it
          // already.
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
          GoogleCloudStorageEventBus.postOnException();
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
        boolean isDeleted = storageWrapper.delete(bucketName);
        if (!isDeleted) {
          innerExceptions.add(createFileNotFoundException(bucketName, null, null));
        }
      } catch (StorageException e) {
        GoogleCloudStorageEventBus.postOnException();
        innerExceptions.add(
            new IOException(String.format("Error deleting '%s' bucket", bucketName), e));
      }
    }

    if (!innerExceptions.isEmpty()) {
      throw GoogleCloudStorageExceptions.createCompositeException(innerExceptions);
    }
  }

  /** See {@link GoogleCloudStorage#getItemInfos(List)} for details about expected behavior. */
  @Override
  public List<GoogleCloudStorageItemInfo> getItemInfos(List<StorageResourceId> resourceIds)
      throws IOException {
    logger.atFiner().log("getItemInfos(%s)", resourceIds);

    if (resourceIds.isEmpty()) {
      return new ArrayList<>();
    }

    Map<StorageResourceId, GoogleCloudStorageItemInfo> itemInfos =
        new ConcurrentHashMap<>(resourceIds.size());
    Set<IOException> innerExceptions = newConcurrentHashSet();
    BatchExecutor executor = new BatchExecutor(storageOptions.getBatchThreads());
    // For each resourceId, we'll either directly add ROOT_INFO, enqueue a Bucket
    // fetch request,
    // or enqueue a StorageObject fetch request.
    try {
      for (StorageResourceId resourceId : resourceIds) {
        if (resourceId.isRoot()) {
          itemInfos.put(resourceId, GoogleCloudStorageItemInfo.ROOT_INFO);
        } else if (resourceId.isBucket()) {
          executor.queue(
              () -> getBucket(resourceId.getBucketName()),
              getBucketCallback(resourceId, innerExceptions, itemInfos));

        } else if (resourceId.isStorageObject()) {
          executor.queue(
              () -> getBlob(resourceId), getBlobCallback(resourceId, innerExceptions, itemInfos));
        }
      }
    } finally {
      executor.shutdown();
    }
    if (!innerExceptions.isEmpty()) {
      GoogleCloudStorageEventBus.postOnException();
      throw GoogleCloudStorageExceptions.createCompositeException(innerExceptions);
    }

    // Assemble the return list in the same order as the input arguments.
    List<GoogleCloudStorageItemInfo> sortedItemInfos = new ArrayList<>();
    for (StorageResourceId resourceId : resourceIds) {
      checkState(
          itemInfos.containsKey(resourceId),
          "Somehow missing resourceId '%s' from map: %s",
          resourceId,
          itemInfos);
      sortedItemInfos.add(itemInfos.get(resourceId));
    }

    // We expect the return list to be the same size, even if some entries were "not
    // found".
    checkState(
        sortedItemInfos.size() == resourceIds.size(),
        "sortedItemInfos.size() (%s) != resourceIds.size() (%s). infos: %s, ids: %s",
        sortedItemInfos.size(),
        resourceIds.size(),
        sortedItemInfos,
        resourceIds);
    return sortedItemInfos;
  }

  private FutureCallback<Bucket> getBucketCallback(
      StorageResourceId resourceId,
      Set<IOException> innerExceptions,
      Map<StorageResourceId, GoogleCloudStorageItemInfo> itemInfos) {
    return new FutureCallback<>() {
      @Override
      public void onSuccess(@Nullable Bucket bucket) {
        if (bucket != null) {
          logger.atFiner().log(
              "getItemInfos: Successfully fetched bucket: %s for resourceId: %s",
              bucket, resourceId);
          itemInfos.put(resourceId, createItemInfoForBucket(resourceId, bucket));
        } else {
          logger.atFiner().log("getItemInfos: bucket '%s' not found", resourceId.getBucketName());
          itemInfos.put(resourceId, GoogleCloudStorageItemInfo.createNotFound(resourceId));
        }
      }

      @Override
      public void onFailure(Throwable throwable) {
        GoogleCloudStorageEventBus.postOnException();
        innerExceptions.add(
            new IOException(
                String.format("Error getting %s bucket", resourceId.getBucketName()), throwable));
      }
    };
  }

  private FutureCallback<Blob> getBlobCallback(
      StorageResourceId resourceId,
      Set<IOException> innerExceptions,
      Map<StorageResourceId, GoogleCloudStorageItemInfo> itemInfos) {

    return new FutureCallback<>() {
      @Override
      public void onSuccess(@Nullable Blob blob) {
        if (blob != null) {
          logger.atFiner().log(
              "getItemInfos: Successfully fetched object '%s' for resourceId '%s'",
              blob, resourceId);
          itemInfos.put(resourceId, createItemInfoForBlob(resourceId, blob));
        } else {
          logger.atFiner().log("getItemInfos: object '%s' not found", resourceId);
          itemInfos.put(resourceId, GoogleCloudStorageItemInfo.createNotFound(resourceId));
        }
      }

      @Override
      public void onFailure(Throwable throwable) {
        GoogleCloudStorageEventBus.postOnException();
        innerExceptions.add(
            new IOException(String.format("Error getting %s object", resourceId), throwable));
      }
    };
  }

  /**
   * See {@link GoogleCloudStorage#getItemInfo(StorageResourceId)} for details about expected
   * behavior.
   */
  @Override
  public GoogleCloudStorageItemInfo getItemInfo(StorageResourceId resourceId) throws IOException {
    logger.atFiner().log("getItemInfo(%s)", resourceId);

    // Handle ROOT case first.
    if (resourceId.isRoot()) {
      return GoogleCloudStorageItemInfo.ROOT_INFO;
    }
    GoogleCloudStorageItemInfo itemInfo = null;

    if (resourceId.isBucket()) {
      Bucket bucket = getBucket(resourceId.getBucketName());
      if (bucket != null) {
        itemInfo = createItemInfoForBucket(resourceId, bucket);
      } else {
        logger.atFiner().log("getBucket(%s): not found", resourceId.getBucketName());
      }
    } else {
      Blob blob = getBlob(resourceId);
      if (blob != null) {
        itemInfo = createItemInfoForBlob(resourceId, blob);
      } else {
        logger.atFiner().log("getObject(%s): not found", resourceId);
      }
    }

    if (itemInfo == null) {
      itemInfo = GoogleCloudStorageItemInfo.createNotFound(resourceId);
    }
    logger.atFiner().log("getItemInfo: %s", itemInfo);
    return itemInfo;
  }

  /**
   * Gets the bucket with the given name.
   *
   * @param bucketName name of the bucket to get
   * @return the bucket with the given name or null if bucket not found
   * @throws IOException if the bucket exists but cannot be accessed
   */
  @Nullable
  private Bucket getBucket(String bucketName) throws IOException {
    logger.atFiner().log("getBucket(%s)", bucketName);
    checkArgument(!isNullOrEmpty(bucketName), "bucketName must not be null or empty");
    try {
      return storageWrapper.get(bucketName);
    } catch (StorageException e) {
      GoogleCloudStorageEventBus.postOnException();
      if (errorExtractor.getErrorType(e) == ErrorType.NOT_FOUND) {
        return null;
      }
      throw new IOException("Error accessing Bucket " + bucketName, e);
    }
  }

  /**
   * Gets the object with the given resourceId.
   *
   * @param resourceId identifies a StorageObject
   * @return the object with the given name or null if object not found
   * @throws IOException if the object exists but cannot be accessed
   */
  @Nullable
  Blob getBlob(StorageResourceId resourceId) throws IOException {
    checkArgument(
        resourceId.isStorageObject(), "Expected full StorageObject id, got %s", resourceId);
    String bucketName = resourceId.getBucketName();
    String objectName = resourceId.getObjectName();
    Blob blob;
    try {
      blob =
          storageWrapper.get(
              BlobId.of(bucketName, objectName),
              BlobGetOption.fields(BLOB_FIELDS.toArray(new BlobField[0])));
    } catch (StorageException e) {
      GoogleCloudStorageEventBus.postOnException();
      throw new IOException("Error accessing " + resourceId, e);
    }
    return blob;
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
    GoogleCloudStorageItemInfo gcsItemInfo = itemInfo;
    if (gcsItemInfo == null
        && (storageOptions.isBidiEnabled() || readOptions.isFastFailOnNotFoundEnabled())) {
      gcsItemInfo = getItemInfo(resourceId);
    }
    // TODO(dhritichorpa) Microbenchmark the latency of using
    // storage.get(gcsItemInfo.getBucketName()).getLocationType() here instead of
    // flag
    if (storageOptions.isBidiEnabled()) {
      return new GoogleCloudStorageBidiReadChannel(
          storageWrapper.getStorage(),
          gcsItemInfo,
          readOptions,
          getBoundedThreadPool(readOptions.getBidiThreadCount()));
    } else {
      return new GoogleCloudStorageClientReadChannel(
          storageWrapper.getStorage(),
          resourceId,
          gcsItemInfo,
          readOptions,
          errorExtractor,
          storageOptions);
    }
  }

  private ExecutorService getBoundedThreadPool(int bidiThreadCount) {
    if (boundedThreadPool == null) {
      boundedThreadPool =
          new ThreadPoolExecutor(
              bidiThreadCount,
              bidiThreadCount,
              0L,
              TimeUnit.MILLISECONDS,
              taskQueue,
              new ThreadFactoryBuilder()
                  .setNameFormat("bidiRead-range-pool-%d")
                  .setDaemon(true)
                  .build());
    }
    return boundedThreadPool;
  }

  @Override
  public void close() {
    try {
      try {
        storageWrapper.close();
      } catch (Exception e) {
        logger.atWarning().withCause(e).log("Error occurred while closing the storage client");
      }
      try {
        super.close();
      } finally {
        backgroundTasksThreadPool.shutdown();
        if (boundedThreadPool != null) {
          boundedThreadPool.shutdown();
        }
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
            () -> storageWrapper.update(blobUpdate),
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
                GoogleCloudStorageEventBus.postOnException();
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
      GoogleCloudStorageEventBus.postOnException();
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

    // We expect the return list to be the same size, even if some entries were "not
    // found".
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
        GoogleCloudStorageEventBus.postOnException();
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
      composedBlob = storageWrapper.compose(request);
    } catch (StorageException e) {
      GoogleCloudStorageEventBus.postOnException();
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
    GoogleCloudStorageEventBus.postOnException();
    throw new FileAlreadyExistsException(String.format("Object %s already exists.", resourceId));
  }

  static ImmutableMap<String, String> getUpdatedHeaders(
      GoogleCloudStorageOptions storageOptions, FeatureHeaderGenerator featureHeaderGenerator) {
    ImmutableMap<String, String> httpRequestHeaders =
        MoreObjects.firstNonNull(storageOptions.getHttpRequestHeaders(), ImmutableMap.of());
    String appName = storageOptions.getAppName();
    ImmutableMap.Builder<String, String> headersBuilder =
        ImmutableMap.<String, String>builder().putAll(httpRequestHeaders);
    if (!httpRequestHeaders.containsKey(USER_AGENT) && !Strings.isNullOrEmpty(appName)) {
      logger.atFiner().log("Setting useragent %s", appName);
      headersBuilder.put(USER_AGENT, appName);
    }
    if (featureHeaderGenerator == null) {
      return headersBuilder.build();
    }
    String featureHeaderString = featureHeaderGenerator.getValue();
    if (httpRequestHeaders.containsKey(FeatureHeaderGenerator.HEADER_NAME)) {
      logger.atFiner().log(
          "Not setting feature usage header, unexpectedly already set to %s",
          httpRequestHeaders.get(FeatureHeaderGenerator.HEADER_NAME));
    } else if (!Strings.isNullOrEmpty(featureHeaderString)) {
      logger.atFiner().log("Setting feature usage header %s", featureHeaderString);
      headersBuilder.put(FeatureHeaderGenerator.HEADER_NAME, featureHeaderString);
    }
    return headersBuilder.build();
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

    public abstract Builder setFeatureHeaderGenerator(
        @Nullable FeatureHeaderGenerator featureHeaderGenerator);

    public abstract Builder setGRPCInterceptors(
        @Nullable ImmutableList<ClientInterceptor> gRPCInterceptors);

    @VisibleForTesting
    public abstract Builder setClientLibraryStorage(@Nullable Storage clientLibraryStorage);

    @VisibleForTesting
    public abstract Builder setPCUExecutorService(@Nullable ExecutorService pCUExecutorService);

    public abstract GoogleCloudStorageClientImpl build() throws IOException;
  }
}
