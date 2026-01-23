/*
 * Copyright 2013 Google Inc.
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

import static com.google.cloud.hadoop.gcsio.FolderInfo.BUCKET_PREFIX;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageExceptions.createFileNotFoundException;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageExceptions.createJsonResponseException;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageItemInfo.createInferredDirectory;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;

import com.google.api.client.googleapis.batch.json.JsonBatchCallback;
import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.InputStreamContent;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.Data;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.Sleeper;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.StorageRequest;
import com.google.api.services.storage.model.Bucket;
import com.google.api.services.storage.model.BucketStorageLayout;
import com.google.api.services.storage.model.Buckets;
import com.google.api.services.storage.model.ComposeRequest;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.RewriteResponse;
import com.google.api.services.storage.model.StorageObject;
import com.google.auth.Credentials;
import com.google.auto.value.AutoBuilder;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions.MetricsSink;
import com.google.cloud.hadoop.util.AbstractGoogleAsyncWriteChannel;
import com.google.cloud.hadoop.util.AccessBoundary;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.cloud.hadoop.util.ChainingHttpRequestInitializer;
import com.google.cloud.hadoop.util.ClientRequestHelper;
import com.google.cloud.hadoop.util.GoogleCloudStorageEventBus;
import com.google.cloud.hadoop.util.HttpTransportFactory;
import com.google.cloud.hadoop.util.ITraceOperation;
import com.google.cloud.hadoop.util.ResilientOperation;
import com.google.cloud.hadoop.util.RetryBoundedBackOff;
import com.google.cloud.hadoop.util.RetryDeterminer;
import com.google.cloud.hadoop.util.RetryHttpInitializer;
import com.google.cloud.hadoop.util.TraceOperation;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.flogger.GoogleLogger;
import com.google.common.io.BaseEncoding;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.storage.control.v2.CreateFolderRequest;
import com.google.storage.control.v2.Folder;
import com.google.storage.control.v2.FolderName;
import com.google.storage.control.v2.GetFolderRequest;
import com.google.storage.control.v2.ListFoldersRequest;
import com.google.storage.control.v2.RenameFolderRequest;
import com.google.storage.control.v2.StorageControlClient;
import com.google.storage.control.v2.StorageControlClient.ListFoldersPagedResponse;
import com.google.storage.control.v2.StorageControlSettings;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileAlreadyExistsException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
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
 * the appropriate API call(s) via the generated JSON API client, while adding reliability and
 * performance features such as setting up low-level retries, translating low-level exceptions, and
 * request batching.
 */
public class GoogleCloudStorageImpl implements GoogleCloudStorage {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  // Maximum number of times to retry deletes in the case of precondition failures.
  private static final int MAXIMUM_PRECONDITION_FAILURES_IN_DELETE = 4;

  private static final String USER_PROJECT_FIELD_NAME = "userProject";

  static final CreateObjectOptions EMPTY_OBJECT_CREATE_OPTIONS =
      CreateObjectOptions.DEFAULT_OVERWRITE.toBuilder()
          .setEnsureEmptyObjectsMetadataMatch(false)
          .build();

  private static Cache<String, Boolean> cache =
      CacheBuilder.newBuilder().expireAfterWrite(10, TimeUnit.MINUTES).build();

  // Object field that are used in GoogleCloudStorageItemInfo
  static final String OBJECT_FIELDS =
      String.join(
          /* delimiter= */ ",",
          "bucket",
          "name",
          "timeCreated",
          "updated",
          "generation",
          "metageneration",
          "size",
          "contentType",
          "contentEncoding",
          "md5Hash",
          "crc32c",
          "metadata");

  private final MetricsRecorder metricsRecorder;

  private final Credentials credential;

  // Lazily created since HN folders may not be enabled.
  private StorageControlClient storageControlClient;

  // A function to encode metadata map values
  static String encodeMetadataValues(byte[] bytes) {
    return bytes == null ? Data.NULL_STRING : BaseEncoding.base64().encode(bytes);
  }

  @Nullable
  private static byte[] decodeMetadataValues(String value) {
    try {
      return BaseEncoding.base64().decode(value);
    } catch (IllegalArgumentException iae) {
      GoogleCloudStorageEventBus.postOnException();
      logger.atSevere().withCause(iae).log(
          "Failed to parse base64 encoded attribute value %s - %s", value, iae);
      return null;
    }
  }

  public static void validateCopyArguments(
      String srcBucketName,
      List<String> srcObjectNames,
      String dstBucketName,
      List<String> dstObjectNames,
      GoogleCloudStorage googleCloudStorage)
      throws IOException {
    checkArgument(srcObjectNames != null, "srcObjectNames must not be null");
    checkArgument(dstObjectNames != null, "dstObjectNames must not be null");
    checkArgument(
        srcObjectNames.size() == dstObjectNames.size(),
        "Must supply same number of elements in srcObjects and dstObjects");
    Map<StorageResourceId, StorageResourceId> srcToDestObjectsMap =
        new HashMap<>(srcObjectNames.size());
    for (int i = 0; i < srcObjectNames.size(); i++) {
      srcToDestObjectsMap.put(
          new StorageResourceId(srcBucketName, srcObjectNames.get(i)),
          new StorageResourceId(dstBucketName, dstObjectNames.get(i)));
    }
    validateCopyArguments(srcToDestObjectsMap, googleCloudStorage);
  }

  /** A factory for producing BackOff objects. */
  public interface BackOffFactory {

    BackOffFactory DEFAULT =
        () -> new RetryBoundedBackOff(new ExponentialBackOff(), /* maxRetries= */ 10);

    BackOff newBackOff();
  }

  private final LoadingCache<String, Boolean> autoBuckets =
      CacheBuilder.newBuilder()
          .expireAfterWrite(Duration.ofHours(1))
          .build(
              new CacheLoader<>() {
                final List<String> iamPermissions = ImmutableList.of("storage.buckets.get");

                @Override
                public Boolean load(String bucketName) {
                  try {
                    storage
                        .buckets()
                        .testIamPermissions(bucketName, iamPermissions)
                        .executeUnparsed()
                        .disconnect();
                  } catch (IOException e) {
                    GoogleCloudStorageEventBus.postOnException();
                    return errorExtractor.userProjectMissing(e);
                  }
                  return false;
                }
              });

  // GCS access instance.
  @VisibleForTesting Storage storage;

  // Factory to create requests that override storage api.
  @VisibleForTesting StorageRequestFactory storageRequestFactory;

  // Thread-pool used for background tasks.
  private ExecutorService backgroundTasksThreadPool =
      Executors.newCachedThreadPool(
          new ThreadFactoryBuilder()
              .setNameFormat("gcs-async-channel-pool-%d")
              .setDaemon(true)
              .build());

  // Thread-pool for manual matching of metadata tasks.
  // TODO(user): Wire out GoogleCloudStorageOptions for these.
  private ExecutorService manualBatchingThreadPool = createManualBatchingThreadPool();

  // Helper delegate for turning IOExceptions from API calls into higher-level semantics.
  private ApiErrorExtractor errorExtractor = ApiErrorExtractor.INSTANCE;

  // Helper for interacting with objects involved with the API client libraries.
  private final ClientRequestHelper<StorageObject> clientRequestHelper =
      new ClientRequestHelper<>();

  // Factory for BatchHelpers setting up BatchRequests; can be swapped out for testing purposes.
  private BatchHelper.Factory batchFactory = new BatchHelper.Factory();

  // Request initializer to use for batch and non-batch requests.
  private final HttpRequestInitializer httpRequestInitializer;

  // Backporting GhfsInstrumentation

  private final StatisticsTrackingHttpRequestInitializer httpStatistics =
      new StatisticsTrackingHttpRequestInitializer();

  // Configuration values for this instance
  private final GoogleCloudStorageOptions storageOptions;

  // Object to use to perform sleep operations
  static final Sleeper sleeper = Sleeper.DEFAULT;

  // BackOff objects are per-request, use this to make new ones.
  private final BackOffFactory backOffFactory = BackOffFactory.DEFAULT;

  // Determine if a given IOException is due to rate-limiting.
  private RetryDeterminer<IOException> rateLimitedRetryDeterminer = errorExtractor::rateLimited;

  // Function that generates downscoped access token.
  private final Function<List<AccessBoundary>, String> downscopedAccessTokenFn;

  // Feature header generator to track connector features used in requests.
  private final FeatureHeaderGenerator featureHeaderGenerator;
  /**
   * Constructs an instance of GoogleCloudStorageImpl.
   *
   * @param options {@link GoogleCloudStorageOptions} to use to initialize the object
   * @param credentials OAuth2 credentials that allows access to GCS
   * @param httpTransport transport used for HTTP requests
   * @param httpRequestInitializer request initializer used to initialize all HTTP requests
   * @param downscopedAccessTokenFn Function that generates downscoped access token
   * @throws IOException on IO error
   */
  GoogleCloudStorageImpl(
      GoogleCloudStorageOptions options,
      @Nullable Credentials credentials,
      @Nullable HttpTransport httpTransport,
      @Nullable HttpRequestInitializer httpRequestInitializer,
      @Nullable Function<List<AccessBoundary>, String> downscopedAccessTokenFn,
      @Nullable FeatureHeaderGenerator featureHeaderGenerator)
      throws IOException {
    logger.atFiner().log("GCS(options: %s)", options);

    checkNotNull(options, "options must not be null").throwIfNotValid();

    this.storageOptions = options;

    Credentials finalCredentials;
    // If credentials is null then use httpRequestInitializer to initialize finalCredentials
    if (credentials == null && httpRequestInitializer != null) {
      checkArgument(
          httpRequestInitializer instanceof RetryHttpInitializer,
          "httpRequestInitializerParam (%s) should be an instance of RetryHttpInitializer for"
              + " credentials initialization",
          httpRequestInitializer.getClass());
      finalCredentials = ((RetryHttpInitializer) httpRequestInitializer).getCredentials();
    } else {
      finalCredentials = credentials;
    }

    // If httpRequestInitializer is null then use
    // finalCredentials to initialize finalHttpRequestInitializer
    HttpRequestInitializer finalHttpRequestInitializer;
    if (httpRequestInitializer == null) {
      finalHttpRequestInitializer =
          new RetryHttpInitializer(finalCredentials, options.toRetryHttpInitializerOptions());
    } else {
      logger.atWarning().log(
          "ALERT: Overriding httpRequestInitializer - this should not be done in production!");
      finalHttpRequestInitializer = httpRequestInitializer;
    }
    this.httpRequestInitializer =
        options.isTraceLogEnabled()
            ? new ChainingHttpRequestInitializer(
                httpStatistics,
                finalHttpRequestInitializer,
                new EventLoggingHttpRequestInitializer())
            : new ChainingHttpRequestInitializer(httpStatistics, finalHttpRequestInitializer);

    this.downscopedAccessTokenFn = downscopedAccessTokenFn;
    this.featureHeaderGenerator = featureHeaderGenerator;

    HttpTransport finalHttpTransport =
        httpTransport == null
            ? HttpTransportFactory.createHttpTransport(
                options.getProxyAddress(),
                options.getProxyUsername(),
                options.getProxyPassword(),
                options.getHttpRequestReadTimeout())
            : httpTransport;
    this.storage =
        new Storage.Builder(
                finalHttpTransport, GsonFactory.getDefaultInstance(), this.httpRequestInitializer)
            .setRootUrl(options.getStorageRootUrl())
            .setServicePath(options.getStorageServicePath())
            .setApplicationName(options.getAppName())
            .build();
    this.credential = credentials;
    this.storageRequestFactory = new StorageRequestFactory(storage);

    this.metricsRecorder =
        MetricsSink.CLOUD_MONITORING == options.getMetricsSink()
            ? CloudMonitoringMetricsRecorder.create(options.getProjectId(), finalCredentials)
            : new NoOpMetricsRecorder();
  }

  private ExecutorService createManualBatchingThreadPool() {
    ThreadPoolExecutor service =
        new ThreadPoolExecutor(
            /* corePoolSize= */ 10,
            /* maximumPoolSize= */ 20,
            /* keepAliveTime= */ 10L,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            new ThreadFactoryBuilder()
                .setNameFormat("gcs-manual-batching-pool-%d")
                .setDaemon(true)
                .build());
    // allowCoreThreadTimeOut needs to be enabled for cases where the encapsulating class does not
    service.allowCoreThreadTimeOut(true);
    return service;
  }

  @VisibleForTesting
  void setBackgroundTasksThreadPool(ExecutorService backgroundTasksThreadPool) {
    this.backgroundTasksThreadPool = backgroundTasksThreadPool;
  }

  @VisibleForTesting
  void setErrorExtractor(ApiErrorExtractor errorExtractor) {
    this.errorExtractor = errorExtractor;
    this.rateLimitedRetryDeterminer = errorExtractor::rateLimited;
  }

  @VisibleForTesting
  void setBatchFactory(BatchHelper.Factory batchFactory) {
    this.batchFactory = batchFactory;
  }

  @Override
  public GoogleCloudStorageOptions getOptions() {
    return storageOptions;
  }

  @Override
  public WritableByteChannel create(StorageResourceId resourceId, CreateObjectOptions options)
      throws IOException {
    logger.atFiner().log("create(%s)", resourceId);
    checkArgument(
        resourceId.isStorageObject(), "Expected full StorageObject id, got %s", resourceId);

    // IMPORTANT: Do not modify or change this logic unless absolutely sure that you've addressed
    // all out-of-order semantics.
    //
    // When performing mutations in GCS, even when we aren't concerned with parallel writers,
    // we need to protect ourselves from what appear to be out-of-order writes to the writer.
    // These most commonly manifest themselves as a sequence of:
    // 1) Perform mutation M1 on object O1, which results in an HTTP 503 error,
    //    but can be any 5XX class error.
    // 2) Retry mutation M1, which yields a 200 OK
    // 3) Perform mutation M2 on O1, which yields a 200 OK
    // 4) Some time later, get O1 and see M1 and not M2, even though M2 appears to have happened
    //    later.
    //
    // To counter this we need to perform mutations with a condition attached, always. This prevents
    // the race condition as described in:
    // https://cloud.google.com/storage/docs/generations-preconditions#preventing_the_race_condition

    Optional<Long> writeGeneration =
        resourceId.hasGenerationId()
            ? Optional.of(resourceId.getGenerationId())
            : Optional.of(getWriteGeneration(resourceId, options.isOverwriteExisting()));

    ObjectWriteConditions writeConditions =
        ObjectWriteConditions.builder()
            .setContentGenerationMatch(writeGeneration.orElse(null))
            .build();

    AbstractGoogleAsyncWriteChannel<?> channel =
        new GoogleCloudStorageWriteChannel(
            storage,
            clientRequestHelper,
            backgroundTasksThreadPool,
            storageOptions.getWriteChannelOptions(),
            resourceId,
            options,
            writeConditions) {
          @Override
          public Storage.Objects.Insert createRequest(InputStreamContent inputStream)
              throws IOException {
            return initializeRequest(super.createRequest(inputStream), resourceId.getBucketName());
          }
        };
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

    Bucket bucket =
        new Bucket()
            .setName(bucketName)
            .setHierarchicalNamespace(
                new Bucket.HierarchicalNamespace()
                    .setEnabled(options.getHierarchicalNamespaceEnabled()))
            .setLocation(options.getLocation())
            .setStorageClass(options.getStorageClass());

    if (options.getHierarchicalNamespaceEnabled()) {
      bucket.setIamConfiguration(
          new Bucket.IamConfiguration()
              .setUniformBucketLevelAccess(
                  new Bucket.IamConfiguration.UniformBucketLevelAccess().setEnabled(true)));
    }

    if (options.getTtl() != null) {
      Bucket.Lifecycle.Rule lifecycleRule =
          new Bucket.Lifecycle.Rule()
              .setAction(new Bucket.Lifecycle.Rule.Action().setType("Delete"))
              .setCondition(
                  new Bucket.Lifecycle.Rule.Condition()
                      .setAge(toIntExact(options.getTtl().toDays())));
      bucket.setLifecycle(new Bucket.Lifecycle().setRule(ImmutableList.of(lifecycleRule)));
    }

    Storage.Buckets.Insert insertBucket =
        initializeRequest(
            storage.buckets().insert(storageOptions.getProjectId(), bucket), bucketName);
    // TODO(user): To match the behavior of throwing FileNotFoundException for 404, we probably
    // want to throw org.apache.commons.io.FileExistsException for 409 here.
    try {
      ResilientOperation.retry(
          insertBucket::execute,
          backOffFactory.newBackOff(),
          rateLimitedRetryDeterminer,
          IOException.class,
          sleeper);
    } catch (InterruptedException e) {
      GoogleCloudStorageEventBus.postOnException();
      Thread.currentThread().interrupt();
      throw new IOException("Failed to create bucket", e);
    } catch (IOException e) {
      GoogleCloudStorageEventBus.postOnException();
      if (ApiErrorExtractor.INSTANCE.itemAlreadyExists(e)) {
        throw (FileAlreadyExistsException)
            new FileAlreadyExistsException(String.format("Bucket '%s' already exists.", bucketName))
                .initCause(e);
      }
      throw e;
    }
  }

  @Override
  public void createEmptyObject(StorageResourceId resourceId, CreateObjectOptions options)
      throws IOException {
    checkArgument(
        resourceId.isStorageObject(), "Expected full StorageObject id, got %s", resourceId);

    Storage.Objects.Insert insertObject = prepareEmptyInsert(resourceId, options);
    try (ITraceOperation op = TraceOperation.addToExistingTrace("gcs.objects.insert")) {
      insertObject.execute();
    } catch (IOException e) {
      if (canIgnoreExceptionForEmptyObject(e, resourceId, options)) {
        logger.atInfo().log(
            "Ignoring exception of type %s; verified object already exists with desired state.",
            e.getClass().getSimpleName());
        logger.atFine().withCause(e).log("Ignored exception while creating empty object");
      } else {
        GoogleCloudStorageEventBus.postOnException();
        if (ApiErrorExtractor.INSTANCE.itemAlreadyExists(e)) {
          throw (FileAlreadyExistsException)
              new FileAlreadyExistsException(
                      String.format("Object '%s' already exists.", resourceId))
                  .initCause(e);
        }
        throw e;
      }
    }
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

  @Override
  public void createEmptyObjects(List<StorageResourceId> resourceIds, CreateObjectOptions options)
      throws IOException {
    // TODO(user): This method largely follows a pattern similar to
    // deleteObjects(List<StorageResourceId>); extract a generic method for both.
    logger.atFiner().log("createEmptyObjects(%s)", resourceIds);

    if (resourceIds.isEmpty()) {
      return;
    }

    // Don't go through batch interface for a single-item case to avoid batching overhead.
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
    CountDownLatch latch = new CountDownLatch(resourceIds.size());
    for (StorageResourceId resourceId : resourceIds) {
      Storage.Objects.Insert insertObject = prepareEmptyInsert(resourceId, options);
      manualBatchingThreadPool.execute(
          () -> {
            try {
              insertObject.execute();
              logger.atFiner().log("Successfully inserted %s", resourceId);
            } catch (IOException ioe) {
              boolean canIgnoreException = false;
              try {
                canIgnoreException = canIgnoreExceptionForEmptyObject(ioe, resourceId, options);
              } catch (Exception e) {
                // Make sure to catch Exception instead of only IOException so that we can correctly
                // wrap other such exceptions and propagate them out cleanly inside innerExceptions;
                // common sources of non-IOExceptions include Preconditions checks which get
                // enforced at various layers in the library stack.
                innerExceptions.add(
                    new IOException("Error re-fetching after rate-limit error: " + resourceId, e));
              }
              if (canIgnoreException) {
                logger.atInfo().log(
                    "Ignoring exception of type %s; verified object already exists with desired"
                        + " state.",
                    ioe.getClass().getSimpleName());
                logger.atFine().withCause(ioe).log("Ignored exception while creating empty object");
              } else {
                innerExceptions.add(new IOException("Error inserting " + resourceId, ioe));
              }
            } catch (Exception e) {
              innerExceptions.add(new IOException("Error inserting " + resourceId, e));
            } finally {
              latch.countDown();
            }
          });
    }

    try {
      latch.await();
    } catch (InterruptedException ie) {
      GoogleCloudStorageEventBus.postOnException();
      Thread.currentThread().interrupt();
      throw new IOException("Failed to create empty objects", ie);
    }

    if (!innerExceptions.isEmpty()) {
      GoogleCloudStorageEventBus.postOnException();
      throw GoogleCloudStorageExceptions.createCompositeException(innerExceptions);
    }
  }

  /**
   * See {@link GoogleCloudStorage#createEmptyObjects(List)} for details about expected behavior.
   */
  @Override
  public void createEmptyObjects(List<StorageResourceId> resourceIds) throws IOException {
    createEmptyObjects(resourceIds, EMPTY_OBJECT_CREATE_OPTIONS);
  }

  /** See {@link GoogleCloudStorage#open(StorageResourceId)} for details about expected behavior. */
  @Override
  public SeekableByteChannel open(
      StorageResourceId resourceId, GoogleCloudStorageReadOptions readOptions) throws IOException {
    logger.atFiner().log("open(%s, %s)", resourceId, readOptions);
    checkArgument(
        resourceId.isStorageObject(), "Expected full StorageObject id, got %s", resourceId);

    return open(resourceId, /* itemInfo= */ null, readOptions);
  }

  /**
   * See {@link GoogleCloudStorage#open(GoogleCloudStorageItemInfo)} for details about expected
   * behavior.
   */
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
    if (itemInfo != null && !itemInfo.exists()) {
      throw createFileNotFoundException(
          resourceId.getBucketName(), resourceId.getObjectName(), /* cause= */ null);
    }

    return new GoogleCloudStorageReadChannel(
        storage, resourceId, errorExtractor, clientRequestHelper, readOptions) {

      @Override
      @Nullable
      protected GoogleCloudStorageItemInfo getInitialMetadata() {
        return itemInfo;
      }

      @Override
      protected Storage.Objects.Get createDataRequest() throws IOException {
        return initializeRequest(super.createDataRequest(), resourceId.getBucketName());
      }

      @Override
      protected Storage.Objects.Get createMetadataRequest() throws IOException {
        return initializeRequest(super.createMetadataRequest(), resourceId.getBucketName());
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
      Storage.Buckets.Delete deleteBucket =
          initializeRequest(storage.buckets().delete(bucketName), bucketName);

      try {
        ResilientOperation.retry(
            deleteBucket::execute,
            backOffFactory.newBackOff(),
            rateLimitedRetryDeterminer,
            IOException.class,
            sleeper);
      } catch (IOException e) {
        innerExceptions.add(
            errorExtractor.itemNotFound(e)
                ? createFileNotFoundException(bucketName, null, e)
                : new IOException(String.format("Error deleting '%s' bucket", bucketName), e));
      } catch (InterruptedException e) {
        GoogleCloudStorageEventBus.postOnException();
        Thread.currentThread().interrupt();
        throw new IOException("Failed to delete buckets", e);
      }
    }
    if (!innerExceptions.isEmpty()) {
      GoogleCloudStorageEventBus.postOnException();
      throw GoogleCloudStorageExceptions.createCompositeException(innerExceptions);
    }
  }

  /** See {@link GoogleCloudStorage#deleteObjects(List)} for details about expected behavior. */
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
    String traceContext = String.format("batchDelete(size=%s)", fullObjectNames.size());
    try (ITraceOperation to = TraceOperation.addToExistingTrace(traceContext)) {
      BatchHelper batchHelper =
          batchFactory.newBatchHelper(
              httpRequestInitializer,
              storage,
              storageOptions.getMaxRequestsPerBatch(),
              fullObjectNames.size(),
              storageOptions.getBatchThreads(),
              traceContext);

      for (StorageResourceId fullObjectName : fullObjectNames) {
        queueSingleObjectDelete(fullObjectName, innerExceptions, batchHelper, 1);
      }

      batchHelper.flush();

      if (!innerExceptions.isEmpty()) {
        GoogleCloudStorageEventBus.postOnException();
        throw GoogleCloudStorageExceptions.createCompositeException(innerExceptions);
      }
    }
  }

  /** See {@link GoogleCloudStorage#deleteFolders(List)} for details about expected behavior. */
  @Override
  public void deleteFolders(List<FolderInfo> folders) throws IOException {
    String traceContext = String.format("batchFolderDelete(size=%s)", folders.size());
    DeleteFolderOperation deleteFolderOperation =
        new DeleteFolderOperation(folders, storageOptions, lazyGetStorageControlClient());
    try (ITraceOperation to = TraceOperation.addToExistingTrace(traceContext)) {
      deleteFolderOperation.performDeleteOperation();
    }
    if (!deleteFolderOperation.encounteredNoExceptions()) {
      GoogleCloudStorageEventBus.postOnException();
      throw GoogleCloudStorageExceptions.createCompositeException(
          deleteFolderOperation.getAllExceptions());
    }
  }

  /** Helper to create a callback for a particular deletion request. */
  private JsonBatchCallback<Void> getDeletionCallback(
      StorageResourceId resourceId,
      ConcurrentHashMap.KeySetView<IOException, Boolean> innerExceptions,
      BatchHelper batchHelper,
      int attempt,
      long generation) {
    return new JsonBatchCallback<>() {
      @Override
      public void onSuccess(Void obj, HttpHeaders responseHeaders) {
        logger.atFiner().log("Successfully deleted %s at generation %s", resourceId, generation);
      }

      @Override
      public void onFailure(GoogleJsonError jsonError, HttpHeaders responseHeaders)
          throws IOException {
        GoogleJsonResponseException cause = createJsonResponseException(jsonError, responseHeaders);
        GoogleCloudStorageEventBus.postOnException();
        if (errorExtractor.itemNotFound(cause)) {
          // Ignore item-not-found errors. We do not have to delete what we cannot find.
          // This error typically shows up when we make a request to delete something and the
          // server receives the request, but we get a retry-able error before we get a response.
          // During a retry, we no longer find the item because the server had deleted it already.
          logger.atFiner().log("Delete object '%s' not found:%n%s", resourceId, jsonError);
        } else if (errorExtractor.preconditionNotMet(cause)
            && attempt <= MAXIMUM_PRECONDITION_FAILURES_IN_DELETE) {
          logger.atInfo().log(
              "Precondition not met while deleting '%s' at generation %s. Attempt %s."
                  + " Retrying:%n%s",
              resourceId, generation, attempt, jsonError);
          queueSingleObjectDelete(resourceId, innerExceptions, batchHelper, attempt + 1);
        } else {
          innerExceptions.add(
              new IOException(
                  String.format(
                      "Error deleting '%s', stage 2 with generation %s", resourceId, generation),
                  cause));
        }
      }
    };
  }

  private void queueSingleObjectDelete(
      StorageResourceId resourceId,
      ConcurrentHashMap.KeySetView<IOException, Boolean> innerExceptions,
      BatchHelper batchHelper,
      int attempt)
      throws IOException {
    String bucketName = resourceId.getBucketName();
    String objectName = resourceId.getObjectName();

    if (resourceId.hasGenerationId()) {
      // We can go direct to the deletion request instead of first fetching generation id.
      long generationId = resourceId.getGenerationId();
      Storage.Objects.Delete deleteObject =
          initializeRequest(storage.objects().delete(bucketName, objectName), bucketName)
              .setIfGenerationMatch(generationId);
      batchHelper.queue(
          deleteObject,
          getDeletionCallback(resourceId, innerExceptions, batchHelper, attempt, generationId));
    } else {
      // We first need to get the current object version to issue a safe delete for only the latest
      // version of the object.
      Storage.Objects.Get getObject =
          initializeRequest(
                  storageRequestFactory.objectsGetMetadata(bucketName, objectName), bucketName)
              .setFields("generation");
      batchHelper.queue(
          getObject,
          new JsonBatchCallback<>() {
            @Override
            public void onSuccess(StorageObject storageObject, HttpHeaders httpHeaders)
                throws IOException {
              Long generation =
                  checkNotNull(storageObject.getGeneration(), "generation can not be null");
              Storage.Objects.Delete deleteObject =
                  initializeRequest(storage.objects().delete(bucketName, objectName), bucketName)
                      .setIfGenerationMatch(generation);

              batchHelper.queue(
                  deleteObject,
                  getDeletionCallback(
                      resourceId, innerExceptions, batchHelper, attempt, generation));
            }

            @Override
            public void onFailure(GoogleJsonError jsonError, HttpHeaders responseHeaders) {
              GoogleCloudStorageEventBus.postOnException();
              GoogleJsonResponseException cause =
                  createJsonResponseException(jsonError, responseHeaders);
              if (errorExtractor.itemNotFound(cause)) {
                // If the item isn't found, treat it the same as if it's not found
                // in the delete case: assume the user wanted the object gone, and now it is.
                logger.atFiner().log(
                    "deleteObjects(%s): get not found:%n%s", resourceId, jsonError);
              } else {
                innerExceptions.add(
                    new IOException(
                        String.format("Error deleting '%s', stage 1", resourceId), cause));
              }
            }
          });
    }
  }

  /**
   * Validates basic argument constraints like non-null, non-empty Strings, using {@code
   * Preconditions} in addition to checking for src/dst bucket existence and compatibility of bucket
   * properties such as location and storage-class.
   *
   * @param gcsImpl A GoogleCloudStorage for retrieving bucket info via getItemInfo, but only if
   *     srcBucketName != dstBucketName; passed as a parameter so that this static method can be
   *     used by other implementations of GoogleCloudStorage that want to preserve the validation
   *     behavior of GoogleCloudStorageImpl, including disallowing cross-location copies.
   */
  @VisibleForTesting
  public static void validateCopyArguments(
      Map<StorageResourceId, StorageResourceId> sourceToDestinationObjectsMap,
      GoogleCloudStorage gcsImpl)
      throws IOException {
    checkNotNull(sourceToDestinationObjectsMap, "srcObjects must not be null");

    if (sourceToDestinationObjectsMap.isEmpty()) {
      return;
    }

    Map<StorageResourceId, GoogleCloudStorageItemInfo> bucketInfoCache = new HashMap<>();

    for (Map.Entry<StorageResourceId, StorageResourceId> entry :
        sourceToDestinationObjectsMap.entrySet()) {
      StorageResourceId source = entry.getKey();
      StorageResourceId destination = entry.getValue();
      String srcBucketName = source.getBucketName();
      String dstBucketName = destination.getBucketName();
      // Avoid copy across locations or storage classes.
      if (!srcBucketName.equals(dstBucketName)) {
        StorageResourceId srcBucketResourceId = new StorageResourceId(srcBucketName);
        GoogleCloudStorageItemInfo srcBucketInfo =
            getGoogleCloudStorageItemInfo(gcsImpl, bucketInfoCache, srcBucketResourceId);
        if (!srcBucketInfo.exists()) {
          GoogleCloudStorageEventBus.postOnException();
          throw new FileNotFoundException("Bucket not found: " + srcBucketName);
        }

        StorageResourceId dstBucketResourceId = new StorageResourceId(dstBucketName);
        GoogleCloudStorageItemInfo dstBucketInfo =
            getGoogleCloudStorageItemInfo(gcsImpl, bucketInfoCache, dstBucketResourceId);
        if (!dstBucketInfo.exists()) {
          GoogleCloudStorageEventBus.postOnException();
          throw new FileNotFoundException("Bucket not found: " + dstBucketName);
        }

        if (!gcsImpl.getOptions().isCopyWithRewriteEnabled()) {
          if (!srcBucketInfo.getLocation().equals(dstBucketInfo.getLocation())) {
            GoogleCloudStorageEventBus.postOnException();
            throw new UnsupportedOperationException(
                "This operation is not supported across two different storage locations.");
          }

          if (!srcBucketInfo.getStorageClass().equals(dstBucketInfo.getStorageClass())) {
            GoogleCloudStorageEventBus.postOnException();
            throw new UnsupportedOperationException(
                "This operation is not supported across two different storage classes.");
          }
        }
      }
      checkArgument(
          !isNullOrEmpty(source.getObjectName()), "srcObjectName must not be null or empty");
      checkArgument(
          !isNullOrEmpty(destination.getObjectName()), "dstObjectName must not be null or empty");
      if (srcBucketName.equals(dstBucketName)
          && source.getObjectName().equals(destination.getObjectName())) {
        GoogleCloudStorageEventBus.postOnException();
        throw new IllegalArgumentException(
            String.format(
                "Copy destination must be different from source for %s.",
                StringPaths.fromComponents(srcBucketName, source.getObjectName())));
      }
    }
  }

  /**
   * Validates basic argument constraints like non-null, non-empty Strings, using {@code
   * Preconditions} in addition to checking for src/dst bucket equality.
   */
  @VisibleForTesting
  public static void validateMoveArguments(
      Map<StorageResourceId, StorageResourceId> sourceToDestinationObjectsMap) throws IOException {
    checkNotNull(sourceToDestinationObjectsMap, "srcObjects must not be null");

    if (sourceToDestinationObjectsMap.isEmpty()) {
      return;
    }

    for (Map.Entry<StorageResourceId, StorageResourceId> entry :
        sourceToDestinationObjectsMap.entrySet()) {
      StorageResourceId source = entry.getKey();
      StorageResourceId destination = entry.getValue();
      String srcBucketName = source.getBucketName();
      String dstBucketName = destination.getBucketName();
      // Avoid move across buckets.
      if (!srcBucketName.equals(dstBucketName)) {
        throw new UnsupportedOperationException(
            "This operation is not supported across two different buckets.");
      }
      checkArgument(
          !isNullOrEmpty(source.getObjectName()), "srcObjectName must not be null or empty");
      checkArgument(
          !isNullOrEmpty(destination.getObjectName()), "dstObjectName must not be null or empty");
      if (srcBucketName.equals(dstBucketName)
          && source.getObjectName().equals(destination.getObjectName())) {
        GoogleCloudStorageEventBus.postOnException();
        throw new IllegalArgumentException(
            String.format(
                "Move destination must be different from source for %s.",
                StringPaths.fromComponents(srcBucketName, source.getObjectName())));
      }
    }
  }

  private static GoogleCloudStorageItemInfo getGoogleCloudStorageItemInfo(
      GoogleCloudStorage gcsImpl,
      Map<StorageResourceId, GoogleCloudStorageItemInfo> bucketInfoCache,
      StorageResourceId resourceId)
      throws IOException {
    GoogleCloudStorageItemInfo storageItemInfo = bucketInfoCache.get(resourceId);
    if (storageItemInfo != null) {
      return storageItemInfo;
    }
    storageItemInfo = gcsImpl.getItemInfo(resourceId);
    bucketInfoCache.put(resourceId, storageItemInfo);
    return storageItemInfo;
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

    String traceContext = String.format("batchcopy(size=%s)", sourceToDestinationObjectsMap.size());
    try (ITraceOperation to = TraceOperation.addToExistingTrace(traceContext)) {
      // Perform the copy operations.

      BatchHelper batchHelper =
          batchFactory.newBatchHelper(
              httpRequestInitializer,
              storage,
              storageOptions.getMaxRequestsPerBatch(),
              sourceToDestinationObjectsMap.size(),
              storageOptions.getBatchThreads(),
              "batchcopy");

      for (Map.Entry<StorageResourceId, StorageResourceId> entry :
          sourceToDestinationObjectsMap.entrySet()) {
        StorageResourceId srcObject = entry.getKey();
        StorageResourceId dstObject = entry.getValue();
        if (storageOptions.isCopyWithRewriteEnabled()) {
          // Rewrite request has the same effect as Copy, but it can handle moving
          // large objects that may potentially time out a Copy request.
          rewriteInternal(
              batchHelper,
              innerExceptions,
              srcObject.getBucketName(),
              srcObject.getObjectName(),
              dstObject.getBucketName(),
              dstObject.getObjectName());
        } else {
          copyInternal(
              batchHelper,
              innerExceptions,
              srcObject.getBucketName(),
              srcObject.getObjectName(),
              dstObject.getGenerationId(),
              dstObject.getBucketName(),
              dstObject.getObjectName());
        }
      }

      // Execute any remaining requests not divisible by the max batch size.
      batchHelper.flush();

      if (!innerExceptions.isEmpty()) {
        GoogleCloudStorageEventBus.postOnException();
        throw GoogleCloudStorageExceptions.createCompositeException(innerExceptions);
      }
    }
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

    String traceContext = String.format("batchmove(size=%s)", sourceToDestinationObjectsMap.size());
    try (ITraceOperation to = TraceOperation.addToExistingTrace(traceContext)) {
      // Perform the move operations.

      BatchHelper batchHelper =
          batchFactory.newBatchHelper(
              httpRequestInitializer,
              storage,
              storageOptions.getMaxRequestsPerBatch(),
              sourceToDestinationObjectsMap.size(),
              storageOptions.getBatchThreads(),
              "batchmove");

      for (Map.Entry<StorageResourceId, StorageResourceId> entry :
          sourceToDestinationObjectsMap.entrySet()) {
        StorageResourceId srcObject = entry.getKey();
        StorageResourceId dstObject = entry.getValue();
        moveInternal(
            batchHelper,
            innerExceptions,
            srcObject.getBucketName(),
            srcObject.getGenerationId(),
            srcObject.getObjectName(),
            dstObject.getGenerationId(),
            dstObject.getObjectName());
      }

      // Execute any remaining requests not divisible by the max batch size.
      batchHelper.flush();

      if (!innerExceptions.isEmpty()) {
        GoogleCloudStorageEventBus.postOnException();
        throw GoogleCloudStorageExceptions.createCompositeException(innerExceptions);
      }
    }
  }

  /**
   * Performs copy operation using GCS Rewrite requests
   *
   * @see GoogleCloudStorage#copy(String, List, String, List)
   */
  private void rewriteInternal(
      BatchHelper batchHelper,
      ConcurrentHashMap.KeySetView<IOException, Boolean> innerExceptions,
      String srcBucketName,
      String srcObjectName,
      String dstBucketName,
      String dstObjectName)
      throws IOException {
    Storage.Objects.Rewrite rewriteObject =
        initializeRequest(
            storage
                .objects()
                .rewrite(srcBucketName, srcObjectName, dstBucketName, dstObjectName, null),
            srcBucketName);
    if (storageOptions.getMaxRewriteChunkSize() > 0) {
      rewriteObject.setMaxBytesRewrittenPerCall(storageOptions.getMaxRewriteChunkSize());
    }

    // TODO(b/79750454) do not batch rewrite requests because they time out in batches.
    batchHelper.queue(
        rewriteObject,
        new JsonBatchCallback<>() {
          @Override
          public void onSuccess(RewriteResponse rewriteResponse, HttpHeaders responseHeaders) {
            String srcString = StringPaths.fromComponents(srcBucketName, srcObjectName);
            String dstString = StringPaths.fromComponents(dstBucketName, dstObjectName);

            if (rewriteResponse.getDone()) {
              logger.atFiner().log("Successfully copied %s to %s", srcString, dstString);
            } else {
              // If an object is very large, we need to continue making successive calls to rewrite
              // until the operation completes.
              logger.atFiner().log(
                  "Copy (%s to %s) did not complete. Resuming...", srcString, dstString);
              try {
                Storage.Objects.Rewrite rewriteObjectWithToken =
                    initializeRequest(
                        storage
                            .objects()
                            .rewrite(
                                srcBucketName, srcObjectName, dstBucketName, dstObjectName, null),
                        srcBucketName);
                if (storageOptions.getMaxRewriteChunkSize() > 0) {
                  rewriteObjectWithToken.setMaxBytesRewrittenPerCall(
                      storageOptions.getMaxRewriteChunkSize());
                }
                rewriteObjectWithToken.setRewriteToken(rewriteResponse.getRewriteToken());
                batchHelper.queue(rewriteObjectWithToken, this);
              } catch (IOException e) {
                innerExceptions.add(e);
              }
            }
          }

          @Override
          public void onFailure(GoogleJsonError e, HttpHeaders responseHeaders) {
            GoogleCloudStorageEventBus.postOnException();
            onCopyFailure(innerExceptions, e, responseHeaders, srcBucketName, srcObjectName);
          }
        });
  }

  /**
   * Performs copy operation using GCS Copy requests
   *
   * @see GoogleCloudStorage#copy(String, List, String, List)
   */
  private void copyInternal(
      BatchHelper batchHelper,
      ConcurrentHashMap.KeySetView<IOException, Boolean> innerExceptions,
      String srcBucketName,
      String srcObjectName,
      long dstContentGeneration,
      String dstBucketName,
      String dstObjectName)
      throws IOException {
    Storage.Objects.Copy copy =
        storage.objects().copy(srcBucketName, srcObjectName, dstBucketName, dstObjectName, null);

    if (dstContentGeneration != StorageResourceId.UNKNOWN_GENERATION_ID) {
      copy.setIfGenerationMatch(dstContentGeneration);
    }

    Storage.Objects.Copy copyObject = initializeRequest(copy, srcBucketName);

    batchHelper.queue(
        copyObject,
        new JsonBatchCallback<>() {
          @Override
          public void onSuccess(StorageObject copyResponse, HttpHeaders responseHeaders) {
            String srcString = StringPaths.fromComponents(srcBucketName, srcObjectName);
            String dstString = StringPaths.fromComponents(dstBucketName, dstObjectName);
            logger.atFiner().log("Successfully copied %s to %s", srcString, dstString);
          }

          @Override
          public void onFailure(GoogleJsonError jsonError, HttpHeaders responseHeaders) {
            GoogleCloudStorageEventBus.postOnException();
            onCopyFailure(
                innerExceptions, jsonError, responseHeaders, srcBucketName, srcObjectName);
          }
        });
  }

  /**
   * Performs move operation using GCS MoveObject requests
   *
   * <p>See {@link GoogleCloudStorage#move(Map<StorageResourceId, StorageResourceId>)}
   */
  private void moveInternal(
      BatchHelper batchHelper,
      ConcurrentHashMap.KeySetView<IOException, Boolean> innerExceptions,
      String bucketName,
      long srcContentGeneration,
      String srcObjectName,
      long dstContentGeneration,
      String dstObjectName)
      throws IOException {
    Storage.Objects.Move moveObject =
        createMoveObjectRequest(
            bucketName, srcObjectName, dstObjectName, srcContentGeneration, dstContentGeneration);

    batchHelper.queue(
        moveObject,
        new JsonBatchCallback<>() {
          @Override
          public void onSuccess(StorageObject moveResponse, HttpHeaders responseHeaders) {
            String srcString = StringPaths.fromComponents(bucketName, srcObjectName);
            String dstString = StringPaths.fromComponents(bucketName, dstObjectName);
            logger.atFiner().log("Successfully moved %s to %s", srcString, dstString);
          }

          @Override
          public void onFailure(GoogleJsonError jsonError, HttpHeaders responseHeaders) {
            GoogleCloudStorageEventBus.postOnException();
            GoogleJsonResponseException cause =
                createJsonResponseException(jsonError, responseHeaders);
            innerExceptions.add(
                errorExtractor.itemNotFound(cause)
                    ? createFileNotFoundException(bucketName, srcObjectName, cause)
                    : new IOException(
                        String.format(
                            "Error moving '%s' to '%s'",
                            StringPaths.fromComponents(bucketName, srcObjectName),
                            StringPaths.fromComponents(bucketName, dstObjectName)),
                        cause));
          }
        });
  }

  /** Creates a {@link Storage.Objects.Move} request, configured with generation matches. */
  private Storage.Objects.Move createMoveObjectRequest(
      String bucketName,
      String srcObjectName,
      String dstObjectName,
      long srcContentGeneration,
      long dstContentGeneration)
      throws IOException {
    Storage.Objects.Move move = storage.objects().move(bucketName, srcObjectName, dstObjectName);

    if (srcContentGeneration != StorageResourceId.UNKNOWN_GENERATION_ID) {
      move.setIfSourceGenerationMatch(srcContentGeneration);
    }

    if (dstContentGeneration != StorageResourceId.UNKNOWN_GENERATION_ID) {
      move.setIfGenerationMatch(dstContentGeneration);
    }
    return initializeRequest(move, bucketName);
  }

  /** Processes failed copy requests */
  private void onCopyFailure(
      ConcurrentHashMap.KeySetView<IOException, Boolean> innerExceptions,
      GoogleJsonError jsonError,
      HttpHeaders responseHeaders,
      String srcBucketName,
      String srcObjectName) {
    GoogleJsonResponseException cause = createJsonResponseException(jsonError, responseHeaders);
    innerExceptions.add(
        errorExtractor.itemNotFound(cause)
            ? createFileNotFoundException(srcBucketName, srcObjectName, cause)
            : new IOException(
                String.format(
                    "Error copying '%s'", StringPaths.fromComponents(srcBucketName, srcObjectName)),
                cause));
  }

  /**
   * Shared helper for actually dispatching buckets().list() API calls and accumulating paginated
   * results; these can then be used to either extract just their names, or to parse into full
   * GoogleCloudStorageItemInfos.
   */
  private List<Bucket> listBucketsInternal() throws IOException {
    logger.atFiner().log("listBucketsInternal()");
    checkNotNull(storageOptions.getProjectId(), "projectId must not be null");
    List<Bucket> allBuckets = new ArrayList<>();
    Storage.Buckets.List listBucket =
        initializeRequest(storage.buckets().list(storageOptions.getProjectId()), null);

    // Set number of items to retrieve per call.
    listBucket.setMaxResults((long) storageOptions.getMaxListItemsPerCall());

    // Loop till we fetch all items.
    String pageToken = null;
    int page = 0;
    do {
      page = page + 1;
      if (pageToken != null) {
        logger.atFiner().log("listBucketsInternal: next page %s", pageToken);
        listBucket.setPageToken(pageToken);
      }

      try (ITraceOperation op =
          TraceOperation.addToExistingTrace(String.format("gcs.buckets.list(page=%d)", page))) {
        Buckets items = listBucket.execute();

        op.annotate("resultSize", items == null ? 0 : items.size());
        // Accumulate buckets (if any).
        List<Bucket> buckets = items.getItems();
        if (buckets != null) {
          logger.atFiner().log("listed %s items", buckets.size());
          allBuckets.addAll(buckets);
        }

        pageToken = items.getNextPageToken();
      }
    } while (pageToken != null);

    return allBuckets;
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
   * Helper for creating a Storage.Objects.Copy object ready for dispatch given a bucket and object
   * for an empty object to be created. Caller must already verify that {@code resourceId}
   * represents a StorageObject and not a bucket.
   */
  private Storage.Objects.Insert prepareEmptyInsert(
      StorageResourceId resourceId, CreateObjectOptions createObjectOptions) throws IOException {
    Map<String, String> rewrittenMetadata = encodeMetadata(createObjectOptions.getMetadata());
    StorageObject object =
        new StorageObject()
            .setName(resourceId.getObjectName())
            .setMetadata(rewrittenMetadata)
            .setContentEncoding(createObjectOptions.getContentEncoding());

    // Ideally we'd use EmptyContent, but Storage requires an AbstractInputStreamContent and not
    // just an HttpContent, so we'll just use the next easiest thing.
    ByteArrayContent emptyContent =
        new ByteArrayContent(createObjectOptions.getContentType(), new byte[0]);
    Storage.Objects.Insert insertObject =
        initializeRequest(
            storage.objects().insert(resourceId.getBucketName(), object, emptyContent),
            resourceId.getBucketName());
    insertObject.setDisableGZipContent(true);
    clientRequestHelper.setDirectUploadEnabled(insertObject, true);

    if (resourceId.hasGenerationId()) {
      insertObject.setIfGenerationMatch(resourceId.getGenerationId());
    } else if (resourceId.isDirectory() || !createObjectOptions.isOverwriteExisting()) {
      insertObject.setIfGenerationMatch(0L);
    }
    return insertObject;
  }

  private List<StorageObject> listStorageObjects(
      String bucketName, String startOffset, ListObjectOptions listOptions) throws IOException {
    logger.atFiner().log("listStorageObjects(%s, %s, %s)", bucketName, startOffset, listOptions);
    checkArgument(
        listOptions.getDelimiter() == null,
        "Delimiter shouldn't be used while listing objects starting from an offset");

    long maxResults =
        listOptions.getMaxResults() > 0 ? listOptions.getMaxResults() : LIST_MAX_RESULTS;

    Storage.Objects.List listObject =
        createListRequest(
            bucketName,
            /* objectNamePrefix */ null,
            startOffset,
            listOptions.getFields(),
            /* delimiter */ null,
            maxResults,
            listOptions.isIncludeFoldersAsPrefixes());
    String pageToken = null;
    LinkedList<StorageObject> listedObjects = new LinkedList<>();
    // paginated call is required because we may filter all the items, if they are "directory"
    // Avoid calling another list API as soon as we have some files listed.
    int page = 0;
    do {
      page += 1;
      if (pageToken != null) {
        logger.atFiner().log(
            "listStorageObjects: next page %s, listedObjects: %d", pageToken, listedObjects.size());
        listObject.setPageToken(pageToken);
      }
      pageToken =
          listStorageObjectsFilteredPage(
              listObject, listOptions, listedObjects, Integer.toString(page));
    } while (pageToken != null && listedObjects.size() == 0);

    return listedObjects;
  }

  /**
   * Helper for both listObjectInfo that executes the actual API calls to get paginated lists,
   * accumulating the StorageObjects and String prefixes into the params {@code listedObjects} and
   * {@code listedPrefixes}.
   *
   * @param bucketName bucket name
   * @param objectNamePrefix object name prefix or null if all objects in the bucket are desired
   * @param listOptions options to use when listing objects
   * @param listedObjects output parameter into which retrieved StorageObjects will be added
   * @param listedPrefixes output parameter into which retrieved prefixes will be added
   */
  private void listStorageObjectsAndPrefixes(
      String bucketName,
      String objectNamePrefix,
      ListObjectOptions listOptions,
      List<StorageObject> listedObjects,
      List<String> listedPrefixes)
      throws IOException {
    logger.atFiner().log(
        "listStorageObjectsAndPrefixes(%s, %s, %s)", bucketName, objectNamePrefix, listOptions);

    checkArgument(
        listedObjects != null && listedObjects.isEmpty(),
        "Must provide a non-null empty container for listedObjects.");
    checkArgument(
        listedPrefixes != null && listedPrefixes.isEmpty(),
        "Must provide a non-null empty container for listedPrefixes.");

    // List +1 object if prefix is not included in the result, because GCS always includes prefix
    // object in the result if it exists, and we filter it out.
    //
    // Example:
    //
    // Existing GCS objects:
    //   gs://bucket/a/
    //   gs://bucket/a/b
    //   gs://bucket/a/c
    //
    // In response to `gs://bucket/a/` list request with max results set to `1` GCS will return only
    // `gs://bucket/a/` object. But this object will be filtered out from response if
    // `isIncludePrefix` is set to `false`.
    //
    // To prevent this situation we increment max results by 1, which will allow to list
    // `gs://bucket/a/b` in the above case.
    long maxResults =
        listOptions.getMaxResults() > 0
            ? listOptions.getMaxResults() + (listOptions.isIncludePrefix() ? 0 : 1)
            : listOptions.getMaxResults();

    Storage.Objects.List listObject =
        createListRequest(
            bucketName,
            objectNamePrefix,
            /* startOffset */ null,
            listOptions.getFields(),
            listOptions.getDelimiter(),
            maxResults,
            listOptions.isIncludeFoldersAsPrefixes());

    String pageToken = null;
    int page = 0;
    do {
      page += 1;
      if (pageToken != null) {
        logger.atFiner().log("listStorageObjectsAndPrefixes: next page %s", pageToken);
        listObject.setPageToken(pageToken);
      }
      pageToken =
          listStorageObjectsAndPrefixesPage(
              listObject, listOptions, listedObjects, listedPrefixes, Integer.toString(page));
    } while (pageToken != null
        && getMaxRemainingResults(listOptions.getMaxResults(), listedPrefixes, listedObjects) > 0);
  }

  /*
   * Helper function to list files which are lexicographically higher than the offset (inclusive).
   * It strictly expects no delimiter to be provided.
   * It also filters out all the objects which are "directories"
   */
  private String listStorageObjectsFilteredPage(
      Storage.Objects.List listObject,
      ListObjectOptions listOptions,
      List<StorageObject> listedObjects,
      String pageContext)
      throws IOException {
    logger.atFiner().log("listStorageObjectsPage(%s, %s)", listObject, listOptions);
    checkNotNull(listedObjects, "Must provide a non-null container for listedObjects.");
    // We don't want any prefixes[] and filter the objects which are "directories" manually.
    checkArgument(
        listObject.getDelimiter() == null,
        "Delimiter shouldn't be set while listing object from an offset");

    Objects response = executeListCall(listObject, pageContext);
    if (response == null || response.getItems() == null) {
      return null;
    }
    /* filter the objects which are directory */
    for (StorageObject object : response.getItems()) {
      if (!object.getName().endsWith(PATH_DELIMITER)) {
        listedObjects.add(object);
      }
    }
    return response.getNextPageToken();
  }

  @Nullable
  private String listStorageObjectsAndPrefixesPage(
      Storage.Objects.List listObject,
      ListObjectOptions listOptions,
      List<StorageObject> listedObjects,
      List<String> listedPrefixes,
      String pageContext)
      throws IOException {
    logger.atFiner().log("listStorageObjectsAndPrefixesPage(%s, %s)", listObject, listOptions);

    checkNotNull(listedObjects, "Must provide a non-null container for listedObjects.");
    checkNotNull(listedPrefixes, "Must provide a non-null container for listedPrefixes.");

    // Deduplicate prefixes and items, because if 'includeTrailingDelimiter' set to true
    // then returned items will contain "prefix objects" too.
    Set<String> prefixes = new LinkedHashSet<>(listedPrefixes);
    Objects items;
    try {
      items = executeListCall(listObject, pageContext);
    } catch (IOException e) {
      GoogleCloudStorageEventBus.postOnException();
      String resource = StringPaths.fromComponents(listObject.getBucket(), listObject.getPrefix());
      if (errorExtractor.itemNotFound(e)) {
        logger.atFiner().withCause(e).log(
            "listStorageObjectsAndPrefixesPage(%s, %s): item not found", resource, listOptions);
        return null;
      }

      GoogleCloudStorageEventBus.postOnException();
      throw new IOException("Error listing " + resource, e);
    }

    // Add prefixes (if any).
    List<String> pagePrefixes = items.getPrefixes();
    if (pagePrefixes != null) {
      logger.atFiner().log(
          "listStorageObjectsAndPrefixesPage(%s, %s): listed %s prefixes",
          listObject, listOptions, pagePrefixes.size());
      long maxRemainingResults =
          getMaxRemainingResults(listOptions.getMaxResults(), prefixes, listedObjects);
      // Do not cast 'maxRemainingResults' to int here, it could overflow
      long maxPrefixes = min(maxRemainingResults, pagePrefixes.size());
      prefixes.addAll(pagePrefixes.subList(0, (int) maxPrefixes));
    }

    // Add object names (if any).
    List<StorageObject> objects = items.getItems();
    if (objects != null) {
      logger.atFiner().log(
          "listStorageObjectsAndPrefixesPage(%s, %s): listed %d objects",
          listObject, listOptions, objects.size());

      // Although GCS does not implement a file system, it treats objects that end
      // in delimiter as different from other objects when listing objects.
      //
      // If caller sends foo/ as the prefix, foo/ is returned as an object name.
      // That is inconsistent with listing items in a directory.
      // Not sure if that is a bug in GCS or the intended behavior.
      //
      // In this case, we do not want foo/ in the returned list because we want to
      // keep the behavior more like a file system without calling it as such.
      // Therefore, we filter out such entry.

      // Determine if the caller sent a directory name as a prefix.
      String objectNamePrefix = listObject.getPrefix();
      boolean objectPrefixEndsWithDelimiter =
          !isNullOrEmpty(objectNamePrefix) && objectNamePrefix.endsWith(PATH_DELIMITER);

      long maxRemainingResults =
          getMaxRemainingResults(listOptions.getMaxResults(), prefixes, listedObjects);
      for (StorageObject object : objects) {
        if (!objectPrefixEndsWithDelimiter || !object.getName().equals(objectNamePrefix)) {
          if (prefixes.remove(object.getName())) {
            listedObjects.add(object);
          } else if (maxRemainingResults > 0) {
            listedObjects.add(object);
            maxRemainingResults--;
          }
          // Do not break here, because we want to be sure
          // that we replaced all prefixes with prefix objects
        } else if (listOptions.isIncludePrefix() && object.getName().equals(objectNamePrefix)) {
          checkState(
              listedObjects.isEmpty(), "prefix object should be the first object in the result");
          listedObjects.add(object);
        }
      }
    }

    listedPrefixes.clear();
    listedPrefixes.addAll(prefixes);

    return items.getNextPageToken();
  }

  private Objects executeListCall(Storage.Objects.List listObject, String pageContext)
      throws IOException {
    try (ITraceOperation op =
        TraceOperation.addToExistingTrace(
            String.format("gcs.objects.list(page=%s)", pageContext))) {
      Objects items =
          ResilientOperation.retry(
              listObject::execute,
              backOffFactory.newBackOff(),
              (e) -> {
                // Retry on Rate Limit
                if (errorExtractor.rateLimited(e)) {
                  return true;
                }
                // Retry on Network Errors (SocketException)
                // Ignore 4xx errors (GoogleJsonResponseException)
                if (e instanceof IOException && !(e instanceof GoogleJsonResponseException)) {
                  return true;
                }
                return false;
              },
              IOException.class,
              sleeper);
      op.annotate("resultSize", items == null ? 0 : items.size());
      return items;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Thread interrupted while listing objects", e);
    }
  }

  private Storage.Objects.List createListRequest(
      String bucketName,
      String objectNamePrefix,
      String startOffset,
      String objectFields,
      String delimiter,
      long maxResults,
      boolean includeFoldersAsPrefixes)
      throws IOException {
    logger.atFiner().log(
        "createListRequest(%s, %s, %s, %s, %d, %b)",
        bucketName,
        objectNamePrefix,
        objectFields,
        delimiter,
        maxResults,
        includeFoldersAsPrefixes);
    checkArgument(!isNullOrEmpty(bucketName), "bucketName must not be null or empty");

    Storage.Objects.List listObject =
        initializeRequest(
            storage
                .objects()
                .list(bucketName)
                .setStartOffset(emptyToNull(startOffset))
                .setPrefix(emptyToNull(objectNamePrefix)),
            bucketName);

    // Set delimiter if supplied.
    if (delimiter != null) {
      listObject.setDelimiter(delimiter).setIncludeTrailingDelimiter(true);
    }

    listObject.setIncludeFoldersAsPrefixes(includeFoldersAsPrefixes);

    // Set number of items to retrieve per call.
    listObject.setMaxResults(
        maxResults <= 0 || maxResults >= storageOptions.getMaxListItemsPerCall()
            ? storageOptions.getMaxListItemsPerCall()
            : maxResults);

    // Request only fields used in GoogleCloudStorageItemInfo:
    // https://cloud.google.com/storage/docs/json_api/v1/objects#resource-representations
    if (!isNullOrEmpty(objectFields)) {
      listObject.setFields(String.format("items(%s),prefixes,nextPageToken", objectFields));
    }

    return listObject;
  }

  private static long getMaxRemainingResults(
      long maxResults, Collection<String> prefixes, List<StorageObject> objects) {
    if (maxResults <= 0) {
      return Long.MAX_VALUE;
    }
    long numResults = (long) prefixes.size() + objects.size();
    return maxResults - numResults;
  }

  /**
   * @see GoogleCloudStorage#listObjectInfo(String, String, ListObjectOptions)
   */
  @Override
  public List<GoogleCloudStorageItemInfo> listObjectInfo(
      String bucketName, String objectNamePrefix, ListObjectOptions listOptions)
      throws IOException {
    logger.atFiner().log("listObjectInfo(%s, %s, %s)", bucketName, objectNamePrefix, listOptions);

    // Helper will handle going through pages of list results and accumulating them.
    List<StorageObject> listedObjects = new ArrayList<>();
    List<String> listedPrefixes = new ArrayList<>();
    listStorageObjectsAndPrefixes(
        bucketName, objectNamePrefix, listOptions, listedObjects, listedPrefixes);

    return getGoogleCloudStorageItemInfos(
        bucketName, objectNamePrefix, listOptions, listedPrefixes, listedObjects);
  }

  @Override
  public List<GoogleCloudStorageItemInfo> listObjectInfoStartingFrom(
      String bucketName, String startOffset, ListObjectOptions listOptions) throws IOException {
    logger.atFiner().log(
        "listObjectInfoStartingFrom(%s, %s, %s)", bucketName, startOffset, listOptions);
    checkArgument(
        listOptions.getFields() != null && listOptions.getFields().contains("name"),
        "Name is a required field for listing files, provided fields %s",
        listOptions.getFields());
    try {
      List<StorageObject> listedObjects = listStorageObjects(bucketName, startOffset, listOptions);
      return getGoogleCloudStorageItemInfos(
          bucketName,
          /* objectNamePrefix= */ null,
          listOptions,
          Collections.EMPTY_LIST,
          listedObjects);
    } catch (Exception e) {
      throw new IOException(
          String.format(
              "Having issue while listing files from offset: %s for bucket: %s and options: %s",
              startOffset, bucketName, listOptions),
          e);
    }
  }

  /**
   * @see GoogleCloudStorage#listObjectInfoPage(String, String, ListObjectOptions, String)
   */
  @Override
  public ListPage<GoogleCloudStorageItemInfo> listObjectInfoPage(
      String bucketName, String objectNamePrefix, ListObjectOptions listOptions, String pageToken)
      throws IOException {
    logger.atFiner().log(
        "listObjectInfoPage(%s, %s, %s, %s)", bucketName, objectNamePrefix, listOptions, pageToken);

    checkArgument(
        listOptions.getMaxResults() == MAX_RESULTS_UNLIMITED,
        "maxResults should be unlimited for 'listObjectInfoPage' call, but was %s",
        listOptions.getMaxResults());

    Storage.Objects.List listObject =
        createListRequest(
            bucketName,
            objectNamePrefix,
            /* startOffset */ null,
            listOptions.getFields(),
            listOptions.getDelimiter(),
            listOptions.getMaxResults(),
            listOptions.isIncludeFoldersAsPrefixes());
    if (pageToken != null) {
      logger.atFiner().log("listObjectInfoPage: next page %s", pageToken);
      listObject.setPageToken(pageToken);
    }

    // Helper will handle going through pages of list results and accumulating them.
    List<StorageObject> listedObjects = new ArrayList<>();
    List<String> listedPrefixes = new ArrayList<>();
    String nextPageToken =
        listStorageObjectsAndPrefixesPage(
            listObject, listOptions, listedObjects, listedPrefixes, "");
    List<GoogleCloudStorageItemInfo> objectInfos =
        getGoogleCloudStorageItemInfos(
            bucketName, objectNamePrefix, listOptions, listedPrefixes, listedObjects);
    return new ListPage<>(objectInfos, nextPageToken);
  }

  /**
   * @see GoogleCloudStorage#listFolderInfoForPrefixPage(String, String, ListFolderOptions, String)
   */
  @Override
  public ListPage<FolderInfo> listFolderInfoForPrefixPage(
      String bucketName,
      String objectNamePrefix,
      ListFolderOptions listFolderOptions,
      String pageToken)
      throws IOException {
    logger.atFiner().log(
        "listFolderInfoForPrefixPage(%s, %s, %s, %s)",
        bucketName, objectNamePrefix, listFolderOptions, pageToken);

    ListFoldersRequest.Builder listFoldersRequest =
        createFolderListRequest(bucketName, objectNamePrefix, listFolderOptions, pageToken);

    if (!isNullOrEmpty(pageToken)) {
      logger.atFiner().log("listFolderInfoForPrefixPage: next page %s", pageToken);
      listFoldersRequest.setPageToken(pageToken);
    }

    List<FolderInfo> listedFolders = new LinkedList<>();
    String nextPageToken =
        listStorageFoldersAndPrefixesPage(listFoldersRequest.build(), listedFolders);
    while (!isNullOrEmpty(nextPageToken)) {
      nextPageToken =
          listStorageFoldersAndPrefixesPage(
              listFoldersRequest.setPageToken(nextPageToken).build(), listedFolders);
    }

    return new ListPage<>(listedFolders, nextPageToken);
  }

  private ListFoldersRequest.Builder createFolderListRequest(
      String bucketName,
      String objectNamePrefix,
      ListFolderOptions listFolderOptions,
      String pageToken) {
    logger.atFiner().log(
        "createListFolderRequest(%s, %s, %s, %s)",
        bucketName, objectNamePrefix, listFolderOptions, pageToken);
    checkArgument(!isNullOrEmpty(bucketName), "bucketName must not be null or empty");

    ListFoldersRequest.Builder request =
        ListFoldersRequest.newBuilder()
            .setPageSize(listFolderOptions.getPageSize())
            .setParent(BUCKET_PREFIX + bucketName);

    if (!Strings.isNullOrEmpty(objectNamePrefix)) {
      request.setPrefix(objectNamePrefix);
    }
    return request;
  }

  private String listStorageFoldersAndPrefixesPage(
      ListFoldersRequest listFoldersRequest, List<FolderInfo> listedFolder) throws IOException {
    checkNotNull(listedFolder, "Must provide a non-null container for listedFolder.");

    ListFoldersPagedResponse listFolderRespose =
        lazyGetStorageControlClient().listFolders(listFoldersRequest);
    try (ITraceOperation op = TraceOperation.addToExistingTrace("gcs.folders.list")) {
      Iterator<Folder> itemsIterator = listFolderRespose.getPage().getValues().iterator();
      while (itemsIterator.hasNext()) {
        listedFolder.add(new FolderInfo(itemsIterator.next()));
      }
      op.annotate("resultSize", itemsIterator == null ? 0 : listedFolder.size());
    }

    logger.atFiner().log(
        "listFolders(%s): listed %d objects", listFoldersRequest, listedFolder.size());
    return listFolderRespose.getNextPageToken();
  }

  private List<GoogleCloudStorageItemInfo> getGoogleCloudStorageItemInfos(
      String bucketName,
      String objectNamePrefix,
      ListObjectOptions listOptions,
      List<String> listedPrefixes,
      List<StorageObject> listedObjects) {
    List<GoogleCloudStorageItemInfo> objectInfos =
        // Size to accommodate inferred directories for listed prefixes and prefix object
        new ArrayList<>(listedPrefixes.size() + listedObjects.size() + 1);

    // Create inferred directory for the prefix object if necessary
    if (listOptions.isIncludePrefix()
        // Only add an inferred directory for non-null prefix name
        && objectNamePrefix != null
        // Only add an inferred directory if listing in directory mode (non-flat listing)
        && listOptions.getDelimiter() != null
        // Only add an inferred directory if listed any prefixes or objects, i.e. prefix "exists"
        && (!listedPrefixes.isEmpty() || !listedObjects.isEmpty())
        // Only add an inferred directory if prefix object is not listed already
        && (listedObjects.isEmpty() || !listedObjects.get(0).getName().equals(objectNamePrefix))) {
      objectInfos.add(createInferredDirectory(new StorageResourceId(bucketName, objectNamePrefix)));
    }

    // For the listedObjects, we simply parse each item into a GoogleCloudStorageItemInfo without
    // further work.
    for (StorageObject obj : listedObjects) {
      objectInfos.add(createItemInfoForStorageObject(obj));
    }

    handlePrefixes(bucketName, listedPrefixes, objectInfos);

    objectInfos.sort(Comparator.comparing(GoogleCloudStorageItemInfo::getObjectName));

    return objectInfos;
  }

  /** Handle prefixes without prefix objects. */
  private void handlePrefixes(
      String bucketName, List<String> prefixes, List<GoogleCloudStorageItemInfo> objectInfos) {
    for (String prefix : prefixes) {
      objectInfos.add(createInferredDirectory(new StorageResourceId(bucketName, prefix)));
    }
  }

  /** Helper for converting a StorageResourceId + Bucket into a GoogleCloudStorageItemInfo. */
  public static GoogleCloudStorageItemInfo createItemInfoForBucket(
      StorageResourceId resourceId, Bucket bucket) {
    checkArgument(resourceId != null, "resourceId must not be null");
    checkArgument(bucket != null, "bucket must not be null");
    checkArgument(resourceId.isBucket(), "resourceId must be a Bucket. resourceId: %s", resourceId);
    checkArgument(
        resourceId.getBucketName().equals(bucket.getName()),
        "resourceId.getBucketName() must equal bucket.getName(): '%s' vs '%s'",
        resourceId.getBucketName(),
        bucket.getName());

    // For buckets, size is 0.
    return GoogleCloudStorageItemInfo.createBucket(
        resourceId,
        bucket.getTimeCreated().getValue(),
        bucket.getUpdated().getValue(),
        bucket.getLocation(),
        bucket.getStorageClass());
  }

  public static GoogleCloudStorageItemInfo createItemInfoForStorageObject(StorageObject object) {
    checkNotNull(object, "object must not be null");
    checkArgument(!isNullOrEmpty(object.getBucket()), "object must have a bucket: %s", object);
    checkArgument(!isNullOrEmpty(object.getName()), "object must have a name: %s", object);
    return createItemInfoForStorageObject(
        new StorageResourceId(object.getBucket(), object.getName()), object);
  }

  /**
   * Helper for converting a StorageResourceId + StorageObject into a GoogleCloudStorageItemInfo.
   */
  public static GoogleCloudStorageItemInfo createItemInfoForStorageObject(
      StorageResourceId resourceId, StorageObject object) {
    checkArgument(resourceId != null, "resourceId must not be null");
    checkArgument(object != null, "object must not be null");
    checkArgument(
        resourceId.isStorageObject(),
        "resourceId must be a StorageObject. resourceId: %s",
        resourceId);
    checkArgument(
        resourceId.getBucketName().equals(object.getBucket()),
        "resourceId.getBucketName() must equal object.getBucket(): '%s' vs '%s'",
        resourceId.getBucketName(),
        object.getBucket());
    checkArgument(
        resourceId.getObjectName().equals(object.getName()),
        "resourceId.getObjectName() must equal object.getName(): '%s' vs '%s'",
        resourceId.getObjectName(),
        object.getName());

    Map<String, byte[]> decodedMetadata =
        object.getMetadata() == null ? null : decodeMetadata(object.getMetadata());

    byte[] md5Hash = null;
    byte[] crc32c = null;

    if (!isNullOrEmpty(object.getCrc32c())) {
      crc32c = BaseEncoding.base64().decode(object.getCrc32c());
    }

    if (!isNullOrEmpty(object.getMd5Hash())) {
      md5Hash = BaseEncoding.base64().decode(object.getMd5Hash());
    }

    return GoogleCloudStorageItemInfo.createObject(
        resourceId,
        object.getTimeCreated() == null ? 0 : object.getTimeCreated().getValue(),
        object.getUpdated() == null ? 0 : object.getUpdated().getValue(),
        object.getSize() == null ? 0 : object.getSize().longValue(),
        object.getContentType(),
        object.getContentEncoding(),
        decodedMetadata,
        object.getGeneration() == null ? 0 : object.getGeneration(),
        object.getMetageneration() == null ? 0 : object.getMetageneration(),
        new VerificationAttributes(md5Hash, crc32c));
  }

  /**
   * Helper for converting from a Map&lt;String, byte[]&gt; metadata map that may be in a
   * StorageObject into a Map&lt;String, String&gt; suitable for placement inside a
   * GoogleCloudStorageItemInfo.
   */
  @VisibleForTesting
  static Map<String, String> encodeMetadata(Map<String, byte[]> metadata) {
    return Maps.transformValues(metadata, GoogleCloudStorageImpl::encodeMetadataValues);
  }

  /** Inverse function of {@link #encodeMetadata(Map)}. */
  @VisibleForTesting
  static Map<String, byte[]> decodeMetadata(Map<String, String> metadata) {
    return Maps.transformValues(metadata, GoogleCloudStorageImpl::decodeMetadataValues);
  }

  /** See {@link GoogleCloudStorage#getItemInfos(List)} for details about expected behavior. */
  @Override
  public List<GoogleCloudStorageItemInfo> getItemInfos(List<StorageResourceId> resourceIds)
      throws IOException {
    logger.atFiner().log("getItemInfos(%s)", resourceIds);

    if (resourceIds.isEmpty()) {
      return new ArrayList<>();
    }

    Map<StorageResourceId, GoogleCloudStorageItemInfo> itemInfos = new ConcurrentHashMap<>();
    Set<IOException> innerExceptions = newConcurrentHashSet();
    String traceContext = String.format("batchGetItemInfos(size=%s)", resourceIds.size());
    try (ITraceOperation to = TraceOperation.addToExistingTrace(traceContext)) {
      BatchHelper batchHelper =
          batchFactory.newBatchHelper(
              httpRequestInitializer,
              storage,
              storageOptions.getMaxRequestsPerBatch(),
              resourceIds.size(),
              storageOptions.getBatchThreads(),
              traceContext);

      // For each resourceId, we'll either directly add ROOT_INFO, enqueue a Bucket fetch request,
      // or enqueue a StorageObject fetch request.
      for (StorageResourceId resourceId : resourceIds) {
        if (resourceId.isRoot()) {
          itemInfos.put(resourceId, GoogleCloudStorageItemInfo.ROOT_INFO);
        } else if (resourceId.isBucket()) {
          batchHelper.queue(
              initializeRequest(
                  storage.buckets().get(resourceId.getBucketName()), resourceId.getBucketName()),
              new JsonBatchCallback<>() {
                @Override
                public void onSuccess(Bucket bucket, HttpHeaders responseHeaders) {
                  logger.atFiner().log(
                      "getItemInfos: Successfully fetched bucket: %s for resourceId: %s",
                      bucket, resourceId);
                  itemInfos.put(resourceId, createItemInfoForBucket(resourceId, bucket));
                }

                @Override
                public void onFailure(GoogleJsonError jsonError, HttpHeaders responseHeaders) {
                  GoogleJsonResponseException cause =
                      createJsonResponseException(jsonError, responseHeaders);
                  if (errorExtractor.itemNotFound(cause)) {
                    logger.atFiner().log(
                        "getItemInfos: bucket '%s' not found:%n%s",
                        resourceId.getBucketName(), jsonError);
                    itemInfos.put(
                        resourceId, GoogleCloudStorageItemInfo.createNotFound(resourceId));
                  } else {
                    innerExceptions.add(
                        new IOException(
                            String.format("Error getting '%s' bucket", resourceId.getBucketName()),
                            cause));
                  }
                }
              });
        } else {
          String bucketName = resourceId.getBucketName();
          String objectName = resourceId.getObjectName();
          batchHelper.queue(
              initializeRequest(
                      storageRequestFactory.objectsGetMetadata(bucketName, objectName), bucketName)
                  // Request only fields used in GoogleCloudStorageItemInfo:
                  // https://cloud.google.com/storage/docs/json_api/v1/objects#resource-representations
                  .setFields(OBJECT_FIELDS),
              new JsonBatchCallback<>() {
                @Override
                public void onSuccess(StorageObject obj, HttpHeaders responseHeaders) {
                  logger.atFiner().log(
                      "getItemInfos: Successfully fetched object '%s' for resourceId '%s'",
                      obj, resourceId);
                  itemInfos.put(resourceId, createItemInfoForStorageObject(resourceId, obj));
                }

                @Override
                public void onFailure(GoogleJsonError jsonError, HttpHeaders responseHeaders) {
                  GoogleJsonResponseException cause =
                      createJsonResponseException(jsonError, responseHeaders);
                  if (errorExtractor.itemNotFound(cause)) {
                    logger.atFiner().log(
                        "getItemInfos: object '%s' not found:%n%s", resourceId, jsonError);
                    itemInfos.put(
                        resourceId, GoogleCloudStorageItemInfo.createNotFound(resourceId));
                  } else {
                    innerExceptions.add(
                        new IOException(
                            String.format("Error getting '%s' object", resourceId), cause));
                  }
                }
              });
        }
      }

      batchHelper.flush();

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

      // We expect the return list to be the same size, even if some entries were "not found".
      checkState(
          sortedItemInfos.size() == resourceIds.size(),
          "sortedItemInfos.size() (%s) != resourceIds.size() (%s). infos: %s, ids: %s",
          sortedItemInfos.size(),
          resourceIds.size(),
          sortedItemInfos,
          resourceIds);
      return sortedItemInfos;
    }
  }

  @Override
  public List<GoogleCloudStorageItemInfo> updateItems(List<UpdatableItemInfo> itemInfoList)
      throws IOException {
    logger.atFiner().log("updateItems(%s)", itemInfoList);

    if (itemInfoList.isEmpty()) {
      return new ArrayList<>();
    }

    Map<StorageResourceId, GoogleCloudStorageItemInfo> resultItemInfos = new ConcurrentHashMap<>();
    Set<IOException> innerExceptions = newConcurrentHashSet();
    String traceContext = String.format("batchUpdateItems(size=%s)", itemInfoList.size());
    try (ITraceOperation to = TraceOperation.addToExistingTrace(traceContext)) {
      BatchHelper batchHelper =
          batchFactory.newBatchHelper(
              httpRequestInitializer,
              storage,
              storageOptions.getMaxRequestsPerBatch(),
              itemInfoList.size(),
              storageOptions.getBatchThreads(),
              "batchUpdateItems");

      for (UpdatableItemInfo itemInfo : itemInfoList) {
        checkArgument(
            !itemInfo.getStorageResourceId().isBucket()
                && !itemInfo.getStorageResourceId().isRoot(),
            "Buckets and GCS Root resources are not supported for updateItems");
      }

      for (UpdatableItemInfo itemInfo : itemInfoList) {
        StorageResourceId resourceId = itemInfo.getStorageResourceId();
        String bucketName = resourceId.getBucketName();
        String objectName = resourceId.getObjectName();

        Map<String, byte[]> originalMetadata = itemInfo.getMetadata();
        Map<String, String> rewrittenMetadata = encodeMetadata(originalMetadata);

        Storage.Objects.Patch patch =
            initializeRequest(
                storage
                    .objects()
                    .patch(
                        bucketName, objectName, new StorageObject().setMetadata(rewrittenMetadata)),
                bucketName);

        batchHelper.queue(
            patch,
            new JsonBatchCallback<>() {
              @Override
              public void onSuccess(StorageObject obj, HttpHeaders responseHeaders) {
                logger.atFiner().log(
                    "updateItems: Successfully updated object '%s' for resourceId '%s'",
                    obj, resourceId);
                resultItemInfos.put(resourceId, createItemInfoForStorageObject(resourceId, obj));
              }

              @Override
              public void onFailure(GoogleJsonError jsonError, HttpHeaders responseHeaders) {
                GoogleJsonResponseException cause =
                    createJsonResponseException(jsonError, responseHeaders);
                if (errorExtractor.itemNotFound(cause)) {
                  logger.atFiner().log(
                      "updateItems: object not found %s:%n%s", resourceId, jsonError);
                  resultItemInfos.put(
                      resourceId, GoogleCloudStorageItemInfo.createNotFound(resourceId));
                } else {
                  innerExceptions.add(
                      new IOException(
                          String.format("Error updating '%s' object", resourceId), cause));
                }
              }
            });
      }
      batchHelper.flush();

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

    // Determine object size.
    //
    // For buckets, size is 0.
    // For objects not found, size is -1.
    // For objects that exist, size is in number of bytes.
    if (resourceId.isBucket()) {
      Bucket bucket = getBucket(resourceId.getBucketName());
      if (bucket != null) {
        itemInfo = createItemInfoForBucket(resourceId, bucket);
      }
    } else {
      StorageObject object = getObject(resourceId);
      if (object != null) {
        itemInfo = createItemInfoForStorageObject(resourceId, object);
      }
    }

    if (itemInfo == null) {
      itemInfo = GoogleCloudStorageItemInfo.createNotFound(resourceId);
    }
    logger.atFiner().log("getItemInfo: %s", itemInfo);
    return itemInfo;
  }

  /** See {@link GoogleCloudStorage#close()} for details about expected behavior. */
  @Override
  public void close() {
    // Calling shutdown() is a no-op if it was already called earlier,
    // therefore no need to guard against that by setting threadPool to null.
    logger.atFiner().log("close()");
    try {
      // TODO: add try-catch around each shutdown() call to make sure
      //  that all resources are shut down
      backgroundTasksThreadPool.shutdown();
      manualBatchingThreadPool.shutdown();
    } finally {
      backgroundTasksThreadPool = null;
      manualBatchingThreadPool = null;
    }

    if (this.storageControlClient != null) {
      this.storageControlClient.close();
      this.storageControlClient = null;
    }
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
    Storage.Buckets.Get getBucket =
        initializeRequest(storage.buckets().get(bucketName), bucketName);
    try (ITraceOperation op = TraceOperation.addToExistingTrace("gcs.buckets.get")) {
      return getBucket.execute();
    } catch (IOException e) {
      if (errorExtractor.itemNotFound(e)) {
        logger.atFiner().withCause(e).log("getBucket(%s): not found", bucketName);
        return null;
      }
      throw new IOException("Error accessing Bucket " + bucketName, e);
    }
  }

  /**
   * Gets the object generation for a write operation
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

  /**
   * Gets the object with the given resourceId.
   *
   * @param resourceId identifies a StorageObject
   * @return the object with the given name or null if object not found
   * @throws IOException if the object exists but cannot be accessed
   */
  @Nullable
  private StorageObject getObject(StorageResourceId resourceId) throws IOException {
    logger.atFiner().log("getObject(%s)", resourceId);
    checkArgument(
        resourceId.isStorageObject(), "Expected full StorageObject id, got %s", resourceId);
    String bucketName = resourceId.getBucketName();
    String objectName = resourceId.getObjectName();
    Storage.Objects.Get getObject =
        initializeRequest(
                storageRequestFactory.objectsGetMetadata(bucketName, objectName), bucketName)
            // Request only fields used in GoogleCloudStorageItemInfo:
            // https://cloud.google.com/storage/docs/json_api/v1/objects#resource-representations
            .setFields(OBJECT_FIELDS);
    try (ITraceOperation op = TraceOperation.addToExistingTrace("gcs.objects.get")) {
      return getObject.execute();
    } catch (IOException e) {
      if (errorExtractor.itemNotFound(e)) {
        logger.atFiner().withCause(e).log("getObject(%s): not found", resourceId);
        return null;
      }
      GoogleCloudStorageEventBus.postOnException();
      throw new IOException("Error accessing " + resourceId, e);
    }
  }

  /**
   * Helper to check whether an empty object already exists with the expected metadata specified in
   * {@code options}, to be used to determine whether it's safe to ignore an exception that was
   * thrown when trying to create the object, {@code exceptionOnCreate}.
   */
  private boolean canIgnoreExceptionForEmptyObject(
      IOException exceptionOnCreate, StorageResourceId resourceId, CreateObjectOptions options)
      throws IOException {
    // TODO(user): Maybe also add 409 errors if they pop up in this use case.
    // 500 ISE and 503 Service Unavailable tend to be raised when spamming GCS with create requests:
    if (errorExtractor.rateLimited(exceptionOnCreate)
        || errorExtractor.internalServerError(exceptionOnCreate)
        || (resourceId.isDirectory() && errorExtractor.preconditionNotMet(exceptionOnCreate))) {
      // We know that this is an error that is most often associated with trying to create an empty
      // object from multiple workers at the same time. We perform the following assuming that we
      // will eventually succeed and find an existing object. This will add up to a user-defined
      // maximum delay that caller will wait to receive an exception in the case of an incorrect
      // assumption and this being a scenario other than the multiple workers racing situation.
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
            GoogleCloudStorageEventBus.postOnException();
            // We caught an InterruptedException, we should set the interrupted bit on this thread.
            Thread.currentThread().interrupt();
            nextSleep = BackOff.STOP;
          }
        }
        existingInfo = getItemInfo(resourceId);
        nextSleep = nextSleep == BackOff.STOP ? BackOff.STOP : backOff.nextBackOffMillis();
      } while (!existingInfo.exists() && nextSleep != BackOff.STOP);

      // Compare existence, size, and metadata; for 429 errors creating an empty object,
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

  @Override
  public boolean isHnBucket(URI src) throws IOException {
    String bucketName = src.getAuthority();
    Boolean isEnabled = cache.getIfPresent(bucketName);
    if (isEnabled != null) {
      return isEnabled;
    }

    Storage.Buckets.GetStorageLayout request =
        initializeRequest(storage.buckets().getStorageLayout(bucketName), bucketName);
    try (ITraceOperation to = TraceOperation.addToExistingTrace("getStorageLayout.HN")) {
      BucketStorageLayout layout = request.execute();
      BucketStorageLayout.HierarchicalNamespace hierarchicalNamespace =
          layout.getHierarchicalNamespace();
      boolean result = hierarchicalNamespace != null && hierarchicalNamespace.getEnabled();

      logger.atInfo().log("Checking if %s is HN enabled returned %s", src, result);

      cache.put(bucketName, result);

      return result;
    }
  }

  private StorageControlClient lazyGetStorageControlClient() throws IOException {
    if (this.storageControlClient == null) {
      this.storageControlClient =
          StorageControlClient.create(
              StorageControlSettings.newBuilder()
                  .setCredentialsProvider(FixedCredentialsProvider.create(this.credential))
                  .build());
    }

    return this.storageControlClient;
  }

  /**
   * See {@link GoogleCloudStorage#getFolderInfo(StorageResourceId)} for details about expected
   * behavior.
   */
  @Override
  public GoogleCloudStorageItemInfo getFolderInfo(StorageResourceId resourceId) throws IOException {
    logger.atInfo().log("getFolderInfo(%s)", resourceId);

    // If the provided resource ID is for a bucket, retrieve its information directly.
    if (resourceId.isBucket()) {
      Bucket bucket = getBucket(resourceId.getBucketName());
      if (bucket != null) {
        return createItemInfoForBucket(resourceId, bucket);
      }
      return GoogleCloudStorageItemInfo.createNotFound(resourceId);
    }

    GetFolderRequest request =
        GetFolderRequest.newBuilder()
            .setName(FolderName.format("_", resourceId.getBucketName(), resourceId.getObjectName()))
            .build();
    try (ITraceOperation op = TraceOperation.addToExistingTrace("gcs.folders.get")) {
      Folder folder = lazyGetStorageControlClient().getFolder(request);
      return GoogleCloudStorageItemInfo.createFolder(resourceId, folder);
    } catch (Exception e) {
      // Any exception, including NotFound or PermissionDenied, is treated as not found.
      // This is intentional. The primary caller of this method, getFileInfo(),
      // relies on this behavior to trigger its fallback logic which uses a different
      // method (getFileInfoInternal) to check for the file.
      return GoogleCloudStorageItemInfo.createNotFound(resourceId);
    }
  }

  /**
   * See {@link GoogleCloudStorage#createFolder(StorageResourceId,boolean)} for details about
   * expected behavior.
   */
  @Override
  public void createFolder(StorageResourceId resourceId, boolean recursive) throws IOException {
    logger.atInfo().log("createFolder(%s)", resourceId);
    checkArgument(
        resourceId.isStorageObject(), "Expected full StorageObject id, got %s", resourceId);

    String bucketName = resourceId.getBucketName();

    // Format the bucket name into the fully-qualified resource name
    // that the StorageControlClient API requires.
    String parentResourceName = String.format("projects/_/buckets/%s", bucketName);

    // Build the request with the correctly formatted parent name.
    CreateFolderRequest request =
        CreateFolderRequest.newBuilder()
            .setFolderId(resourceId.getObjectName())
            .setParent(parentResourceName)
            .setRecursive(recursive)
            .build();
    // Add the tracing wrapper
    try (ITraceOperation op = TraceOperation.addToExistingTrace("gcs.folders.create")) {
      logger.atFine().log("Create folder: %s%n", resourceId);
      Folder newFolder = lazyGetStorageControlClient().createFolder(request);
      logger.atFine().log("Created folder: %s%n", newFolder.getName());
    } catch (AlreadyExistsException e) {
      GoogleCloudStorageEventBus.postOnException();
      throw (FileAlreadyExistsException)
          new FileAlreadyExistsException(String.format("Folder '%s' already exists.", resourceId))
              .initCause(e);
    } catch (Throwable t) {
      logger.atSevere().withCause(t).log("CreateFolder for %s failed", resourceId);
      throw t;
    }
  }

  @Override
  public void renameHnFolder(URI src, URI dst) throws IOException {
    String bucketName = src.getAuthority();
    String srcFolder = FolderName.of("_", bucketName, src.getPath().substring(1)).toString();
    RenameFolderRequest request =
        RenameFolderRequest.newBuilder()
            .setDestinationFolderId(dst.getPath().substring(1))
            .setName(srcFolder)
            .build();

    try (ITraceOperation to = TraceOperation.addToExistingTrace("renameHnFolder")) {
      logger.atFine().log("Renaming HN folder (%s -> %s)", src, dst);
      lazyGetStorageControlClient().renameFolderOperationCallable().call(request);
    } catch (Throwable t) {
      logger.atSevere().withCause(t).log("Renaming %s to %s failed", src, dst);
      throw t;
    }
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
    List<ComposeRequest.SourceObjects> sourceObjects =
        sources.stream()
            // TODO(user): Maybe set generationIds for source objects as well here.
            .map(input -> new ComposeRequest.SourceObjects().setName(input.getObjectName()))
            .collect(Collectors.toList());
    Storage.Objects.Compose compose =
        initializeRequest(
            storage
                .objects()
                .compose(
                    destination.getBucketName(),
                    destination.getObjectName(),
                    new ComposeRequest()
                        .setSourceObjects(sourceObjects)
                        .setDestination(
                            new StorageObject()
                                .setContentType(options.getContentType())
                                .setContentEncoding(options.getContentEncoding())
                                .setMetadata(encodeMetadata(options.getMetadata())))),
            destination.getBucketName());

    compose.setIfGenerationMatch(
        destination.hasGenerationId()
            ? destination.getGenerationId()
            : getWriteGeneration(destination, /* overwrite= */ true));

    String traceName = String.format("gcs.objects.compose(size=%d)", sources.size());
    try (ITraceOperation op = TraceOperation.addToExistingTrace(traceName)) {
      StorageObject storageObject = compose.execute();
      GoogleCloudStorageItemInfo compositeInfo =
          createItemInfoForStorageObject(destination, storageObject);
      logger.atFiner().log("composeObjects() done, returning: %s", compositeInfo);
      return compositeInfo;
    }
  }

  /**
   * Initialize HTTP request.
   *
   * <p>Initialize the storage access requests. Since access request authorization are done here and
   * right before sending out the HTTP requests, the passed in storage requests should contain as
   * much information as possible. Callers shouldn't modify storage request content like bucket and
   * object path after calling this function.
   *
   * @param request the storage request to be initialized before send out
   * @param bucketName the bucket name the storage request accesses
   * @return the initialized storage request.
   */
  @VisibleForTesting
  <RequestT extends StorageRequest<?>> RequestT initializeRequest(
      RequestT request, String bucketName) {
    if (downscopedAccessTokenFn != null) {
      List<AccessBoundary> accessBoundaries =
          StorageRequestToAccessBoundaryConverter.fromStorageObjectRequest(request);
      String token = downscopedAccessTokenFn.apply(accessBoundaries);
      request.getRequestHeaders().setAuthorization("Bearer " + token);
    }
    return configureRequest(request, bucketName);
  }

  <RequestT extends StorageRequest<?>> RequestT configureRequest(
      RequestT request, String bucketName) {
    setRequesterPaysProject(request, bucketName);

    if (request instanceof Storage.Objects.Get || request instanceof Storage.Objects.Insert) {
      setEncryptionHeaders(request);
    }

    if (request instanceof Storage.Objects.Rewrite || request instanceof Storage.Objects.Copy) {
      setEncryptionHeaders(request);
      setDecryptionHeaders(request);
    }
    addFeatureUsageHeader(request);
    return request;
  }

  private <RequestT extends StorageRequest<?>> void addFeatureUsageHeader(RequestT request) {
    if (featureHeaderGenerator == null) {
      return;
    }
    String featureUsageHeaderString = this.featureHeaderGenerator.getValue();
    if (!Strings.isNullOrEmpty(featureUsageHeaderString)) {
      logger.atFiner().log("Setting feature usage header: %s", featureUsageHeaderString);
      request.getRequestHeaders().set(FeatureHeaderGenerator.HEADER_NAME, featureUsageHeaderString);
    }
  }

  private <RequestT extends StorageRequest<?>> void setEncryptionHeaders(RequestT request) {
    if (storageOptions.getEncryptionKey() == null) {
      return;
    }

    request
        .getRequestHeaders()
        .set(
            "x-goog-encryption-algorithm",
            checkNotNull(
                storageOptions.getEncryptionAlgorithm(), "encryption algorithm must not be null"))
        .set(
            "x-goog-encryption-key",
            checkNotNull(storageOptions.getEncryptionKey(), "encryption key must not be null")
                .value())
        .set(
            "x-goog-encryption-key-sha256",
            checkNotNull(
                    storageOptions.getEncryptionKeyHash(), "encryption key hash must not be null")
                .value());
  }

  private <RequestT extends StorageRequest<?>> void setDecryptionHeaders(RequestT request) {
    if (storageOptions.getEncryptionKey() == null) {
      return;
    }

    request
        .getRequestHeaders()
        .set(
            "x-goog-copy-source-encryption-algorithm",
            checkNotNull(
                storageOptions.getEncryptionAlgorithm(), "encryption algorithm must not be null"))
        .set(
            "x-goog-copy-source-encryption-key",
            checkNotNull(storageOptions.getEncryptionKey(), "encryption key must not be null")
                .value())
        .set(
            "x-goog-copy-source-encryption-key-sha256",
            checkNotNull(
                    storageOptions.getEncryptionKeyHash(), "encryption key hash must not be null")
                .value());
  }

  private <RequestT extends StorageRequest<?>> void setRequesterPaysProject(
      RequestT request, String bucketName) {
    if (requesterShouldPay(bucketName)) {
      setUserProject(request, storageOptions.getRequesterPaysOptions().getProjectId());
    }
  }

  private boolean requesterShouldPay(String bucketName) {
    if (bucketName == null) {
      return false;
    }

    switch (storageOptions.getRequesterPaysOptions().getMode()) {
      case ENABLED:
        return true;
      case CUSTOM:
        return storageOptions.getRequesterPaysOptions().getBuckets().contains(bucketName);
      case AUTO:
        return autoBuckets.getUnchecked(bucketName);
      default:
        return false;
    }
  }

  @VisibleForTesting
  void setStorageControlClient(StorageControlClient storageControlClient) {
    this.storageControlClient = storageControlClient;
  }

  private static <RequestT extends StorageRequest<?>> void setUserProject(
      RequestT request, String projectId) {
    Field userProjectField = request.getClassInfo().getField(USER_PROJECT_FIELD_NAME);
    if (userProjectField != null) {
      request.set(USER_PROJECT_FIELD_NAME, projectId);
    }
  }

  @Override
  public Map<String, Long> getStatistics() {
    return httpStatistics.getStatistics();
  }

  public static Builder builder() {
    return new AutoBuilder_GoogleCloudStorageImpl_Builder();
  }

  @AutoBuilder(ofClass = GoogleCloudStorageImpl.class)
  public abstract static class Builder {

    public abstract Builder setOptions(GoogleCloudStorageOptions options);

    public abstract Builder setCredentials(@Nullable Credentials credentials);

    public abstract Builder setHttpTransport(@Nullable HttpTransport httpTransport);

    @VisibleForTesting
    public abstract Builder setHttpRequestInitializer(
        @Nullable HttpRequestInitializer httpRequestInitializer);

    public abstract Builder setDownscopedAccessTokenFn(
        @Nullable Function<List<AccessBoundary>, String> downscopedAccessTokenFn);

    public abstract Builder setFeatureHeaderGenerator(
        @Nullable FeatureHeaderGenerator featureHeaderGenerator);

    public abstract GoogleCloudStorageImpl build() throws IOException;
  }
}
