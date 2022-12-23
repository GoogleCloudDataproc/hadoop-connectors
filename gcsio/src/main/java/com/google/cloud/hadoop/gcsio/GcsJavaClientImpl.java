package com.google.cloud.hadoop.gcsio;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.api.client.http.HttpRequestInitializer;
import com.google.auth.Credentials;
import com.google.cloud.hadoop.util.AccessBoundary;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.flogger.GoogleLogger;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileAlreadyExistsException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

/**
 * Provides read/write access to Google Cloud Storage (GCS), using Java nio channel semantics. This
 * is a basic implementation of the GoogleCloudStorage interface that mostly delegates through to
 * the appropriate API call(s) google-cloud-storage client.
 */
@VisibleForTesting
public class GcsJavaClientImpl implements GoogleCloudStorage {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();
  /**
   * Having an instance of gscImpl to redirect calls to Json client while new client implementation
   * is in WIP.
   */
  private GoogleCloudStorageImpl gcsClientDelegate;

  private GoogleCloudStorageOptions storageOptions;
  private Credentials credentials;
  private Storage storage;

  private GcsJavaClientImpl(GcsJavaClientImplBuilder builder) throws IOException {
    this.storageOptions = checkNotNull(builder.storageOptions, "options must not be null");
    this.credentials = checkNotNull(builder.credentials, "credentials must not be null");
    this.storage = checkNotNull(builder.javaClientStorage, "storage must not be null");
    if (builder.httpRequestInitializer != null) {
      logger.atWarning().log(
          "Overriding httpRequestInitializer. ALERT: Should not be hit in production");
      this.gcsClientDelegate =
          new GoogleCloudStorageImpl(this.storageOptions, builder.httpRequestInitializer);
    } else if (builder.storage != null) {
      logger.atWarning().log("Overriding storage. ALERT: Should not be hit in production");
      this.gcsClientDelegate = new GoogleCloudStorageImpl(this.storageOptions, builder.storage);
    } else {
      this.gcsClientDelegate = new GoogleCloudStorageImpl(this.storageOptions, this.credentials);
    }
  }

  @Override
  public GoogleCloudStorageOptions getOptions() {
    return gcsClientDelegate.getOptions();
  }

  @Override
  public WritableByteChannel create(StorageResourceId resourceId, CreateObjectOptions options)
      throws IOException {
    // Update resourceId if generationId is missing
    if (!resourceId.hasGenerationId()) {
      resourceId =
          new StorageResourceId(
              resourceId.getBucketName(),
              resourceId.getObjectName(),
              getWriteGeneration(resourceId, options.isOverwriteExisting()));
    }

    // Thread-pool used for background tasks.
    ExecutorService backgroundTasksThreadPool =
        Executors.newCachedThreadPool(
            new ThreadFactoryBuilder()
                .setNameFormat("gcs-async-channel-pool-%d")
                .setDaemon(true)
                .build());

    GCSJavaClientWriteChannel channel =
        new GCSJavaClientWriteChannel(
            storage, storageOptions, resourceId, options, backgroundTasksThreadPool);
    channel.initialize();
    return channel;
  }

  @Override
  public void createBucket(String bucketName, CreateBucketOptions options) throws IOException {
    gcsClientDelegate.createBucket(bucketName, options);
  }

  @Override
  public void createEmptyObject(StorageResourceId resourceId) throws IOException {
    gcsClientDelegate.createEmptyObject(resourceId);
  }

  @Override
  public void createEmptyObject(StorageResourceId resourceId, CreateObjectOptions options)
      throws IOException {
    gcsClientDelegate.createEmptyObject(resourceId, options);
  }

  @Override
  public void createEmptyObjects(List<StorageResourceId> resourceIds) throws IOException {
    gcsClientDelegate.createEmptyObjects(resourceIds);
  }

  @Override
  public void createEmptyObjects(List<StorageResourceId> resourceIds, CreateObjectOptions options)
      throws IOException {
    gcsClientDelegate.createEmptyObjects(resourceIds, options);
  }

  @Override
  public SeekableByteChannel open(
      StorageResourceId resourceId, GoogleCloudStorageReadOptions readOptions) throws IOException {
    return gcsClientDelegate.open(resourceId, readOptions);
  }

  @Override
  public SeekableByteChannel open(
      GoogleCloudStorageItemInfo itemInfo, GoogleCloudStorageReadOptions readOptions)
      throws IOException {
    return gcsClientDelegate.open(itemInfo, readOptions);
  }

  @Override
  public void deleteBuckets(List<String> bucketNames) throws IOException {
    gcsClientDelegate.deleteBuckets(bucketNames);
  }

  @Override
  public void deleteObjects(List<StorageResourceId> fullObjectNames) throws IOException {
    gcsClientDelegate.deleteObjects(fullObjectNames);
  }

  @Override
  public void copy(
      String srcBucketName,
      List<String> srcObjectNames,
      String dstBucketName,
      List<String> dstObjectNames)
      throws IOException {
    gcsClientDelegate.copy(srcBucketName, srcObjectNames, dstBucketName, dstObjectNames);
  }

  @Override
  public List<String> listBucketNames() throws IOException {
    return gcsClientDelegate.listBucketNames();
  }

  @Override
  public List<GoogleCloudStorageItemInfo> listBucketInfo() throws IOException {
    return gcsClientDelegate.listBucketInfo();
  }

  @Override
  public List<GoogleCloudStorageItemInfo> listObjectInfo(
      String bucketName, String objectNamePrefix, ListObjectOptions listOptions)
      throws IOException {
    return gcsClientDelegate.listObjectInfo(bucketName, objectNamePrefix, listOptions);
  }

  @Override
  public ListPage<GoogleCloudStorageItemInfo> listObjectInfoPage(
      String bucketName, String objectNamePrefix, ListObjectOptions listOptions, String pageToken)
      throws IOException {
    return gcsClientDelegate.listObjectInfoPage(
        bucketName, objectNamePrefix, listOptions, pageToken);
  }

  @Override
  public GoogleCloudStorageItemInfo getItemInfo(StorageResourceId resourceId) throws IOException {
    return gcsClientDelegate.getItemInfo(resourceId);
  }

  @Override
  public List<GoogleCloudStorageItemInfo> getItemInfos(List<StorageResourceId> resourceIds)
      throws IOException {
    return gcsClientDelegate.getItemInfos(resourceIds);
  }

  @Override
  public List<GoogleCloudStorageItemInfo> updateItems(List<UpdatableItemInfo> itemInfoList)
      throws IOException {
    return gcsClientDelegate.updateItems(itemInfoList);
  }

  @Override
  public void compose(
      String bucketName, List<String> sources, String destination, String contentType)
      throws IOException {
    gcsClientDelegate.compose(bucketName, sources, destination, contentType);
  }

  @Override
  public GoogleCloudStorageItemInfo composeObjects(
      List<StorageResourceId> sources, StorageResourceId destination, CreateObjectOptions options)
      throws IOException {
    return gcsClientDelegate.composeObjects(sources, destination, options);
  }

  @Override
  public Map<String, Long> getStatistics() {
    return gcsClientDelegate.getStatistics();
  }

  @Override
  public void close() {
    gcsClientDelegate.close();
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

  public static class GcsJavaClientImplBuilder {

    private Credentials credentials;
    private com.google.api.services.storage.Storage storage;
    private Storage javaClientStorage;
    private HttpRequestInitializer httpRequestInitializer;
    private GoogleCloudStorageOptions storageOptions;
    private Function<List<AccessBoundary>, String> downscopedAccessTokenFn;

    public GcsJavaClientImplBuilder(
        GoogleCloudStorageOptions storageOptions,
        Credentials credentials,
        Function<List<AccessBoundary>, String> downscopedAccessTokenFn) {
      this.storageOptions = storageOptions;
      this.credentials = credentials;
      this.downscopedAccessTokenFn = downscopedAccessTokenFn;
    }

    @VisibleForTesting
    public GcsJavaClientImplBuilder withJavaClientStorage(Storage javaClientStorage) {
      checkNotNull(javaClientStorage, "storage must not be null");
      this.javaClientStorage = javaClientStorage;
      return this;
    }

    @VisibleForTesting
    public GcsJavaClientImplBuilder withApairyClientStorage(
        com.google.api.services.storage.Storage storage) {
      checkNotNull(storage, "storage must not be null");
      this.storage = storage;
      return this;
    }

    @VisibleForTesting
    public GcsJavaClientImplBuilder withHttpRequestInitializer(
        HttpRequestInitializer httpRequestInitializer) {
      checkNotNull(httpRequestInitializer, "storage must not be null");
      this.httpRequestInitializer = httpRequestInitializer;
      return this;
    }

    private Storage createStorage(GoogleCloudStorageOptions options, Credentials credentials) {
      return StorageOptions.grpc()
          .setAttemptDirectPath(true)
          .setCredentials(credentials)
          .build()
          .getService();
    }

    public GcsJavaClientImpl build() throws IOException {
      if (this.javaClientStorage == null) {
        this.javaClientStorage =
            checkNotNull(createStorage(storageOptions, credentials), "storage must not be null");
      }
      return new GcsJavaClientImpl(this);
    }
  }
}
