package com.google.cloud.hadoop.gcsio;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.client.http.HttpRequestInitializer;
import com.google.auth.Credentials;
import com.google.cloud.hadoop.util.AccessBoundary;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Provides read/write access to Google Cloud Storage (GCS), using Java nio channel semantics. This
 * is a basic implementation of the GoogleCloudStorage interface that mostly delegates through to
 * the appropriate API call(s) google-cloud-storage client.
 */
@VisibleForTesting
public class GCSJavaClientImpl implements GoogleCloudStorage {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();
  /**
   * Having an instance of gscImpl to redirect calls to Json client while new client implementation
   * is in WIP.
   */
  private GoogleCloudStorageImpl gcsClientDelegate;

  private GoogleCloudStorageOptions storageOptions;
  private Credentials credentials;

  private GCSJavaClientImpl(GCSJavaClientImplBuilder builder) throws IOException {
    this.storageOptions = checkNotNull(builder.storageOptions, "options must not be null");
    this.credentials = checkNotNull(builder.credentials, "credentials must not be null");

    if (builder.httpRequestInitializer != null) {
      logger.atWarning().log(
          "Overriding httpRequestInitializer. ALERT: Should not be hit in production");
      gcsClientDelegate =
          new GoogleCloudStorageImpl(storageOptions, builder.httpRequestInitializer);
    } else if (builder.storage != null) {
      logger.atWarning().log("Overriding storage. ALERT: Should not be hit in production");
      gcsClientDelegate = new GoogleCloudStorageImpl(storageOptions, builder.storage);
    } else {
      gcsClientDelegate = new GoogleCloudStorageImpl(storageOptions, credentials);
    }
  }

  @Override
  public GoogleCloudStorageOptions getOptions() {
    return gcsClientDelegate.getOptions();
  }

  @Override
  public WritableByteChannel create(StorageResourceId resourceId, CreateObjectOptions options)
      throws IOException {
    return gcsClientDelegate.create(resourceId, options);
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

  public static class GCSJavaClientImplBuilder {

    private Credentials credentials;
    private com.google.api.services.storage.Storage storage;
    private HttpRequestInitializer httpRequestInitializer;
    private GoogleCloudStorageOptions storageOptions;
    private Function<List<AccessBoundary>, String> downscopedAccessTokenFn;

    public GCSJavaClientImplBuilder(
        GoogleCloudStorageOptions storageOptions,
        Credentials credentials,
        Function<List<AccessBoundary>, String> downscopedAccessTokenFn) {
      this.storageOptions = storageOptions;
      this.credentials = credentials;
      this.downscopedAccessTokenFn = downscopedAccessTokenFn;
    }

    @VisibleForTesting
    public GCSJavaClientImplBuilder withApairyClientStorage(
        com.google.api.services.storage.Storage storage) {
      this.storage = storage;
      return this;
    }

    @VisibleForTesting
    public GCSJavaClientImplBuilder withHttpRequestInitializer(
        HttpRequestInitializer httpRequestInitializer) {
      this.httpRequestInitializer = httpRequestInitializer;
      return this;
    }

    public GCSJavaClientImpl build() throws IOException {
      return new GCSJavaClientImpl(this);
    }
  }
}
