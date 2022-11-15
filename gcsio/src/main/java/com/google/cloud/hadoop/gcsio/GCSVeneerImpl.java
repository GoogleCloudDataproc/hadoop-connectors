package com.google.cloud.hadoop.gcsio;

import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.services.storage.Storage;
import com.google.cloud.hadoop.util.AccessBoundary;
import com.google.common.annotations.VisibleForTesting;
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
public class GCSVeneerImpl implements GoogleCloudStorage {
  /**
   * Having an instance of gscImpl to redirect calls to Json client while new client implementation
   * is in WIP.
   */
  private GoogleCloudStorageImpl oldGCSClient;

  public GCSVeneerImpl(
      GoogleCloudStorageOptions options, HttpRequestInitializer httpRequestInitializer)
      throws IOException {
    oldGCSClient = new GoogleCloudStorageImpl(options, httpRequestInitializer);
  }

  public GCSVeneerImpl(
      GoogleCloudStorageOptions options, com.google.api.services.storage.Storage storage) {
    oldGCSClient = new GoogleCloudStorageImpl(options, storage);
  }

  @VisibleForTesting
  public GCSVeneerImpl(
      GoogleCloudStorageOptions options,
      Storage storage,
      Function<List<AccessBoundary>, String> downscopedAccessTokenFn) {
    oldGCSClient = new GoogleCloudStorageImpl(options, storage, downscopedAccessTokenFn);
  }

  @Override
  public GoogleCloudStorageOptions getOptions() {
    return oldGCSClient.getOptions();
  }

  @Override
  public WritableByteChannel create(StorageResourceId resourceId, CreateObjectOptions options)
      throws IOException {
    return oldGCSClient.create(resourceId, options);
  }

  @Override
  public void createBucket(String bucketName, CreateBucketOptions options) throws IOException {
    oldGCSClient.createBucket(bucketName, options);
  }

  @Override
  public void createEmptyObject(StorageResourceId resourceId) throws IOException {
    oldGCSClient.createEmptyObject(resourceId);
  }

  @Override
  public void createEmptyObject(StorageResourceId resourceId, CreateObjectOptions options)
      throws IOException {
    oldGCSClient.createEmptyObject(resourceId, options);
  }

  @Override
  public void createEmptyObjects(List<StorageResourceId> resourceIds) throws IOException {
    oldGCSClient.createEmptyObjects(resourceIds);
  }

  @Override
  public void createEmptyObjects(List<StorageResourceId> resourceIds, CreateObjectOptions options)
      throws IOException {
    oldGCSClient.createEmptyObjects(resourceIds, options);
  }

  @Override
  public SeekableByteChannel open(
      StorageResourceId resourceId, GoogleCloudStorageReadOptions readOptions) throws IOException {
    return oldGCSClient.open(resourceId, readOptions);
  }

  @Override
  public SeekableByteChannel open(
      GoogleCloudStorageItemInfo itemInfo, GoogleCloudStorageReadOptions readOptions)
      throws IOException {
    return oldGCSClient.open(itemInfo, readOptions);
  }

  @Override
  public void deleteBuckets(List<String> bucketNames) throws IOException {
    oldGCSClient.deleteBuckets(bucketNames);
  }

  @Override
  public void deleteObjects(List<StorageResourceId> fullObjectNames) throws IOException {
    oldGCSClient.deleteObjects(fullObjectNames);
  }

  @Override
  public void copy(
      String srcBucketName,
      List<String> srcObjectNames,
      String dstBucketName,
      List<String> dstObjectNames)
      throws IOException {
    oldGCSClient.copy(srcBucketName, srcObjectNames, dstBucketName, dstObjectNames);
  }

  @Override
  public List<String> listBucketNames() throws IOException {
    return oldGCSClient.listBucketNames();
  }

  @Override
  public List<GoogleCloudStorageItemInfo> listBucketInfo() throws IOException {
    return oldGCSClient.listBucketInfo();
  }

  @Override
  public List<GoogleCloudStorageItemInfo> listObjectInfo(
      String bucketName, String objectNamePrefix, ListObjectOptions listOptions)
      throws IOException {
    return oldGCSClient.listObjectInfo(bucketName, objectNamePrefix, listOptions);
  }

  @Override
  public ListPage<GoogleCloudStorageItemInfo> listObjectInfoPage(
      String bucketName, String objectNamePrefix, ListObjectOptions listOptions, String pageToken)
      throws IOException {
    return oldGCSClient.listObjectInfoPage(bucketName, objectNamePrefix, listOptions, pageToken);
  }

  @Override
  public GoogleCloudStorageItemInfo getItemInfo(StorageResourceId resourceId) throws IOException {
    return oldGCSClient.getItemInfo(resourceId);
  }

  @Override
  public List<GoogleCloudStorageItemInfo> getItemInfos(List<StorageResourceId> resourceIds)
      throws IOException {
    return oldGCSClient.getItemInfos(resourceIds);
  }

  @Override
  public List<GoogleCloudStorageItemInfo> updateItems(List<UpdatableItemInfo> itemInfoList)
      throws IOException {
    return oldGCSClient.updateItems(itemInfoList);
  }

  @Override
  public void compose(
      String bucketName, List<String> sources, String destination, String contentType)
      throws IOException {
    oldGCSClient.compose(bucketName, sources, destination, contentType);
  }

  @Override
  public GoogleCloudStorageItemInfo composeObjects(
      List<StorageResourceId> sources, StorageResourceId destination, CreateObjectOptions options)
      throws IOException {
    return oldGCSClient.composeObjects(sources, destination, options);
  }

  @Override
  public Map<String, Long> getStatistics() {
    return oldGCSClient.getStatistics();
  }

  @Override
  public void close() {
    oldGCSClient.close();
  }
}
