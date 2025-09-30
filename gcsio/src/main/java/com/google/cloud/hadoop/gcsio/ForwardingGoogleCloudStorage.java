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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.List;
import java.util.Map;

/** A class that wraps a {@link GoogleCloudStorage} object, delegating all calls to it. */
public class ForwardingGoogleCloudStorage implements GoogleCloudStorage {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  /** The stored delegate. */
  private final GoogleCloudStorage delegate;

  /** The simple name of the delegate's class. Used for debug logging. */
  private final String delegateClassName;

  /**
   * Creates a new GoogleCloudStorageWrapper.
   *
   * @param delegate the {@link GoogleCloudStorage} to delegate calls to.
   */
  public ForwardingGoogleCloudStorage(GoogleCloudStorage delegate) {
    checkArgument(delegate != null, "delegate must not be null.");

    this.delegate = delegate;
    delegateClassName = delegate.getClass().getSimpleName();
  }

  @Override
  public GoogleCloudStorageOptions getOptions() {
    logger.atFiner().log("%s.getOptions()", delegateClassName);
    return delegate.getOptions();
  }

  @Override
  public WritableByteChannel create(StorageResourceId resourceId, CreateObjectOptions options)
      throws IOException {
    logger.atFiner().log("%s.create(%s, %s)", delegateClassName, resourceId, options);
    return delegate.create(resourceId, options);
  }

  @Override
  public void createBucket(String bucketName, CreateBucketOptions options) throws IOException {
    logger.atFiner().log("%s.createBucket(%s, %s)", delegateClassName, bucketName, options);
    delegate.createBucket(bucketName, options);
  }

  @Override
  public void createEmptyObject(StorageResourceId resourceId) throws IOException {
    logger.atFiner().log("%s.createEmptyObject(%s)", delegateClassName, resourceId);
    delegate.createEmptyObject(resourceId);
  }

  @Override
  public void createEmptyObject(StorageResourceId resourceId, CreateObjectOptions options)
      throws IOException {
    logger.atFiner().log("%s.createEmptyObject(%s, %s)", delegateClassName, resourceId, options);
    delegate.createEmptyObject(resourceId, options);
  }

  @Override
  public void createEmptyObjects(List<StorageResourceId> resourceIds) throws IOException {
    logger.atFiner().log("%s.createEmptyObjects(%s)", delegateClassName, resourceIds);
    delegate.createEmptyObjects(resourceIds);
  }

  @Override
  public void createEmptyObjects(List<StorageResourceId> resourceIds, CreateObjectOptions options)
      throws IOException {
    logger.atFiner().log("%s.createEmptyObjects(%s, %s)", delegateClassName, resourceIds, options);
    delegate.createEmptyObjects(resourceIds, options);
  }

  @Override
  public SeekableByteChannel open(
      StorageResourceId resourceId, GoogleCloudStorageReadOptions readOptions) throws IOException {
    logger.atFiner().log("%s.open(%s, %s)", delegateClassName, resourceId, readOptions);
    return delegate.open(resourceId, readOptions);
  }

  @Override
  public SeekableByteChannel open(
      GoogleCloudStorageItemInfo itemInfo, GoogleCloudStorageReadOptions readOptions)
      throws IOException {
    logger.atFiner().log("%s.open(%s, %s)", delegateClassName, itemInfo, readOptions);
    return delegate.open(itemInfo, readOptions);
  }

  @Override
  public void deleteBuckets(List<String> bucketNames) throws IOException {
    logger.atFiner().log("%s.deleteBuckets(%s)", delegateClassName, bucketNames);
    delegate.deleteBuckets(bucketNames);
  }

  @Override
  public void deleteObjects(List<StorageResourceId> fullObjectNames) throws IOException {
    logger.atFiner().log("%s.deleteObjects(%s)", delegateClassName, fullObjectNames);
    delegate.deleteObjects(fullObjectNames);
  }

  @Override
  public Map<String, Long> getStatistics() {
    return delegate.getStatistics();
  }

  @Override
  public void deleteFolders(List<FolderInfo> folders) throws IOException {
    logger.atFiner().log("%s.deleteFolders(%s)", delegateClassName, folders);
    delegate.deleteFolders(folders);
  }

  @Override
  public void copy(
      String srcBucketName,
      List<String> srcObjectNames,
      String dstBucketName,
      List<String> dstObjectNames)
      throws IOException {
    logger.atFiner().log(
        "%s.copy(%s, %s, %s, %s)",
        delegateClassName, srcBucketName, srcObjectNames, dstBucketName, dstObjectNames);
    delegate.copy(srcBucketName, srcObjectNames, dstBucketName, dstObjectNames);
  }

  @Override
  public void copy(Map<StorageResourceId, StorageResourceId> sourceToDestinationObjectsMap)
      throws IOException {
    logger.atFiner().log("%s.copy(%s)", delegateClassName, sourceToDestinationObjectsMap);
    delegate.copy(sourceToDestinationObjectsMap);
  }

  @Override
  public void move(Map<StorageResourceId, StorageResourceId> sourceToDestinationObjectsMap)
      throws IOException {
    logger.atFiner().log("%s.move(%s)", delegateClassName, sourceToDestinationObjectsMap);
    delegate.move(sourceToDestinationObjectsMap);
  }

  @Override
  public GoogleCloudStorageItemInfo getFolderInfo(StorageResourceId resourceId) throws IOException {
    logger.atInfo().log("getFolderInfo(%s)", resourceId);
    return delegate.getFolderInfo(resourceId);
  }

  @Override
  public void createFolder(StorageResourceId resourceId, boolean recursive) throws IOException {
    logger.atInfo().log("createFolder(%s)", resourceId);
    delegate.createFolder(resourceId, recursive);
  }

  @Override
  public boolean isHnBucket(URI src) throws IOException {
    return delegate.isHnBucket(src);
  }

  @Override
  public List<String> listBucketNames() throws IOException {
    logger.atFiner().log("%s.listBucketNames()", delegateClassName);
    return delegate.listBucketNames();
  }

  @Override
  public void renameHnFolder(URI src, URI dst) throws IOException {
    delegate.renameHnFolder(src, dst);
  }

  @Override
  public List<GoogleCloudStorageItemInfo> listBucketInfo() throws IOException {
    logger.atFiner().log("%s.listBucketInfo()", delegateClassName);
    return delegate.listBucketInfo();
  }

  @Override
  public List<GoogleCloudStorageItemInfo> listObjectInfo(
      String bucketName, String objectNamePrefix, ListObjectOptions listOptions)
      throws IOException {
    logger.atFiner().log(
        "%s.listObjectInfo(%s, %s, %s)",
        delegateClassName, bucketName, objectNamePrefix, listOptions);
    return delegate.listObjectInfo(bucketName, objectNamePrefix, listOptions);
  }

  @Override
  public List<GoogleCloudStorageItemInfo> listObjectInfoStartingFrom(
      String bucketName, String startOffset, ListObjectOptions listOptions) throws IOException {
    return delegate.listObjectInfoStartingFrom(bucketName, startOffset, listOptions);
  }

  @Override
  public ListPage<GoogleCloudStorageItemInfo> listObjectInfoPage(
      String bucketName, String objectNamePrefix, ListObjectOptions listOptions, String pageToken)
      throws IOException {
    logger.atFiner().log(
        "%s.listObjectInfoPage(%s, %s, %s, %s)",
        delegateClassName, bucketName, objectNamePrefix, listOptions, pageToken);
    return delegate.listObjectInfoPage(bucketName, objectNamePrefix, listOptions, pageToken);
  }

  @Override
  public ListPage<FolderInfo> listFolderInfoForPrefixPage(
      String bucketName,
      String folderNamePrefix,
      ListFolderOptions listFolderOptions,
      String pageToken)
      throws IOException {
    return delegate.listFolderInfoForPrefixPage(
        bucketName, folderNamePrefix, listFolderOptions, pageToken);
  }

  @Override
  public GoogleCloudStorageItemInfo getItemInfo(StorageResourceId resourceId) throws IOException {
    logger.atFiner().log("%s.getItemInfo(%s)", delegateClassName, resourceId);
    return delegate.getItemInfo(resourceId);
  }

  @Override
  public List<GoogleCloudStorageItemInfo> getItemInfos(List<StorageResourceId> resourceIds)
      throws IOException {
    logger.atFiner().log("%s.getItemInfos(%s)", delegateClassName, resourceIds);
    return delegate.getItemInfos(resourceIds);
  }

  @Override
  public List<GoogleCloudStorageItemInfo> updateItems(List<UpdatableItemInfo> itemInfoList)
      throws IOException {
    logger.atFiner().log("%s.updateItems(%s)", delegateClassName, itemInfoList);
    return delegate.updateItems(itemInfoList);
  }

  @Override
  public void close() {
    logger.atFiner().log("%s.close()", delegateClassName);
    delegate.close();
  }

  @Override
  public void compose(
      String bucketName, List<String> sources, String destination, String contentType)
      throws IOException {
    logger.atFiner().log(
        "%s.compose(%s, %s, %s, %s)",
        delegateClassName, bucketName, sources, destination, contentType);
    delegate.compose(bucketName, sources, destination, contentType);
  }

  @Override
  public GoogleCloudStorageItemInfo composeObjects(
      List<StorageResourceId> sources, StorageResourceId destination, CreateObjectOptions options)
      throws IOException {
    logger.atFiner().log(
        "%s.composeObjects(%s, %s, %s)", delegateClassName, sources, destination, options);
    return delegate.composeObjects(sources, destination, options);
  }

  /**
   * Gets the {@link GoogleCloudStorage} objected wrapped by this class.
   *
   * @return the {@link GoogleCloudStorage} objected wrapped by this class.
   */
  public GoogleCloudStorage getDelegate() {
    return delegate;
  }
}
