/*
 * Copyright 2013 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.hadoop.gcsio;

import com.google.common.base.Preconditions;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.List;

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
    Preconditions.checkArgument(delegate != null, "delegate must not be null.");

    this.delegate = delegate;
    delegateClassName = delegate.getClass().getSimpleName();
  }

  @Override
  public GoogleCloudStorageOptions getOptions() {
    logger.atFine().log("%s.getOptions()", delegateClassName);
    return delegate.getOptions();
  }

  @Override
  public WritableByteChannel create(StorageResourceId resourceId) throws IOException {
    logger.atFine().log("%s.create(%s)", delegateClassName, resourceId);
    return delegate.create(resourceId);
  }

  @Override
  public WritableByteChannel create(StorageResourceId resourceId, CreateObjectOptions options)
      throws IOException {
    logger.atFine().log("%s.create(%s, %s)", delegateClassName, resourceId, options);
    return delegate.create(resourceId, options);
  }

  @Override
  public void createBucket(String bucketName) throws IOException {
    logger.atFine().log("%s.createBucket(%s)", delegateClassName, bucketName);
    delegate.createBucket(bucketName);
  }

  @Override
  public void createBucket(String bucketName, CreateBucketOptions options) throws IOException {
    logger.atFine().log("%s.createBucket(%s, %s)", delegateClassName, bucketName, options);
    delegate.createBucket(bucketName, options);
  }

  @Override
  public void createEmptyObject(StorageResourceId resourceId) throws IOException {
    logger.atFine().log("%s.createEmptyObject(%s)", delegateClassName, resourceId);
    delegate.createEmptyObject(resourceId);
  }

  @Override
  public void createEmptyObject(StorageResourceId resourceId, CreateObjectOptions options)
      throws IOException {
    logger.atFine().log("%s.createEmptyObject(%s, %s)", delegateClassName, resourceId, options);
    delegate.createEmptyObject(resourceId, options);
  }

  @Override
  public void createEmptyObjects(List<StorageResourceId> resourceIds) throws IOException {
    logger.atFine().log("%s.createEmptyObjects(%s)", delegateClassName, resourceIds);
    delegate.createEmptyObjects(resourceIds);
  }

  @Override
  public void createEmptyObjects(List<StorageResourceId> resourceIds, CreateObjectOptions options)
      throws IOException {
    logger.atFine().log("%s.createEmptyObjects(%s, %s)", delegateClassName, resourceIds, options);
    delegate.createEmptyObjects(resourceIds, options);
  }

  @Override
  public SeekableByteChannel open(StorageResourceId resourceId) throws IOException {
    logger.atFine().log("%s.open(%s)", delegateClassName, resourceId);
    return delegate.open(resourceId);
  }

  @Override
  public SeekableByteChannel open(
      StorageResourceId resourceId, GoogleCloudStorageReadOptions readOptions) throws IOException {
    logger.atFine().log("%s.open(%s, %s)", delegateClassName, resourceId, readOptions);
    return delegate.open(resourceId, readOptions);
  }

  @Override
  public void deleteBuckets(List<String> bucketNames) throws IOException {
    logger.atFine().log("%s.deleteBuckets(%s)", delegateClassName, bucketNames);
    delegate.deleteBuckets(bucketNames);
  }

  @Override
  public void deleteObjects(List<StorageResourceId> fullObjectNames) throws IOException {
    logger.atFine().log("%s.deleteObjects(%s)", delegateClassName, fullObjectNames);
    delegate.deleteObjects(fullObjectNames);
  }

  @Override
  public void copy(
      String srcBucketName,
      List<String> srcObjectNames,
      String dstBucketName,
      List<String> dstObjectNames)
      throws IOException {
    logger.atFine().log(
        "%s.copy(%s, %s, %s, %s)",
        delegateClassName, srcBucketName, srcObjectNames, dstBucketName, dstObjectNames);
    delegate.copy(srcBucketName, srcObjectNames, dstBucketName, dstObjectNames);
  }

  @Override
  public List<String> listBucketNames() throws IOException {
    logger.atFine().log("%s.listBucketNames()", delegateClassName);
    return delegate.listBucketNames();
  }

  @Override
  public List<GoogleCloudStorageItemInfo> listBucketInfo() throws IOException {
    logger.atFine().log("%s.listBucketInfo()", delegateClassName);
    return delegate.listBucketInfo();
  }

  @Override
  public List<String> listObjectNames(String bucketName, String objectNamePrefix)
      throws IOException {
    logger.atFine().log(
        "%s.listObjectNames(%s, %s)", delegateClassName, bucketName, objectNamePrefix);
    return delegate.listObjectNames(bucketName, objectNamePrefix);
  }

  @Override
  public List<String> listObjectNames(
      String bucketName, String objectNamePrefix, ListObjectOptions listOptions)
      throws IOException {
    logger.atFine().log(
        "%s.listObjectNames(%s, %s, %s)",
        delegateClassName, bucketName, objectNamePrefix, listOptions);
    return delegate.listObjectNames(bucketName, objectNamePrefix, listOptions);
  }

  @Override
  public List<GoogleCloudStorageItemInfo> listObjectInfo(String bucketName, String objectNamePrefix)
      throws IOException {
    logger.atFine().log(
        "%s.listObjectInfo(%s, %s)", delegateClassName, bucketName, objectNamePrefix);
    return delegate.listObjectInfo(bucketName, objectNamePrefix);
  }

  @Override
  public List<GoogleCloudStorageItemInfo> listObjectInfo(
      String bucketName, String objectNamePrefix, ListObjectOptions listOptions)
      throws IOException {
    logger.atFine().log(
        "%s.listObjectInfo(%s, %s, %s)",
        delegateClassName, bucketName, objectNamePrefix, listOptions);
    return delegate.listObjectInfo(bucketName, objectNamePrefix, listOptions);
  }

  @Override
  public ListPage<GoogleCloudStorageItemInfo> listObjectInfoPage(
      String bucketName, String objectNamePrefix, ListObjectOptions listOptions, String pageToken)
      throws IOException {
    logger.atFine().log(
        "%s.listObjectInfoPage(%s, %s, %s, %s)",
        delegateClassName, bucketName, objectNamePrefix, listOptions, pageToken);
    return delegate.listObjectInfoPage(bucketName, objectNamePrefix, listOptions, pageToken);
  }

  @Override
  public GoogleCloudStorageItemInfo getItemInfo(StorageResourceId resourceId) throws IOException {
    logger.atFine().log("%s.getItemInfo(%s)", delegateClassName, resourceId);
    return delegate.getItemInfo(resourceId);
  }

  @Override
  public List<GoogleCloudStorageItemInfo> getItemInfos(List<StorageResourceId> resourceIds)
      throws IOException {
    logger.atFine().log("%s.getItemInfos(%s)", delegateClassName, resourceIds);
    return delegate.getItemInfos(resourceIds);
  }

  @Override
  public List<GoogleCloudStorageItemInfo> updateItems(List<UpdatableItemInfo> itemInfoList)
      throws IOException {
    logger.atFine().log("%s.updateItems(%s)", delegateClassName, itemInfoList);
    return delegate.updateItems(itemInfoList);
  }

  @Override
  public void close() {
    logger.atFine().log("%s.close()", delegateClassName);
    delegate.close();
  }

  @Override
  public void compose(
      String bucketName, List<String> sources, String destination, String contentType)
      throws IOException {
    logger.atFine().log(
        "%s.compose(%s, %s, %s, %s)",
        delegateClassName, bucketName, sources, destination, contentType);
    delegate.compose(bucketName, sources, destination, contentType);
  }

  @Override
  public GoogleCloudStorageItemInfo composeObjects(
      List<StorageResourceId> sources, StorageResourceId destination, CreateObjectOptions options)
      throws IOException {
    logger.atFine().log(
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
