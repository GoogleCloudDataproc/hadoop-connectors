/*
 * Copyright 2013 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.gcsio;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.List;

/**
 * Interface for exposing the Google Cloud Storage API behavior in a way more amenable to writing
 * filesystem semantics on top of it, without having to deal with API-specific considerations such
 * as HttpTransports, credentials, network errors, batching, etc.
 * <p>
 * Please read the following document to get familiarity with basic GCS concepts:
 * https://developers.google.com/storage/docs/concepts-techniques
 */
public interface GoogleCloudStorage {

  /**
   * Pseudo path delimiter.
   *
   * <p>GCS does not implement full concept of file system paths but it does expose some notion of a
   * delimiter that can be used with ${code Storage.Objects.List} to control which items are listed.
   */
  String PATH_DELIMITER = "/";

  /** Value indicating all objects should be returned from GCS, no limit. */
  long MAX_RESULTS_UNLIMITED = -1;

  /** The maximum number of objects that can be composed in one operation. */
  int MAX_COMPOSE_OBJECTS = 32;

  /** Retrieve the options that were used to create this GoogleCloudStorage. */
  GoogleCloudStorageOptions getOptions();

  /**
   * Creates and opens an object for writing. The bucket must already exist. If the object already
   * exists and {@code resourceId} doesn't have a explicit generationId set, it is deleted. If a
   * generationId is provided in {@code resourceId}, that is used in place of normal overwrite
   * semantics (overwrite will only succeed if the provided generationId matches the existing
   * object, where a generationId of 0 indicates no existing object expected).
   *
   * @param resourceId identifies a StorageObject
   * @return a channel for writing to the given object
   * @throws IOException on IO error
   */
  WritableByteChannel create(StorageResourceId resourceId) throws IOException;

  /**
   * Creates and opens an object for writing. The bucket must already exist. If {@code resourceId}
   * contains a known generationId or a generationId of 0, it is used instead of any "overwrite"
   * settings in the provided {@code CreateObjectOptions} (overwrite will only succeed if the
   * provided generationId matches the existing object, where a generationId of 0 indicates no
   * existing object expected).
   *
   * @param resourceId identifies a StorageObject
   * @param options Options to use when creating the object
   * @return a channel for writing to the given object
   * @throws IOException on IO error
   */
  WritableByteChannel create(StorageResourceId resourceId, CreateObjectOptions options)
      throws IOException;

  /**
   * Creates a bucket.
   *
   * @param bucketName name of the bucket to create
   * @throws IOException on IO error
   */
  void createBucket(String bucketName) throws IOException;

  /**
   * Creates a bucket.
   *
   * @param bucketName name of the bucket to create
   * @param options options to use when creating bucket
   * @throws IOException on IO error
   */
  void createBucket(String bucketName, CreateBucketOptions options) throws IOException;

  /**
   * Creates an empty object, useful for placeholders representing, for example, directories. The
   * bucket must already exist. If the object already exists, it is overwritten. See {@link
   * #create(StorageResourceId)} for the behavior if StorageResourceId.getGenerationId() is
   * explicitly set.
   *
   * @param resourceId identifies a StorageObject
   * @throws IOException on IO error
   */
  void createEmptyObject(StorageResourceId resourceId) throws IOException;

  /**
   * Creates an empty object, useful for placeholders representing, for example, directories.
   * The bucket must already exist. If the object already exists, it is overwritten.
   * See {@link #create(StorageResourceId)} for the behavior if StorageResourceId.getGenerationId()
   * is explicitly set.
   *
   * @param resourceId identifies a StorageObject
   * @param options options to use when creating the object
   * @throws IOException on IO error
   */
  void createEmptyObject(StorageResourceId resourceId, CreateObjectOptions options)
      throws IOException;

  /**
   * Creates a list of empty objects; see {@link #createEmptyObject(StorageResourceId)} for
   * the single-item version of this method. Implementations may use different flow than the
   * single-item version for greater efficiency.
   * See {@link #create(StorageResourceId)} for the behavior if StorageResourceId.getGenerationId()
   * is explicitly set.
   */
  void createEmptyObjects(List<StorageResourceId> resourceIds)
      throws IOException;

  /**
   * Creates a list of empty objects; see {@link #createEmptyObject(StorageResourceId)} for
   * the single-item version of this method. Implementations may use different flow than the
   * single-item version for greater efficiency.
   * See {@link #create(StorageResourceId)} for the behavior if StorageResourceId.getGenerationId()
   * is explicitly set.
   */
  void createEmptyObjects(List<StorageResourceId> resourceIds, CreateObjectOptions options)
      throws IOException;

  /**
   * Opens an object for reading.
   *
   * @param resourceId identifies a StorageObject
   * @return a channel for reading from the given object
   * @throws java.io.FileNotFoundException if the given object does not exist
   * @throws IOException if object exists but cannot be opened
   */
  SeekableByteChannel open(StorageResourceId resourceId) throws IOException;

  /**
   * Opens an object for reading.
   *
   * @param resourceId identifies a StorageObject
   * @param readOptions Fine-grained options for behaviors of retries, buffering, etc.
   * @return a channel for reading from the given object
   * @throws java.io.FileNotFoundException if the given object does not exist
   * @throws IOException if object exists but cannot be opened
   */
  SeekableByteChannel open(StorageResourceId resourceId, GoogleCloudStorageReadOptions readOptions)
      throws IOException;

  /**
   * Deletes a list of buckets. Does not throw any exception for "bucket not found" errors.
   *
   * @param bucketNames name of the buckets to delete
   * @throws java.io.FileNotFoundException if the given bucket does not exist
   * @throws IOException on IO error
   */
  void deleteBuckets(List<String> bucketNames) throws IOException;

  /**
   * Deletes the given objects. Does not throw any exception for "object not found" errors.
   *
   * @param fullObjectNames names of objects to delete with their respective bucketNames.
   * @throws java.io.FileNotFoundException if the given object does not exist
   * @throws IOException if object exists but cannot be deleted
   */
  void deleteObjects(List<StorageResourceId> fullObjectNames) throws IOException;

  /**
   * Copies metadata of the given objects. After the copy is successfully complete, each object blob
   * is reachable by two different names. Copying between two different locations or between two
   * different storage classes is not allowed.
   *
   * @param srcBucketName name of the bucket containing the objects to copy
   * @param srcObjectNames names of the objects to copy
   * @param dstBucketName name of the bucket to copy to
   * @param dstObjectNames names of the objects after copy
   * @throws java.io.FileNotFoundException if the source object or the destination bucket does not
   *     exist
   * @throws IOException in all other error cases
   */
  void copy(
      String srcBucketName,
      List<String> srcObjectNames,
      String dstBucketName,
      List<String> dstObjectNames)
      throws IOException;

  /**
   * Gets a list of names of buckets in this project.
   */
  List<String> listBucketNames()
      throws IOException;

  /**
   * Gets a list of GoogleCloudStorageItemInfo for all buckets of this project. This is no more
   * expensive than calling listBucketNames(), since the list API for buckets already retrieves
   * all the relevant bucket metadata.
   */
  List<GoogleCloudStorageItemInfo> listBucketInfo()
      throws IOException;

  /**
   * Gets names of objects contained in the given bucket and whose names begin with the given
   * prefix.
   *
   * <p>Note: Although GCS does not implement a file system, it treats objects that contain a
   * delimiter ({@link ListObjectOptions#getDelimiter()}) as different from other objects when
   * listing objects. This will be clearer with an example.
   *
   * <p>Consider a bucket with objects: {@code o1}, {@code d1/}, {@code d1/o1}, {@code d1/o2}
   *
   * <ul>
   *   <li/>With {@code prefix == null} and {@code delimiter == /}, we get: {@code d1/}, {@code o1}
   *   <li/>With {@code prefix == null} and {@code delimiter == null}, we get: {@code o1}, {@code
   *       d1/}, {@code d1/o1}, {@code d1/o2}
   * </ul>
   *
   * <p>Thus when delimiter is {@code null}, the entire key name is considered an opaque string,
   * otherwise only the part up to the first delimiter is considered.
   *
   * @param bucketName bucket name
   * @param objectNamePrefix object name prefix or null if all objects in the bucket are desired
   * @return list of object names
   * @throws IOException on IO error
   */
  List<String> listObjectNames(String bucketName, String objectNamePrefix) throws IOException;

  /**
   * Gets names of objects contained in the given bucket and whose names begin with the given
   * prefix.
   *
   * <p>Note: Although GCS does not implement a file system, it treats objects that contain a
   * delimiter ({@link ListObjectOptions#getDelimiter()}) as different from other objects when
   * listing objects. This will be clearer with an example.
   *
   * <p>Consider a bucket with objects: {@code o1}, {@code d1/}, {@code d1/o1}, {@code d1/o2}
   *
   * <ul>
   *   <li/>With {@code prefix == null} and {@code delimiter == /}, we get: {@code d1/}, {@code o1}
   *   <li/>With {@code prefix == null} and {@code delimiter == null}, we get: {@code o1}, {@code
   *       d1/}, {@code d1/o1}, {@code d1/o2}
   * </ul>
   *
   * <p>Thus when delimiter is {@code null}, the entire key name is considered an opaque string,
   * otherwise only the part up to the first delimiter is considered.
   *
   * @param bucketName bucket name
   * @param objectNamePrefix object name prefix or null if all objects in the bucket are desired
   * @param listOptions options to use when listing objects
   * @return list of object names
   * @throws IOException on IO error
   */
  List<String> listObjectNames(
      String bucketName, String objectNamePrefix, ListObjectOptions listOptions) throws IOException;

  /**
   * Same name-matching semantics as {@link #listObjectNames} except this method retrieves the full
   * {@link GoogleCloudStorageItemInfo} for each item as well.
   *
   * @param bucketName bucket name
   * @param objectNamePrefix object name prefix or null if all objects in the bucket are desired
   * @return list of object info
   * @throws IOException on IO error
   */
  List<GoogleCloudStorageItemInfo> listObjectInfo(String bucketName, String objectNamePrefix)
      throws IOException;

  /**
   * Same name-matching semantics as {@link #listObjectNames} except this method retrieves the full
   * {@link GoogleCloudStorageItemInfo} for each item as well.
   *
   * @param bucketName bucket name
   * @param objectNamePrefix object name prefix or null if all objects in the bucket are desired
   * @param listOptions options to use when listing objects
   * @return list of object info
   * @throws IOException on IO error
   */
  List<GoogleCloudStorageItemInfo> listObjectInfo(
      String bucketName, String objectNamePrefix, ListObjectOptions listOptions) throws IOException;

  /**
   * The same semantics as {@link #listObjectInfo}, but returns only result of single list request
   * (1 page).
   *
   * @param bucketName bucket name
   * @param objectNamePrefix object name prefix or null if all objects in the bucket are desired
   * @param listOptions options to use when listing objects
   * @param pageToken the page token
   * @return {@link ListPage} object with listed {@link GoogleCloudStorageItemInfo}s and next page
   *     token if any
   * @throws IOException on IO error
   */
  ListPage<GoogleCloudStorageItemInfo> listObjectInfoPage(
      String bucketName, String objectNamePrefix, ListObjectOptions listOptions, String pageToken)
      throws IOException;

  /**
   * Gets information about an object or a bucket.
   *
   * @param resourceId identifies either root, a Bucket, or a StorageObject
   * @return information about the given item
   * @throws IOException on IO error
   */
  GoogleCloudStorageItemInfo getItemInfo(StorageResourceId resourceId)
      throws IOException;

  /**
   * Gets information about multiple objects and/or buckets. Items that are "not found" will still
   * have an entry in the returned list; exists() will return false for these entries. The return
   * list will have the same ordering as the input arguments.
   *
   * @param resourceIds names of the GCS StorageObjects or Buckets for which to retrieve info.
   * @return information about the given resourceIds.
   * @throws IOException on IO error
   */
  List<GoogleCloudStorageItemInfo> getItemInfos(List<StorageResourceId> resourceIds)
      throws IOException;

  /**
   * Attempt to update metadata of the objects referenced within the passed itemInfo objects.
   * @return Updated GoogleCloudStorageItemInfo objects for the referenced objects.
   * @throws IOException on IO error
   */
  List<GoogleCloudStorageItemInfo> updateItems(List<UpdatableItemInfo> itemInfoList)
      throws IOException;

  /**
   * Releases resources used by this instance.
   */
  void close();

  /**
   * Composes inputs into a single GCS object. This performs a GCS Compose. Objects will be composed
   * according to the order they appear in the input. The destination object, if already present,
   * will be overwritten. Sources and destination are assumed to be in the same bucket.
   *
   * @param bucketName name of the bucket containing the sources and the destination
   * @param sources names of the objects to be composed
   * @param destination name of the resulting object
   * @param contentType content-type of composed object
   * @throws IOException if the Compose operation was unsuccessful
   */
  void compose(String bucketName, List<String> sources, String destination, String contentType)
      throws IOException;

  /**
   * Composes inputs into a single GCS object. This performs a GCS Compose. Objects will be composed
   * according to the order they appear in the input. The destination object will have metadata set
   * according to {@code options}. Overwrite semantics for the destination object will follow the
   * same semantics as {@link #create(StorageResourceId, CreateObjectOptions)}.
   * See {@link #create(StorageResourceId)} for the behavior if StorageResourceId.getGenerationId()
   * is explicitly set. The bucket must be the same for all sources and the destination.
   */
  GoogleCloudStorageItemInfo composeObjects(
      List<StorageResourceId> sources, StorageResourceId destination, CreateObjectOptions options)
      throws IOException;

  /** Paged list request response */
  class ListPage<T> {
    private final List<T> items;
    private final String nextPageToken;

    public ListPage(List<T> items, String nextPageToken) {
      this.items = items;
      this.nextPageToken = nextPageToken;
    }

    public List<T> getItems() {
      return items;
    }

    public String getNextPageToken() {
      return nextPageToken;
    }
  }
}
