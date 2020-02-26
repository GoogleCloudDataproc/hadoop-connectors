/*
 * Copyright 2020 Google LLC. All Rights Reserved.
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

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorage.PATH_DELIMITER;
import static com.google.common.base.Preconditions.checkNotNull;

import java.net.URI;
import java.net.URISyntaxException;

/** Utility methods for URI GCS paths */
public final class UriPaths {

  private UriPaths() {}

  /**
   * Converts the given path to look like a directory path. If the path already looks like a
   * directory path then this call is a no-op.
   *
   * @param path Path to convert.
   * @return Directory path for the given path.
   */
  public static URI toDirectory(URI path) {
    StorageResourceId resourceId = StorageResourceId.fromUriPath(path, true);

    if (resourceId.isStorageObject() && !resourceId.isDirectory()) {
      resourceId = resourceId.toDirectoryId();
      path = fromResourceId(resourceId, /* allowEmptyObjectName= */ false);
    }
    return path;
  }

  /**
   * Gets the parent directory of the given path.
   *
   * @param path Path to convert.
   * @return Path of parent directory of the given item or null for root path.
   */
  public static URI getParentPath(URI path) {
    checkNotNull(path);

    // Root path has no parent.
    if (path.equals(GoogleCloudStorageFileSystem.GCS_ROOT)) {
      return null;
    }

    StorageResourceId resourceId = StorageResourceId.fromUriPath(path, true);

    if (resourceId.isBucket()) {
      return GoogleCloudStorageFileSystem.GCS_ROOT;
    }

    String objectName = resourceId.getObjectName();
    int index =
        StringPaths.isDirectoryPath(objectName)
            ? objectName.lastIndexOf(PATH_DELIMITER, objectName.length() - 2)
            : objectName.lastIndexOf(PATH_DELIMITER);
    return index < 0
        ? fromStringPathComponents(
            resourceId.getBucketName(), /* objectName= */ null, /* allowEmptyObjectName= */ true)
        : fromStringPathComponents(
            resourceId.getBucketName(),
            objectName.substring(0, index + 1),
            /* allowEmptyObjectName= */ false);
  }

  /** Constructs and returns full path for the given bucket and object names. */
  public static URI fromResourceId(StorageResourceId resourceId, boolean allowEmptyObjectName) {
    return fromStringPathComponents(
        resourceId.getBucketName(), resourceId.getObjectName(), allowEmptyObjectName);
  }

  /** Constructs and returns full path for the given bucket and object names. */
  public static URI fromStringPathComponents(
      String bucketName, String objectName, boolean allowEmptyObjectName) {
    if (allowEmptyObjectName && bucketName == null && objectName == null) {
      return GoogleCloudStorageFileSystem.GCS_ROOT;
    }

    String authority = StringPaths.validateBucketName(bucketName);
    String path = PATH_DELIMITER + StringPaths.validateObjectName(objectName, allowEmptyObjectName);

    try {
      return new URI(
          GoogleCloudStorageFileSystem.SCHEME,
          authority,
          path,
          /* query= */ null,
          /* fragment= */ null);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(
          String.format("Invalid bucket name (%s) or object name (%s)", bucketName, objectName), e);
    }
  }
}
