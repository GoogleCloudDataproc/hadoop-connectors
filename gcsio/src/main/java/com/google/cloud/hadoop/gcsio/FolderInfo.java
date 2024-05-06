/*
 * Copyright 2024 Google Inc. All Rights Reserved.
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

import static com.google.common.base.Preconditions.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.storage.control.v2.Folder;
import javax.annotation.Nonnull;

@VisibleForTesting
/** Contains information about a Folder resource and is applicable for only HN enabled bucket */
public class FolderInfo {
  public static final String BUCKET_PREFIX = "projects/_/buckets/"; // TODO : check it !!
  public static final String FOLDER_PREFIX = "/folders/";
  private static final String PATH = "/";

  private final String bucket;

  private final String folderName;

  /**
   * Constructs an instance of FolderInfo.
   *
   * @param folder Information about the underlying folder.
   */
  public FolderInfo(@Nonnull Folder folder) {
    checkState(!Strings.isNullOrEmpty(folder.getName()), "Folder resource has invalid path");
    this.bucket = getBucketString(folder.getName());
    this.folderName = getFolderString(folder.getName());
  }

  /**
   * Returns the folder object with provided bucket and path
   *
   * @param bucketName
   * @param folderName
   * @return FolderInfo object
   */
  public static Folder createFolderInfoObject(String bucketName, String folderName)
      throws RuntimeException {
    checkState(
        !Strings.isNullOrEmpty(bucketName), "Folder resource has invalid bucket name", bucketName);
    checkState(folderName != null, "Folder resource has invalid folder name", bucketName);

    return Folder.newBuilder()
        .setName(String.join("", BUCKET_PREFIX, bucketName, FOLDER_PREFIX, folderName))
        .build();
  }

  private String getBucketString(String path) {
    checkState(
        path.startsWith(BUCKET_PREFIX),
        "Invalid bucket resource name. Bucket resource name must begin with 'projects/_/buckets/' for global-namespaced buckets or 'projects/{project number}/buckets/' for project-namespaced buckets, and contain no invalid characters or patterns.");
    int startIndexOfBucketPrefix = path.indexOf(BUCKET_PREFIX) + BUCKET_PREFIX.length();
    return path.substring(startIndexOfBucketPrefix, path.indexOf(PATH, startIndexOfBucketPrefix));
  }

  private String getFolderString(String path) {
    checkState(path.contains(FOLDER_PREFIX), "Invalid folder path");
    int startIndex = path.indexOf(FOLDER_PREFIX) + FOLDER_PREFIX.length();
    return path.substring(startIndex);
  }

  /** Gets the path of this file or directory. */
  public String getBucket() {
    return this.bucket;
  }

  /** Returns the folder name, ie path excluding the bucket name */
  public String getFolderName() {
    return this.folderName;
  }

  public boolean isBucket() {
    return this.folderName.equals("");
  }

  /**
   * Returns the parent folder name if exists, else returns empty string
   *
   * @return parent folderName
   */
  public String getParentFolderName() {
    int lastIndex = this.folderName.lastIndexOf(PATH, this.folderName.length() - 2);
    return this.folderName.substring(0, lastIndex + 1);
  }

  /** Gets string representation of this instance. */
  public String toString() {
    return String.join("", BUCKET_PREFIX, this.bucket, FOLDER_PREFIX, this.folderName);
  }
}
