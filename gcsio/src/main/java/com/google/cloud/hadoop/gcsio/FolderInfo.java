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
  public static final String BUCKET_PREFIX = "projects/_/buckets/";
  public static final String FOLDER_PREFIX = "/folders/";
  public static final String PATH = "/";

  private final String bucket;

  private final String folderName;

  /**
   * Constructs an instance of FolderInfo.
   *
   * @param folder Information about the underlying folder.
   */
  public FolderInfo(@Nonnull Folder folder) {
    checkState(
        !Strings.isNullOrEmpty(folder.getName()),
        "Folder resource has invalid path : %s",
        folder.getName());
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
  public static Folder createFolderInfoObject(String bucketName, String folderName) {
    checkState(
        !Strings.isNullOrEmpty(bucketName),
        "Folder resource has invalid bucket name: %s",
        bucketName);
    checkState(folderName != null, "Folder resource has invalid folder name: %s", folderName);

    // Add "/" suffix only if foldername is not empty and does not end with "/"
    String suffix = (folderName.equals("") ? "" : (folderName.endsWith(PATH) ? "" : PATH));
    return Folder.newBuilder()
        .setName(String.join("", BUCKET_PREFIX, bucketName, FOLDER_PREFIX, folderName, suffix))
        .build();
  }

  /**
   * Returns the bucket string. Eg : /projects/_/buckets/BUCKET_NAME/folders/FOLDER_NAME is the
   * template of path, then bucket string will be BUCKET_NAME
   *
   * @param path
   * @return bucket string
   */
  private String getBucketString(String path) {
    checkState(
        path.startsWith(BUCKET_PREFIX),
        "Invalid bucket resource name. Bucket resource name must begin with 'projects/_/buckets/' for global-namespaced buckets and contain no invalid characters or patterns : %s",
        path);
    int startIndexOfBucketPrefix = path.indexOf(BUCKET_PREFIX) + BUCKET_PREFIX.length();
    return path.substring(startIndexOfBucketPrefix, path.indexOf(PATH, startIndexOfBucketPrefix));
  }

  /**
   * Returns the Folder string. Eg : /projects/_/buckets/BUCKET_NAME/folders/FOLDER_NAME is the
   * template of path, then folder string will be FOLDER_NAME eg :
   * /projects/_/buckets/BUCKET_NAME/folders/A/B/ -> returns A/B/ eg :
   * /projects/_/buckets/BUCKET_NAME/folders/ -> returns ""
   *
   * <p>Since this method is always called after createFolderInfoObject() method, "/" suffix is
   * already taken care of.
   *
   * @param path
   * @return
   */
  private String getFolderString(String path) {
    checkState(path.contains(FOLDER_PREFIX), "Invalid folder path: %s", path);
    int startIndex = path.indexOf(FOLDER_PREFIX) + FOLDER_PREFIX.length();
    return path.substring(startIndex);
  }

  /** Gets the path of this file or directory. */
  public String getBucket() {
    return bucket;
  }

  /** Returns the folder name, ie path excluding the bucket name */
  public String getFolderName() {
    return folderName;
  }

  public boolean isBucket() {
    return folderName.equals("");
  }

  /**
   * Returns the parent folder name if exists, else returns empty string
   *
   * @return parent folderName
   */
  public String getParentFolderName() {
    int lastIndex = folderName.lastIndexOf(PATH, folderName.length() - 2);
    return folderName.substring(0, lastIndex + 1);
  }

  /** Gets string representation of this instance. */
  public String toString() {
    return String.join("", BUCKET_PREFIX, bucket, FOLDER_PREFIX, folderName);
  }
}
