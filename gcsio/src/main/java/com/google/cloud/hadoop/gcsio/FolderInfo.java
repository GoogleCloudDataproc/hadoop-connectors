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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.storage.control.v2.Folder;
import javax.annotation.Nonnull;

@VisibleForTesting
/** Contains information about a Folder resource and is applicable for only HN enabled bucket */
public class FolderInfo {
  public static final String BUCKET_PREFIX = "projects/_/buckets/";
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
    this.bucket = getBucket(folder.getName());
    this.folderName = getName(folder.getName());
  }

  public static Folder createFolderInfoObject(@Nonnull String bucketName, String folderName)
      throws RuntimeException {
    if (Strings.isNullOrEmpty(bucketName) || folderName.equals(null))
      throw new RuntimeException("Incorrect folder argument");

    return Folder.newBuilder()
        .setName(BUCKET_PREFIX + bucketName + FOLDER_PREFIX + folderName)
        .build();
  }

  private String getBucket(String path) {
    if (Strings.isNullOrEmpty(path)) return "";
    path = path.split(BUCKET_PREFIX)[1];
    String bucketName = path.equals("") ? "" : path.substring(0, path.indexOf(PATH));
    return bucketName;
  }

  private String getName(String path) {
    if (Strings.isNullOrEmpty(path)) return "";
    String folderName = path.split(FOLDER_PREFIX).length > 1 ? path.split(FOLDER_PREFIX)[1] : "";
    return folderName;
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
    return !Strings.isNullOrEmpty(this.bucket) && Strings.isNullOrEmpty(this.folderName);
  }

  /**
   * Returns the parent folder name if exists, else returns empty string
   *
   * @return parent folderName
   */
  public String getParentFolderName() {
    if (Strings.isNullOrEmpty(this.folderName)) return "";
    int lastIndex = this.folderName.lastIndexOf(PATH, this.folderName.length() - 2);
    return this.folderName.substring(0, lastIndex + 1);
  }

  /** Gets string representation of this instance. */
  public String toString() {
    return BUCKET_PREFIX + getBucket() + FOLDER_PREFIX + getFolderName();
  }
}
