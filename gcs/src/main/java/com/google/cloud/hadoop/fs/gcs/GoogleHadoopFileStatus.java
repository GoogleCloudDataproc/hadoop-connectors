/*
 * Copyright 2021 Google Inc. All Rights Reserved.
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

package com.google.cloud.hadoop.fs.gcs;

import com.google.cloud.hadoop.gcsio.FileInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageItemInfo;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * File status for a GCS "file".
 *
 * <p>Additional support for File Status to access GoogleCloudStorageItemInfo that reduces requests
 * to gcs
 */
public class GoogleHadoopFileStatus extends FileStatus {
  // Meta data of gcs file from file info to be used when opening file with status
  private final GoogleCloudStorageItemInfo itemInfo;

  /**
   * Constructs an instance GoogleHadoopFileStatus
   *
   * @param fileInfo
   * @param filePath
   * @param REPLICATION_FACTOR_DEFAULT
   * @param defaultBlockSize
   * @param reportedPermissions
   * @param userName
   */
  GoogleHadoopFileStatus(
      FileInfo fileInfo,
      Path filePath,
      int REPLICATION_FACTOR_DEFAULT,
      long defaultBlockSize,
      FsPermission reportedPermissions,
      String userName) {
    // Retaining the actual constructor of the parent FileStatus
    super(
        fileInfo.getSize(),
        fileInfo.isDirectory(),
        REPLICATION_FACTOR_DEFAULT,
        defaultBlockSize,
        /* modificationTime= */ fileInfo.getModificationTime(),
        /* accessTime= */ fileInfo.getModificationTime(),
        reportedPermissions,
        /* owner= */ userName,
        /* group= */ userName,
        filePath);
    // Adding item info that will be used for open() call
    this.itemInfo = fileInfo.getItemInfo();
  }

  /** Returns the given files GCS Item Info */
  GoogleCloudStorageItemInfo getItemInfo() {
    return this.itemInfo;
  }
}
