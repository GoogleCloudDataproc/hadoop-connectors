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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * {@link FileStatus} for a {@link GoogleHadoopFileSystem} operations.
 *
 * <p>{@link FileStatus} that has access {@link FileInfo} that has additional GCS metadata.
 */
class GoogleHadoopFileStatus extends FileStatus {

  private final FileInfo fileInfo;

  /** Constructs an instance GoogleHadoopFileStatus */
  GoogleHadoopFileStatus(
      FileInfo fileInfo,
      Path filePath,
      int blockReplication,
      long defaultBlockSize,
      FsPermission reportedPermissions,
      String userName) {
    super(
        fileInfo.getSize(),
        fileInfo.isDirectory(),
        blockReplication,
        defaultBlockSize,
        /* modificationTime= */ fileInfo.getModificationTime(),
        /* accessTime= */ fileInfo.getModificationTime(),
        reportedPermissions,
        /* owner= */ userName,
        /* group= */ userName,
        filePath);
    this.fileInfo = fileInfo;
  }

  /** Returns GCS ItemInfo for the corresponding GoogleHadoopFileStatus instance */
  FileInfo getFileInfo() {
    return fileInfo;
  }
}
