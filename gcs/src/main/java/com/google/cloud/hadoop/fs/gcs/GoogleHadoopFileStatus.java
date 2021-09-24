package com.google.cloud.hadoop.fs.gcs;

import com.google.cloud.hadoop.gcsio.FileInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageItemInfo;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

public class GoogleHadoopFileStatus extends FileStatus {
  private final GoogleCloudStorageItemInfo itemInfo;

  GoogleHadoopFileStatus(
      FileInfo fileInfo,
      Path filePath,
      int REPLICATION_FACTOR_DEFAULT,
      long defaultBlockSize,
      FsPermission reportedPermissions,
      String userName) {
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
    this.itemInfo = fileInfo.getItemInfo();
  }

  GoogleCloudStorageItemInfo getItemInfo() {
    return this.itemInfo;
  }
}
