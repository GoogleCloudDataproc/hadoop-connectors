package com.google.cloud.hadoop.fs.gcs;

import com.google.cloud.hadoop.gcsio.FileInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageItemInfo;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
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
    if (fileInfo.exists()) {
      if (!fileInfo.isDirectory()) {
        this.itemInfo =
            GoogleCloudStorageItemInfo.createObject(
                StorageResourceId.fromStringPath(filePath.toString()),
                fileInfo.getCreationTime(),
                fileInfo.getModificationTime(),
                fileInfo.getSize(),
                fileInfo.getContentType(),
                fileInfo.getContentEncoding(),
                fileInfo.getAttributes(),
                fileInfo.getContentGeneration(),
                fileInfo.getMetaGeneration(),
                fileInfo.getVerifyingAttributes());
      } else {

        this.itemInfo =
            GoogleCloudStorageItemInfo.createInferredDirectory(
                StorageResourceId.fromStringPath(filePath.toString()));
      }
    } else {
      this.itemInfo =
          GoogleCloudStorageItemInfo.createNotFound(
              StorageResourceId.fromStringPath(filePath.toString()));
    }
  }

  GoogleCloudStorageItemInfo getItemInfo() {
    return this.itemInfo;
  }
}
