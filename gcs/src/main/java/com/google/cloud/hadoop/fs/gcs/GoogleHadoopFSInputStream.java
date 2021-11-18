package com.google.cloud.hadoop.fs.gcs;

import com.google.cloud.hadoop.gcsio.FileInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions;
import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.fs.FileSystem;

public class GoogleHadoopFSInputStream extends GoogleHadoopFSInputStreamInstrumentation {
  GoogleHadoopFSInputStream(
      GoogleHadoopFileSystemBase ghfs,
      URI gcsPath,
      GoogleCloudStorageReadOptions readOptions,
      FileSystem.Statistics statistics)
      throws IOException {
    super(ghfs, gcsPath, readOptions, statistics);
  }

  GoogleHadoopFSInputStream(
      GoogleHadoopFileSystemBase ghfs, FileInfo fileInfo, FileSystem.Statistics statistics)
      throws IOException {
    super(ghfs, fileInfo, statistics);
  }
}
