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

package com.google.cloud.hadoop.fs.gcs;

import com.google.cloud.hadoop.gcsio.CreateFileOptions;
import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.fs.FileSystem;

public class GoogleHadoopOutputStream extends InstrumentatedGoogleHadoopOutputStream {

  /**
   * Constructs an instance of GoogleHadoopOutputStream object.
   *
   * @param ghfs Instance of GoogleHadoopFileSystemBase.
   * @param gcsPath Path of the file to write to.
   * @param statistics File system statistics object.
   * @param createFileOptions options for file creation
   * @throws IOException if an IO error occurs.
   */
  GoogleHadoopOutputStream(
      GoogleHadoopFileSystemBase ghfs,
      URI gcsPath,
      FileSystem.Statistics statistics,
      CreateFileOptions createFileOptions)
      throws IOException {
    super(ghfs, gcsPath, statistics, createFileOptions);
  }
}
