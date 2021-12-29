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
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;

public class InstrumentatedGoogleHadoopOutputStream extends GoogleHadoopOutputStreamBase
    implements IOStatisticsSource {

  // Statistics tracker for outputstream related statistics
  private final GhfsOutputStreamStatistics streamStatistics;

  // IO Statistics tracker from Output Stream
  private final IOStatistics ioStatistics;

  InstrumentatedGoogleHadoopOutputStream(
      GoogleHadoopFileSystemBase ghfs,
      URI gcsPath,
      FileSystem.Statistics statistics,
      CreateFileOptions createFileOptions)
      throws IOException {
    super(ghfs, gcsPath, statistics, createFileOptions);
    this.streamStatistics =
        ((InstrumentatedGoogleHadoopFileSystem) ghfs)
            .getInstrumentation()
            .newOutputStreamStatistics(statistics);
    this.ioStatistics = streamStatistics.getIOStatistics();
  }

  @Override
  public void write(int b) throws IOException {
    streamStatistics.writeBytes(1);
    super.write(b);
  }

  @Override
  public void write(byte[] b, int offset, int len) throws IOException {
    streamStatistics.writeBytes(len);
    super.write(b, offset, len);
  }

  @Override
  public void close() throws IOException {
    streamStatistics.close();
    super.close();
  }

  @Override
  void throwIfNotOpen() throws IOException {
    try {
      super.throwIfNotOpen();
    } catch (Exception e) {
      streamStatistics.writeException();
      throw e;
    }
  }

  public GhfsOutputStreamStatistics getStreamStatistics() {
    return streamStatistics;
  }

  /** Get the current IOStatistics from output stream */
  @Override
  public IOStatistics getIOStatistics() {
    return ioStatistics;
  }
}
