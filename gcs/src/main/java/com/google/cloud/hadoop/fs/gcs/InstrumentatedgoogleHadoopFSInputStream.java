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

import com.google.cloud.hadoop.gcsio.FileInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions;
import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;

/** A class to update the input stream statistics of {@link GoogleHadoopFSInputStreamBase} */
public class InstrumentatedgoogleHadoopFSInputStream extends GoogleHadoopFSInputStreamBase
    implements IOStatisticsSource {

  // Statistic tracker of the Input stream
  private final GhfsInputStreamStatistics streamStatistics;

  // IO Statistics of the current input stream
  private final IOStatistics ioStatistics;

  InstrumentatedgoogleHadoopFSInputStream(
      GoogleHadoopFileSystemBase ghfs,
      URI gcsPath,
      GoogleCloudStorageReadOptions readOptions,
      FileSystem.Statistics statistics)
      throws IOException {
    super(ghfs, gcsPath, readOptions, statistics);
    this.streamStatistics =
        ((InstrumentatedGoogleHadoopFileSystem) ghfs)
            .getInstrumentation()
            .newInputStreamStatistics(statistics);
    this.ioStatistics = streamStatistics.getIOStatistics();
  }

  InstrumentatedgoogleHadoopFSInputStream(
      GoogleHadoopFileSystemBase ghfs, FileInfo fileInfo, FileSystem.Statistics statistics)
      throws IOException {
    super(ghfs, fileInfo, statistics);
    this.streamStatistics =
        ((InstrumentatedGoogleHadoopFileSystem) ghfs)
            .getInstrumentation()
            .newInputStreamStatistics(statistics);
    this.ioStatistics = streamStatistics.getIOStatistics();
  }

  @Override
  public synchronized int read() throws IOException {
    streamStatistics.readOperationStarted(getPos(), 1);
    int response = 0;
    try {
      response = super.read();
    } catch (IOException e) {
      streamStatistics.readException();
      streamStatistics.readOperationCompleted(1, response);
    }
    streamStatistics.bytesRead(1);
    streamStatistics.readOperationCompleted(1, response);
    return response;
  }

  @Override
  public synchronized int read(byte[] buf, int offset, int length) throws IOException {
    streamStatistics.readOperationStarted(getPos(), length);
    int response = 0;
    try {
      response = super.read(buf, offset, length);
    } catch (IOException e) {
      streamStatistics.readException();
    }
    streamStatistics.bytesRead(response);
    streamStatistics.readOperationCompleted(length, response);
    return response;
  }

  @Override
  public synchronized int read(long position, byte[] buf, int offset, int length)
      throws IOException {
    streamStatistics.readOperationStarted(position, length);
    int response = super.read(position, buf, offset, length);
    if (response > 0) {
      streamStatistics.bytesRead(response);
    } else {
      streamStatistics.readException();
    }
    streamStatistics.readOperationCompleted(length, response);
    return response;
  }

  @Override
  public synchronized void seek(long pos) throws IOException {
    long curPos = getPos();
    long diff = pos - curPos;
    if (diff > 0) {
      streamStatistics.seekForwards(diff);
    } else {
      streamStatistics.seekBackwards(diff);
    }
    super.seek(pos);
  }

  @Override
  public synchronized void close() throws IOException {
    streamStatistics.close();
    super.close();
  }

  /**
   * Get the current IOStatistics from input stream
   *
   * @return the iostatistics of the input stream
   */
  @Override
  public IOStatistics getIOStatistics() {
    return this.ioStatistics;
  }
}
