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

/**
 * A class to update the input stream statistics of {@link
 * com.google.cloud.hadoop.fs.gcs.GoogleHadoopFSInputStream}
 */
public class ForwardingGoogleHadoopFSInputStream extends GoogleHadoopFSInputStream {

  // Statistic tracker of the Input stream
  private final GhfsInputStreamStatistics streamStatistics;

  // IO Statistics of the current input stream
  private final IOStatistics ioStatistics;

  ForwardingGoogleHadoopFSInputStream(
      GoogleHadoopFileSystemBase ghfs,
      URI gcsPath,
      GoogleCloudStorageReadOptions readOptions,
      FileSystem.Statistics statistics)
      throws IOException {
    super(ghfs, gcsPath, readOptions, statistics);
    this.streamStatistics = ghfs.getInstrumentation().newInputStreamStatistics(statistics);
    this.ioStatistics = streamStatistics.getIOStatistics();
  }

  ForwardingGoogleHadoopFSInputStream(
      GoogleHadoopFileSystemBase ghfs, FileInfo fileInfo, FileSystem.Statistics statistics)
      throws IOException {
    super(ghfs, fileInfo, statistics);
    this.streamStatistics = ghfs.getInstrumentation().newInputStreamStatistics(statistics);
    this.ioStatistics = streamStatistics.getIOStatistics();
  }

  @Override
  public synchronized int read() throws IOException {
    // update the statistics of read operation
    streamStatistics.readOperationStarted(getPos(), 1);
    int response = 0;
    try {
      response = super.read();
    } catch (IOException e) {
      // update the statistics of read Exception
      streamStatistics.readException();
      // update the statistics of read operation completion
      streamStatistics.readOperationCompleted(1, response);
    }
    // update the statistics of number of bytes read
    streamStatistics.bytesRead(1);
    // update the statistics of read operation completion
    streamStatistics.readOperationCompleted(1, response);
    return response;
  }

  @Override
  public synchronized int read(byte[] buf, int offset, int length) throws IOException {
    // update the statistics of read operation
    streamStatistics.readOperationStarted(getPos(), length);
    int response = super.read(buf, offset, length);
    if (response > 0) {
      // update the statistics of number of bytes read
      streamStatistics.bytesRead(response);
    } else {
      // update the statistics of read Exception
      streamStatistics.readException();
    }
    // update the statistics of read operation completion
    streamStatistics.readOperationCompleted(length, response);
    return response;
  }

  @Override
  public synchronized int read(long position, byte[] buf, int offset, int length)
      throws IOException {
    // update the statistics of read operation
    streamStatistics.readOperationStarted(position, length);
    int response = super.read(position, buf, offset, length);
    if (response > 0) {
      // update the statistics of number of bytes read
      streamStatistics.bytesRead(response);
    } else {
      // update the statistics of read Exception
      streamStatistics.readException();
    }
    // update the statistics of read operation completion
    streamStatistics.readOperationCompleted(length, response);
    return response;
  }

  @Override
  public synchronized void seek(long pos) throws IOException {
    long curPos = getPos();
    long diff = pos - curPos;
    try {
      super.seek(pos);
      if (diff > 0) {
        // update the statistics related to seek forward operations
        streamStatistics.seekForwards(diff);
      } else {
        // update the statistics related to seek backward operations
        streamStatistics.seekBackwards(diff);
      }
    } catch (IOException e) {
      throw e;
    }
  }

  @Override
  public synchronized void close() throws IOException {
    // Merge the current stream statistics with the instrumentation
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
