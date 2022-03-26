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
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.common.flogger.GoogleLogger;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import javax.annotation.Nonnull;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;

class GoogleHadoopOutputStream extends OutputStream implements IOStatisticsSource {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  // Path of the file to write to.
  private final URI gcsPath;
  // Output stream corresponding to channel.
  private OutputStream outputStream;

  // Statistics tracker provided by the parent GoogleHadoopFileSystem for recording
  // numbers of bytes written.
  private final FileSystem.Statistics statistics;
  // Statistics tracker for output stream related statistics
  private final GhfsOutputStreamStatistics streamStatistics;

  /**
   * Constructs an instance of GoogleHadoopOutputStream object.
   *
   * @param ghfs Instance of {@link GoogleHadoopFileSystem}.
   * @param gcsPath Path of the file to write to.
   * @param statistics File system statistics object.
   * @param createFileOptions options for file creation
   * @throws IOException if an IO error occurs.
   */
  GoogleHadoopOutputStream(
      GoogleHadoopFileSystem ghfs,
      URI gcsPath,
      FileSystem.Statistics statistics,
      CreateFileOptions createFileOptions)
      throws IOException {
    logger.atFiner().log(
        "GoogleHadoopOutputStream(gcsPath: %s, createFileOptions: %s)", gcsPath, createFileOptions);
    this.gcsPath = gcsPath;
    this.outputStream = createOutputStream(ghfs.getGcsFs(), gcsPath, createFileOptions);
    this.statistics = statistics;
    this.streamStatistics = ghfs.getInstrumentation().newOutputStreamStatistics(statistics);
  }

  private static OutputStream createOutputStream(
      GoogleCloudStorageFileSystem gcsfs, URI gcsPath, CreateFileOptions options)
      throws IOException {
    WritableByteChannel channel;
    try {
      channel = gcsfs.create(gcsPath, options);
    } catch (java.nio.file.FileAlreadyExistsException e) {
      throw (FileAlreadyExistsException)
          new FileAlreadyExistsException(String.format("'%s' already exists", gcsPath))
              .initCause(e);
    }
    OutputStream outputStream = Channels.newOutputStream(channel);
    int bufferSize =
        gcsfs.getOptions().getCloudStorageOptions().getWriteChannelOptions().getBufferSize();
    return bufferSize > 0 ? new BufferedOutputStream(outputStream, bufferSize) : outputStream;
  }

  @Override
  public void write(int b) throws IOException {
    throwIfNotOpen();
    streamStatistics.writeBytes(1);
    outputStream.write(b);
    statistics.incrementBytesWritten(1);
    statistics.incrementWriteOps(1);
  }

  @Override
  public void write(@Nonnull byte[] b, int offset, int len) throws IOException {
    throwIfNotOpen();
    streamStatistics.writeBytes(len);
    outputStream.write(b, offset, len);
    statistics.incrementBytesWritten(len);
    statistics.incrementWriteOps(1);
  }

  @Override
  public void close() throws IOException {
    logger.atFiner().log("close(%s)", gcsPath);
    if (outputStream != null) {
      try {
        outputStream.close();
      } finally {
        outputStream = null;
      }
    }
    streamStatistics.close();
  }

  private void throwIfNotOpen() throws IOException {
    if (outputStream == null) {
      throw new ClosedChannelException();
    }
  }

  OutputStream getInternalOutputStream() {
    return outputStream;
  }

  public GhfsOutputStreamStatistics getStreamStatistics() {
    return streamStatistics;
  }

  /** Get the current IOStatistics from output stream */
  @Override
  public IOStatistics getIOStatistics() {
    return streamStatistics.getIOStatistics();
  }
}
