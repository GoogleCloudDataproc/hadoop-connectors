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
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.common.flogger.GoogleLogger;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;

class GoogleHadoopOutputStream extends OutputStream implements IOStatisticsSource {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();
  // Statistics tracker for outputstream related statistics
  private final GhfsOutputStreamStatistics streamStatistics;
  // IO Statistics tracker from Output Stream
  private final IOStatistics ioStatistics;
  // Path of the file to write to.
  private final URI gcsPath;
  // Statistics tracker provided by the parent GoogleHadoopFileSystem for recording
  // numbers of bytes written.
  private final FileSystem.Statistics statistics;
  // All store IO access goes through this.
  private WritableByteChannel channel;
  // Output stream corresponding to channel.
  private OutputStream out;

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
    this.statistics = statistics;
    GoogleCloudStorageFileSystem gcsfs = ghfs.getGcsFs();
    this.channel = createChannel(gcsfs, gcsPath, createFileOptions);
    this.out = createOutputStream(this.channel, gcsfs.getOptions().getCloudStorageOptions());
    this.streamStatistics = ghfs.getInstrumentation().newOutputStreamStatistics(statistics);
    this.ioStatistics = GoogleHadoopOutputStream.this.streamStatistics.getIOStatistics();
  }

  private static WritableByteChannel createChannel(
      GoogleCloudStorageFileSystem gcsfs, URI gcsPath, CreateFileOptions options)
      throws IOException {
    try {
      return gcsfs.create(gcsPath, options);
    } catch (java.nio.file.FileAlreadyExistsException e) {
      throw (FileAlreadyExistsException)
          new FileAlreadyExistsException(String.format("'%s' already exists", gcsPath))
              .initCause(e);
    }
  }

  private static OutputStream createOutputStream(
      WritableByteChannel channel, GoogleCloudStorageOptions gcsOptions) {
    OutputStream out = Channels.newOutputStream(channel);
    int bufferSize = gcsOptions.getWriteChannelOptions().getBufferSize();
    return bufferSize > 0 ? new BufferedOutputStream(out, bufferSize) : out;
  }

  @Override
  public void write(int b) throws IOException {
    streamStatistics.writeBytes(1);
    throwIfNotOpen();
    out.write(b);
    statistics.incrementBytesWritten(1);
    statistics.incrementWriteOps(1);
  }

  @Override
  public void write(byte[] b, int offset, int len) throws IOException {
    streamStatistics.writeBytes(len);
    throwIfNotOpen();
    out.write(b, offset, len);
    statistics.incrementBytesWritten(len);
    statistics.incrementWriteOps(1);
  }

  @Override
  public void close() throws IOException {
    streamStatistics.close();
    logger.atFiner().log("close(%s)", gcsPath);
    if (out != null) {
      try {
        out.close();
      } finally {
        out = null;
        channel = null;
      }
    }
  }

  void throwIfNotOpen() throws IOException {
    try {
      if (!isOpen()) {
        throw new ClosedChannelException();
      }
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

  private boolean isOpen() {
    return out != null;
  }

  WritableByteChannel getInternalChannel() {
    return channel;
  }
}
