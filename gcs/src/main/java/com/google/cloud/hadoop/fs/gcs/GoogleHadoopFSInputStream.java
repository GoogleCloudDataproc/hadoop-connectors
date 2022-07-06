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

import static com.google.cloud.hadoop.fs.gcs.GhfsTimeStatistic.SEEK;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.max;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.trackDuration;

import com.google.cloud.hadoop.gcsio.FileInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.common.base.Stopwatch;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;

class GoogleHadoopFSInputStream extends FSInputStream implements IOStatisticsSource {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  // Used for single-byte reads.
  private final byte[] singleReadBuf = new byte[1];

  // Path of the file to read.
  private final URI gcsPath;
  // All store IO access goes through this.
  private final SeekableByteChannel channel;
  // Number of bytes read through this channel.
  private long totalBytesRead = 0;

  // Statistics tracker provided by the parent GoogleHadoopFileSystem for recording
  // numbers of bytes read.
  private final FileSystem.Statistics statistics;
  // Statistic tracker of the Input stream
  private final GhfsInputStreamStatistics streamStatistics;
  // Instrumentation to track Statistics
  private GhfsInstrumentation instrumentation;

  static GoogleHadoopFSInputStream create(
      GoogleHadoopFileSystem ghfs, URI gcsPath, FileSystem.Statistics statistics)
      throws IOException {
    logger.atFiner().log("create(gcsPath: %s)", gcsPath);
    GoogleCloudStorageFileSystem gcsFs = ghfs.getGcsFs();
    SeekableByteChannel channel =
        gcsFs.open(gcsPath, gcsFs.getOptions().getCloudStorageOptions().getReadChannelOptions());
    return new GoogleHadoopFSInputStream(ghfs, gcsPath, channel, statistics);
  }

  static GoogleHadoopFSInputStream create(
      GoogleHadoopFileSystem ghfs, FileInfo fileInfo, FileSystem.Statistics statistics)
      throws IOException {
    logger.atFiner().log("create(fileInfo: %s)", fileInfo);
    GoogleCloudStorageFileSystem gcsFs = ghfs.getGcsFs();
    SeekableByteChannel channel =
        gcsFs.open(fileInfo, gcsFs.getOptions().getCloudStorageOptions().getReadChannelOptions());
    return new GoogleHadoopFSInputStream(ghfs, fileInfo.getPath(), channel, statistics);
  }

  private GoogleHadoopFSInputStream(
      GoogleHadoopFileSystem ghfs,
      URI gcsPath,
      SeekableByteChannel channel,
      FileSystem.Statistics statistics) {
    logger.atFiner().log("GoogleHadoopFSInputStream(gcsPath: %s)", gcsPath);
    this.gcsPath = gcsPath;
    this.channel = channel;
    this.statistics = statistics;
    this.streamStatistics = ghfs.getInstrumentation().newInputStreamStatistics(statistics);
    this.instrumentation = ghfs.getInstrumentation();
  }

  @Override
  public synchronized int read() throws IOException {
    int numRead = read(singleReadBuf, /* offset= */ 0, /* length= */ 1);
    checkState(
        numRead == -1 || numRead == 1,
        "Read %s bytes using single-byte buffer for path %s ending in position %s",
        numRead,
        gcsPath,
        channel.position());
    return numRead > 0 ? singleReadBuf[0] & 0xff : numRead;
  }

  @Override
  public synchronized int read(@Nonnull byte[] buf, int offset, int length) throws IOException {
    streamStatistics.readOperationStarted(getPos(), length);
    checkNotNull(buf, "buf must not be null");
    if (offset < 0 || length < 0 || length > buf.length - offset) {
      throw new IndexOutOfBoundsException();
    }
    int response = 0;
    try {
      // TODO(user): Wrap this in a while-loop if we ever introduce a non-blocking mode for the
      // underlying channel.
      int numRead = channel.read(ByteBuffer.wrap(buf, offset, length));
      if (numRead > 0) {
        // -1 means we actually read 0 bytes, but requested at least one byte.
        totalBytesRead += numRead;
        statistics.incrementBytesRead(numRead);
        statistics.incrementReadOps(1);
      }
      response = numRead;
    } catch (IOException e) {
      streamStatistics.readException();
    }
    streamStatistics.bytesRead(max(response, 0));
    streamStatistics.readOperationCompleted(length, max(response, 0));
    return response;
  }

  @Override
  public synchronized void seek(long pos) throws IOException {
    trackDuration(
        instrumentation,
        GhfsTimeStatistic.SEEK.getSymbol(),
        () -> {
          logger.atFiner().log("seek(%d)", pos);
          long curPos = getPos();
          long diff = pos - curPos;
          if (diff > 0) {
            streamStatistics.seekForwards(diff);
          } else {
            streamStatistics.seekBackwards(diff);
          }
          try {
            channel.position(pos);
          } catch (IllegalArgumentException e) {
            throw new IOException(e);
          }
          return null;
        });
  }

  @Override
  public synchronized void close() throws IOException {
    logger.atFiner().log("close(): %s", gcsPath);
    streamStatistics.close();
    if (channel != null) {
      logger.atFiner().log("Closing '%s' file with %d total bytes read", gcsPath, totalBytesRead);
      channel.close();
    }
  }

  /**
   * Gets the current position within the file being read.
   *
   * @return The current position within the file being read.
   * @throws IOException if an IO error occurs.
   */
  @Override
  public synchronized long getPos() throws IOException {
    long pos = channel.position();
    logger.atFiner().log("getPos(): %d", pos);
    return pos;
  }

  /**
   * Seeks a different copy of the data. Not supported.
   *
   * @return true if a new source is found, false otherwise.
   */
  @Override
  public boolean seekToNewSource(long targetPos) {
    logger.atFiner().log("seekToNewSource(%d): false", targetPos);
    return false;
  }

  @Override
  public int available() throws IOException {
    logger.atFiner().log("available()");
    if (!channel.isOpen()) {
      throw new ClosedChannelException();
    }
    return 0;
  }

  /**
   * Get the current IOStatistics from input stream
   *
   * @return the IOStatistics of the input stream
   */
  @Override
  public IOStatistics getIOStatistics() {
    return streamStatistics.getIOStatistics();
  }
}
