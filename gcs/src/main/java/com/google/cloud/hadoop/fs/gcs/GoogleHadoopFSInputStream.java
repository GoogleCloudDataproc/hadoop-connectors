/*
 * Copyright 2013 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.fs.gcs;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.cloud.hadoop.gcsio.FileInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.cloud.hadoop.util.GoogleCloudStorageEventBus;
import com.google.cloud.hadoop.util.ITraceFactory;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;
import javax.annotation.Nonnull;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
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

  /**
   * Closed bit. Volatile so reads are non-blocking. Updates must be in a synchronized block to
   * guarantee an atomic check and set
   */
  private volatile boolean closed;

  private final ITraceFactory traceFactory;

  // Statistics tracker provided by the parent GoogleHadoopFileSystem for recording
  // numbers of bytes read.
  private final FileSystem.Statistics statistics;
  // Statistic tracker of the Input stream
  private final GhfsStorageStatistics storageStatistics;

  private final GhfsStreamStats streamStats;
  private final GhfsStreamStats seekStreamStats;

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
    this.storageStatistics = ghfs.getStorageStatistics();

    this.streamStats =
        new GhfsStreamStats(storageStatistics, GhfsStatistic.STREAM_READ_OPERATIONS, gcsPath);
    this.seekStreamStats =
        new GhfsStreamStats(storageStatistics, GhfsStatistic.STREAM_READ_SEEK_OPERATIONS, gcsPath);

    this.traceFactory = ghfs.getTraceFactory();
  }

  @Override
  public synchronized int read() throws IOException {
    checkNotClosed();
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
    long startTimeNs = System.nanoTime();

    checkNotClosed();
    checkNotNull(buf, "buf must not be null");
    if (offset < 0 || length < 0 || length > buf.length - offset) {
      throw new IndexOutOfBoundsException();
    }

    // TODO(user): Wrap this in a while-loop if we ever introduce a non-blocking mode for
    // the underlying channel.
    int numRead = channel.read(ByteBuffer.wrap(buf, offset, length));
    if (numRead > 0) {
      // -1 means we actually read 0 bytes, but requested at least one byte.
      totalBytesRead += numRead;
      statistics.incrementBytesRead(numRead);
      statistics.incrementReadOps(1);
      streamStats.updateReadStreamStats(numRead, startTimeNs);
    }

    storageStatistics.streamReadOperationInComplete(length, Math.max(numRead, 0));

    return numRead;
  }

  @Override
  public synchronized void seek(long pos) throws IOException {
    long startTimeNs = System.nanoTime();

    checkNotClosed();
    logger.atFiner().log("seek(%d)", pos);
    long curPos = getPos();
    long diff = pos - curPos;
    if (diff > 0) {
      storageStatistics.streamReadSeekForward(diff);
    } else {
      storageStatistics.streamReadSeekBackward(diff);
    }
    try {
      channel.position(pos);
    } catch (IllegalArgumentException e) {
      GoogleCloudStorageEventBus.postOnException();
      throw new IOException(e);
    }

    seekStreamStats.updateReadStreamSeekStats(startTimeNs);
  }

  @Override
  public synchronized void close() throws IOException {
    GhfsStorageStatistics.trackDuration(
        storageStatistics,
        GhfsStatistic.STREAM_READ_CLOSE_OPERATIONS,
        gcsPath,
        traceFactory,
        () -> {
          try {
            logger.atFiner().log("close(): %s", gcsPath);
            if (channel != null) {
              logger.atFiner().log(
                  "Closing '%s' file with %d total bytes read", gcsPath, totalBytesRead);
              channel.close();
            }
          } finally {
            streamStats.close();
            seekStreamStats.close();
          }

          return null;
        });
  }

  /**
   * Gets the current position within the file being read.
   *
   * @return The current position within the file being read.
   * @throws IOException if an IO error occurs.
   */
  @Override
  public synchronized long getPos() throws IOException {
    checkNotClosed();
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
    if (!channel.isOpen()) {
      GoogleCloudStorageEventBus.postOnException();
      throw new ClosedChannelException();
    }
    return super.available();
    // }
  }

  /**
   * Verify that the input stream is open. Non-blocking; this gives the last state of the volatile
   * {@link #closed} field.
   *
   * @throws IOException if the connection is closed.
   */
  private void checkNotClosed() throws IOException {
    if (closed) {
      GoogleCloudStorageEventBus.postOnException();
      throw new IOException(gcsPath + ": " + FSExceptionMessages.STREAM_IS_CLOSED);
    }
  }
}
