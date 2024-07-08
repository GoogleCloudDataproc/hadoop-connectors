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

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions;
import com.google.cloud.hadoop.util.GoogleCloudStorageEventBus;
import com.google.cloud.hadoop.util.ITraceFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSet;
import com.google.common.flogger.GoogleLogger;
import com.google.common.flogger.LazyArgs;
import com.google.gson.Gson;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystem.Statistics;

/** A seekable and positionable FSInputStream that provides read access to a file. */
class GoogleHadoopFSInputStream extends FSInputStream {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();
  @VisibleForTesting static final String READ_METHOD = "gcsFSRead";
  @VisibleForTesting static final String POSITIONAL_READ_METHOD = "gcsFSReadPositional";
  @VisibleForTesting static final String SEEK_METHOD = "gcsFSSeek";
  @VisibleForTesting static final String CLOSE_METHOD = "gcsFSClose";
  @VisibleForTesting static final String DURATION_NS = "durationNs";
  @VisibleForTesting static final String BYTES_READ = "bytesRead";
  @VisibleForTesting static final String GCS_PATH = "gcsPath";
  @VisibleForTesting static final String METHOD = "method";
  @VisibleForTesting static final String POSITION = "position";
  @VisibleForTesting static final String LENGTH = "length";
  @VisibleForTesting static final String OFFSET = "offset";

  private static final Gson gson = new Gson();

  private final boolean isTraceLoggingEnabled;

  // All store IO access goes through this.
  private final SeekableByteChannel channel;

  // This is used to only log operations which took more time than the specified threshold. This can
  // be used to reduce the amount of logs which is getting logged.
  private final long logThreshold;

  // This is used to control which all log properties are getting logged. This can be used to reduce
  // the amount of logs which is getting logged.
  private final ImmutableSet<String> logFilterProperties;
  private final GhfsStorageStatistics storageStatistics;
  private final ITraceFactory traceFactory;

  // Path of the file to read.
  private URI gcsPath;

  // Number of bytes read through this channel.
  private long totalBytesRead;

  // Statistics tracker provided by the parent GoogleHadoopFileSystemBase for recording
  // numbers of bytes read.
  private final FileSystem.Statistics statistics;

  // Used for single-byte reads.
  private final byte[] singleReadBuf = new byte[1];

  private final GhfsStreamStats streamStats;
  private final GhfsStreamStats seekStreamStats;

  /**
   * Constructs an instance of GoogleHadoopFSInputStream object.
   *
   * @param ghfs Instance of GoogleHadoopFileSystemBase.
   * @param gcsPath Path of the file to read from.
   * @param statistics File system statistics object.
   * @throws IOException if an IO error occurs.
   */
  GoogleHadoopFSInputStream(
      GoogleHadoopFileSystemBase ghfs,
      URI gcsPath,
      GoogleCloudStorageReadOptions readOptions,
      Statistics statistics)
      throws IOException {
    logger.atFiner().log(
        "GoogleHadoopFSInputStream(gcsPath: %s, readOptions: %s)", gcsPath, readOptions);
    this.gcsPath = gcsPath;
    this.statistics = statistics;
    this.totalBytesRead = 0;
    this.isTraceLoggingEnabled = readOptions.isTraceLogEnabled();
    this.channel = ghfs.getGcsFs().open(gcsPath, readOptions);
    this.logThreshold = readOptions.getTraceLogTimeThreshold();
    this.logFilterProperties = readOptions.getTraceLogExcludeProperties();
    this.storageStatistics = ghfs.getStorageStatistics();
    this.streamStats =
        new GhfsStreamStats(storageStatistics, GhfsStatistic.STREAM_READ_OPERATIONS, gcsPath);
    this.seekStreamStats =
        new GhfsStreamStats(storageStatistics, GhfsStatistic.STREAM_READ_SEEK_OPERATIONS, gcsPath);
    this.traceFactory = ghfs.getTraceFactory();
  }

  /**
   * Reads a single byte from the underlying store.
   *
   * @return A single byte from the underlying store or -1 on EOF.
   * @throws IOException if an IO error occurs.
   */
  @Override
  public synchronized int read() throws IOException {
    long startTime = System.nanoTime();
    // TODO(user): Wrap this in a while-loop if we ever introduce a non-blocking mode for the
    // underlying channel.
    int numRead = channel.read(ByteBuffer.wrap(singleReadBuf));
    if (numRead == -1) {
      return -1;
    }
    if (numRead != 1) {
      GoogleCloudStorageEventBus.postOnException();
      throw new IOException(
          String.format(
              "Somehow read %d bytes using single-byte buffer for path %s ending in position %d!",
              numRead, gcsPath, channel.position()));
    }
    byte b = singleReadBuf[0];

    totalBytesRead++;
    statistics.incrementBytesRead(1);
    statistics.incrementReadOps(1);

    // Using a lightweight implementation to update instrumentation. This method can be called quite
    // frequently and need to be lightweight.
    streamStats.updateReadStreamStats(1, startTime);

    return (b & 0xff);
  }

  /**
   * Reads up to length bytes from the underlying store and stores them starting at the specified
   * offset in the given buffer. Less than length bytes may be returned.
   *
   * @param buf The buffer into which data is returned.
   * @param offset The offset at which data is written.
   * @param length Maximum number of bytes to read.
   * @return Number of bytes read or -1 on EOF.
   * @throws IOException if an IO error occurs.
   */
  @Override
  public synchronized int read(byte[] buf, int offset, int length) throws IOException {
    long startTimeNs = System.nanoTime();

    Preconditions.checkNotNull(buf, "buf must not be null");
    if (offset < 0 || length < 0 || length > buf.length - offset) {
      throw new IndexOutOfBoundsException();
    }
    int numRead = channel.read(ByteBuffer.wrap(buf, offset, length));

    readAPITrace(READ_METHOD, startTimeNs, 0, offset, length, numRead, Level.INFO);

    if (numRead > 0) {
      // -1 means we actually read 0 bytes, but requested at least one byte.
      statistics.incrementBytesRead(numRead);
      statistics.incrementReadOps(1);
      totalBytesRead += numRead;
      streamStats.updateReadStreamStats(numRead, startTimeNs);
    }

    storageStatistics.streamReadOperationInComplete(length, Math.max(numRead, 0));

    return numRead;
  }

  /**
   * Reads up to length bytes from the underlying store and stores them starting at the specified
   * offset in the given buffer. Less than length bytes may be returned. Reading starts at the given
   * position.
   *
   * @param position Data is read from the stream starting at this position.
   * @param buf The buffer into which data is returned.
   * @param offset The offset at which data is written.
   * @param length Maximum number of bytes to read.
   * @return Number of bytes read or -1 on EOF.
   * @throws IOException if an IO error occurs.
   */
  @Override
  public synchronized int read(long position, byte[] buf, int offset, int length)
      throws IOException {
    long startTimeNs = System.nanoTime();

    // Note: don't update byteReadMetric here as it will be reported by super.read
    int result = super.read(position, buf, offset, length);
    readAPITrace(POSITIONAL_READ_METHOD, startTimeNs, position, offset, length, result, Level.FINE);

    return result;
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
   * Sets the current position within the file being read.
   *
   * @param pos The position to seek to.
   * @throws IOException if an IO error occurs or if the target position is invalid.
   */
  @Override
  public synchronized void seek(long pos) throws IOException {
    logger.atFiner().log("seek(%d)", pos);
    long startTimeNs = System.nanoTime();

    long curPos = getPos();
    long diff = pos - curPos;
    if (diff > 0) {
      storageStatistics.streamReadSeekForward(diff);
    } else {
      storageStatistics.streamReadSeekBackward(diff);
    }

    try {
      channel.position(pos);
      seekAPITrace(SEEK_METHOD, startTimeNs, pos);
    } catch (IllegalArgumentException e) {
      GoogleCloudStorageEventBus.postOnException();
      throw new IOException(e);
    }

    seekStreamStats.updateReadStreamSeekStats(startTimeNs);
  }

  /**
   * Seeks a different copy of the data. Not supported.
   *
   * @return true if a new source is found, false otherwise.
   */
  @Override
  public synchronized boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }

  /**
   * Closes the current stream.
   *
   * @throws IOException if an IO error occurs.
   */
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
            Stopwatch stopwatch = Stopwatch.createStarted();
            Map<String, Object> apiTraces = new HashMap<>();
            if (channel != null) {
              logger.atFiner().log(
                  "Closing '%s' file with %d total bytes read", gcsPath, totalBytesRead);
              channel.close();
              closeAPITrace(CLOSE_METHOD, stopwatch);
            }
          } finally {
            streamStats.close();
            seekStreamStats.close();
          }

          return null;
        });
  }

  /**
   * Indicates whether this stream supports the 'mark' functionality.
   *
   * @return false (functionality not supported).
   */
  @Override
  public boolean markSupported() {
    // HDFS does not support it either and most Hadoop tools do not expect it.
    return false;
  }

  @Override
  public int available() throws IOException {
    if (!channel.isOpen()) {
      GoogleCloudStorageEventBus.postOnException();
      throw new ClosedChannelException();
    }
    return super.available();
  }

  private void readAPITrace(
      String method,
      long startTimeNs,
      long position,
      int offset,
      int length,
      int bytesRead,
      Level logLevel) {
    if (shouldLog(startTimeNs)) {
      Map<String, Object> jsonMap = new HashMap<>();
      addLogProperty(METHOD, method, jsonMap);
      addLogProperty(GCS_PATH, gcsPath, jsonMap);
      addLogProperty(DURATION_NS, System.nanoTime() - startTimeNs, jsonMap);
      addLogProperty(POSITION, position, jsonMap);
      addLogProperty(OFFSET, offset, jsonMap);
      addLogProperty(LENGTH, length, jsonMap);
      addLogProperty(BYTES_READ, bytesRead, jsonMap);
      captureAPITraces(jsonMap, logLevel);
    }
  }

  private void seekAPITrace(String method, long startTimeNs, long pos) {
    if (isTraceLoggingEnabled) {
      Map<String, Object> jsonMap = new HashMap<>();
      addLogProperty(METHOD, method, jsonMap);
      addLogProperty(GCS_PATH, gcsPath, jsonMap);
      addLogProperty(DURATION_NS, System.nanoTime() - startTimeNs, jsonMap);
      addLogProperty(POSITION, pos, jsonMap);
      captureAPITraces(jsonMap, Level.FINE);
    }
  }

  private void addLogProperty(
      String propertyName, Object propertyValue, Map<String, Object> jsonMap) {
    if (logFilterProperties.contains(propertyName.toLowerCase(Locale.US))) {
      return;
    }

    jsonMap.put(propertyName, propertyValue);
  }

  private boolean shouldLog(Stopwatch stopwatch) {
    return shouldLog(stopwatch.elapsed(TimeUnit.NANOSECONDS));
  }

  private boolean shouldLog(long startTime) {
    return isTraceLoggingEnabled && getElapsedMillisFromStartTime(startTime) >= logThreshold;
  }

  private static long getElapsedMillisFromStartTime(long timeNs) {
    return (System.nanoTime() - timeNs) / 1000_1000;
  }

  private void closeAPITrace(String method, Stopwatch stopwatch) {
    if (shouldLog(stopwatch)) {
      Map<String, Object> jsonMap = new HashMap<>();
      addLogProperty(METHOD, method, jsonMap);
      addLogProperty(GCS_PATH, gcsPath, jsonMap);
      addLogProperty(DURATION_NS, stopwatch.elapsed(TimeUnit.NANOSECONDS), jsonMap);
      captureAPITraces(jsonMap, Level.INFO);
    }
  }

  private void captureAPITraces(Map<String, Object> apiTraces, Level logLevel) {
    if (isTraceLoggingEnabled) {
      logger.at(logLevel).log("%s", LazyArgs.lazy(() -> gson.toJson(apiTraces)));
    }
  }
}
