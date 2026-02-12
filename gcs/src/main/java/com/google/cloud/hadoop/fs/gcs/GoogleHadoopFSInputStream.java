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

import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.STREAM_READ_OPERATIONS;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.STREAM_READ_SEEK_OPERATIONS;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.STREAM_READ_VECTORED_OPERATIONS;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.max;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.trackDuration;

import com.google.cloud.hadoop.gcsio.FileInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions;
import com.google.cloud.hadoop.gcsio.ReadVectoredSeekableByteChannel;
import com.google.cloud.hadoop.gcsio.VectoredIORange;
import com.google.cloud.hadoop.util.GoogleCloudStorageEventBus;
import com.google.cloud.hadoop.util.ITraceFactory;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileRange;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.statistics.DurationTrackerFactory;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.util.functional.CallableRaisingIOE;

class GoogleHadoopFSInputStream extends FSInputStream implements IOStatisticsSource {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  // Used for single-byte reads.
  private final byte[] singleReadBuf = new byte[1];

  // Path of the file to read.
  private final URI gcsPath;
  // File Info of gcsPath, will be pre-populated in some cases i.e. when Json client is used and
  // failFast is disabled.
  private final FileInfo fileInfo;
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
  private final GhfsGlobalStorageStatistics storageStatistics;

  private final GhfsStreamStats streamStats;
  private final GhfsStreamStats seekStreamStats;
  private final GhfsStreamStats vectoredReadStats;
  private final ConcurrentHashMap<String, Long> rangeReadThreadStats;

  // Statistic tracker of the Input stream
  private final GhfsInputStreamStatistics streamStatistics;
  private final Supplier<VectoredIOImpl> vectoredIOSupplier;
  private final GoogleCloudStorageFileSystem gcsFs;

  static GoogleHadoopFSInputStream create(
      GoogleHadoopFileSystem ghfs, URI gcsPath, FileSystem.Statistics statistics)
      throws IOException {
    logger.atFiner().log("create(gcsPath: %s)", gcsPath);
    GoogleCloudStorageFileSystem gcsFs = ghfs.getGcsFs();
    FileInfo fileInfo = null;
    SeekableByteChannel channel;
    // Extract out the fileInfo call here and use it in readChannel as well as in vectoredRead API
    if (shouldPreFetchFileInfo(gcsFs.getOptions())) {
      // ingest the fileInfo extracted while creating gcsio channel to avoid duplicate call.
      fileInfo = gcsFs.getFileInfoObject(gcsPath);
      channel =
          gcsFs.open(fileInfo, gcsFs.getOptions().getCloudStorageOptions().getReadChannelOptions());
    } else {
      // cases where fileInfo wouldn't have been requested in gcsio layer.
      channel =
          gcsFs.open(gcsPath, gcsFs.getOptions().getCloudStorageOptions().getReadChannelOptions());
    }
    return new GoogleHadoopFSInputStream(ghfs, gcsPath, fileInfo, channel, statistics);
  }

  private static boolean shouldPreFetchFileInfo(GoogleCloudStorageFileSystemOptions gcsFSOptions) {
    // FileInfo is requested while opening the channel in gcsio channel layer when
    // failFastOnNotFound is enabled
    if (gcsFSOptions
        .getCloudStorageOptions()
        .getReadChannelOptions()
        .isFastFailOnNotFoundEnabled()) {
      return true;
    }
    return false;
  }

  static GoogleHadoopFSInputStream create(
      GoogleHadoopFileSystem ghfs, FileInfo fileInfo, FileSystem.Statistics statistics)
      throws IOException {
    logger.atFiner().log("create(fileInfo: %s)", fileInfo);
    GoogleCloudStorageFileSystem gcsFs = ghfs.getGcsFs();
    SeekableByteChannel channel =
        gcsFs.open(fileInfo, gcsFs.getOptions().getCloudStorageOptions().getReadChannelOptions());
    return new GoogleHadoopFSInputStream(ghfs, fileInfo.getPath(), fileInfo, channel, statistics);
  }

  private GoogleHadoopFSInputStream(
      GoogleHadoopFileSystem ghfs,
      URI gcsPath,
      FileInfo fileInfo,
      SeekableByteChannel channel,
      FileSystem.Statistics statistics) {
    logger.atFiner().log("GoogleHadoopFSInputStream(gcsPath: %s)", gcsPath);
    this.gcsPath = gcsPath;
    this.channel = channel;
    this.fileInfo = fileInfo;
    this.gcsFs = ghfs.getGcsFs();
    this.statistics = statistics;
    this.storageStatistics = ghfs.getGlobalGcsStorageStatistics();

    this.streamStatistics = ghfs.getInstrumentation().newInputStreamStatistics(statistics);

    this.streamStats =
        new GhfsStreamStats(storageStatistics, GhfsStatistic.STREAM_READ_OPERATIONS, gcsPath);
    this.seekStreamStats =
        new GhfsStreamStats(storageStatistics, GhfsStatistic.STREAM_READ_SEEK_OPERATIONS, gcsPath);
    this.vectoredReadStats =
        new GhfsStreamStats(
            storageStatistics, GhfsStatistic.STREAM_READ_VECTORED_OPERATIONS, gcsPath);
    this.rangeReadThreadStats = new ConcurrentHashMap<>();

    this.traceFactory = ghfs.getTraceFactory();
    this.vectoredIOSupplier = ghfs.getVectoredIOSupplier();
  }

  /**
   * {@inheritDoc} Vectored read implementation for GoogleHadoopFSInputStream.
   *
   * @param ranges the byte ranges to read.
   * @param allocate the function to allocate ByteBuffer.
   * @throws IOException IOE if any.
   */
  @Override
  public void readVectored(List<? extends FileRange> ranges, IntFunction<ByteBuffer> allocate)
      throws IOException {
    trackDuration(
        streamStatistics,
        STREAM_READ_VECTORED_OPERATIONS.getSymbol(),
        () -> {
          if (channel instanceof ReadVectoredSeekableByteChannel) {
            ReadVectoredSeekableByteChannel readVectoredSeekableByteChannelChannel =
                (ReadVectoredSeekableByteChannel) channel;
            ranges.forEach(
                range -> {
                  CompletableFuture<ByteBuffer> result = new CompletableFuture<>();
                  range.setData(result);
                });
            readVectoredSeekableByteChannelChannel.readVectored(
                ranges.stream()
                    .map(
                        range ->
                            VectoredIORange.builder()
                                .setLength(range.getLength())
                                .setOffset(range.getOffset())
                                .setData(range.getData())
                                .build())
                    .collect(Collectors.toList()),
                allocate);
          } else {
            long startTimeNs = System.nanoTime();
            vectoredIOSupplier
                .get()
                .readVectored(
                    ranges,
                    allocate,
                    gcsFs,
                    fileInfo,
                    gcsPath,
                    streamStatistics,
                    rangeReadThreadStats);
            statistics.incrementReadOps(1);
            vectoredReadStats.updateVectoredReadStreamStats(startTimeNs);
          }
          return null;
        });
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

    return trackDuration(
        streamStatistics,
        STREAM_READ_OPERATIONS.getSymbol(),
        () -> {
          long startTimeNs = System.nanoTime();
          checkNotClosed();
          // streamStatistics.readOperationStarted(getPos(), length);
          checkNotNull(buf, "buf must not be null");
          if (offset < 0 || length < 0 || length > buf.length - offset) {
            throw new IndexOutOfBoundsException();
          }
          int response = 0;
          try {
            // TODO(user): Wrap this in a while-loop if we ever introduce a non-blocking mode for
            // the underlying channel.
            int numRead = channel.read(ByteBuffer.wrap(buf, offset, length));
            if (numRead > 0) {
              // -1 means we actually read 0 bytes, but requested at least one byte.
              totalBytesRead += numRead;
              statistics.incrementReadOps(1);
              streamStats.updateReadStreamStats(numRead, startTimeNs);
            }

            storageStatistics.streamReadOperationInComplete(length, Math.max(numRead, 0));
            response = numRead;
          } catch (IOException e) {
            streamStatistics.readException();
            throw e;
          }
          streamStatistics.bytesRead(max(response, 0));
          streamStatistics.readOperationCompleted(length, max(response, 0));
          return response;
        });
  }

  @Override
  public synchronized void seek(long pos) throws IOException {

    trackDuration(
        streamStatistics,
        STREAM_READ_SEEK_OPERATIONS.getSymbol(),
        () -> {
          long startTimeNs = System.nanoTime();
          checkNotClosed();
          logger.atFiner().log("seek(%d)", pos);
          long curPos = getPos();
          long diff = pos - curPos;
          if (diff > 0) {
            streamStatistics.seekForwards(diff);
            storageStatistics.streamReadSeekForward(diff);
          } else {
            streamStatistics.seekBackwards(diff);
            storageStatistics.streamReadSeekBackward(diff);
          }
          try {
            channel.position(pos);
          } catch (IllegalArgumentException e) {
            GoogleCloudStorageEventBus.postOnException();
            throw new IOException(e);
          }

          seekStreamStats.updateReadStreamSeekStats(startTimeNs);
          return null;
        });
  }

  @Override
  public synchronized void close() throws IOException {
    boolean isClosed = closed;
    trackDurationWithTracing(
        streamStatistics,
        storageStatistics,
        GhfsStatistic.STREAM_READ_CLOSE_OPERATIONS,
        gcsPath,
        traceFactory,
        () -> {
          if (!closed) {
            closed = true;
            try {
              logger.atFiner().log("close(): %s", gcsPath);
              try {
                if (channel != null) {
                  logger.atFiner().log(
                      "Closing '%s' file with %d total bytes read", gcsPath, totalBytesRead);
                  channel.close();
                }
              } catch (Exception e) {
                logger.atWarning().withCause(e).log(
                    "Error while closing underneath read channel resources for path: %s", gcsPath);
              }
            } finally {
              statistics.incrementBytesRead(streamStatistics.getBytesRead());
              mergeRangeReadThreadStats();
              rangeReadThreadStats.clear();
              streamStats.close();
              seekStreamStats.close();
              vectoredReadStats.close();
            }
          }
          return null;
        });

    if (!isClosed) {
      streamStatistics.close();
    }
  }

  /**
   * Merges the range read stats collected in readVectored call to threadLocal stats of main thread.
   */
  private void mergeRangeReadThreadStats() {
    GhfsThreadLocalStatistics tlStats = storageStatistics.getThreadLocalStatistics();
    rangeReadThreadStats.forEach(
        (key, value) -> {
          tlStats.increment(key, value);
        });
  }

  /**
   * Tracks the duration of the operation {@code operation}. Also setup operation tracking using
   * {@code ThreadTrace}.
   */
  private <B> B trackDurationWithTracing(
      DurationTrackerFactory durationTracker,
      @Nonnull GhfsGlobalStorageStatistics stats,
      GhfsStatistic statistic,
      Object context,
      ITraceFactory traceFactory,
      CallableRaisingIOE<B> operation)
      throws IOException {

    return GhfsGlobalStorageStatistics.trackDuration(
        durationTracker, stats, statistic, context, traceFactory, operation);
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
