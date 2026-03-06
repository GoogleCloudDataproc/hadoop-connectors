/*
 * Copyright 2024 Google LLC
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

import static org.apache.hadoop.fs.VectoredReadUtils.isOrderedDisjoint;
import static org.apache.hadoop.fs.VectoredReadUtils.mergeSortedRanges;
import static org.apache.hadoop.fs.VectoredReadUtils.sliceTo;
import static org.apache.hadoop.fs.VectoredReadUtils.validateRangeRequest;

import com.google.cloud.hadoop.gcsio.FileInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.flogger.GoogleLogger;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;
import javax.annotation.Nonnull;
import org.apache.hadoop.fs.FileRange;
import org.apache.hadoop.fs.StorageStatistics.LongStatistic;
import org.apache.hadoop.fs.VectoredReadUtils;
import org.apache.hadoop.fs.impl.CombinedFileRange;

@VisibleForTesting
public class VectoredIOImpl implements Closeable {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();
  private final BlockingQueue taskQueue = new LinkedBlockingQueue<Runnable>();
  private final VectoredReadOptions vectoredReadOptions;
  private final GhfsGlobalStorageStatistics storageStatistics;
  private ExecutorService boundedThreadPool;

  public VectoredIOImpl(
      VectoredReadOptions vectoredReadOptions, GhfsGlobalStorageStatistics storageStatistics) {
    this.vectoredReadOptions = vectoredReadOptions;
    // same fixedThreadPool executor, but provided a way to query task queue
    this.boundedThreadPool =
        new ThreadPoolExecutor(
            vectoredReadOptions.getReadThreads(),
            vectoredReadOptions.getReadThreads(),
            0L,
            TimeUnit.MILLISECONDS,
            taskQueue,
            new ThreadFactoryBuilder()
                .setNameFormat("vectoredRead-range-pool-%d")
                .setDaemon(true)
                .build());
    this.storageStatistics = storageStatistics;
  }

  /**
   * Reads data from Google Cloud Storage using vectored I/O operations.
   *
   * @param ranges List of file ranges to read.
   * @param allocate Function to allocate ByteBuffer for reading.
   * @throws IOException if an I/O error occurs.
   */
  /**
   * Reads data from Google Cloud Storage using vectored I/O operations.
   *
   * @param ranges List of file ranges to read.
   * @param allocate Function to allocate ByteBuffer for reading.
   * @param gcsFs GCFS implementation to use while creating channel and reading content for ranges.
   * @param fileInfo FileInfo of the gcs object agaisnt which range request are fired, this can be
   *     null for some code path fall back to URI path provided.
   * @param gcsPath URI of the gcs object for which the range requests are fired.
   * @param rangeReadThreadStats concurrent map to capture all threadLocal stats collected during
   *     range processing.
   * @throws IOException If invalid range is requested, offset<0.
   */
  public void readVectored(
      List<? extends FileRange> ranges,
      IntFunction<ByteBuffer> allocate,
      GoogleCloudStorageFileSystem gcsFs,
      FileInfo fileInfo,
      @Nonnull URI gcsPath,
      GhfsInputStreamStatistics streamStatistics,
      final ConcurrentHashMap<String, Long> rangeReadThreadStats)
      throws IOException {
    VectoredReadChannel vectoredReadChannel =
        new VectoredReadChannel(gcsFs, fileInfo, gcsPath, streamStatistics, rangeReadThreadStats);
    vectoredReadChannel.readVectored(ranges, allocate);
  }

  class VectoredReadChannel {

    private final GhfsInputStreamStatistics streamStatistics;
    private final ReadChannelProvider channelProvider;
    // aggregates the threadLocal stats for individual ranges.
    private final ConcurrentHashMap<String, Long> rangeStatsAccumulator;

    public VectoredReadChannel(
        GoogleCloudStorageFileSystem gcsFs,
        FileInfo fileInfo,
        URI gcsPath,
        GhfsInputStreamStatistics streamStatistics,
        final ConcurrentHashMap<String, Long> rangeStatsAccumulator) {
      this.channelProvider = new ReadChannelProvider(gcsFs, fileInfo, gcsPath);
      this.streamStatistics = streamStatistics;
      this.rangeStatsAccumulator = rangeStatsAccumulator;
    }

    private void readVectored(List<? extends FileRange> ranges, IntFunction<ByteBuffer> allocate)
        throws IOException {
      List<? extends FileRange> sortedRanges = validateNonOverlappingAndReturnSortedRanges(ranges);
      for (FileRange range : ranges) {
        // TODO(user): upgrade to use validateAndSortRanges
        validateRangeRequest(range);
        CompletableFuture<ByteBuffer> result = new CompletableFuture<>();
        range.setData(result);
      }
      if (shouldMergeRanges(ranges)) {
        updateRangeSizeCounters(sortedRanges.size(), sortedRanges.size());
        // case when ranges are not merged
        for (FileRange sortedRange : sortedRanges) {
          long startTimer = System.currentTimeMillis();
          boundedThreadPool.submit(
              () -> {
                logger.atFiner().log("Submitting range %s for execution.", sortedRange);
                // Reset thread stats as a new task is being submitted to this thread.
                // all required stats must be collected before task is finished.
                resetThreadLocalStats();
                readSingleRange(sortedRange, allocate, channelProvider);
                long endTimer = System.currentTimeMillis();
                storageStatistics.updateStats(
                    GhfsStatistic.STREAM_READ_VECTORED_READ_RANGE_DURATION,
                    endTimer - startTimer,
                    channelProvider.gcsPath);
              });
        }
      } else {
        List<CombinedFileRange> combinedFileRanges = getCombinedFileRange(sortedRanges);
        updateRangeSizeCounters(sortedRanges.size(), combinedFileRanges.size());
        // case where ranges can be merged
        for (CombinedFileRange combinedFileRange : combinedFileRanges) {
          CompletableFuture<ByteBuffer> result = new CompletableFuture<>();
          combinedFileRange.setData(result);
          long startTimer = System.currentTimeMillis();
          boundedThreadPool.submit(
              () -> {
                logger.atFiner().log(
                    "Submitting combinedRange %s for execution.", combinedFileRange);

                // Reset thread stats as a new task is being submitted to this thread.
                // all required stats must be collected before task is finished.
                resetThreadLocalStats();
                readCombinedRange(combinedFileRange, allocate, channelProvider);
                long endTimer = System.currentTimeMillis();
                storageStatistics.updateStats(
                    GhfsStatistic.STREAM_READ_VECTORED_READ_RANGE_DURATION,
                    endTimer - startTimer,
                    channelProvider.gcsPath);
              });
        }
      }
    }

    private void updateRangeSizeCounters(int incomingRangeSize, int combinedRangeSize) {
      storageStatistics.incrementCounter(
          GhfsStatistic.STREAM_READ_VECTORED_READ_INCOMING_RANGES, incomingRangeSize);
      storageStatistics.incrementCounter(
          GhfsStatistic.STREAM_READ_VECTORED_READ_COMBINED_RANGES, combinedRangeSize);
    }

    private void updateBytesRead(int readBytes) {
      streamStatistics.bytesRead(readBytes);
      storageStatistics.streamReadBytes(readBytes);
    }

    /**
     * Function to capture the thread local stats of thread processing the range to shared stats
     * map. It also resets them so same stats wouldn't get captured multiple times.
     */
    private void captureAndResetThreadLocalStats() {
      GhfsThreadLocalStatistics tlStats = storageStatistics.getThreadLocalStatistics();
      Iterator<LongStatistic> itr = tlStats.getLongStatistics();
      while (itr.hasNext()) {
        LongStatistic lStats = itr.next();
        String lKey = lStats.getName();
        long lValue = lStats.getValue();
        rangeStatsAccumulator.merge(lKey, lValue, Long::sum);
      }
      // given we have merged the threadLocal stats, resetting it so that it wouldn't captured
      // again.
      resetThreadLocalStats();
    }

    /**
     * Reset the thread local stats of caller thread. It is suppose to be called form the reader
     * thread of range request. If called on main thread all thread local stats will be lost.
     */
    private void resetThreadLocalStats() {
      storageStatistics.getThreadLocalStatistics().reset();
    }

    private List<CombinedFileRange> getCombinedFileRange(List<? extends FileRange> sortedRanges) {
      return mergeSortedRanges(
          sortedRanges,
          1,
          vectoredReadOptions.getMinSeekVectoredReadSize(),
          vectoredReadOptions.getMergeRangeMaxSize());
    }

    /**
     * function for reading combined or merged FileRanges. It reads the range and update the child
     * fileRange's content.
     *
     * @param combinedFileRange merge file range, keeps track of source file ranges which were
     *     merged
     * @param allocate Byte buffer allocator
     */
    private void readCombinedRange(
        CombinedFileRange combinedFileRange,
        IntFunction<ByteBuffer> allocate,
        ReadChannelProvider channelProvider) {
      try (SeekableByteChannel channel = channelProvider.getReadChannel()) {
        channel.position(combinedFileRange.getOffset());
        ByteBuffer readContent = allocate.apply(combinedFileRange.getLength());
        int numRead = channel.read(readContent);

        // making it ready for reading
        readContent.flip();
        logger.atFiner().log(
            "Read combinedFileRange completed from range: %s, path: %s, readBytes: %d",
            combinedFileRange, channelProvider.gcsPath, numRead);
        if (numRead < 0) {
          throw new EOFException(
              String.format(
                  "EOF reached while reading combinedFileRange, range: %s, path: %s, numRead: %d",
                  combinedFileRange, channelProvider.gcsPath, numRead));
        }

        // populate child ranges
        long currentPosition = combinedFileRange.getOffset();
        long totalBytesRead = 0;
        // Note: child ranges will be sorted as well, given Range merge was called on sortedList
        for (FileRange child : combinedFileRange.getUnderlying()) {
          logger.atFiner().log(
              "Populating childRange: %s from combinedRange:%s", child, combinedFileRange);
          int discardedBytes = (int) (child.getOffset() - currentPosition);
          logger.atFiner().log(
              "Discarding %d bytes at offset: %d from read combinedRange %s while updating childRange: %s",
              discardedBytes, currentPosition, combinedFileRange, child);
          totalBytesRead += discardedBytes + child.getLength();
          currentPosition = child.getOffset() + child.getLength();

          storageStatistics.incrementCounter(
              GhfsStatistic.STREAM_READ_VECTORED_EXTRA_READ_BYTES, discardedBytes);

          captureAndResetThreadLocalStats();
          if (numRead >= totalBytesRead) {
            ByteBuffer childBuffer = sliceTo(readContent, combinedFileRange.getOffset(), child);
            child.getData().complete(childBuffer);
            updateBytesRead(child.getLength());
          } else {
            throw new EOFException(
                String.format(
                    "EOF reached before all child ranges can be populated, combinedFileRange: %s, expected length: %s, readBytes: %s, path: %s",
                    combinedFileRange,
                    combinedFileRange.getLength(),
                    numRead,
                    channelProvider.gcsPath));
          }
        }
        combinedFileRange.getData().complete(readContent);
      } catch (Exception e) {
        logger.atWarning().withCause(e).log(
            "Exception while reading combinedFileRange:%s for path: %s",
            combinedFileRange, channelProvider.gcsPath);
        captureAndResetThreadLocalStats();
        combinedFileRange.getData().completeExceptionally(e);
        // complete exception all the underlying ranges which have not already
        // finished.
        completeExceptionally(combinedFileRange, e);
      }
    }

    private void completeExceptionally(CombinedFileRange combinedFileRange, Throwable e) {
      for (FileRange child : combinedFileRange.getUnderlying()) {
        if (!child.getData().isDone()) {
          logger.atFiner().withCause(e).log(
              "Marking child:%s as `completeExceptionally` of combinedRange:%s",
              child, combinedFileRange);
          child
              .getData()
              .completeExceptionally(
                  new IOException(
                      String.format(
                          "Error while populating childRange: %s from combinedRange: %s",
                          child, combinedFileRange),
                      e));
        }
      }
    }

    /**
     * Read data from GCS for this range and populate the buffer.
     *
     * @param range range of data to read.
     * @param allocate lambda function to allocate byteBuffer.
     */
    private void readSingleRange(
        FileRange range, IntFunction<ByteBuffer> allocate, ReadChannelProvider channelProvider) {
      try (SeekableByteChannel channel = channelProvider.getReadChannel()) {
        channel.position(range.getOffset());
        ByteBuffer dst = allocate.apply(range.getLength());
        int numRead = channel.read(dst.duplicate());
        if (numRead < range.getLength()) {
          throw new EOFException(
              String.format(
                  "EOF reached before whole range can be read, range: %s, path: %s",
                  range, channelProvider.gcsPath));
        }
        updateBytesRead(range.getLength());
        captureAndResetThreadLocalStats();
        logger.atFiner().log(
            "Read single range completed from range: %s, path: %s", range, channelProvider.gcsPath);
        range.getData().complete(dst);
      } catch (Exception e) {
        logger.atWarning().withCause(e).log(
            "Exception while reading range:%s for path: %s", range, channelProvider.gcsPath);
        captureAndResetThreadLocalStats();
        range.getData().completeExceptionally(e);
      }
    }

    private boolean shouldMergeRanges(List<? extends FileRange> ranges) {
      return (isOrderedDisjoint(ranges, 1, vectoredReadOptions.getMinSeekVectoredReadSize()));
    }

    private class ReadChannelProvider {
      private final GoogleCloudStorageFileSystem gcsFs;
      private final FileInfo fileInfo;
      private final URI gcsPath;

      public ReadChannelProvider(
          GoogleCloudStorageFileSystem gcsFS, FileInfo fileInfo, URI gcsPath) {
        this.gcsFs = gcsFS;
        this.fileInfo = fileInfo;
        this.gcsPath = gcsPath;
      }

      public SeekableByteChannel getReadChannel() throws IOException {
        GoogleCloudStorageReadOptions options =
            channelReadOptions(gcsFs.getOptions().getCloudStorageOptions().getReadChannelOptions());
        if (fileInfo != null) {
          return gcsFs.open(fileInfo, options);
        }
        return gcsFs.open(gcsPath, options);
      }

      /**
       * Returns Overriden GCS read options. These options will be used while creating channel per
       * FileRange. By default, channel is optimized to perform multiple read request from same
       * channel. Given in readVectored, only one read is performed per channel overriding some
       * configuration to optimize it.
       *
       * @param readOptions original read options extracted from GCSFileSystem
       * @return The modified read options.
       */
      private GoogleCloudStorageReadOptions channelReadOptions(
          GoogleCloudStorageReadOptions readOptions) {
        GoogleCloudStorageReadOptions.Builder builder = readOptions.toBuilder();
        // For single range read we don't want Read channel to adjust around on channel boundaries
        // as
        // channel is used just for one read request.
        builder.setReadExactRequestedBytesEnabled(true);
        return builder.build();
      }
    }
  }

  /**
   * Check if the input ranges are overlapping in nature. We call two ranges to be overlapping when
   * start offset of second is less than the end offset of first. End offset is calculated as start
   * offset + length.
   *
   * @param input list if input ranges.
   * @return true/false based on logic explained above.
   */
  @VisibleForTesting
  public List<? extends FileRange> validateNonOverlappingAndReturnSortedRanges(
      List<? extends FileRange> input) {

    if (input.size() == 1) {
      return input;
    }
    FileRange[] sortedRanges = VectoredReadUtils.sortRanges(input);
    FileRange prev = null;
    for (FileRange current : sortedRanges) {
      if (prev != null) {
        if (current.getOffset() < prev.getOffset() + prev.getLength()) {
          throw new IllegalArgumentException(
              String.format(
                  "Overlapping ranges not supported, overlapping range: %s, %s", prev, current));
        }
      }
      prev = current;
    }
    return Arrays.asList(sortedRanges);
  }

  /** Closes the VectoredIOImpl instance, releasing any allocated resources. */
  @Override
  public void close() {
    try {
      if (boundedThreadPool != null) {
        boundedThreadPool.shutdown();
      }
    } finally {
      boundedThreadPool = null;
    }
  }
}
