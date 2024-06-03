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
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;
import org.apache.hadoop.fs.FileRange;
import org.apache.hadoop.fs.VectoredReadUtils;
import org.apache.hadoop.fs.impl.CombinedFileRange;

@VisibleForTesting
public class VectoredIOImpl implements Closeable {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();
  private final URI gcsPath;
  private final FileInfo fileInfo;
  private final GoogleCloudStorageReadOptions channelReadOptions;
  private final VectoredReadOptions vectoredReadOptions;
  private final GoogleCloudStorageFileSystem gcsFs;
  private final ExecutorService boundedThreadPool;

  public VectoredIOImpl(
      GoogleCloudStorageFileSystem gcsFs,
      URI gcsPath,
      FileInfo fileInfo,
      VectoredReadOptions vectoredReadOptions) {
    this.gcsFs = gcsFs;
    this.gcsPath = gcsPath;
    this.fileInfo = fileInfo;
    this.vectoredReadOptions = vectoredReadOptions;
    this.channelReadOptions =
        channelReadOptions(gcsFs.getOptions().getCloudStorageOptions().getReadChannelOptions());
    // TODO: this needs to be thought about a little bit more, should we have our own executor
    // service, what about waiting tasks and it's queue
    this.boundedThreadPool =
        Executors.newFixedThreadPool(this.vectoredReadOptions.getReadThreads());
  }

  /**
   * Reads data from Google Cloud Storage using vectored I/O operations.
   *
   * @param ranges List of file ranges to read.
   * @param allocate Function to allocate ByteBuffer for reading.
   * @throws IOException if an I/O error occurs.
   */
  public void readVectored(List<? extends FileRange> ranges, IntFunction<ByteBuffer> allocate)
      throws IOException {
    List<? extends FileRange> sortedRanges = validateNonOverlappingAndReturnSortedRanges(ranges);
    for (FileRange range : ranges) {
      validateRangeRequest(range);
      CompletableFuture<ByteBuffer> result = new CompletableFuture<>();
      range.setData(result);
    }

    if (shouldMergeRanges(ranges)) {
      // case when ranges are not merged
      for (FileRange range : sortedRanges) {
        boundedThreadPool.submit(() -> readSingleRange(range, allocate));
      }
    } else {
      // case where ranges can be merged
      List<CombinedFileRange> combinedFileRanges =
          mergeSortedRanges(
              sortedRanges,
              1,
              vectoredReadOptions.getMinSeekVectoredReadSize(),
              vectoredReadOptions.getMergeRangeMaxSize());
      for (CombinedFileRange combinedFileRange : combinedFileRanges) {
        CompletableFuture<ByteBuffer> result = new CompletableFuture<>();
        combinedFileRange.setData(result);
        // how do we make sure whatever was submitted first is the one getting processed first?
        boundedThreadPool.submit(() -> readCombinedRange(combinedFileRange, allocate));
      }
    }
  }

  /**
   * function for reading combined or merged FileRanges. It reads the range and update the child
   * fileRange's content.
   *
   * @param combinedFileRange merge file range, keeps track of source file ranges which were merged
   * @param allocate Byte buffer allocator
   */
  private void readCombinedRange(
      CombinedFileRange combinedFileRange, IntFunction<ByteBuffer> allocate) {
    try (SeekableByteChannel channel = getReadChannel()) {
      channel.position(combinedFileRange.getOffset());
      ByteBuffer dst = allocate.apply(combinedFileRange.getLength());
      int numRead = channel.read(dst.duplicate());
      if (numRead < 0) {
        throw new EOFException(
            String.format(
                "EOF reached before whole combinedFileRange can be read, range: %s, path: %s",
                combinedFileRange, gcsPath));
      }
      combinedFileRange.getData().complete(dst);
      logger.atFiner().log(
          "Read combinedFileRange completed from range: %s, path: %s", combinedFileRange, gcsPath);
      for (FileRange child : combinedFileRange.getUnderlying()) {
        ByteBuffer childBuffer = sliceTo(dst, combinedFileRange.getOffset(), child);
        child.getData().complete(childBuffer);
      }
    } catch (Exception e) {
      logger.atWarning().withCause(e).log(
          "Exception while reading combinedFileRange:%s for path: %s", combinedFileRange, gcsPath);
      combinedFileRange.getData().completeExceptionally(e);
      // complete exception all the underlying ranges which have not already
      // finished.
      for (FileRange child : combinedFileRange.getUnderlying()) {
        if (!child.getData().isDone()) {
          child
              .getData()
              .completeExceptionally(
                  new IOException(
                      String.format(
                          "Error while populating childRange: %s from combinedRange: %s for path: %s",
                          child, combinedFileRange, gcsPath),
                      e));
        }
      }
    }
  }

  /**
   * Returns Overriden GCS read options. These options will be used while creating channel per
   * FileRange. By default, channel is optimized to perform multiple read request from same channel.
   * Given in readVectored, only one read is performed per channel overriding some configuration to
   * optimize it.
   *
   * @param readOptions original read options extracted from GCSFileSystem
   * @return The modified read options.
   */
  private GoogleCloudStorageReadOptions channelReadOptions(
      GoogleCloudStorageReadOptions readOptions) {
    GoogleCloudStorageReadOptions.Builder builder = readOptions.toBuilder();
    // For single range read we don't want Read channel to adjust around on channel boundaries as
    // channel is used just for one read request.
    builder.setFadvise(GoogleCloudStorageReadOptions.Fadvise.SEQUENTIAL);
    return builder.build();
  }

  /**
   * Read data from GCS for this range and populate the buffer.
   *
   * @param range range of data to read.
   * @param allocate lambda function to allocate byteBuffer.
   */
  private void readSingleRange(FileRange range, IntFunction<ByteBuffer> allocate) {
    try (SeekableByteChannel channel = getReadChannel()) {
      channel.position(range.getOffset());
      ByteBuffer dst = allocate.apply(range.getLength());
      // TODO: will duplicate work for direct buffers
      int numRead = channel.read(dst.duplicate());
      if (numRead < 0) {
        throw new EOFException(
            String.format(
                "EOF reached before whole range can be read, range: %s, path: %s", range, gcsPath));
      }
      range.getData().complete(dst);
      logger.atFiner().log("Read single range completed from range: %s, path: %s", range, gcsPath);
    } catch (Exception e) {
      logger.atWarning().withCause(e).log(
          "Exception while reading range:%s for path: %s", range, gcsPath);
      range.getData().completeExceptionally(e);
    }
  }

  private boolean shouldMergeRanges(List<? extends FileRange> ranges) {
    if (isOrderedDisjoint(ranges, 1, vectoredReadOptions.getMinSeekVectoredReadSize())) {
      return true;
    }
    return false;
  }

  private SeekableByteChannel getReadChannel() throws IOException {
    if (fileInfo != null) {
      return gcsFs.open(fileInfo, channelReadOptions);
    }
    return gcsFs.open(gcsPath, channelReadOptions);
  }

  /**
   * Check if the input ranges are overlapping in nature. We call two ranges to be overlapping when
   * start offset of second is less than the end offset of first. End offset is calculated as start
   * offset + length.
   *
   * @param input list if input ranges.
   * @return true/false based on logic explained above.
   */
  private List<? extends FileRange> validateNonOverlappingAndReturnSortedRanges(
      List<? extends FileRange> input) {

    if (input.size() <= 1) {
      return input;
    }
    FileRange[] sortedRanges = VectoredReadUtils.sortRanges(input);
    FileRange prev = sortedRanges[0];
    for (int i = 1; i < sortedRanges.length; i++) {
      if (sortedRanges[i].getOffset() < prev.getOffset() + prev.getLength()) {
        throw new UnsupportedOperationException("Overlapping ranges are not supported");
      }
      prev = sortedRanges[i];
    }
    return Arrays.asList(sortedRanges);
  }

  /** Closes the VectoredIOImpl instance, releasing any allocated resources. */
  @Override
  public void close() {
    // TODO: this needs to have better exception handling.
    boundedThreadPool.shutdown();
    try {
      if (!boundedThreadPool.awaitTermination(10, TimeUnit.SECONDS)) {
        logger.atWarning().log(
            "Executor did not terminate within timeout. Forcibly shutting down.");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } finally {
      boundedThreadPool.shutdownNow();
    }
  }
}
