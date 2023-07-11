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
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.Fadvise;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.flogger.GoogleLogger;
import com.google.common.flogger.LazyArgs;
import com.google.gson.Gson;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileRange;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.VectoredReadUtils;
import org.apache.hadoop.fs.impl.CombinedFileRange;

/** A seekable and positionable FSInputStream that provides read access to a file. */
class GoogleHadoopFSInputStream extends FSInputStream {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();
  @VisibleForTesting static final String READ_METHOD = "gcsFSRead";
  @VisibleForTesting static final String READ_VECTORED_METHOD = "readVectored";
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
  private final GoogleCloudStorageReadOptions readVectoredOptions;
  private final GoogleHadoopFileSystemBase ghfs;
  private final int maxReadVectoredParallelism = 8;
  private final int minimumRangeSize = 2 * 1024 * 1024;
  private final int maximumRangeSize = 128 * 1024 * 1024;
  private final ThreadPoolExecutor unboundedThreadPool;

  // Path of the file to read.
  private URI gcsPath;

  // Number of bytes read through this channel.
  private long totalBytesRead;
  private String uuid;

  // Statistics tracker provided by the parent GoogleHadoopFileSystemBase for recording
  // numbers of bytes read.
  private final FileSystem.Statistics statistics;

  // Used for single-byte reads.
  private final byte[] singleReadBuf = new byte[1];

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
      FileSystem.Statistics statistics)
      throws IOException {
    logger.atFiner().log(
        "GoogleHadoopFSInputStream(gcsPath: %s, readOptions: %s)", gcsPath, readOptions);
    this.gcsPath = gcsPath;
    this.statistics = statistics;
    this.totalBytesRead = 0;
    this.isTraceLoggingEnabled = readOptions.isTraceLogEnabled();
    this.channel = ghfs.getGcsFs().open(gcsPath, readOptions);
    this.readVectoredOptions = getReadVectoredOptions(readOptions);
    this.ghfs = ghfs;
    this.unboundedThreadPool =
        new ThreadPoolExecutor(
            maxReadVectoredParallelism, 8, 3L, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
    this.uuid = UUID.randomUUID().toString();
  }

  /**
   * Returns modified GoogleCloudStorageReadOptions with vectored read options set.
   *
   * @param readOptions The original read options.
   * @return The modified read options with vectored read options set.
   */
  private GoogleCloudStorageReadOptions getReadVectoredOptions(
      GoogleCloudStorageReadOptions readOptions) {
    GoogleCloudStorageReadOptions.Builder builder = readOptions.toBuilder();
    builder.setFastFailOnNotFound(false);
    builder.setFadvise(Fadvise.SEQUENTIAL);
    return builder.build();
  }

  /**
   * Read data from GCS for this range and populate the buffer.
   *
   * @param range range of data to read.
   * @param buffer buffer to fill.
   */
  private void readSingleRange(FileRange range, ByteBuffer buffer) {
    try {
      Stopwatch stopwatch = Stopwatch.createStarted();
      VectoredReadUtils.validateRangeRequest(range);
      try (SeekableByteChannel channel = ghfs.getGcsFs().open(gcsPath, readVectoredOptions)) {
        channel.position(range.getOffset());
        int numRead = channel.read(ByteBuffer.wrap(buffer.array(), 0, range.getLength()));
        range.getData().complete(buffer);
        stopwatch.stop();

        readSingleRangeTrace("readSingleRange", stopwatch, range.getOffset(), numRead);
      }
    } catch (Exception ex) {
      logger.atInfo().withCause(ex).log("Exception while reading a range %s", range.toString());
      range.getData().completeExceptionally(ex);
    }
  }

  /**
   * Calculates the optimal range size based on the given list of file ranges and the maximum
   * parallelism.
   *
   * @param ranges The list of file ranges to determine the optimal range size for.
   * @return The optimal range size.
   */
  private int getOptimalRangeSize(List<? extends FileRange> ranges) {
    int l = minimumRangeSize;
    int r = maximumRangeSize + 1;

    while (l < r) {
      int mid = (l + r) / 2;
      int count = 0;
      for (FileRange range : ranges) {
        count += (range.getLength() + mid - 1) / mid;
      }

      if (count > maxReadVectoredParallelism) {
        l = mid + 1;
      } else {
        r = mid;
      }
    }
    return r;
  }
  /**
   * Splits and reads the ranges in parallel, combining the split ranges into the original ranges.
   *
   * @param ranges the byte ranges to read.
   * @param allocate the function to allocate ByteBuffer.
   * @throws IOException if an I/O error occurs during reading.
   */
  public void splitAndReadRanges(List<? extends FileRange> ranges, IntFunction<ByteBuffer> allocate)
      throws IOException {
    int optimalRangeSize = getOptimalRangeSize(ranges);

    List<FileRange> list = new ArrayList<>();
    for (FileRange range : ranges) {
      long offset = range.getOffset();
      int length = range.getLength();
      while (length > 0) {
        int curLength = Math.min(length, optimalRangeSize);
        CompletableFuture<ByteBuffer> result = new CompletableFuture<>();
        FileRange fileRange = FileRange.createFileRange(offset, curLength);
        fileRange.setData(result);
        list.add(fileRange);
        offset += curLength;
        length -= curLength;
      }
    }

    for (FileRange range : list) {
      ByteBuffer buffer = allocate.apply(range.getLength());
      unboundedThreadPool.submit(() -> readSingleRange(range, buffer));
    }

    int k = 0;
    for (FileRange range : ranges) {
      int length = range.getLength();
      ByteBuffer buffer = allocate.apply(range.getLength());
      while (length > 0) {
        int curLength = Math.min(length, optimalRangeSize);
        ByteBuffer buff = awaitFuture(list.get(k++).getData());
        buffer.put(buff);
        length -= curLength;
        statistics.incrementBytesRead(curLength);
        statistics.incrementReadOps(1);
        totalBytesRead += curLength;
      }
      buffer.flip();
      range.getData().complete(buffer);
    }
  }

  /**
   * {@inheritDoc} Vectored read implementation for S3AInputStream.
   *
   * @param ranges the byte ranges to read.
   * @param allocate the function to allocate ByteBuffer.
   * @throws IOException IOE if any.
   */
  @Override
  public void readVectored(List<? extends FileRange> ranges, IntFunction<ByteBuffer> allocate)
      throws IOException {

    Stopwatch stopwatch = Stopwatch.createStarted();
    long bytesRead = 0;

    for (FileRange range : ranges) {
      VectoredReadUtils.validateRangeRequest(range);
      CompletableFuture<ByteBuffer> result = new CompletableFuture<>();
      range.setData(result);
      bytesRead += range.getLength();
    }

    if (VectoredReadUtils.isOrderedDisjoint(ranges, 1, minSeekForVectorReads())) {
      splitAndReadRanges(ranges, allocate);
    } else {
      List<CombinedFileRange> combinedFileRanges =
          sortAndMergeRanges(ranges, 1, minSeekForVectorReads(), maxReadSizeForVectorReads());
      for (CombinedFileRange combinedFileRange : combinedFileRanges) {
        CompletableFuture<ByteBuffer> result = new CompletableFuture<>();
        combinedFileRange.setData(result);
      }

      splitAndReadRanges(combinedFileRanges, allocate);
      for (CombinedFileRange combinedFileRange : combinedFileRanges) {
        try {
          ByteBuffer combinedBuffer = awaitFuture(combinedFileRange.getData());
          for (FileRange child : combinedFileRange.getUnderlying()) {
            updateOriginalRange(child, combinedBuffer, combinedFileRange);
          }
        } catch (Exception ex) {
          logger.atSevere().withCause(ex).log(
              "Exception occurred while reading combined range from file");
          for (FileRange child : combinedFileRange.getUnderlying()) {
            child.getData().completeExceptionally(ex);
          }
        }
      }
    }
    stopwatch.stop();
    readVectoredTrace(READ_VECTORED_METHOD, stopwatch, bytesRead);
  }

  /**
   * Update data in child range from combined range.
   *
   * @param child child range.
   * @param combinedBuffer combined buffer.
   * @param combinedFileRange combined range.
   */
  private void updateOriginalRange(
      FileRange child, ByteBuffer combinedBuffer, CombinedFileRange combinedFileRange) {
    ByteBuffer childBuffer =
        VectoredReadUtils.sliceTo(combinedBuffer, combinedFileRange.getOffset(), child);
    child.getData().complete(childBuffer);
  }
  /**
   * Sort and merge ranges to optimize the access from the underlying file system.
   *
   * @param input list of ranges based on offset.
   * @param chunkSize round the start and end points to multiples of chunkSize
   * @param minimumSeek the smallest gap that we should seek over in bytes
   * @param maxSize the largest org.apache.hadoop.shaded.com.ined file range in bytes
   * @return the list of sorted CombinedFileRanges that cover the input
   */
  private List<CombinedFileRange> sortAndMergeRanges(
      List<? extends FileRange> input, int chunkSize, int minimumSeek, int maxSize) {
    // sort the ranges by offset
    FileRange[] ranges = input.toArray(new FileRange[0]);
    Arrays.sort(ranges, Comparator.comparingLong(FileRange::getOffset));
    CombinedFileRange current = null;
    List<CombinedFileRange> result = new ArrayList<>(ranges.length);

    // now merge together the ones that merge
    for (FileRange range : ranges) {
      long start = range.getOffset();
      long end = start + range.getLength();
      if (current == null || !current.merge(start, end, range, minimumSeek, maxSize)) {
        current = new CombinedFileRange(start, end, range);
        result.add(current);
      }
    }
    return result;
  }

  /**
   * Waits for the completion of a Future and returns its result.
   *
   * @param future The Future to await completion for.
   * @param <T> The type of the Future's result.
   * @return The result of the completed Future.
   * @throws IOException if an I/O error occurs while waiting for the Future's completion.
   * @throws InterruptedIOException if the waiting thread is interrupted.
   */
  public static <T> T awaitFuture(final Future<T> future)
      throws IOException, InterruptedIOException {
    try {
      return future.get();
    } catch (InterruptedException e) {
      throw (InterruptedIOException) new InterruptedIOException(e.toString()).initCause(e);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Reads a single byte from the underlying store.
   *
   * @return A single byte from the underlying store or -1 on EOF.
   * @throws IOException if an IO error occurs.
   */
  @Override
  public synchronized int read() throws IOException {
    // TODO(user): Wrap this in a while-loop if we ever introduce a non-blocking mode for the
    // underlying channel.
    int numRead = channel.read(ByteBuffer.wrap(singleReadBuf));
    if (numRead == -1) {
      return -1;
    }
    if (numRead != 1) {
      throw new IOException(
          String.format(
              "Somehow read %d bytes using single-byte buffer for path %s ending in position %d!",
              numRead, gcsPath, channel.position()));
    }
    byte b = singleReadBuf[0];

    totalBytesRead++;
    statistics.incrementBytesRead(1);
    statistics.incrementReadOps(1);
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
    Stopwatch stopwatch = Stopwatch.createStarted();

    Preconditions.checkNotNull(buf, "buf must not be null");
    if (offset < 0 || length < 0 || length > buf.length - offset) {
      throw new IndexOutOfBoundsException();
    }
    int numRead = channel.read(ByteBuffer.wrap(buf, offset, length));

    readAPITrace(READ_METHOD, stopwatch, 0, offset, length, numRead);

    if (numRead > 0) {
      // -1 means we actually read 0 bytes, but requested at least one byte.
      statistics.incrementBytesRead(numRead);
      statistics.incrementReadOps(1);
      totalBytesRead += numRead;
    }

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
    Stopwatch stopwatch = Stopwatch.createStarted();

    int result = super.read(position, buf, offset, length);
    readAPITrace(POSITIONAL_READ_METHOD, stopwatch, position, offset, length, result);
    if (result > 0) {
      // -1 means we actually read 0 bytes, but requested at least one byte.
      statistics.incrementBytesRead(result);
      totalBytesRead += result;
    }
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
    Stopwatch stopwatch = Stopwatch.createStarted();

    try {
      channel.position(pos);
      seekAPITrace(SEEK_METHOD, stopwatch, pos);
    } catch (IllegalArgumentException e) {
      throw new IOException(e);
    }
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
    logger.atFiner().log("close(): %s", gcsPath);
    Stopwatch stopwatch = Stopwatch.createStarted();
    unboundedThreadPool.shutdown();
    try {
      if (!unboundedThreadPool.awaitTermination(10, TimeUnit.SECONDS)) {
        logger.atWarning().log(
            "Executor did not terminate within timeout. Forcibly shutting down.");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } finally {
      unboundedThreadPool.shutdownNow();
    }
    if (channel != null) {
      logger.atFiner().log("Closing '%s' file with %d total bytes read", gcsPath, totalBytesRead);
      channel.close();
      closeAPITrace(CLOSE_METHOD, stopwatch);
    }
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
      throw new ClosedChannelException();
    }
    return super.available();
  }

  private void readAPITrace(
      String method, Stopwatch stopwatch, long position, int offset, int length, int bytesRead) {
    if (isTraceLoggingEnabled) {
      Map<String, Object> jsonMap = new HashMap<>();
      jsonMap.put(METHOD, method);
      jsonMap.put(GCS_PATH, gcsPath);
      jsonMap.put(DURATION_NS, stopwatch.elapsed(TimeUnit.NANOSECONDS));
      jsonMap.put(POSITION, position);
      jsonMap.put(OFFSET, offset);
      jsonMap.put(LENGTH, length);
      jsonMap.put(BYTES_READ, bytesRead);
      jsonMap.put("UUID", uuid);
      captureAPITraces(jsonMap);
    }
  }

  private void readSingleRangeTrace(
      String method, Stopwatch stopwatch, long offset, int bytesRead) {
    if (isTraceLoggingEnabled) {
      Map<String, Object> jsonMap = new HashMap<>();
      jsonMap.put(METHOD, method);
      jsonMap.put(GCS_PATH, gcsPath);
      jsonMap.put(DURATION_NS, stopwatch.elapsed(TimeUnit.NANOSECONDS));
      jsonMap.put(OFFSET, offset);
      jsonMap.put("UUID", uuid);
      jsonMap.put(BYTES_READ, bytesRead);
      captureAPITraces(jsonMap);
    }
  }

  private void seekAPITrace(String method, Stopwatch stopwatch, long pos) {
    if (isTraceLoggingEnabled) {
      Map<String, Object> jsonMap = new HashMap<>();
      jsonMap.put(METHOD, method);
      jsonMap.put(GCS_PATH, gcsPath);
      jsonMap.put(DURATION_NS, stopwatch.elapsed(TimeUnit.NANOSECONDS));
      jsonMap.put(POSITION, pos);
      jsonMap.put("UUID", uuid);
      captureAPITraces(jsonMap);
    }
  }

  private void readVectoredTrace(String method, Stopwatch stopwatch, long bytesRead) {
    if (isTraceLoggingEnabled) {
      Map<String, Object> jsonMap = new HashMap<>();
      jsonMap.put(METHOD, method);
      jsonMap.put(GCS_PATH, gcsPath);
      jsonMap.put(DURATION_NS, stopwatch.elapsed(TimeUnit.NANOSECONDS));
      jsonMap.put(BYTES_READ, bytesRead);
      jsonMap.put("UUID", uuid);
      captureAPITraces(jsonMap);
    }
  }

  private void closeAPITrace(String method, Stopwatch stopwatch) {
    if (isTraceLoggingEnabled) {
      Map<String, Object> jsonMap = new HashMap<>();
      jsonMap.put(METHOD, method);
      jsonMap.put(GCS_PATH, gcsPath);
      jsonMap.put(DURATION_NS, stopwatch.elapsed(TimeUnit.NANOSECONDS));
      jsonMap.put("UUID", uuid);
      captureAPITraces(jsonMap);
    }
  }

  private void captureAPITraces(Map<String, Object> apiTraces) {
    if (isTraceLoggingEnabled) {
      logger.atInfo().log("%s", LazyArgs.lazy(() -> gson.toJson(apiTraces)));
    }
  }
}
