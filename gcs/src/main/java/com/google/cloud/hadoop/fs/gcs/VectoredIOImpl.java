package com.google.cloud.hadoop.fs.gcs;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions;
import com.google.common.flogger.GoogleLogger;
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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.VectoredReadUtils;
import org.apache.hadoop.fs.impl.CombinedFileRange;
import org.apache.hadoop.util.functional.FutureIO;

public class VectoredIOImpl {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();
  private final FileSystem.Statistics statistics;
  private final URI gcsPath;
  private final GoogleCloudStorageReadOptions readVectoredOptions;
  private final GoogleHadoopFileSystem ghfs;
  private final int maxReadVectoredParallelism = 8;
  private final int maxReadSizeForVectorReads = 128 * 1024 * 1024;
  private final int minSeekForVectorReads = 4 * 1024;
  private final ExecutorService boundedThreadPool;

  static VectoredIOImpl create(
      GoogleHadoopFileSystem ghfs, URI gcsPath, FileSystem.Statistics statistics) {
    return new VectoredIOImpl(ghfs, gcsPath, statistics);
  }

  private VectoredIOImpl(
      GoogleHadoopFileSystem ghfs, URI gcsPath, FileSystem.Statistics statistics) {
    this.ghfs = ghfs;
    this.gcsPath = gcsPath;
    this.statistics = statistics;
    this.readVectoredOptions =
        getReadVectoredOptions(
            ghfs.getGcsFs().getOptions().getCloudStorageOptions().getReadChannelOptions());
    this.boundedThreadPool = Executors.newFixedThreadPool(maxReadVectoredParallelism);
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
      VectoredReadUtils.validateRangeRequest(range);
      CompletableFuture<ByteBuffer> result = new CompletableFuture<>();
      range.setData(result);
    }

    if (VectoredReadUtils.isOrderedDisjoint(ranges, 1, minSeekForVectorReads)) {
      for (FileRange range : sortedRanges) {
        ByteBuffer buffer = allocate.apply(range.getLength());
        boundedThreadPool.submit(() -> readSingleRange(range, buffer));
        statistics.incrementBytesRead(range.getLength());
        statistics.incrementReadOps(1);
      }
    } else {
      List<CombinedFileRange> combinedFileRanges =
          VectoredReadUtils.mergeSortedRanges(
              sortedRanges, 1, minSeekForVectorReads, maxReadSizeForVectorReads);
      for (CombinedFileRange combinedFileRange : combinedFileRanges) {
        CompletableFuture<ByteBuffer> result = new CompletableFuture<>();
        combinedFileRange.setData(result);

        ByteBuffer buffer = allocate.apply(combinedFileRange.getLength());
        boundedThreadPool.submit(() -> readSingleRange(combinedFileRange, buffer));
      }

      for (CombinedFileRange combinedFileRange : combinedFileRanges) {
        try {
          ByteBuffer combinedBuffer = FutureIO.awaitFuture(combinedFileRange.getData());
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
        statistics.incrementBytesRead(combinedFileRange.getLength());
        statistics.incrementReadOps(1);
      }
    }
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
    builder.setFastFailOnNotFoundEnabled(false);
    builder.setFadvise(GoogleCloudStorageReadOptions.Fadvise.SEQUENTIAL);
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
      VectoredReadUtils.validateRangeRequest(range);
      try (SeekableByteChannel channel = ghfs.getGcsFs().open(gcsPath, readVectoredOptions)) {
        channel.position(range.getOffset());
        int numRead = channel.read(ByteBuffer.wrap(buffer.array(), 0, range.getLength()));
        range.getData().complete(buffer);
        logger.atInfo().log(
            "Read single range completed from offset: %d with a length of %d",
            range.getOffset(), numRead);
      }
    } catch (Exception ex) {
      logger.atInfo().withCause(ex).log("Exception while reading a range %s", range.toString());
      range.getData().completeExceptionally(ex);
    }
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
  public void close() {
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
