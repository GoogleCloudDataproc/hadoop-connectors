/*
 * Copyright 2023 Google LLC
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

package com.google.cloud.hadoop.gcsio;

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageExceptions.createFileNotFoundException;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;

import com.google.cloud.ReadChannel;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.Fadvise;
import com.google.cloud.hadoop.util.ErrorTypeExtractor;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobSourceOption;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.flogger.GoogleLogger;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Provides seekable read access to GCS via java-storage library. */
@VisibleForTesting
class GoogleCloudStorageClientReadChannel implements SeekableByteChannel {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private final StorageResourceId resourceId;
  private final GoogleCloudStorageReadOptions readOptions;
  private final GoogleCloudStorageOptions storageOptions;
  private final Storage storage;
  // The size of this object generation, in bytes.
  private final long objectSize;
  private final ErrorTypeExtractor errorExtractor;
  private ContentReadChannel contentReadChannel;

  private boolean open = true;

  // Current position in this channel, it could be different from contentChannelCurrentPosition if
  // position(long) method calls were made without calls to read(ByteBuffer) method.
  private long currentPosition = 0;

  public GoogleCloudStorageClientReadChannel(
      Storage storage,
      GoogleCloudStorageItemInfo itemInfo,
      GoogleCloudStorageReadOptions readOptions,
      ErrorTypeExtractor errorExtractor,
      GoogleCloudStorageOptions storageOptions)
      throws IOException {
    validate(itemInfo);
    this.storage = storage;
    this.errorExtractor = errorExtractor;
    this.resourceId =
        new StorageResourceId(
            itemInfo.getBucketName(), itemInfo.getObjectName(), itemInfo.getContentGeneration());
    this.readOptions = readOptions;
    this.storageOptions = storageOptions;
    this.objectSize = itemInfo.getSize();
    this.contentReadChannel = new ContentReadChannel(readOptions, resourceId);
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    throwIfNotOpen();

    // Don't try to read if the buffer has no space.
    if (dst.remaining() == 0) {
      return 0;
    }
    logger.atFiner().log(
        "Reading %d bytes at %d position from '%s'", dst.remaining(), currentPosition, resourceId);
    if (currentPosition == objectSize) {
      return -1;
    }
    return contentReadChannel.readContent(dst);
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    throw new UnsupportedOperationException("Cannot mutate read-only channel");
  }

  @Override
  public long position() throws IOException {
    return currentPosition;
  }

  /**
   * Sets this channel's position.
   *
   * <p>This method will throw an exception if {@code newPosition} is greater than object size,
   * which contradicts {@link SeekableByteChannel#position(long) SeekableByteChannel} contract.
   * TODO(user): decide if this needs to be fixed.
   *
   * @param newPosition the new position, counting the number of bytes from the beginning.
   * @return this channel instance
   * @throws FileNotFoundException if the underlying object does not exist.
   * @throws IOException on IO error
   */
  @Override
  public SeekableByteChannel position(long newPosition) throws IOException {
    throwIfNotOpen();

    if (newPosition == currentPosition) {
      return this;
    }

    validatePosition(newPosition);
    logger.atFiner().log(
        "Seek from %s to %s position for '%s'", currentPosition, newPosition, resourceId);
    currentPosition = newPosition;
    return this;
  }

  @Override
  public long size() throws IOException {
    return objectSize;
  }

  @Override
  public SeekableByteChannel truncate(long size) throws IOException {
    throw new UnsupportedOperationException("Cannot mutate read-only channel");
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public void close() throws IOException {
    if (open) {
      try {
        logger.atFiner().log("Closing channel for '%s'", resourceId);
        contentReadChannel.closeContentChannel();
      } catch (Exception e) {
        throw new IOException(
            String.format("Exception occurred while closing channel '%s'", resourceId), e);
      } finally {
        contentReadChannel = null;
        open = false;
      }
    }
  }

  /**
   * This class own the responsibility of opening up contentChannel. It also implements the Fadvise,
   * which helps in deciding the boundaries of content channel being opened and also caching the
   * footer of an object.
   */
  private class ContentReadChannel {

    // Size of buffer to allocate for skipping bytes in-place when performing in-place seeks.
    private static final int SKIP_BUFFER_SIZE = 8192;
    private final BlobId blobId;

    // This is the actual current position in `contentChannel` from where read can happen.
    // This remains unchanged of position(long) method call.
    private long contentChannelCurrentPosition = -1;
    private long contentChannelEnd = -1;
    // Prefetched footer content.
    private byte[] footerContent;
    // Used as scratch space when reading bytes just to discard them when trying to perform small
    // in-place seeks.
    private byte[] skipBuffer = null;
    private ReadableByteChannel byteChannel = null;
    private boolean randomAccess;

    public ContentReadChannel(
        GoogleCloudStorageReadOptions readOptions, StorageResourceId resourceId) {
      this.blobId =
          BlobId.of(
              resourceId.getBucketName(), resourceId.getObjectName(), resourceId.getGenerationId());
      this.randomAccess = readOptions.getFadvise() == Fadvise.RANDOM;
    }

    public int readContent(ByteBuffer dst) throws IOException {

      performPendingSeeks();

      checkState(
          contentChannelCurrentPosition == currentPosition || byteChannel == null,
          "contentChannelCurrentPosition (%s) should be equal to currentPosition (%s) after lazy seek, if channel is open",
          contentChannelCurrentPosition,
          currentPosition);

      int totalBytesRead = 0;
      // We read from a streaming source. We may not get all the bytes we asked for
      // in the first read. Therefore, loop till we either read the required number of
      // bytes or we reach end-of-stream.
      while (dst.hasRemaining()) {
        int remainingBeforeRead = dst.remaining();
        try {
          if (byteChannel == null) {
            byteChannel = openByteChannel(dst.remaining());
          }
          int bytesRead = readFromByteChannel(dst);
          if (bytesRead == 0) {
            throw new IOException(
                String.format("Read 0 bytes without blocking from object: '%s'", resourceId));
          }

          if (bytesRead < 0) {
            if (currentPosition != contentChannelEnd && currentPosition != objectSize) {
              throw new IOException(
                  String.format(
                      "Received end of stream result before all requestedBytes were received;"
                          + "EndOf stream signal received at offset: %d where as stream was suppose to end at: %d for resource: %s of size: %d",
                      currentPosition, contentChannelEnd, resourceId, objectSize));
            }
            // If we have reached an end of a contentChannel but not an end of an object.
            // then close contentChannel and continue reading an object if necessary.
            if (contentChannelEnd != objectSize && currentPosition == contentChannelEnd) {
              closeContentChannel();
              continue;
            } else {
              break;
            }
          }
          totalBytesRead += bytesRead;
          currentPosition += bytesRead;
          contentChannelCurrentPosition += bytesRead;
          checkState(
              contentChannelCurrentPosition == currentPosition,
              "contentChannelPosition (%s) should be equal to currentPosition (%s)"
                  + " after successful read",
              contentChannelCurrentPosition,
              currentPosition);
        } catch (Exception e) {
          int partialBytes = partiallyReadBytes(remainingBeforeRead, dst);
          totalBytesRead += partialBytes;
          currentPosition += partialBytes;
          logger.atFine().log(
              "Closing contentChannel after %s exception for '%s'.", e.getMessage(), resourceId);
          closeContentChannel();
          throw convertError(e);
        }
      }
      return totalBytesRead;
    }

    private int readFromByteChannel(ByteBuffer buffer) throws IOException {
      Stopwatch stopwatch = Stopwatch.createStarted();
      int numBytesRead = byteChannel.read(buffer);
      readAPITrace(stopwatch, numBytesRead);
      return numBytesRead;
    }

    private void readAPITrace(Stopwatch stopwatch, int bytesRead) {
      long elapsedTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);
      if (storageOptions.isTraceLogEnabled()) {
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("method", "java-storage-read");
        jsonMap.put("bytesRead", bytesRead);
        jsonMap.put("gcs-path", resourceId);
        jsonMap.put("durationMs", elapsedTime);
        captureTraces(jsonMap);
      }
    }

    private void captureTraces(Map<String, Object> apiTraces) {
      if (storageOptions.isTraceLogEnabled()) {
        EventLoggingHttpRequestInitializer.logDetails(apiTraces);
      }
    }

    private int partiallyReadBytes(int remainingBeforeRead, ByteBuffer dst) {
      int partialReadBytes = 0;
      if (remainingBeforeRead != dst.remaining()) {
        partialReadBytes = remainingBeforeRead - dst.remaining();
      }
      return partialReadBytes;
    }

    private boolean shouldDetectRandomAccess() {
      return !randomAccess && readOptions.getFadvise() == Fadvise.AUTO;
    }

    private void setRandomAccess() {
      randomAccess = true;
    }

    private ReadableByteChannel openByteChannel(long bytesToRead) throws IOException {
      checkArgument(
          bytesToRead > 0, "bytesToRead should be greater than 0, but was %s", bytesToRead);
      checkState(
          byteChannel == null && contentChannelEnd < 0,
          "contentChannel and contentChannelEnd should be not initialized yet for '%s'",
          resourceId);

      if (footerContent != null && currentPosition >= objectSize - footerContent.length) {
        return serveFooterContent();
      }

      contentChannelCurrentPosition = getRangeRequestStart();
      contentChannelEnd = getRangeRequestEnd(contentChannelCurrentPosition, bytesToRead);
      checkState(
          contentChannelEnd >= contentChannelCurrentPosition,
          String.format(
              "Start position should be <= endPosition startPosition:%d, endPosition: %d",
              contentChannelCurrentPosition, contentChannelEnd));

      ReadableByteChannel readableByteChannel =
          getStorageReadChannel(contentChannelCurrentPosition, contentChannelEnd);

      if (contentChannelEnd == objectSize
          && (contentChannelEnd - contentChannelCurrentPosition)
              <= readOptions.getMinRangeRequestSize()) {

        if (footerContent == null) {
          cacheFooter(readableByteChannel);
        }
        return serveFooterContent();
      }
      checkState(
          contentChannelCurrentPosition == currentPosition,
          "position of read offset isn't in alignment with channel's read offset");
      return readableByteChannel;
    }

    private void cacheFooter(ReadableByteChannel readableByteChannel) throws IOException {
      int footerSize = toIntExact(objectSize - contentChannelCurrentPosition);
      footerContent = new byte[footerSize];
      try (InputStream footerStream = Channels.newInputStream(readableByteChannel)) {
        int totalBytesRead = 0;
        int bytesRead;
        do {
          Stopwatch stopwatch = Stopwatch.createStarted();
          bytesRead = footerStream.read(footerContent, totalBytesRead, footerSize - totalBytesRead);
          if (bytesRead >= 0) {
            totalBytesRead += bytesRead;
          }
        } while (bytesRead >= 0 && totalBytesRead < footerSize);
        checkState(
            bytesRead >= 0,
            "footerStream shouldn't be empty before reading the footer of size %s, totalBytesRead %s, read via last call %s, for '%s'",
            footerSize,
            totalBytesRead,
            bytesRead,
            resourceId);
        checkState(
            totalBytesRead == footerSize,
            "totalBytesRead (%s) should equal footerSize (%s) for '%s'",
            totalBytesRead,
            footerSize,
            resourceId);
      } catch (Exception e) {
        footerContent = null;
        throw e;
      }
      logger.atFiner().log("Prefetched %s bytes footer for '%s'", footerContent.length, resourceId);
    }

    private ReadableByteChannel serveFooterContent() {
      contentChannelCurrentPosition = currentPosition;
      int offset = toIntExact(currentPosition - (objectSize - footerContent.length));
      int length = footerContent.length - offset;
      logger.atFiner().log(
          "Opened channel (prefetched footer) from %d position for '%s'",
          currentPosition, resourceId);
      return Channels.newChannel(new ByteArrayInputStream(footerContent, offset, length));
    }

    private long getRangeRequestStart() {
      if (readOptions.getFadvise() != Fadvise.SEQUENTIAL && isFooterRead()) {
        // Prefetch footer and adjust start position to footerStart.
        return max(0, objectSize - readOptions.getMinRangeRequestSize());
      }
      return currentPosition;
    }

    public void closeContentChannel() {
      if (byteChannel != null) {
        logger.atFiner().log("Closing internal contentChannel for '%s'", resourceId);
        try {
          byteChannel.close();
        } catch (Exception e) {
          logger.atFine().withCause(e).log(
              "Got an exception on contentChannel.close() for '%s'; ignoring it.", resourceId);
        } finally {
          byteChannel = null;
          reset();
        }
      }
    }

    private void reset() {
      checkState(byteChannel == null, "contentChannel should be null for '%s'", resourceId);
      contentChannelCurrentPosition = -1;
      contentChannelEnd = -1;
    }

    private boolean isInRangeSeek() {
      long seekDistance = currentPosition - contentChannelCurrentPosition;
      if (byteChannel != null
          && seekDistance > 0
          && seekDistance <= readOptions.getInplaceSeekLimit()
          && currentPosition < contentChannelEnd) {
        return true;
      }
      return false;
    }

    private void skipInPlace() {
      if (skipBuffer == null) {
        skipBuffer = new byte[SKIP_BUFFER_SIZE];
      }
      long seekDistance = currentPosition - contentChannelCurrentPosition;
      while (seekDistance > 0 && byteChannel != null) {
        try {
          int bufferSize = toIntExact(min(skipBuffer.length, seekDistance));
          int bytesRead = readFromByteChannel(ByteBuffer.wrap(skipBuffer, 0, bufferSize));
          if (bytesRead < 0) {
            logger.atInfo().log(
                "Somehow read %d bytes trying to skip %d bytes to seek to position %d, size: %d",
                bytesRead, seekDistance, currentPosition, objectSize);
            closeContentChannel();
          } else {
            seekDistance -= bytesRead;
            contentChannelCurrentPosition += bytesRead;
          }
        } catch (Exception e) {
          logger.atInfo().withCause(e).log(
              "Got an IO exception on contentChannel.read(), a lazy-seek will be pending for '%s'",
              resourceId);
          closeContentChannel();
        }
      }
      checkState(
          byteChannel == null || contentChannelCurrentPosition == currentPosition,
          "contentChannelPosition (%s) should be equal to currentPosition (%s)"
              + " after successful in-place skip",
          contentChannelCurrentPosition,
          currentPosition);
    }

    private void performPendingSeeks() {

      // Return quickly if there is no pending seek operation, i.e. position didn't change.
      if (currentPosition == contentChannelCurrentPosition && byteChannel != null) {
        return;
      }

      logger.atFiner().log(
          "Performing lazySeek from %s to %s position '%s'",
          contentChannelCurrentPosition, currentPosition, resourceId);

      if (isInRangeSeek()) {
        skipInPlace();
      } else {
        if (isRandomAccessPattern()) {
          setRandomAccess();
        }
        // close existing contentChannel as requested bytes can't be served from current
        // contentChannel;
        closeContentChannel();
      }
    }

    private boolean isRandomAccessPattern() {
      if (!shouldDetectRandomAccess()) {
        return false;
      }
      if (currentPosition < contentChannelCurrentPosition) {
        logger.atFine().log(
            "Detected backward read from %s to %s position, switching to random IO for '%s'",
            contentChannelCurrentPosition, currentPosition, resourceId);
        return true;
      }
      if (contentChannelCurrentPosition >= 0
          && contentChannelCurrentPosition + readOptions.getInplaceSeekLimit() < currentPosition) {
        logger.atFine().log(
            "Detected forward read from %s to %s position over %s threshold,"
                + " switching to random IO for '%s'",
            contentChannelCurrentPosition,
            currentPosition,
            readOptions.getInplaceSeekLimit(),
            resourceId);
        return true;
      }
      return false;
    }

    private ReadableByteChannel getStorageReadChannel(long seek, long limit) throws IOException {
      ReadChannel readChannel = storage.reader(blobId, generateReadOptions(blobId));
      try {
        readChannel.seek(seek);
        readChannel.limit(limit);
        if (storageOptions.isTraceLogEnabled()) {
          Map<String, Object> jsonMap = new HashMap<>();
          jsonMap.put("method", "open-byte-channel");
          captureTraces(jsonMap);
        }
        return readChannel;
      } catch (Exception e) {
        throw new IOException(
            String.format(
                "Unable to update the boundaries/Range of contentChannel %s",
                resourceId.toString()),
            e);
      }
    }

    private BlobSourceOption[] generateReadOptions(BlobId blobId) {
      List<BlobSourceOption> blobReadOptions = new ArrayList<>();
      if (blobId.getGeneration() != null) {
        blobReadOptions.add(BlobSourceOption.generationMatch(blobId.getGeneration()));
      }
      if (storageOptions.getEncryptionKey() != null) {
        blobReadOptions.add(
            BlobSourceOption.decryptionKey(storageOptions.getEncryptionKey().value()));
      }
      return blobReadOptions.toArray(new BlobSourceOption[blobReadOptions.size()]);
    }

    private boolean isFooterRead() {
      return objectSize - currentPosition <= readOptions.getMinRangeRequestSize();
    }

    private long getRangeRequestEnd(long startPosition, long bytesToRead) {
      long endPosition = objectSize;
      if (randomAccess) {
        // opening a channel for whole object doesn't make sense as anyhow it will not be utilized
        // for further reads.
        endPosition = startPosition + max(bytesToRead, readOptions.getMinRangeRequestSize());
      }
      if (footerContent != null) {
        // If footer is cached open just till footerStart.
        // Remaining content ill be served from cached footer itself.
        endPosition = min(endPosition, objectSize - footerContent.length);
      }
      return endPosition;
    }
  }

  @VisibleForTesting
  boolean randomAccessStatus() {
    return contentReadChannel.randomAccess;
  }

  private static void validate(GoogleCloudStorageItemInfo itemInfo) throws IOException {
    checkNotNull(itemInfo, "itemInfo cannot be null");
    StorageResourceId resourceId = itemInfo.getResourceId();
    checkArgument(
        resourceId.isStorageObject(), "Can not open a non-file object for read: %s", resourceId);
    if (!itemInfo.exists()) {
      throw new FileNotFoundException(String.format("Item not found: %s", resourceId));
    }
    // The non-gRPC read channel has special support for gzip.
    // TODO: enable support for gzip if required.
    String contentEncoding = itemInfo.getContentEncoding();
    if (contentEncoding != null && contentEncoding.contains("gzip")) {

      throw new IOException(
          String.format(
              "Cannot read GZIP-encoded file (%s) (not supported via gRPC API): %s",
              contentEncoding, resourceId));
    }
  }

  private IOException convertError(Exception error) {
    String msg = String.format("Error reading '%s'", resourceId);
    switch (errorExtractor.getErrorType(error)) {
      case NOT_FOUND:
        return createFileNotFoundException(
            resourceId.getBucketName(), resourceId.getObjectName(), new IOException(msg, error));
      case OUT_OF_RANGE:
        return (IOException) new EOFException(msg).initCause(error);
      default:
        return new IOException(msg, error);
    }
  }

  /** Validates that the given position is valid for this channel. */
  private void validatePosition(long position) throws IOException {
    if (position < 0) {
      throw new EOFException(
          String.format(
              "Invalid seek offset: position value (%d) must be >= 0 for '%s'",
              position, resourceId));
    }

    if (objectSize >= 0 && position >= objectSize) {
      throw new EOFException(
          String.format(
              "Invalid seek offset: position value (%d) must be between 0 and %d for '%s'",
              position, objectSize, resourceId));
    }
  }

  /** Throws if this channel is not currently open. */
  private void throwIfNotOpen() throws IOException {
    if (!isOpen()) {
      throw new ClosedChannelException();
    }
  }
}
