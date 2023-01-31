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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;

import com.google.cloud.ReadChannel;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.Fadvise;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobSourceOption;
import com.google.common.annotations.VisibleForTesting;
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
import java.util.List;
import javax.annotation.Nonnull;

@VisibleForTesting
class GoogleCloudStorageClientReadChannel implements SeekableByteChannel {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private static final int SKIP_BUFFER_SIZE = 8192;
  private final StorageResourceId resourceId;
  private final BlobId blobId;
  private final GoogleCloudStorageReadOptions readOptions;
  private final Storage storage;
  // The size of this object generation, in bytes.
  private final long objectSize;
  private boolean randomAccess;
  // True if this channel is open, false otherwise.
  private boolean channelIsOpen = true;

  // Current position in this channel, it could be different from contentChannelPosition if
  // position(long) method calls were made without calls to read(ByteBuffer) method.
  private long currentPosition = 0;
  private ReadableByteChannel contentReadChannel = null;
  // This is the actual current position in `contentChannel` from where read can happen.
  // This remains unchanged of position(long) method call.
  private long contentChannelPosition = -1;
  private long contentChannelEnd = -1;
  private final int footerSize;

  // Used as scratch space when reading bytes just to discard them when trying to perform small
  // in-place seeks.
  private byte[] skipBuffer = null;
  // Size of buffer to allocate for skipping bytes in-place when performing in-place seeks.

  // Prefetched footer content.
  private byte[] footerContent;

  public GoogleCloudStorageClientReadChannel(
      Storage gcs,
      GoogleCloudStorageItemInfo itemInfo,
      @Nonnull GoogleCloudStorageReadOptions readOptions)
      throws IOException {
    validate(itemInfo);
    this.storage = gcs;
    this.resourceId =
        new StorageResourceId(
            itemInfo.getBucketName(), itemInfo.getObjectName(), itemInfo.getContentGeneration());
    this.blobId =
        BlobId.of(
            resourceId.getBucketName(), resourceId.getObjectName(), resourceId.getGenerationId());
    this.readOptions = readOptions;
    this.objectSize = itemInfo.getSize();
    this.randomAccess = readOptions.getFadvise().equals(Fadvise.RANDOM) ? true : false;
    this.footerSize = (int) readOptions.getMinRangeRequestSize();
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

    int totalBytesRead = 0;
    while (dst.hasRemaining()) {
      try {
        performPendingSeeks(dst.remaining());
        int bytesRead = contentReadChannel.read(dst);

        if (bytesRead > 0) {
          totalBytesRead += bytesRead;
          currentPosition += bytesRead;
          contentChannelPosition += bytesRead;
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
      } catch (Exception e) {
        logger.atFine().log(
            "Closing contentChannel after %s exception for '%s'.", e.getMessage(), resourceId);
        closeContentChannel();
        throw e;
      }
    }
    return totalBytesRead;
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    throw new UnsupportedOperationException("Cannot mutate read-only channel: " + this);
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
    return null;
  }

  @Override
  public boolean isOpen() {
    return channelIsOpen;
  }

  @Override
  public void close() {
    if (!channelIsOpen) {
      logger.atFiner().log("Ignoring close: channel for '%s' is not open.", resourceId);
      return;
    }
    logger.atFiner().log("Closing channel for '%s'", resourceId);
    channelIsOpen = false;
    closeContentChannel();
  }

  private void cacheFooter(ReadableByteChannel readableByteChannel) throws IOException {
    footerContent = new byte[footerSize];
    try (InputStream footerStream = Channels.newInputStream(readableByteChannel)) {
      int totalBytesRead = 0;
      int bytesRead;
      do {
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
    contentChannelPosition = currentPosition;
    int offset = toIntExact(currentPosition - (objectSize - footerContent.length));
    int length = footerContent.length - offset;
    logger.atFiner().log(
        "Opened channel (prefetched footer) from %d position for '%s'",
        currentPosition, resourceId);
    return Channels.newChannel(new ByteArrayInputStream(footerContent, offset, length));
  }

  private ReadableByteChannel openReadChannel(long bytesToRead) throws IOException {
    checkArgument(bytesToRead > 0, "bytesToRead should be greater than 0, but was %s", bytesToRead);
    checkState(
        contentReadChannel == null && contentChannelEnd < 0,
        "contentChannel and contentChannelEnd should be not initialized yet for '%s'",
        resourceId);

    if (footerContent != null && currentPosition >= objectSize - footerContent.length) {
      return serveFooterContent();
    }

    contentChannelPosition = getRangeRequestStart();
    long rangeLength = getRangeRequestLength(bytesToRead);
    contentChannelEnd = rangeLength == -1 ? objectSize : contentChannelPosition + rangeLength;
    checkState(
        contentChannelEnd >= contentChannelPosition,
        String.format(
            "Start position should be <= endPosition startPosition:%d, endPosition: %d",
            contentChannelPosition, contentChannelEnd));

    ReadableByteChannel readableByteChannel =
        getStorageReadChannel(contentChannelPosition, contentChannelEnd);

    // Check if footer was fetched
    if (contentChannelEnd == objectSize
        && (contentChannelEnd - contentChannelPosition) == footerSize) {
      if (footerContent == null) {
        cacheFooter(readableByteChannel);
      }
      return serveFooterContent();
    }
    checkState(
        contentChannelPosition == currentPosition,
        "position of read offset isn't in alignment with channel's read offset");
    return readableByteChannel;
  }

  private long getRangeRequestStart() {
    if (readOptions.getFadvise() != Fadvise.SEQUENTIAL && isFooterRead()) {
      // Prefetch footer and adjust start position to footerStart.
      return Math.max(0, objectSize - footerSize);
    }
    return currentPosition;
  }

  /**
   * Returns the length/size of byte range to be requested
   *
   * @param bytesToRead
   * @return -1 if Whole objected is requested else the length/size of range.
   */
  private long getRangeRequestLength(long bytesToRead) {
    if (randomAccess) {
      // if randomAccess is enabled, limit fetched content
      return Math.max(bytesToRead, readOptions.getMinRangeRequestSize());
    }
    return -1;
  }

  private void setRandomAccess() {
    randomAccess = true;
  }

  private boolean shouldDetectRandomAccess() {
    return !randomAccess && readOptions.getFadvise() == Fadvise.AUTO;
  }

  private boolean isRandomAccessPattern(long oldPosition) {
    if (!shouldDetectRandomAccess()) {
      return false;
    }
    // backwardSeek
    if (currentPosition < oldPosition) {
      logger.atFine().log(
          "Detected backward read from %s to %s position, switching to random IO for '%s'",
          oldPosition, currentPosition, resourceId);
      return true;
    }
    if (oldPosition >= 0 && oldPosition + readOptions.getInplaceSeekLimit() < currentPosition) {
      logger.atFine().log(
          "Detected forward read from %s to %s position over %s threshold,"
              + " switching to random IO for '%s'",
          oldPosition, currentPosition, readOptions.getInplaceSeekLimit(), resourceId);
      return true;
    }
    return false;
  }

  private void skipInPlace(long seekDistance) {
    if (skipBuffer == null) {
      skipBuffer = new byte[SKIP_BUFFER_SIZE];
    }
    while (seekDistance > 0 && contentReadChannel != null) {
      try {
        int bufferSize = toIntExact(min(skipBuffer.length, seekDistance));
        int bytesRead = contentReadChannel.read(ByteBuffer.wrap(skipBuffer, 0, bufferSize));
        if (bytesRead < 0) {
          logger.atInfo().log(
              "Somehow read %d bytes trying to skip %d bytes to seek to position %d, size: %d",
              bytesRead, seekDistance, currentPosition, objectSize);
          closeContentChannel();
        } else {
          seekDistance -= bytesRead;
          contentChannelPosition += bytesRead;
        }
      } catch (Exception e) {
        logger.atInfo().withCause(e).log(
            "Got an IO exception on contentChannel.read(), a lazy-seek will be pending for '%s'",
            resourceId);
        closeContentChannel();
      }
    }
    checkState(
        contentReadChannel == null || contentChannelPosition == currentPosition,
        "contentChannelPosition (%s) should be equal to currentPosition (%s)"
            + " after successful in-place skip",
        contentChannelPosition,
        currentPosition);
  }

  private void resetContentChannel() {
    checkState(contentReadChannel == null, "contentChannel should be null for '%s'", resourceId);
    contentChannelPosition = -1;
    contentChannelEnd = -1;
  }

  protected void closeContentChannel() {
    if (contentReadChannel != null) {
      logger.atFiner().log("Closing internal contentChannel for '%s'", resourceId);
      try {
        contentReadChannel.close();
      } catch (Exception e) {
        logger.atFine().withCause(e).log(
            "Got an exception on contentChannel.close() for '%s'; ignoring it.", resourceId);
      } finally {
        contentReadChannel = null;
        resetContentChannel();
      }
    }
  }

  private void performPendingSeeks(long bytesToRead) throws IOException {
    if (currentPosition == contentChannelPosition && contentReadChannel != null) {
      // No seek is required
      return;
    }

    logger.atFiner().log(
        "Performing lazySeek from %s to %s position for '%s'",
        contentChannelPosition, currentPosition, resourceId);
    // record the variation in channelPosition
    // used to auto-detect random access
    long oldPosition = contentChannelPosition;
    long seekDistance = currentPosition - contentChannelPosition;
    // Check if inPlace seek is a viable option
    // If not close existing channel and open new one.

    if (contentReadChannel != null
        && seekDistance > 0
        && seekDistance <= readOptions.getInplaceSeekLimit()
        && currentPosition < contentChannelEnd) {
      skipInPlace(seekDistance);
    } else {
      closeContentChannel();
    }
    if (contentReadChannel == null) {
      if (isRandomAccessPattern(oldPosition)) {
        setRandomAccess();
      }
      contentReadChannel = openReadChannel(bytesToRead);
    }
  }

  // Update this function to create the reader and then set limits
  private ReadableByteChannel getStorageReadChannel(long seek, long limit) throws IOException {
    ReadChannel readChannel = storage.reader(blobId, generateReadOptions(blobId));
    try {
      readChannel.seek(seek);
      // TODO: should we also add a check to not set limit if contentChannelEnd == objectSize?
      if (limit != -1) {
        readChannel.limit(limit);
      }
      return readChannel;
    } catch (Exception e) {
      throw new IOException(
          String.format(
              "Unable to update the boundaries/Range of contentChannel %s", resourceId.toString()),
          e);
    }
  }

  private static BlobSourceOption[] generateReadOptions(BlobId blobId) {
    List<BlobSourceOption> readOptions = new ArrayList<>();
    if (blobId.getGeneration() != null) {
      readOptions.add(BlobSourceOption.generationMatch(blobId.getGeneration()));
    }
    return readOptions.toArray(new BlobSourceOption[readOptions.size()]);
  }

  private boolean isFooterRead() {
    return objectSize - currentPosition <= footerSize;
  }

  @VisibleForTesting
  boolean randomAccessStatus() {
    return randomAccess;
  }

  private static void validate(GoogleCloudStorageItemInfo itemInfo) throws IOException {
    checkNotNull(itemInfo, "itemInfo cannot be null");
    StorageResourceId resourceId = itemInfo.getResourceId();
    checkArgument(
        resourceId.isStorageObject(), "Can not open a non-file object for read: %s", resourceId);
    if (!itemInfo.exists()) {
      throw new FileNotFoundException(String.format("File not found: %s", resourceId));
    }
    // The non-gRPC read channel has special support for gzip. This channel doesn't
    // decompress gzip-encoded objects on the fly, so best to fail fast rather than return
    // gibberish unexpectedly.
    String contentEncoding = itemInfo.getContentEncoding();
    if (contentEncoding != null && contentEncoding.contains("gzip")) {
      throw new IOException(
          String.format(
              "Cannot read GZIP-encoded file (%s) (not supported via gRPC API): %s",
              contentEncoding, resourceId));
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
