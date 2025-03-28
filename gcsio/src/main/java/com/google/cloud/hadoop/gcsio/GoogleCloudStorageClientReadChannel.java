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
import static com.google.common.base.Strings.nullToEmpty;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;

import com.google.cloud.ReadChannel;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.Fadvise;
import com.google.cloud.hadoop.util.ErrorTypeExtractor;
import com.google.cloud.hadoop.util.GoogleCloudStorageEventBus;
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
import javax.annotation.Nullable;

/** Provides seekable read access to GCS via java-storage library. */
@VisibleForTesting
class GoogleCloudStorageClientReadChannel implements SeekableByteChannel {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private static final String GZIP_ENCODING = "gzip";

  private final StorageResourceId resourceId;
  private final GoogleCloudStorageReadOptions readOptions;
  private final GoogleCloudStorageOptions storageOptions;
  private final Storage storage;
  // The size of this object generation, in bytes.
  private long objectSize;
  private final ErrorTypeExtractor errorExtractor;
  private ContentReadChannel contentReadChannel;
  private boolean gzipEncoded = false;
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
    this.contentReadChannel = new ContentReadChannel(readOptions, resourceId);
    initMetadata(itemInfo.getContentEncoding(), itemInfo.getSize());
  }

  protected void initMetadata(@Nullable String encoding, long sizeFromMetadata) throws IOException {
    gzipEncoded = nullToEmpty(encoding).contains(GZIP_ENCODING);
    if (gzipEncoded && !readOptions.getSupportGzipEncoding()) {
      throw new IOException(
          "Cannot read GZIP encoded files - content encoding support is disabled.");
    }
    objectSize = gzipEncoded ? Long.MAX_VALUE : sizeFromMetadata;
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
    GoogleCloudStorageEventBus.postOnException();
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
    GoogleCloudStorageEventBus.postOnException();
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
        GoogleCloudStorageEventBus.postOnException();
        throw new IOException(
            String.format(
                "Exception occurred while closing channel '%s'. cause=%s",
                resourceId, e.getMessage()),
            e);
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
    private final FileAccessPatternManager fileAccessManager;

    public ContentReadChannel(
        GoogleCloudStorageReadOptions readOptions, StorageResourceId resourceId) {
      this.blobId =
          BlobId.of(
              resourceId.getBucketName(), resourceId.getObjectName(), resourceId.getGenerationId());
      this.fileAccessManager = new FileAccessPatternManager(resourceId, readOptions);
      if (gzipEncoded) {
        fileAccessManager.overrideAccessPattern(false);
      }
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
            // We adjust the start index of content channel in following cases
            // 1. request range is in footer boundaries --> request the whole footer
            // 2. requested content is gzip encoded -> request always from start of file.
            // Case(1) is handled with reading and caching the extra read bytes, for all other cases
            // we need to skip all the unrequested bytes before start reading from current position.
            if (currentPosition > contentChannelCurrentPosition) {
              skipInPlace();
            }
            // making sure that currentPosition is in alignment with currentReadPosition before
            // actual read starts to avoid read discrepancies.
            checkState(
                contentChannelCurrentPosition == currentPosition,
                "position of read offset isn't in alignment with channel's read offset");
          }
          int bytesRead = byteChannel.read(dst);

          /*
          As we are using the zero copy implementation of byteChannel, it can return even zero bytes,
          while reading,
          we should not treat it as an error scenario anymore.
          */
          if (bytesRead == 0) {
            logger.atFiner().log(
                "Read %d from storage-client's byte channel at position: %d with channel ending at: %d for resourceId: %s of size: %d",
                bytesRead, currentPosition, contentChannelEnd, resourceId, objectSize);
          }

          if (bytesRead < 0) {
            // Because we don't know decompressed object size for gzip-encoded objects,
            // assume that this is an object end.
            if (gzipEncoded) {
              objectSize = currentPosition;
              contentChannelEnd = currentPosition;
            }

            if (currentPosition != contentChannelEnd && currentPosition != objectSize) {
              GoogleCloudStorageEventBus.postOnException();
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
          contentChannelCurrentPosition += partialBytes;
          logger.atFine().log(
              "Closing contentChannel after %s exception for '%s'.", e.getMessage(), resourceId);
          closeContentChannel();
          throw convertError(e);
        }
      }
      return totalBytesRead;
    }

    private int partiallyReadBytes(int remainingBeforeRead, ByteBuffer dst) {
      int partialReadBytes = 0;
      if (remainingBeforeRead != dst.remaining()) {
        partialReadBytes = remainingBeforeRead - dst.remaining();
      }
      return partialReadBytes;
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

      // Should be updated only if content is not served from cached footer
      fileAccessManager.updateAccessPattern(currentPosition);

      setChannelBoundaries(bytesToRead);

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
      return readableByteChannel;
    }

    private void setChannelBoundaries(long bytesToRead) {
      contentChannelCurrentPosition = getRangeRequestStart();
      contentChannelEnd = getRangeRequestEnd(contentChannelCurrentPosition, bytesToRead);
      checkState(
          contentChannelEnd >= contentChannelCurrentPosition,
          String.format(
              "Start position should be <= endPosition startPosition:%d, endPosition: %d",
              contentChannelCurrentPosition, contentChannelEnd));
    }

    private void cacheFooter(ReadableByteChannel readableByteChannel) throws IOException {
      int footerSize = toIntExact(objectSize - contentChannelCurrentPosition);
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
      contentChannelCurrentPosition = currentPosition;
      int offset = toIntExact(currentPosition - (objectSize - footerContent.length));
      int length = footerContent.length - offset;
      logger.atFiner().log(
          "Opened channel (prefetched footer) from %d position for '%s'",
          currentPosition, resourceId);
      return Channels.newChannel(new ByteArrayInputStream(footerContent, offset, length));
    }

    private long getRangeRequestStart() {
      if (gzipEncoded) {
        return 0;
      }
      if (readOptions.getFadvise() != Fadvise.SEQUENTIAL && isFooterRead()) {
        // Prefetch footer and adjust start position to footerStart.
        return max(0, objectSize - readOptions.getMinRangeRequestSize());
      }
      return currentPosition;
    }

    private long getRangeRequestEnd(long startPosition, long bytesToRead) {
      // Always read gzip-encoded files till the end - they do not support range reads.
      if (gzipEncoded) {
        return objectSize;
      }
      long endPosition = objectSize;
      if (fileAccessManager.shouldAdaptToRandomAccess()) {
        // opening a channel for whole object doesn't make sense as anyhow it will not be utilized
        // for further reads.
        endPosition = startPosition + max(bytesToRead, readOptions.getMinRangeRequestSize());
      } else {
        if (readOptions.getFadvise() == Fadvise.AUTO_RANDOM) {
          endPosition = min(startPosition + readOptions.getBlockSize(), objectSize);
        }
      }
      if (footerContent != null) {
        // If footer is cached open just till footerStart.
        // Remaining content ill be served from cached footer itself.
        endPosition = min(endPosition, objectSize - footerContent.length);
      }
      return endPosition;
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
          fileAccessManager.updateLastServedIndex(contentChannelCurrentPosition);
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
          // for gzip encoded content always seek in place
          && (gzipEncoded || seekDistance <= readOptions.getInplaceSeekLimit())
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
          int bytesRead = byteChannel.read(ByteBuffer.wrap(skipBuffer, 0, bufferSize));
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
        // close existing contentChannel as requested bytes can't be served from current
        // contentChannel;
        closeContentChannel();
      }
    }

    private ReadableByteChannel getStorageReadChannel(long seek, long limit) throws IOException {
      ReadChannel readChannel = storage.reader(blobId, generateReadOptions(blobId));
      try {
        readChannel.seek(seek);
        readChannel.limit(limit);
        // bypass the storage-client caching layer hence eliminates the need to maintain a copy of
        // chunk
        readChannel.setChunkSize(0);
        return readChannel;
      } catch (Exception e) {
        GoogleCloudStorageEventBus.postOnException();
        throw new IOException(
            String.format(
                "Unable to update the boundaries/Range of contentChannel %s. cause=%s",
                resourceId, e),
            e);
      }
    }

    private BlobSourceOption[] generateReadOptions(BlobId blobId) {
      List<BlobSourceOption> blobReadOptions = new ArrayList<>();
      // To get decoded content
      blobReadOptions.add(BlobSourceOption.shouldReturnRawInputStream(false));

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
  }

  @VisibleForTesting
  boolean randomAccessStatus() {
    return contentReadChannel.fileAccessManager.shouldAdaptToRandomAccess();
  }

  private static void validate(GoogleCloudStorageItemInfo itemInfo) throws IOException {
    checkNotNull(itemInfo, "itemInfo cannot be null");
    StorageResourceId resourceId = itemInfo.getResourceId();
    checkArgument(
        resourceId.isStorageObject(), "Can not open a non-file object for read: %s", resourceId);
    if (!itemInfo.exists()) {
      GoogleCloudStorageEventBus.postOnException();
      throw new FileNotFoundException(String.format("Item not found: %s", resourceId));
    }
  }

  private IOException convertError(Exception error) {
    String msg =
        String.format(
            "Error reading '%s'. cause='%s'", resourceId, error == null ? "" : error.getMessage());
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
      GoogleCloudStorageEventBus.postOnException();
      throw new EOFException(
          String.format(
              "Invalid seek offset: position value (%d) must be >= 0 for '%s'",
              position, resourceId));
    }

    if (objectSize >= 0 && position >= objectSize) {
      GoogleCloudStorageEventBus.postOnException();
      throw new EOFException(
          String.format(
              "Invalid seek offset: position value (%d) must be between 0 and %d for '%s'",
              position, objectSize, resourceId));
    }
  }

  /** Throws if this channel is not currently open. */
  private void throwIfNotOpen() throws IOException {
    if (!isOpen()) {
      GoogleCloudStorageEventBus.postOnException();
      throw new ClosedChannelException();
    }
  }
}
