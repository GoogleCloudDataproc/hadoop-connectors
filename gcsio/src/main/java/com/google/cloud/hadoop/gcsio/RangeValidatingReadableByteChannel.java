package com.google.cloud.hadoop.gcsio;

import com.google.cloud.ReadChannel;
import com.google.cloud.hadoop.util.GoogleCloudStorageEventBus;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobSourceOption;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Locale;

final class RangeValidatingReadableByteChannel implements ReadableByteChannel {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private final String resourceId;
  private final long beginOffset;
  private final long endOffset;
  private final ReadableByteChannel delegate;

  private long position;

  /**
   * @param endOffset expected to be <= to the length of the object -- this class does not possess
   *     the capability of clamping the end offset to the object size
   */
  private RangeValidatingReadableByteChannel(
      String resourceId, long beginOffset, long endOffset, ReadableByteChannel delegate) {
    this.resourceId = resourceId;
    this.beginOffset = beginOffset;
    this.endOffset = endOffset;
    this.position = beginOffset;
    this.delegate = delegate;
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    int expectedMaxRead;
    long expectedChannelRemaining = endOffset - position;
    if (expectedChannelRemaining < dst.remaining()) {
      expectedMaxRead = Math.toIntExact(expectedChannelRemaining);
    } else {
      expectedMaxRead = dst.remaining();
    }
    int read = delegate.read(dst);
    if (read > -1) {
      position += read;
      if (read > expectedMaxRead) {
        // over-read
        logger.atWarning().log(
            "over-read of object %s detected. "
                + "Channel opened with {beginOffset: %s, endOffset: %s}, "
                + "over-read {position: %s, expectedMaxRead: %s, read: %s} (read %s additional bytes)",
            resourceId,
            beginOffset,
            endOffset,
            position,
            expectedMaxRead,
            read,
            read - expectedMaxRead);
      }
    } else {
      if (position < endOffset) {
        // in reality, if this were to ever happen it should be handled as a retryable error, where
        // the next read would begin at the current offset/position.
        // under-read
        throw new IOException(
            String.format(
                Locale.US,
                "under-read of object %s detected. "
                    + "Channel opened with {beginOffset: %s, endOffset: %s}, "
                    + "EOF detected at position: %s (missing %s bytes)",
                resourceId,
                beginOffset,
                endOffset,
                position,
                expectedChannelRemaining));
      }
    }

    return read;
  }

  @Override
  public boolean isOpen() {
    return delegate.isOpen();
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }

  @VisibleForTesting
  static RangeValidatingReadableByteChannel of(
      String resourceId, long beginOffset, long endOffset, ReadableByteChannel delegate) {
    return new RangeValidatingReadableByteChannel(resourceId, beginOffset, endOffset, delegate);
  }

  static RangeValidatingReadableByteChannel of(
      Storage storage,
      BlobId id,
      long beginOffset,
      long endOffset,
      BlobSourceOption... storageReadOptions)
      throws IOException {
    ReadChannel readChannel = storage.reader(id, storageReadOptions);
    try {
      readChannel.seek(beginOffset);
      readChannel.limit(endOffset);
      // disable client level chunk buffering. This also makes the channel semi-non-blocking
      readChannel.setChunkSize(0);
      return of(id.toGsUtilUri(), beginOffset, endOffset, readChannel);
    } catch (Exception e) {
      GoogleCloudStorageEventBus.postOnException();
      throw new IOException(
          String.format(
              "Unable to update the boundaries/Range of contentChannel %s. cause=%s",
              id.toGsUtilUri(), e),
          e);
    }
  }
}
