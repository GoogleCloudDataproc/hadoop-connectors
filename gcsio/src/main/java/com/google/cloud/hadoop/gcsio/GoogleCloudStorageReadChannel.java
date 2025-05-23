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

package com.google.cloud.hadoop.gcsio;

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageExceptions.createFileNotFoundException;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.nullToEmpty;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;

import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.BackOffUtils;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.Sleeper;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.Fadvise;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.cloud.hadoop.util.ClientRequestHelper;
import com.google.cloud.hadoop.util.GoogleCloudStorageEventBus;
import com.google.cloud.hadoop.util.ResilientOperation;
import com.google.cloud.hadoop.util.RetryDeterminer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.common.flogger.GoogleLogger;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Provides seekable read access to GCS. */
public class GoogleCloudStorageReadChannel implements SeekableByteChannel {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  // Size of buffer to allocate for skipping bytes in-place when performing in-place seeks.
  @VisibleForTesting static final int SKIP_BUFFER_SIZE = 8192;

  private static final String GZIP_ENCODING = "gzip";

  // GCS resource/object path
  private StorageResourceId resourceId;

  // Factory that creates requests that override storage api.
  private final StorageRequestFactory storageRequestFactory;

  // GCS object content channel.
  @VisibleForTesting ReadableByteChannel contentChannel;

  // True if this channel is open, false otherwise.
  private boolean channelIsOpen = true;

  // Current position in this channel, it could be different from contentChannelPosition if
  // position(long) method calls were made without calls to read(ByteBuffer) method.
  // TODO(b/120887495): This @VisibleForTesting annotation was being ignored by prod code.
  // Please check that removing it is correct, and remove this comment along with it.
  // @VisibleForTesting
  protected long currentPosition = 0;

  // Current read position in the contentChannel.
  //
  // When a caller calls position(long) to set stream position, we record the target position
  // and defer the actual seek operation until the caller tries to read from the channel.
  // This allows us to avoid an unnecessary seek to position 0 that would take place on creation
  // of this instance in cases where caller intends to start reading at some other offset.
  // If contentChannelPosition is not the same as currentPosition, it indicates that a target
  // position has been set but the actual seek operation is still pending.
  // TODO(b/120887495): This @VisibleForTesting annotation was being ignored by prod code.
  // Please check that removing it is correct, and remove this comment along with it.
  // @VisibleForTesting
  protected long contentChannelPosition = -1;

  // Size of the object being read.
  private long size = -1;

  // Size of the contentChannel.
  private long contentChannelEnd = -1;

  // Whether to use bounded range requests or streaming requests.
  @VisibleForTesting boolean randomAccess;

  // Maximum number of automatic retries when reading from the underlying channel without making
  // progress; each time at least one byte is successfully read, the counter of attempted retries
  // is reset.
  // TODO(user): Wire this setting out to GHFS; it should correspond to adding the wiring for
  // setting the equivalent value inside HttpRequest.java that determines the low-level retries
  // during "execute()" calls. The default in HttpRequest.java is also 10.
  private int maxRetries = 10;

  // Helper delegate for turning IOExceptions from API calls into higher-level semantics.
  private final ApiErrorExtractor errorExtractor;

  // Request helper to use to set extra headers
  private final ClientRequestHelper<StorageObject> clientRequestHelper;

  // Fine-grained options.
  private final GoogleCloudStorageReadOptions readOptions;

  // Sleeper used for waiting between retries.
  private Sleeper sleeper = Sleeper.DEFAULT;

  // read operation gets its own Exponential Backoff Strategy,
  // to avoid interference with other operations in nested retries.
  private Supplier<BackOff> readBackOff = Suppliers.memoize(this::createBackOff);

  // Used as scratch space when reading bytes just to discard them when trying to perform small
  // in-place seeks.
  private byte[] skipBuffer = null;

  // Whether object content is gzip-encoded.
  private boolean gzipEncoded = false;

  // Prefetched footer content.
  // TODO(b/110832992):
  // 1. Test showing footer prefetch avoids another request to GCS.
  // 2. Test showing shorter footer prefetch does not cause any problems.
  // 3. Test that footer prefetch always disabled for gzipped files.
  private byte[] footerContent;

  @VisibleForTesting protected boolean metadataInitialized = false;

  /**
   * Constructs an instance of GoogleCloudStorageReadChannel.
   *
   * @param gcs storage object instance
   * @param resourceId contains information about a specific resource
   * @param requestHelper a ClientRequestHelper used to set any extra headers
   * @param readOptions fine-grained options specifying things like retry settings, buffering, etc.
   *     Could not be null.
   * @throws IOException on IO error
   */
  public GoogleCloudStorageReadChannel(
      Storage gcs,
      StorageResourceId resourceId,
      ApiErrorExtractor errorExtractor,
      ClientRequestHelper<StorageObject> requestHelper,
      @Nonnull GoogleCloudStorageReadOptions readOptions)
      throws IOException {
    this.storageRequestFactory = new StorageRequestFactory(gcs);
    this.clientRequestHelper = requestHelper;
    this.errorExtractor = errorExtractor;
    this.readOptions = readOptions;
    this.resourceId = resourceId;

    // Initialize metadata if available.
    GoogleCloudStorageItemInfo info = getInitialMetadata();
    if (info != null || readOptions.isFastFailOnNotFoundEnabled()) {
      initMetadata(info == null ? fetchInitialMetadata() : info);
    }
  }

  /** Sets the Sleeper used for sleeping between retries. */
  @VisibleForTesting
  void setSleeper(Sleeper sleeper) {
    checkArgument(sleeper != null, "sleeper must not be null!");
    this.sleeper = sleeper;
  }

  /**
   * Sets the back-off for determining sleep duration between read retries.
   *
   * @param backOff {@link BackOff} to use for read retries, could not be null.
   */
  void setReadBackOff(BackOff backOff) {
    this.readBackOff = Suppliers.ofInstance(checkNotNull(backOff, "backOff could not be null"));
  }

  /** Creates new generic BackOff used for retries. */
  @VisibleForTesting
  ExponentialBackOff createBackOff() {
    return new ExponentialBackOff.Builder()
        .setInitialIntervalMillis(toIntExact(readOptions.getBackoffInitialInterval().toMillis()))
        .setRandomizationFactor(readOptions.getBackoffRandomizationFactor())
        .setMultiplier(readOptions.getBackoffMultiplier())
        .setMaxIntervalMillis(toIntExact(readOptions.getBackoffMaxInterval().toMillis()))
        .setMaxElapsedTimeMillis(toIntExact(readOptions.getBackoffMaxElapsedTime().toMillis()))
        .build();
  }

  /**
   * Returns {@link GoogleCloudStorageItemInfo} used to initialize metadata in constructor or {@code
   * null} if {@link GoogleCloudStorageReadOptions#isFastFailOnNotFoundEnabled()} is set to {@code
   * false}.
   */
  @Nullable
  protected GoogleCloudStorageItemInfo getInitialMetadata() throws IOException {
    return null;
  }

  /** Returns {@link GoogleCloudStorageItemInfo} used to initialize metadata in constructor. */
  private GoogleCloudStorageItemInfo fetchInitialMetadata() throws IOException {
    StorageObject object;
    try {
      // Request only fields that are used for metadata initialization
      Storage.Objects.Get getObject =
          createMetadataRequest().setFields("contentEncoding,generation,size");
      object =
          ResilientOperation.retry(
              getObject::execute,
              readBackOff.get(),
              RetryDeterminer.SOCKET_ERRORS,
              IOException.class,
              sleeper);
    } catch (IOException e) {
      GoogleCloudStorageEventBus.postOnException();
      throw errorExtractor.itemNotFound(e)
          ? createFileNotFoundException(resourceId, e)
          : new IOException("Error reading " + resourceId, e);
    } catch (InterruptedException e) {
      GoogleCloudStorageEventBus.postOnException();
      Thread.currentThread().interrupt();
      throw new IOException("Thread interrupt received.", e);
    }
    return GoogleCloudStorageItemInfo.createObject(
        resourceId,
        /* creationTime= */ 0,
        /* modificationTime= */ 0,
        checkNotNull(object.getSize(), "size can not be null for '%s'", resourceId).longValue(),
        /* contentType= */ null,
        object.getContentEncoding(),
        /* metadata= */ null,
        checkNotNull(object.getGeneration(), "generation can not be null for '%s'", resourceId),
        /* metaGeneration= */ 0,
        /* verificationAttributes= */ null);
  }

  /**
   * Sets the number of times to automatically retry by re-opening the underlying contentChannel
   * whenever an exception occurs while reading from it. The count of attempted retries is reset
   * whenever at least one byte is successfully read, so this number of retries refers to retries
   * made without achieving any forward progress.
   */
  public void setMaxRetries(int maxRetries) {
    this.maxRetries = maxRetries;
  }

  /**
   * Reads from this channel and stores read data in the given buffer.
   *
   * <p>On unexpected failure, will attempt to close the channel and clean up state.
   *
   * @param buffer buffer to read data into
   * @return number of bytes read or -1 on end-of-stream
   * @throws IOException on IO error
   */
  @Override
  public int read(ByteBuffer buffer) throws IOException {
    throwIfNotOpen();

    // Don't try to read if the buffer has no space.
    if (buffer.remaining() == 0) {
      return 0;
    }

    logger.atFiner().log(
        "Reading %d bytes at %d position from '%s'",
        buffer.remaining(), currentPosition, resourceId);

    // Do not perform any further reads if we already read everything from this channel.
    if (currentPosition == size) {
      return -1;
    }

    int totalBytesRead = 0;
    int retriesAttempted = 0;

    // We read from a streaming source. We may not get all the bytes we asked for
    // in the first read. Therefore, loop till we either read the required number of
    // bytes or we reach end-of-stream.
    do {
      int remainingBeforeRead = buffer.remaining();
      performLazySeek(remainingBeforeRead);
      checkState(
          contentChannelPosition == currentPosition,
          "contentChannelPosition (%s) should be equal to currentPosition (%s) after lazy seek",
          contentChannelPosition,
          currentPosition);

      try {
        int numBytesRead = contentChannel.read(buffer);
        if (numBytesRead == 0) {
          throw new IOException(
              String.format("Read 0 bytes without blocking from object: '%s'", resourceId));
        }
        if (numBytesRead < 0) {
          // Because we don't know decompressed object size for gzip-encoded objects,
          // assume that this is an object end.
          if (gzipEncoded) {
            size = currentPosition;
            contentChannelEnd = currentPosition;
          }
          // Check that we didn't get a premature End of Stream signal by checking the
          // number of
          // bytes read against the stream size. Unfortunately we don't have information
          // about the
          // actual size of the data stream when stream compression is used, so we can
          // only ignore
          // this case here.
          if (currentPosition != contentChannelEnd && currentPosition != size) {
            GoogleCloudStorageEventBus.postOnException();
            throw new IOException(
                String.format(
                    "Received end of stream result before all the file data has been received;"
                        + " totalBytesRead: %d, currentPosition: %d,"
                        + " contentChannelEnd %d, size: %d, object: '%s'",
                    totalBytesRead, currentPosition, contentChannelEnd, size, resourceId));
          }

          // If we have reached an end of a contentChannel but not an end of an object
          // then close contentChannel and continue reading an object if necessary.
          if (contentChannelEnd != size && currentPosition == contentChannelEnd) {
            closeContentChannel();
          } else {
            break;
          }
        }

        if (numBytesRead > 0) {
          totalBytesRead += numBytesRead;
          currentPosition += numBytesRead;
          contentChannelPosition += numBytesRead;
          checkState(
              contentChannelPosition == currentPosition,
              "contentChannelPosition (%s) should be equal to currentPosition (%s)"
                  + " after successful read",
              contentChannelPosition,
              currentPosition);
        }

        if (retriesAttempted != 0) {
          logger.atInfo().log(
              "Success after %d retries on reading '%s'", retriesAttempted, resourceId);
        }
        // The count of retriesAttempted is per low-level contentChannel.read call;
        // each time we make progress we reset the retry counter.
        retriesAttempted = 0;
      } catch (IOException ioe) {
        logger.atFine().log(
            "Closing contentChannel after %s exception for '%s'.", ioe.getMessage(), resourceId);
        closeContentChannel();

        if (buffer.remaining() != remainingBeforeRead) {
          int partialRead = remainingBeforeRead - buffer.remaining();
          logger.atInfo().log(
              "Despite exception, had partial read of %d bytes from '%s'; resetting retry count.",
              partialRead, resourceId);
          retriesAttempted = 0;
          totalBytesRead += partialRead;
          currentPosition += partialRead;
        }

        // TODO(user): Refactor any reusable logic for retries into a separate RetryHelper
        // class.
        if (retriesAttempted == maxRetries) {
          GoogleCloudStorageEventBus.postOnException();
          logger.atSevere().log(
              "Throwing exception after reaching max read retries (%d) for '%s'.",
              maxRetries, resourceId);
          throw ioe;
        }

        if (retriesAttempted == 0) {
          // If this is the first of a series of retries, we also want to reset the
          // readBackOff
          // to have fresh initial values.
          readBackOff.get().reset();
        }

        ++retriesAttempted;
        logger.atWarning().withCause(ioe).log(
            "Failed read retry #%d/%d for '%s'. Sleeping...",
            retriesAttempted, maxRetries, resourceId);
        try {
          boolean backOffSuccessful = BackOffUtils.next(sleeper, readBackOff.get());
          if (!backOffSuccessful) {
            GoogleCloudStorageEventBus.postOnException();
            logger.atSevere().log(
                "BackOff returned false; maximum total elapsed time exhausted."
                    + " Giving up after %d/%d retries for '%s'",
                retriesAttempted, maxRetries, resourceId);
            throw ioe;
          }
        } catch (InterruptedException ie) {
          GoogleCloudStorageEventBus.postOnException();
          Thread.currentThread().interrupt();
          logger.atSevere().log(
              "Interrupted while sleeping before retry. Giving up after %d/%d retries for '%s'",
              retriesAttempted, maxRetries, resourceId);
          ioe.addSuppressed(ie);
          throw ioe;
        }

        logger.atInfo().log(
            "Done sleeping before retry #%d/%d for '%s'", retriesAttempted, maxRetries, resourceId);
      } catch (RuntimeException r) {
        GoogleCloudStorageEventBus.postOnException();
        closeContentChannel();
        throw r;
      }
    } while (buffer.remaining() > 0 && currentPosition < size);

    // If this method was called when the stream was already at EOF
    // (indicated by totalBytesRead == 0) then return EOF else,
    // return the number of bytes read.
    boolean isEndOfStream = (totalBytesRead == 0);
    if (isEndOfStream) {
      // Check that we didn't get a premature End of Stream signal by checking the number of
      // bytes
      // read against the stream size. Unfortunately we don't have information about the
      // actual size
      // of the data stream when stream compression is used, so we can only ignore this case
      // here.
      if (currentPosition != size) {
        GoogleCloudStorageEventBus.postOnException();
        throw new IOException(
            String.format(
                "Failed to read any data before all the file data has been received;"
                    + " currentPosition: %d, size: %d, object '%s'",
                currentPosition, size, resourceId));
      }
      return -1;
    }
    return totalBytesRead;
  }

  @Override
  public SeekableByteChannel truncate(long size) {
    throw new UnsupportedOperationException("Cannot mutate read-only channel");
  }

  @Override
  public int write(ByteBuffer src) {
    throw new UnsupportedOperationException("Cannot mutate read-only channel");
  }

  /**
   * Tells whether this channel is open.
   *
   * @return a value indicating whether this channel is open
   */
  @Override
  public boolean isOpen() {
    return channelIsOpen;
  }

  /**
   * Closes the underlying {@link ReadableByteChannel}.
   *
   * <p>Catches and ignores all exceptions as there is not a lot the user can do to fix errors here
   * and a new connection will be needed. Especially SSLExceptions since the there's a high
   * probability that SSL connections would be broken in a way that causes {@link Channel#close()}
   * itself to throw an exception, even though underlying sockets have already been cleaned up;
   * close() on an SSLSocketImpl requires a shutdown handshake in order to shut down cleanly, and if
   * the connection has been broken already, then this is not possible, and the SSLSocketImpl was
   * already responsible for performing local cleanup at the time the exception was raised.
   */
  protected void closeContentChannel() {
    if (contentChannel != null) {
      logger.atFiner().log("Closing internal contentChannel for '%s'", resourceId);
      try {
        contentChannel.close();
      } catch (Exception e) {
        GoogleCloudStorageEventBus.postOnException();
        logger.atFine().withCause(e).log(
            "Got an exception on contentChannel.close() for '%s'; ignoring it.", resourceId);
      } finally {
        contentChannel = null;
        resetContentChannel();
      }
    }
  }

  private void resetContentChannel() {
    checkState(contentChannel == null, "contentChannel should be null for '%s'", resourceId);
    contentChannelPosition = -1;
    contentChannelEnd = -1;
  }

  /** Closes this channel. */
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

  /**
   * Returns this channel's current position.
   *
   * @return this channel's current position
   */
  @Override
  public long position() throws IOException {
    throwIfNotOpen();
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
  @CanIgnoreReturnValue
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

  private boolean isRandomAccessPattern(long oldPosition) {
    if (!shouldDetectRandomAccess()) {
      return false;
    }
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

  private boolean shouldDetectRandomAccess() {
    return !gzipEncoded && !randomAccess && readOptions.getFadvise() == Fadvise.AUTO;
  }

  private void setRandomAccess() {
    randomAccess = true;
    checkEncodingAndAccess();
  }

  private void skipInPlace(long seekDistance) {
    if (skipBuffer == null) {
      skipBuffer = new byte[SKIP_BUFFER_SIZE];
    }
    while (seekDistance > 0 && contentChannel != null) {
      try {
        int bufferSize = toIntExact(min(skipBuffer.length, seekDistance));
        int bytesRead = contentChannel.read(ByteBuffer.wrap(skipBuffer, 0, bufferSize));
        if (bytesRead < 0) {
          // Shouldn't happen since we called validatePosition prior to this loop.
          logger.atInfo().log(
              "Somehow read %d bytes trying to skip %d bytes to seek to position %d, size: %d",
              bytesRead, seekDistance, currentPosition, size);
          closeContentChannel();
        } else {
          seekDistance -= bytesRead;
          contentChannelPosition += bytesRead;
        }
      } catch (IOException e) {
        GoogleCloudStorageEventBus.postOnException();
        logger.atInfo().withCause(e).log(
            "Got an IO exception on contentChannel.read(), a lazy-seek will be pending for '%s'",
            resourceId);
        closeContentChannel();
      }
    }
    checkState(
        contentChannel == null || contentChannelPosition == currentPosition,
        "contentChannelPosition (%s) should be equal to currentPosition (%s)"
            + " after successful in-place skip",
        contentChannelPosition,
        currentPosition);
  }

  /**
   * Returns size of the object to which this channel is connected.
   *
   * <p>Note: this method will return -1 until metadata will be lazily initialized during first
   * {@link #read} method call.
   *
   * @return size of the object to which this channel is connected after metadata was initialized
   *     (during first read) or {@code -1} otherwise.
   * @throws IOException on IO error
   */
  @Override
  public long size() throws IOException {
    throwIfNotOpen();
    if (!metadataInitialized) {
      initMetadata(fetchInitialMetadata());
    }
    return size;
  }

  @VisibleForTesting
  long generation() {
    return resourceId.getGenerationId();
  }

  /** Sets size of this channel to the given value. */
  // TODO(b/120887495): This @VisibleForTesting annotation was being ignored by prod code.
  // Please check that removing it is correct, and remove this comment along with it.
  // @VisibleForTesting
  protected void setSize(long size) {
    this.size = size;
  }

  private void checkEncodingAndAccess() {
    checkState(
        !(gzipEncoded && randomAccess),
        "gzipEncoded and randomAccess should not be true at the same time for '%s'",
        resourceId);
  }

  /** Validates that the given position is valid for this channel. */
  protected void validatePosition(long position) throws IOException {
    if (position < 0) {
      GoogleCloudStorageEventBus.postOnException();
      throw new EOFException(
          String.format(
              "Invalid seek offset: position value (%d) must be >= 0 for '%s'",
              position, resourceId));
    }

    if (size >= 0 && position >= size) {
      GoogleCloudStorageEventBus.postOnException();
      throw new EOFException(
          String.format(
              "Invalid seek offset: position value (%d) must be between 0 and %d for '%s'",
              position, size, resourceId));
    }
  }

  /**
   * Seeks to the {@link #currentPosition} in the underlying stream or opens new stream at {@link
   * #currentPosition}.
   *
   * <p>Note: Seek could be an expensive operation if a new stream is opened.
   *
   * @param bytesToRead number of bytes to read, used only if new stream is opened.
   * @throws FileNotFoundException if the underlying object does not exist.
   * @throws IOException on IO error
   */
  @VisibleForTesting
  void performLazySeek(long bytesToRead) throws IOException {
    throwIfNotOpen();

    // Return quickly if there is no pending seek operation, i.e. position didn't change.
    if (currentPosition == contentChannelPosition && contentChannel != null) {
      return;
    }

    logger.atFiner().log(
        "Performing lazySeek from %s to %s position with %s bytesToRead for '%s'",
        contentChannelPosition, currentPosition, bytesToRead, resourceId);

    // used to auto-detect random access
    long oldPosition = contentChannelPosition;

    long seekDistance = currentPosition - contentChannelPosition;
    if (contentChannel != null
        && seekDistance > 0
        // Always skip in place gzip-encoded files, because they do not support range reads.
        && (gzipEncoded || seekDistance <= readOptions.getInplaceSeekLimit())
        && currentPosition < contentChannelEnd) {
      logger.atFiner().log(
          "Seeking forward %d bytes (inplaceSeekLimit: %d) in-place to position %d for '%s'",
          seekDistance, readOptions.getInplaceSeekLimit(), currentPosition, resourceId);
      skipInPlace(seekDistance);
    } else {
      closeContentChannel();
    }

    if (contentChannel == null) {
      if (isRandomAccessPattern(oldPosition)) {
        setRandomAccess();
      }
      openContentChannel(bytesToRead);
    }
  }

  private void openContentChannel(long bytesToRead) throws IOException {
    checkState(
        contentChannel == null,
        "contentChannel should be null, before opening new for '%s'",
        resourceId);
    InputStream objectContentStream =
        footerContent != null && currentPosition >= size - footerContent.length
            ? openFooterStream()
            : openStream(bytesToRead);
    contentChannel = Channels.newChannel(objectContentStream);
    checkState(
        contentChannelPosition == currentPosition,
        "contentChannelPosition (%s) should be equal to currentPosition (%s) for '%s'",
        contentChannelPosition,
        currentPosition,
        resourceId);
  }

  /* Initializes metadata (size, encoding, etc) from {@link GoogleCloudStorageItemInfo} */
  private void initMetadata(GoogleCloudStorageItemInfo info) throws IOException {
    initMetadata(info.getContentEncoding(), info.getSize(), info.getContentGeneration());
  }

  /**
   * Initializes metadata (size, encoding, etc) from HTTP {@code headers}. Used for lazy
   * initialization when fail fast is disabled.
   */
  @VisibleForTesting
  protected void initMetadata(HttpHeaders headers) throws IOException {
    checkState(
        !metadataInitialized,
        "Cannot initialize metadata, it already initialized for '%s'",
        resourceId);

    String generationString = headers.getFirstHeaderStringValue("x-goog-generation");
    if (generationString == null) {
      GoogleCloudStorageEventBus.postOnException();
      throw new IOException(String.format("Failed to retrieve generation for '%s'", resourceId));
    }
    long generation = Long.parseLong(generationString);

    String range = headers.getContentRange();
    long sizeFromMetadata =
        range == null
            ? headers.getContentLength()
            : Long.parseLong(range.substring(range.lastIndexOf('/') + 1));
    initMetadata(headers.getContentEncoding(), sizeFromMetadata, generation);
  }

  /** Initializes metadata (size, encoding, etc) from passed parameters. */
  @VisibleForTesting
  protected void initMetadata(@Nullable String encoding, long sizeFromMetadata, long generation)
      throws IOException {
    checkState(
        !metadataInitialized,
        "Cannot initialize metadata, it already initialized for '%s'",
        resourceId);
    checkState(
        generation != StorageResourceId.UNKNOWN_GENERATION_ID,
        "Generation parameter of %s is invalid for resourceId of '%s'",
        generation,
        resourceId);
    gzipEncoded = nullToEmpty(encoding).contains(GZIP_ENCODING);
    if (gzipEncoded && !readOptions.isGzipEncodingSupportEnabled()) {
      GoogleCloudStorageEventBus.postOnException();
      throw new IOException(
          "Cannot read GZIP encoded files - content encoding support is disabled.");
    }
    size = gzipEncoded ? Long.MAX_VALUE : sizeFromMetadata;
    randomAccess = !gzipEncoded && readOptions.getFadvise() == Fadvise.RANDOM;
    checkEncodingAndAccess();

    if (resourceId.hasGenerationId()) {
      checkState(
          resourceId.getGenerationId() == generation,
          "Provided generation (%s) should be equal to fetched generation (%s) for '%s'",
          resourceId.getGenerationId(),
          generation,
          resourceId);
    } else {
      resourceId =
          new StorageResourceId(resourceId.getBucketName(), resourceId.getObjectName(), generation);
    }

    metadataInitialized = true;

    logger.atFiner().log(
        "Initialized metadata (gzipEncoded=%s, size=%s, randomAccess=%s, generation=%s) for '%s'",
        gzipEncoded, size, randomAccess, resourceId.getGenerationId(), resourceId);
  }

  private void cacheFooter(HttpResponse response) throws IOException {
    checkState(size > 0, "size should be greater than 0 for '%s'", resourceId);
    int footerSize = toIntExact(response.getHeaders().getContentLength());
    footerContent = new byte[footerSize];
    try (InputStream footerStream = response.getContent()) {
      int totalBytesRead = 0;
      int bytesRead = 0;
      do {
        totalBytesRead += bytesRead;
        bytesRead = footerStream.read(footerContent, totalBytesRead, footerSize - totalBytesRead);
      } while (bytesRead >= 0 && totalBytesRead < footerSize);
      checkState(
          footerStream.read() < 0,
          "footerStream should be empty after reading %s bytes from %s bytes for '%s'",
          totalBytesRead,
          footerSize,
          resourceId);
      checkState(
          totalBytesRead == footerSize,
          "totalBytesRead (%s) should equal footerSize (%s) for '%s'",
          totalBytesRead,
          footerSize,
          resourceId);
    } catch (IOException e) {
      GoogleCloudStorageEventBus.postOnException();
      footerContent = null;
      throw e;
    }
    logger.atFiner().log("Prefetched %s bytes footer for '%s'", footerContent.length, resourceId);
  }

  /**
   * Opens the underlying stream from {@link #footerContent}, sets its position to the {@link
   * #currentPosition}.
   */
  private InputStream openFooterStream() {
    contentChannelPosition = currentPosition;
    int offset = toIntExact(currentPosition - (size - footerContent.length));
    int length = footerContent.length - offset;
    logger.atFiner().log(
        "Opened stream (prefetched footer) from %d position for '%s'", currentPosition, resourceId);
    return new ByteArrayInputStream(footerContent, offset, length);
  }

  /**
   * Opens the underlying stream, sets its position to the {@link #currentPosition}.
   *
   * <p>If the file encoding in GCS is gzip (and therefore the HTTP client will decompress it), the
   * entire file is always requested, and we seek to the position requested. If the file encoding is
   * not gzip, only the remaining bytes to be read are requested from GCS.
   *
   * @param bytesToRead number of bytes to read from new stream. Ignored if {@link
   *     GoogleCloudStorageReadOptions#getFadvise()} is equal to {@link Fadvise#SEQUENTIAL}.
   * @throws IOException on IO error
   */
  protected InputStream openStream(long bytesToRead) throws IOException {
    checkArgument(bytesToRead > 0, "bytesToRead should be greater than 0, but was %s", bytesToRead);

    checkState(
        contentChannel == null && contentChannelEnd < 0,
        "contentChannel and contentChannelEnd should be not initialized yet for '%s'",
        resourceId);

    if (size == 0) {
      return new ByteArrayInputStream(new byte[0]);
    }

    String rangeHeader;
    if (readOptions.isReadExactRequestedBytesEnabled() && !gzipEncoded) {
      contentChannelPosition = currentPosition;
      contentChannelEnd = contentChannelPosition + bytesToRead;
      rangeHeader = "bytes=" + contentChannelPosition + "-" + (contentChannelEnd - 1);
    } else if (!metadataInitialized) {
      contentChannelPosition = getContentChannelPositionForFirstRead(bytesToRead);
      rangeHeader = "bytes=" + contentChannelPosition + "-";
      if (readOptions.getFadvise() == Fadvise.RANDOM) {
        long maxBytesToRead = Math.max(readOptions.getMinRangeRequestSize(), bytesToRead);
        rangeHeader += (contentChannelPosition + maxBytesToRead - 1);
      }
    } else if (gzipEncoded) {
      // Do not set range for gzip-encoded files - it's not supported.
      rangeHeader = null;
      // Always read gzip-encoded files till the end - they do not support range reads.
      contentChannelPosition = 0;
      contentChannelEnd = size;
    } else {
      contentChannelPosition =
          readOptions.getFadvise() != Fadvise.SEQUENTIAL && isFooterRead()
              // Pre-fetch footer if reading end of file.
              ? Math.max(0, size - readOptions.getMinRangeRequestSize())
              : currentPosition;

      // Set rangeSize to the size of the file reminder from currentPosition.
      long rangeSize = size - contentChannelPosition;
      if (randomAccess) {
        long randomRangeSize = Math.max(bytesToRead, readOptions.getMinRangeRequestSize());
        // Limit rangeSize to the randomRangeSize.
        rangeSize = min(randomRangeSize, rangeSize);
      }

      contentChannelEnd = contentChannelPosition + rangeSize;
      // Do not read footer again, if it was already pre-fetched.
      if (footerContent != null) {
        contentChannelEnd = min(contentChannelEnd, size - footerContent.length);
      }

      checkState(
          currentPosition < contentChannelEnd,
          "currentPosition (%s) should be less than contentChannelEnd (%s) for '%s'",
          currentPosition,
          contentChannelEnd,
          resourceId);
      checkState(
          contentChannelPosition <= currentPosition,
          "contentChannelPosition (%s) should be less or equal to currentPosition (%s) for '%s'",
          contentChannelPosition,
          currentPosition,
          resourceId);

      rangeHeader = "bytes=" + contentChannelPosition + "-";
      if (randomAccess || contentChannelEnd != size) {
        rangeHeader += (contentChannelEnd - 1);
      }
    }
    checkState(
        !metadataInitialized || contentChannelEnd > 0,
        "contentChannelEnd should be initialized already for '%s'",
        resourceId);

    Storage.Objects.Get getObject = createDataRequest(rangeHeader);
    HttpResponse response;
    try {
      response = getObject.executeMedia();
      // TODO(b/110832992): validate response range header against expected/request range
    } catch (IOException e) {
      if (!metadataInitialized && errorExtractor.rangeNotSatisfiable(e) && currentPosition == 0) {
        GoogleCloudStorageEventBus.postOnException();
        // We don't know the size yet (metadataInitialized == false) and we're seeking to
        // byte 0, but got 'range not satisfiable'; the object must be empty.
        logger.atInfo().log(
            "Got 'range not satisfiable' for reading '%s' at position 0; assuming empty.",
            resourceId);
        size = 0;
        return new ByteArrayInputStream(new byte[0]);
      }
      response = handleExecuteMediaException(e);
    }

    if (!metadataInitialized) {
      initMetadata(response.getHeaders());
      checkState(
          metadataInitialized, "metadata should be initialized already for '%s'", resourceId);
      if (size == 0) {
        resetContentChannel();
        return new ByteArrayInputStream(new byte[0]);
      }
      if (gzipEncoded) {
        // Initialize `contentChannelEnd` to `size` (initialized to Long.MAX_VALUE
        // in `initMetadata` method for gzipped objects) because value of HTTP Content-Length
        // header is usually smaller than decompressed object size.
        if (currentPosition == 0) {
          contentChannelEnd = size;
        } else {
          resetContentChannel();
          return openStream(bytesToRead);
        }
      }
    }

    if (contentChannelEnd < 0) {
      String contentRange = response.getHeaders().getContentRange();
      if (contentRange != null) {
        String contentEnd =
            contentRange.substring(
                contentRange.lastIndexOf('-') + 1, contentRange.lastIndexOf('/'));
        contentChannelEnd = Long.parseLong(contentEnd) + 1;
      } else {
        contentChannelEnd = response.getHeaders().getContentLength();
      }
    }

    checkState(
        contentChannelEnd > 0,
        "contentChannelEnd should be initialized already for '%s'",
        resourceId);

    if (!gzipEncoded
        && readOptions.getFadvise() != Fadvise.SEQUENTIAL
        && contentChannelEnd == size
        && contentChannelEnd - contentChannelPosition <= readOptions.getMinRangeRequestSize()) {
      for (int retriesCount = 0; retriesCount < maxRetries; retriesCount++) {
        try {
          cacheFooter(response);
          if (retriesCount != 0) {
            logger.atInfo().log(
                "Successfully cached footer after %d retries for '%s'", retriesCount, resourceId);
          }
          break;
        } catch (IOException footerException) {
          GoogleCloudStorageEventBus.postOnException();
          logger.atInfo().withCause(footerException).log(
              "Failed to prefetch footer (retry #%d/%d) for '%s'",
              retriesCount + 1, maxRetries, resourceId);
          if (retriesCount == 0) {
            readBackOff.get().reset();
          }
          if (retriesCount == maxRetries) {
            resetContentChannel();
            throw footerException;
          }
          try {
            response = getObject.executeMedia();
            // TODO(b/110832992): validate response range header against
            // expected/request range.
          } catch (IOException e) {
            response = handleExecuteMediaException(e);
          }
        }
      }
      checkState(
          footerContent != null,
          "footerContent should not be null after successful footer prefetch for '%s'",
          resourceId);
      resetContentChannel();
      return openFooterStream();
    }

    try {
      InputStream contentStream = response.getContent();
      logger.atFiner().log(
          "Opened stream from %d position with %s range and %d bytesToRead for '%s'",
          currentPosition, rangeHeader, bytesToRead, resourceId);

      if (contentChannelPosition < currentPosition) {
        long bytesToSkip = currentPosition - contentChannelPosition;
        logger.atFiner().log(
            "Skipping %d bytes from %d position to %d position for '%s'",
            bytesToSkip, contentChannelPosition, currentPosition, resourceId);
        while (bytesToSkip > 0) {
          long skippedBytes = contentStream.skip(bytesToSkip);
          logger.atFiner().log(
              "Skipped %d bytes from %d position for '%s'",
              skippedBytes, contentChannelPosition, resourceId);
          bytesToSkip -= skippedBytes;
          contentChannelPosition += skippedBytes;
        }
      }

      checkState(
          contentChannelPosition == currentPosition,
          "contentChannelPosition (%s) should be equal to currentPosition (%s) for '%s'",
          contentChannelPosition,
          currentPosition,
          resourceId);

      return contentStream;
    } catch (IOException e) {
      GoogleCloudStorageEventBus.postOnException();
      try {
        response.disconnect();
      } catch (IOException closeException) {
        e.addSuppressed(closeException);
      }
      throw e;
    }
  }

  private boolean isFooterRead() {
    return size - currentPosition <= readOptions.getMinRangeRequestSize();
  }

  private long getContentChannelPositionForFirstRead(long bytesToRead) {
    if (readOptions.getFadvise() == Fadvise.SEQUENTIAL
        || bytesToRead >= readOptions.getMinRangeRequestSize()) {
      return currentPosition;
    }
    // Prefetch footer (bytes before 'currentPosition' in case of last byte read) lazily.
    // Max prefetch size is (minRangeRequestSize / 2) bytes.
    if (bytesToRead <= readOptions.getMinRangeRequestSize() / 2) {
      return Math.max(0, currentPosition - readOptions.getMinRangeRequestSize() / 2);
    }
    return Math.max(0, currentPosition - (readOptions.getMinRangeRequestSize() - bytesToRead));
  }

  /**
   * When an IOException is thrown, depending on if the exception is caused by non-existent object
   * generation, and depending on the generation read consistency setting, either retry the read (of
   * the latest generation), or handle the exception directly.
   *
   * @param e IOException thrown while reading from GCS.
   * @return the HttpResponse of reading from GCS from possible retry.
   * @throws IOException either error on retry, or thrown because the original read encounters
   *     error.
   */
  private HttpResponse handleExecuteMediaException(IOException e) throws IOException {
    if (errorExtractor.itemNotFound(e)) {
      GoogleCloudStorageEventBus.postOnException();
      throw createFileNotFoundException(resourceId, e);
    }
    String msg = String.format("Error reading '%s' at position %d", resourceId, currentPosition);
    if (errorExtractor.rangeNotSatisfiable(e)) {
      GoogleCloudStorageEventBus.postOnException();
      throw (EOFException) new EOFException(msg).initCause(e);
    }
    throw new IOException(msg, e);
  }

  private Storage.Objects.Get createDataRequest(String rangeHeader) throws IOException {
    Storage.Objects.Get dataRequest = createDataRequest();

    // Set the headers on the existing request headers that may have
    // been initialized with things like user-agent already.
    HttpHeaders requestHeaders = clientRequestHelper.getRequestHeaders(dataRequest);
    // Disable GCS decompressive transcoding.
    requestHeaders.setAcceptEncoding("gzip");
    requestHeaders.setRange(rangeHeader);

    return dataRequest;
  }

  protected Storage.Objects.Get createDataRequest() throws IOException {
    checkState(
        !metadataInitialized || resourceId.hasGenerationId(),
        "Generation should always be included for resource '%s'",
        resourceId);
    // Start with unset generation and determine what to ask for based on read consistency.
    Storage.Objects.Get getData =
        storageRequestFactory.objectsGetData(
            resourceId.getBucketName(), resourceId.getObjectName());
    if (resourceId.hasGenerationId()) {
      getData.setGeneration(resourceId.getGenerationId());
    }
    return getData;
  }

  protected Storage.Objects.Get createMetadataRequest() throws IOException {
    checkState(
        !metadataInitialized || resourceId.hasGenerationId(),
        "Generation should always be included for resource '%s'",
        resourceId);
    // Start with unset generation and determine what to ask for based on read consistency.
    Storage.Objects.Get getMetadata =
        storageRequestFactory.objectsGetMetadata(
            resourceId.getBucketName(), resourceId.getObjectName());
    if (resourceId.hasGenerationId()) {
      getMetadata.setGeneration(resourceId.getGenerationId());
    }
    return getMetadata;
  }

  /** Throws if this channel is not currently open. */
  private void throwIfNotOpen() throws IOException {
    if (!isOpen()) {
      GoogleCloudStorageEventBus.postOnException();
      throw new ClosedChannelException();
    }
  }
}
