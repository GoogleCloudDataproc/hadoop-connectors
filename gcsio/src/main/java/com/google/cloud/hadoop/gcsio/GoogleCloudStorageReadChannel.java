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

package com.google.cloud.hadoop.gcsio;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.nullToEmpty;

import com.google.api.client.http.HttpResponse;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.BackOffUtils;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.NanoClock;
import com.google.api.client.util.Sleeper;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.cloud.hadoop.util.ClientRequestHelper;
import com.google.cloud.hadoop.util.ResilientOperation;
import com.google.cloud.hadoop.util.RetryBoundedBackOff;
import com.google.cloud.hadoop.util.RetryDeterminer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.time.Duration;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Provides seekable read access to GCS. */
public class GoogleCloudStorageReadChannel implements SeekableByteChannel {

  // Defaults kept here for legacy compatibility; see GoogleCloudStorageReadOptions for details.
  public static final int DEFAULT_BACKOFF_INITIAL_INTERVAL_MILLIS =
      GoogleCloudStorageReadOptions.DEFAULT_BACKOFF_INITIAL_INTERVAL_MILLIS;
  public static final double DEFAULT_BACKOFF_RANDOMIZATION_FACTOR =
      GoogleCloudStorageReadOptions.DEFAULT_BACKOFF_RANDOMIZATION_FACTOR;
  public static final double DEFAULT_BACKOFF_MULTIPLIER =
      GoogleCloudStorageReadOptions.DEFAULT_BACKOFF_MULTIPLIER;
  public static final int DEFAULT_BACKOFF_MAX_INTERVAL_MILLIS =
      GoogleCloudStorageReadOptions.DEFAULT_BACKOFF_MAX_INTERVAL_MILLIS;
  public static final int DEFAULT_BACKOFF_MAX_ELAPSED_TIME_MILLIS =
      GoogleCloudStorageReadOptions.DEFAULT_BACKOFF_MAX_ELAPSED_TIME_MILLIS;

  // Logger.
  private static final Logger LOG = LoggerFactory.getLogger(GoogleCloudStorageReadChannel.class);

  /** Supported GCS file encodings */
  @VisibleForTesting
  protected static enum FileEncoding {
    UNINITIALIZED,
    GZIPPED,
    OTHER
  }

  // Size of buffer to allocate for skipping bytes in-place when performing in-place seeks.
  @VisibleForTesting static final int SKIP_BUFFER_SIZE = 8192;

  @VisibleForTesting static final Cache<String, Boolean> randomAccessObjects =
      CacheBuilder.newBuilder().maximumSize(1_000).expireAfterAccess(Duration.ofHours(1)).build();

  // GCS access instance.
  private final Storage gcs;

  // Name of the bucket containing the object being read.
  private final String bucketName;

  // Name of the object being read.
  private final String objectName;

  // GCS resource/object path, used for logging.
  private final String resourceIdString;

  // Read channel.
  @VisibleForTesting ReadableByteChannel readChannel;

  // Whether to use range requests instead of streaming requests
  private boolean randomAccess;

  // True if this channel is open, false otherwise.
  private boolean channelIsOpen;

  // Current read position in the readChannel.
  //
  // When a caller calls position(long) to set stream position, we record the target position
  // and defer the actual seek operation until the caller tries to read from the channel.
  // This allows us to avoid an unnecessary seek to position 0 that would take place on creation
  // of this instance in cases where caller intends to start reading at some other offset.
  // If readChannelPosition is not the same as currentPosition, it indicates that a target position
  // has been set but the actual seek operation is still pending.
  private long readChannelPosition = -1;

  // Current position in this channel, it could be different from `readChannelPosition`
  // if several `position` method calls were made without clalls to `read` method.
  @VisibleForTesting protected long currentPosition;

  // Size of the object being read.
  private long size = -1;

  // Size of the readChannel.
  private long readChannelEnd = -1;

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

  // The clock used by ExponentialBackOff to determine when the maximum total elapsed time has
  // passed doing a series of retries.
  private NanoClock clock = NanoClock.SYSTEM;

  // Lazily initialized BackOff for sleeping between retries; only ever initialized if a retry is
  // necessary.
  private Supplier<BackOff> backOff = Suppliers.memoize(this::createBackOff);

  // read operation gets its own Exponential Backoff Strategy,
  // to avoid interference with other operations in nested retries.
  private Supplier<BackOff> readBackOff = Suppliers.memoize(this::createBackOff);

  // Used as scratch space when reading bytes just to discard them when trying to perform small
  // in-place seeks.
  private byte[] skipBuffer = null;

  // For files that have Content-Encoding: gzip set in the file metadata, the size of the response
  // from GCS is the size of the compressed file. However, the HTTP client wraps the content
  // in a GZIPInputStream, so the number of bytes that can be read from the stream may be greater
  // than the size of the response. In this case, we allow the position in the stream to be
  // greater than the size of the response during the validation of the position.
  private FileEncoding fileEncoding = FileEncoding.UNINITIALIZED;

  /**
   * Constructs an instance of GoogleCloudStorageReadChannel.
   *
   * @param gcs storage object instance
   * @param bucketName name of the bucket containing the object to read
   * @param objectName name of the object to read
   * @param requestHelper a ClientRequestHelper used to set any extra headers
   * @throws java.io.FileNotFoundException if the given object does not exist
   * @throws IOException on IO error
   */
  public GoogleCloudStorageReadChannel(
      Storage gcs,
      String bucketName,
      String objectName,
      ApiErrorExtractor errorExtractor,
      ClientRequestHelper<StorageObject> requestHelper)
      throws IOException {
    this(
        gcs,
        bucketName,
        objectName,
        errorExtractor,
        requestHelper,
        GoogleCloudStorageReadOptions.DEFAULT);
  }

  /**
   * Constructs an instance of GoogleCloudStorageReadChannel.
   *
   * @param gcs storage object instance
   * @param bucketName name of the bucket containing the object to read
   * @param objectName name of the object to read
   * @param requestHelper a ClientRequestHelper used to set any extra headers
   * @param readOptions fine-grained options specifying things like retry settings, buffering, etc.
   *     Could not be null.
   * @throws java.io.FileNotFoundException if the given object does not exist
   * @throws IOException on IO error
   */
  public GoogleCloudStorageReadChannel(
      Storage gcs,
      String bucketName,
      String objectName,
      ApiErrorExtractor errorExtractor,
      ClientRequestHelper<StorageObject> requestHelper,
      @Nonnull GoogleCloudStorageReadOptions readOptions)
      throws IOException {
    this.gcs = gcs;
    this.clientRequestHelper = requestHelper;
    this.bucketName = bucketName;
    this.objectName = objectName;
    this.errorExtractor = errorExtractor;
    this.readOptions = readOptions;
    this.channelIsOpen = true;
    this.currentPosition = 0;
    this.resourceIdString = StorageResourceId.createReadableString(bucketName, objectName);
    if (!readOptions.getSupportContentEncoding()) {
      // If we don't need to support nonstandard content-encodings, we'll just proceed assuming
      // the content-encoding is the standard/safe FileEncoding.OTHER.
      this.fileEncoding = FileEncoding.OTHER;
    }
    initFileEncodingAndSize();
    // access `randomAccessObjects` cache after `initFileEncodingAndSize()` method call,
    // because it could throw an exception - in this case we don't want to "refresh"
    // access time to this object in `randomAccessObjects` LRU cache
    this.randomAccess = randomAccessObjects.getIfPresent(resourceIdString) != null;
    LOG.debug(
        "Created and initialized new channel (encoding={}, size={}, randomAccess={}) for '{}'",
        fileEncoding, size, randomAccess, resourceIdString);
  }

  /**
   * Used for unit testing only. Do not use elsewhere.
   *
   * <p>Constructs an instance of GoogleCloudStorageReadChannel.
   *
   * @param readOptions fine-grained options specifying things like retry settings, buffering, etc.
   *     Could not be null.
   * @throws IOException on IO error
   */
  @VisibleForTesting
  protected GoogleCloudStorageReadChannel(@Nonnull GoogleCloudStorageReadOptions readOptions)
      throws IOException {
    this(
        /* gcs= */ null,
        /* bucketName= */ null,
        /* objectName= */ null,
        /* errorExtractor= */ null,
        /* requestHelper= */ null,
        readOptions);
  }

  /** Sets the Sleeper used for sleeping between retries. */
  @VisibleForTesting
  void setSleeper(Sleeper sleeper) {
    Preconditions.checkArgument(sleeper != null, "sleeper must not be null!");
    this.sleeper = sleeper;
  }

  /** Sets the clock to be used for determining when max total time has elapsed doing retries. */
  @VisibleForTesting
  void setNanoClock(NanoClock clock) {
    Preconditions.checkArgument(clock != null, "clock must not be null!");
    this.clock = clock;
  }

  /**
   * Sets the backoff for determining sleep duration between retries.
   *
   * @param backOff May be null to force the next usage to auto-initialize with default settings.
   */
  @VisibleForTesting
  void setBackOff(BackOff backOff) {
    this.backOff = Suppliers.ofInstance(checkNotNull(backOff, "backOff could not be null"));
  }

  /**
   * Sets the backoff for determining sleep duration between read retries.
   *
   * @param backOff May be null to force the next usage to auto-initialize with default settings.
   */
  void setReadBackOff(BackOff backOff) {
    this.readBackOff = Suppliers.ofInstance(checkNotNull(backOff, "backOff could not be null"));
  }

  /**
   * Gets the backoff used for determining sleep duration between retries. May be null if it was
   * never lazily initialized.
   */
  @VisibleForTesting
  BackOff getBackOff() {
    return backOff.get();
  }

  /**
   * Gets the backoff used for determining sleep duration between read retries. May be null if it
   * was never lazily initialized.
   */
  @VisibleForTesting
  BackOff getReadBackOff() {
    return readBackOff.get();
  }

  /** Creates new generic BackOff used for retries. */
  @VisibleForTesting
  ExponentialBackOff createBackOff() {
    return new ExponentialBackOff.Builder()
        .setInitialIntervalMillis(readOptions.getBackoffInitialIntervalMillis())
        .setRandomizationFactor(readOptions.getBackoffRandomizationFactor())
        .setMultiplier(readOptions.getBackoffMultiplier())
        .setMaxIntervalMillis(readOptions.getBackoffMaxIntervalMillis())
        .setMaxElapsedTimeMillis(readOptions.getBackoffMaxElapsedTimeMillis())
        .setNanoClock(clock)
        .build();
  }

  /**
   * Sets the number of times to automatically retry by re-opening the underlying readChannel
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

    LOG.debug(
        "Reading {} bytes at {} position from '{}'",
        buffer.remaining(), currentPosition, resourceIdString);

    if (fileEncoding != FileEncoding.GZIPPED && currentPosition == size) {
      return -1;
    }

    int totalBytesRead = 0;
    int retriesAttempted = 0;

    // We read from a streaming source. We may not get all the bytes we asked for
    // in the first read. Therefore, loop till we either read the required number of
    // bytes or we reach end-of-stream.
    do {
      performLazySeek(buffer.remaining());
      checkState(
          readChannelPosition == currentPosition,
          "readChannelPosition (%s) should be equal to currentPosition (%s) after lazy seek",
          readChannelPosition, currentPosition);

      int remainingBeforeRead = buffer.remaining();
      try {
        int numBytesRead = readChannel.read(buffer);
        checkIOPrecondition(numBytesRead != 0, "Read 0 bytes without blocking");
        if (numBytesRead < 0) {
          // Check that we didn't get a premature End of Stream signal by checking the number of
          // bytes read against the stream size. Unfortunately we don't have information about the
          // actual size of the data stream when stream compression is used, so we can only ignore
          // this case here.
          checkIOPrecondition(
              fileEncoding == FileEncoding.GZIPPED
                  || currentPosition == readChannelEnd
                  || currentPosition == size,
              String.format(
                  "Received end of stream result before all the file data has been received; "
                      + "totalBytesRead: %d, currentPosition: %d,"
                      + " readChannelEnd %d, size: %d, object: '%s'",
                  totalBytesRead, currentPosition, readChannelEnd, size, resourceIdString));
          // reached an end of the readChannel in randomAccess mode,
          // need to close it so new readChannel will be opened on next iteration
          if (readChannelEnd != size && currentPosition == readChannelEnd) {
            closeReadChannel();
          } else {
            break;
          }
        }

        if (numBytesRead > 0) {
          totalBytesRead += numBytesRead;
          currentPosition += numBytesRead;
          readChannelPosition += numBytesRead;
        }

        if (retriesAttempted != 0) {
          LOG.info("Success after {} retries on reading '{}'", retriesAttempted, resourceIdString);
        }
        // The count of retriesAttempted is per low-level readChannel.read call; each time we make
        // progress we reset the retry counter.
        retriesAttempted = 0;
      } catch (IOException ioe) {
        // TODO(user): Refactor any reusable logic for retries into a separate RetryHelper class.
        if (retriesAttempted == maxRetries) {
          LOG.error(
              "Already attempted max of {} retries while reading '{}'; throwing exception.",
              maxRetries, resourceIdString);
          closeReadChannel();
          throw ioe;
        } else {
          if (retriesAttempted == 0) {
            // If this is the first of a series of retries, we also want to reset the backOff
            // to have fresh initial values.
            readBackOff.get().reset();
          }

          ++retriesAttempted;
          LOG.warn(
              "Got exception: {} while reading '{}'; retry #{}. Sleeping...",
              ioe.getMessage(), resourceIdString, retriesAttempted);
          try {
            boolean backOffSuccessful = BackOffUtils.next(sleeper, readBackOff.get());
            if (!backOffSuccessful) {
              LOG.error(
                  "BackOff returned false; maximum total elapsed time exhausted."
                      + " Giving up after {} retries for '{}'",
                  retriesAttempted, resourceIdString);
              closeReadChannel();
              throw ioe;
            }
          } catch (InterruptedException ie) {
            LOG.error(
                "Interrupted while sleeping before retry. Giving up after {} retries for '{}'",
                retriesAttempted, resourceIdString);
            ioe.addSuppressed(ie);
            closeReadChannel();
            throw ioe;
          }
          LOG.info(
              "Done sleeping before retry for '{}'; retry #{}", resourceIdString, retriesAttempted);

          if (buffer.remaining() != remainingBeforeRead) {
            int partialRead = remainingBeforeRead - buffer.remaining();
            LOG.info(
                "Despite exception, had partial read of {} bytes from '{}'; resetting retry count.",
                partialRead, resourceIdString);
            retriesAttempted = 0;
            totalBytesRead += partialRead;
            currentPosition += partialRead;
          }

          // Close the channel
          closeReadChannel();
        }
      } catch (RuntimeException r) {
        closeReadChannel();
        throw r;
      }
    } while (buffer.remaining() > 0);

    // If this method was called when the stream was already at EOF
    // (indicated by totalBytesRead == 0) then return EOF else,
    // return the number of bytes read.
    boolean isEndOfStream = (totalBytesRead == 0);
    if (isEndOfStream) {
      // Check that we didn't get a premature End of Stream signal by checking the number of bytes
      // read against the stream size. Unfortunately we don't have information about the actual size
      // of the data stream when stream compression is used, so we can only ignore this case here.
      checkIOPrecondition(
          fileEncoding == FileEncoding.GZIPPED || currentPosition == size,
          String.format(
              "Failed to read any data before all the file data has been received;"
                  + " currentPosition: %d, size: %d, object '%s'",
              currentPosition, size, resourceIdString));
      return -1;
    }
    return totalBytesRead;
  }

  @Override
  public SeekableByteChannel truncate(long size) throws IOException {
    throw new UnsupportedOperationException("Cannot mutate read-only channel");
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
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
   * probability that SSL connections would be broken in a way that causes {@link
   * java.nio.channels.Channel#close()} itself to throw an exception, even though underlying sockets
   * have already been cleaned up; close() on an SSLSocketImpl requires a shutdown handshake in
   * order to shutdown cleanly, and if the connection has been broken already, then this is not
   * possible, and the SSLSocketImpl was already responsible for performing local cleanup at the
   * time the exception was raised.
   */
  protected void closeReadChannel() {
    if (readChannel != null) {
      LOG.debug("Closing internal readChannel for '{}'", resourceIdString);
      try {
        readChannel.close();
      } catch (Exception e) {
        LOG.debug(
            "Got an exception on readChannel.close() for '{}'; ignoring it.", resourceIdString, e);
      } finally {
        readChannel = null;
        readChannelPosition = -1;
        readChannelEnd = -1;
      }
    }
  }

  /**
   * Closes this channel.
   *
   * @throws IOException on IO error
   */
  @Override
  public void close() throws IOException {
    if (!channelIsOpen) {
      LOG.warn("Channel for '{}' is not open.", resourceIdString);
      return;
    }
    LOG.debug("Closing channel for '{}'", resourceIdString);
    channelIsOpen = false;
    closeReadChannel();
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
   * @param newPosition the new position, counting the number of bytes from the beginning.
   * @return this channel instance
   * @throws java.io.FileNotFoundException if the underlying object does not exist.
   * @throws IOException on IO error
   */
  @Override
  public SeekableByteChannel position(long newPosition) throws IOException {
    throwIfNotOpen();
    if (newPosition == currentPosition) {
      return this;
    }
    validatePosition(newPosition);

    // seek backwards is a sign of non-sequential IO, switch to random access.
    if (!randomAccess && newPosition < currentPosition) {
      LOG.debug(
          "Detected backward seek from {} to {} position, switching to random IO for '{}'",
          currentPosition, newPosition, resourceIdString);
      setRandomAccess();
    } else if (!randomAccess && size - newPosition <= 8) {
      LOG.debug(
          "Detected footer seek from {} to {} position (size={}), switching to random IO for '{}'",
          currentPosition, newPosition, size, resourceIdString);
      setRandomAccess();
    } else {
      LOG.debug(
          "Seek from {} to {} position for '{}'", currentPosition, newPosition, resourceIdString);
    }

    currentPosition = newPosition;
    return this;
  }

  private void setRandomAccess() {
    randomAccess = true;
    randomAccessObjects.put(resourceIdString, Boolean.TRUE);
  }

  private void skipInPlace(long seekDistance) {
    if (skipBuffer == null) {
      skipBuffer = new byte[SKIP_BUFFER_SIZE];
    }
    while (seekDistance > 0 && readChannel != null) {
      try {
        int bufferSize = (int) Math.min(skipBuffer.length, seekDistance);
        int bytesRead = readChannel.read(ByteBuffer.wrap(skipBuffer, 0, bufferSize));
        if (bytesRead < 0) {
          // Shouldn't happen since we called validatePosition prior to this loop.
          LOG.info(
              "Somehow read {} bytes trying to skip {} bytes to seek to position {}, size: {}",
              bytesRead, seekDistance, currentPosition, size);
          closeReadChannel();
        }
        seekDistance -= bytesRead;
        readChannelPosition += bytesRead;
      } catch (IOException e) {
        LOG.info(
            "Got an I/O exception on readChannel.read() for '{}'. A lazy-seek will be pending.",
            resourceIdString, e);
        closeReadChannel();
      }
    }
    checkState(
        readChannelPosition == -1 || readChannelPosition == currentPosition,
        "readChannelPosition (%s) should be '-1' or same as currentPosition (%s)"
            + " after in-place skip",
        readChannelPosition, currentPosition);
  }

  /**
   * Returns size of the object to which this channel is connected.
   *
   * @return size of the object to which this channel is connected
   * @throws IOException on IO error
   */
  @Override
  public long size() throws IOException {
    throwIfNotOpen();
    return size;
  }

  /** Sets size of this channel to the given value. */
  protected void setSize(long size) {
    this.size = size;
  }

  /** Sets file encoding of this channel to the given value. */
  @VisibleForTesting
  protected void setFileEncoding(FileEncoding fileEncoding) {
    this.fileEncoding = fileEncoding;
  }

  /** Validates that the given position is valid for this channel. */
  protected void validatePosition(long position) throws IOException {
    checkState(size >= 0, "size should be initialized before validatePosition call");
    // Validate: position >= 0
    if (position < 0) {
      throw new EOFException(
          String.format(
              "Invalid seek offset: position value (%d) must be >= 0 for '%s'",
              position, resourceIdString));
    }

    // Validate: position < size
    // If a file is gzip encoded, the size of the response may be less than the number of bytes
    // that can be read. In this case, the new position may be a valid offset, and we proceed.
    // If not, the size of the response is the number of bytes that can be read and we throw
    // an exception for an invalid seek.
    if (fileEncoding != FileEncoding.GZIPPED && position >= size) {
      throw new EOFException(
          String.format(
              "Invalid seek offset: position value (%d) must be between 0 and %d for '%s'",
              position, size, resourceIdString));
    }
  }

  /**
   * Seeks to the given position in the underlying stream.
   *
   * <p>Note: Seek is an expensive operation because a new stream is opened each time.
   *
   * @throws java.io.FileNotFoundException if the underlying object does not exist.
   * @throws IOException on IO error
   */
  @VisibleForTesting
  void performLazySeek(long limit) throws IOException {
    // Return quickly if there is no pending seek operation, i.e. position didn't change.
    if (currentPosition == readChannelPosition && readChannel != null && readChannel.isOpen()) {
      return;
    }

    LOG.debug(
        "Performing lazySeek from {} to {} position with {} limit for '{}'",
        readChannelPosition, currentPosition, limit, resourceIdString);

    long seekDistance = currentPosition - readChannelPosition;
    if (readChannel != null
        && seekDistance > 0
        && seekDistance <= readOptions.getInplaceSeekLimit()
        && (readChannelEnd <= 0 || currentPosition < readChannelEnd)) {
      LOG.debug(
          "Seeking forward {} bytes (inplaceSeekLimit: {}) in-place to position {} for '{}'",
          seekDistance, readOptions.getInplaceSeekLimit(), currentPosition, resourceIdString);
      skipInPlace(seekDistance);
    } else {
      closeReadChannel();
    }

    if (readChannel == null) {
      InputStream objectContentStream = openStream(limit);
      readChannel = Channels.newChannel(objectContentStream);
      readChannelPosition = currentPosition;
    }
  }

  /**
   * Initializes {@link #fileEncoding} and {@link #size} from object's GCS metadata.
   *
   * @throws IOException on IO error.
   */
  @VisibleForTesting
  protected void initFileEncodingAndSize() throws IOException {
    if (fileEncoding == FileEncoding.UNINITIALIZED || size < 0) {
      StorageObject metadata = getMetadata();
      if (fileEncoding == FileEncoding.UNINITIALIZED) {
        fileEncoding = getEncoding(metadata);
      }
      size = metadata.getSize().longValue();
    }
  }

  /**
   * Retrieve the object's metadata from GCS.
   *
   * @throws IOException on IO error.
   */
  protected StorageObject getMetadata() throws IOException {
    Storage.Objects.Get getObject = createRequest();
    backOff.get().reset();
    try {
      return ResilientOperation.retry(
          ResilientOperation.getGoogleRequestCallable(getObject),
          new RetryBoundedBackOff(maxRetries, backOff.get()),
          RetryDeterminer.SOCKET_ERRORS,
          IOException.class,
          sleeper);
    } catch (IOException e) {
      if (errorExtractor.itemNotFound(e)) {
        throw GoogleCloudStorageExceptions.getFileNotFoundException(bucketName, objectName);
      }
      throw new IOException("Error reading " + resourceIdString, e);
    } catch (InterruptedException e) { // From the sleep
      throw new IOException("Thread interrupt received.", e);
    }
  }

  /**
   * Returns the FileEncoding of a file given its metadata. Currently supports GZIPPED and OTHER.
   *
   * @param metadata the object's metadata.
   * @return FileEncoding.GZIPPED if the response from GCS will have gzip encoding or
   *     FileEncoding.OTHER otherwise.
   */
  protected static FileEncoding getEncoding(StorageObject metadata) {
    String encoding = metadata.getContentEncoding();
    return nullToEmpty(encoding).contains("gzip") ? FileEncoding.GZIPPED : FileEncoding.OTHER;
  }

  /**
   * Opens the underlying stream, sets its position to the given value.
   *
   * <p>If the file encoding in GCS is gzip (and therefore the HTTP client will attempt to
   * decompress it), the entire file is always requested and we seek to the position requested. If
   * the file encoding is not gzip, only the remaining bytes to be read are requested from GCS.
   *
   * @param limit number of bytes to read from new stream. Ignored if {@link #randomAccess} is
   *     false.
   * @throws IOException on IO error
   */
  protected InputStream openStream(long limit) throws IOException {
    checkArgument(limit > 0, "limit should be greater than 0, but was %s", limit);

    if (size == 0) {
      return new ByteArrayInputStream(new byte[0]);
    }

    Storage.Objects.Get getObject = createRequest();

    int bufferSize = readOptions.getBufferSize();

    // Set the range on the existing request headers that may have been initialized with things
    // like user-agent already. If the file is gzip encoded, request the entire file.
    String rangeHeader = "bytes=0-";
    if (fileEncoding != FileEncoding.GZIPPED) {
      setReadChannelEnd(limit, bufferSize);
      // limit bufferSize to the end of the readChannel
      bufferSize = (int) Math.min(readChannelEnd - currentPosition, bufferSize);

      rangeHeader = "bytes=" + currentPosition + "-";
      if (randomAccess) {
        rangeHeader += readChannelEnd - 1;
      }
    }

    clientRequestHelper.getRequestHeaders(getObject).setRange(rangeHeader);

    HttpResponse response;
    try {
      response = getObject.executeMedia();
    } catch (IOException e) {
      if (errorExtractor.itemNotFound(e)) {
        throw GoogleCloudStorageExceptions.getFileNotFoundException(bucketName, objectName);
      }
      String msg =
          String.format("Error reading '%s' at position %d", resourceIdString, currentPosition);
      if (errorExtractor.rangeNotSatisfiable(e)) {
        throw (EOFException) new EOFException(msg).initCause(e);
      }
      throw new IOException(msg, e);
    }

    InputStream content = null;
    try {
      content = response.getContent();

      if (bufferSize > 0) {
        LOG.debug(
            "Opened stream from {} position with {} range, {} limit and {} bytes buffer for '{}'",
            currentPosition, rangeHeader, limit, bufferSize, resourceIdString);
        content = new BufferedInputStream(content, bufferSize);
      } else {
        LOG.debug(
            "Opened stream from {} position with {} range and {} limit for '{}'",
            currentPosition, rangeHeader, limit, resourceIdString);
      }

      // If the file is gzip encoded, we requested the entire file and need to seek in the content
      // to the desired position. If it is not, we only requested the bytes we haven't read.
      if (fileEncoding == FileEncoding.GZIPPED) {
        content.skip(currentPosition);
      }
    } catch (IOException e) {
      try {
        if (content != null) {
          content.close();
        }
      } catch (IOException closeException) { // ignore error on close
        LOG.debug(
            "Caught exception on close after IOException thrown for '{}'",
            resourceIdString, closeException);
        e.addSuppressed(closeException);
      }
      throw e;
    }
    return content;
  }

  private void setReadChannelEnd(long limit, int bufferSize) {
    if (fileEncoding != FileEncoding.GZIPPED) {
      if (randomAccess) {
        readChannelEnd = currentPosition + Math.max(limit, bufferSize);
        // if readChannelEnd exceeds size, set it to size
        readChannelEnd = Math.min(readChannelEnd, size);
      } else {
        readChannelEnd = size;
      }
    }
  }

  protected Storage.Objects.Get createRequest() throws IOException {
    return gcs.objects().get(bucketName, objectName);
  }

  /** Throws if this channel is not currently open. */
  private void throwIfNotOpen() throws IOException {
    if (!isOpen()) {
      throw new ClosedChannelException();
    }
  }

  /**
   * Throws an IOException if precondition is false.
   *
   * <p>This method should be used in place of Preconditions.checkState in cases where the
   * precondition is derived from the status of the IO operation. That makes it possible to retry
   * the operation by catching IOException.
   */
  private void checkIOPrecondition(boolean precondition, String errorMessage) throws IOException {
    if (!precondition) {
      throw new IOException(errorMessage);
    }
  }
}
