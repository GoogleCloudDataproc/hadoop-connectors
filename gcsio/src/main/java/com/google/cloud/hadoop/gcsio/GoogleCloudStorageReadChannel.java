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
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides seekable read access to GCS.
 */
public class GoogleCloudStorageReadChannel
    implements SeekableByteChannel {

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

  private static enum FileEncoding {
    UNINITIALIZED,
    GZIPPED,
    OTHER
  }

  // Size of buffer to allocate for skipping bytes in-place when performing in-place seeks.
  @VisibleForTesting
  static final int SKIP_BUFFER_SIZE = 8192;

  // Used to separate elements of a Content-Range
  private static final Pattern SLASH = Pattern.compile("/");

  // GCS access instance.
  private Storage gcs;

  // Name of the bucket containing the object being read.
  private String bucketName;

  // Name of the object being read.
  private String objectName;

  // Read channel.
  @VisibleForTesting
  ReadableByteChannel readChannel;

  // True if this channel is open, false otherwise.
  private boolean channelIsOpen;

  // Current read position in the channel.
  private long currentPosition = -1;

  // When a caller calls position(long) to set stream position, we record the target position
  // and defer the actual seek operation until the caller tries to read from the channel.
  // This allows us to avoid an unnecessary seek to position 0 that would take place on creation
  // of this instance in cases where caller intends to start reading at some other offset.
  // If lazySeekPending is set to true, it indicates that a target position has been set
  // but the actual seek operation is still pending.
  @VisibleForTesting
  boolean lazySeekPending;

  // Size of the object being read.
  private long size = -1;

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
  private BackOff backOff = null;

  // read operation gets its own Exponential Backoff Strategy,
  // to avoid interference with other operations in nested retries.
  private BackOff readBackOff = null;

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
   * @throws java.io.FileNotFoundException if the given object does not exist
   * @throws IOException on IO error
   */
  public GoogleCloudStorageReadChannel(
      Storage gcs,
      String bucketName,
      String objectName,
      ApiErrorExtractor errorExtractor,
      ClientRequestHelper<StorageObject> requestHelper,
      GoogleCloudStorageReadOptions readOptions)
      throws IOException {
    this.gcs = gcs;
    this.clientRequestHelper = requestHelper;
    this.bucketName = bucketName;
    this.objectName = objectName;
    this.errorExtractor = errorExtractor;
    this.readOptions = readOptions;
    this.channelIsOpen = true;
    position(0);

    if (!readOptions.getSupportContentEncoding()) {
      // If we don't need to support nonstandard content-encodings, we'll just proceed assuming
      // the content-encoding is the standard/safe FileEncoding.OTHER.
      this.fileEncoding = FileEncoding.OTHER;
    }
  }

  /**
   * Constructs an instance of GoogleCloudStorageReadChannel.
   * Used for unit testing only. Do not use elsewhere.
   *
   * @throws IOException on IO error
   */
  @VisibleForTesting
  protected GoogleCloudStorageReadChannel()
      throws IOException {
    this(GoogleCloudStorageReadOptions.DEFAULT);
  }

  /**
   * Constructs an instance of GoogleCloudStorageReadChannel.
   * Used for unit testing only. Do not use elsewhere.
   *
   * @throws IOException on IO error
   */
  @VisibleForTesting
  protected GoogleCloudStorageReadChannel(GoogleCloudStorageReadOptions readOptions)
      throws IOException {
    this.clientRequestHelper = null;
    this.errorExtractor = null;
    this.readOptions = readOptions;
    this.channelIsOpen = true;
    position(0);
  }

  /**
   * Sets the Sleeper used for sleeping between retries.
   */
  @VisibleForTesting
  void setSleeper(Sleeper sleeper) {
    Preconditions.checkArgument(sleeper != null, "sleeper must not be null!");
    this.sleeper = sleeper;
  }

  /**
   * Sets the clock to be used for determining when max total time has elapsed doing retries.
   */
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
    this.backOff = backOff;
  }

  /**
   * Sets the backoff for determining sleep duration between read retries.
   *
   * @param backOff May be null to force the next usage to auto-initialize with default settings.
   */
  void setReadBackOff(BackOff backOff) {
    this.readBackOff = backOff;
  }

  /**
   * Gets the backoff used for determining sleep duration between retries. May be null if it was
   * never lazily initialized.
   */
  @VisibleForTesting
  BackOff getBackOff() {
    return backOff;
  }

  /**
   * Gets the backoff used for determining sleep duration between read retries. May be null if it
   * was never lazily initialized.
   */
  @VisibleForTesting
  BackOff getReadBackOff() {
    return readBackOff;
  }

  /**
   * Helper for resetting the BackOff used for retries. If no backoff is given, a generic one is
   * initialized.
   */
  private BackOff resetOrCreateBackOff(BackOff backOff) throws IOException {
    if (backOff != null){
      backOff.reset();
    } else {
      backOff = new ExponentialBackOff.Builder()
          .setInitialIntervalMillis(readOptions.getBackoffInitialIntervalMillis())
          .setRandomizationFactor(readOptions.getBackoffRandomizationFactor())
          .setMultiplier(readOptions.getBackoffMultiplier())
          .setMaxIntervalMillis(readOptions.getBackoffMaxIntervalMillis())
          .setMaxElapsedTimeMillis(readOptions.getBackoffMaxElapsedTimeMillis())
          .setNanoClock(clock)
          .build();
    }
    return backOff;
  }

  private BackOff resetOrCreateBackOff() throws IOException {
    return backOff = resetOrCreateBackOff(backOff);
  }

  private BackOff resetOrCreateReadBackOff() throws IOException {
    return readBackOff = resetOrCreateBackOff(readBackOff);
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
   * <p> On unexpected failure, will attempt to close the channel and clean up state.
   *
   * @param buffer buffer to read data into
   * @return number of bytes read or -1 on end-of-stream
   * @throws IOException on IO error
   */
  @Override
  public int read(ByteBuffer buffer)
      throws IOException {
    throwIfNotOpen();

    // Don't try to read if the buffer has no space.
    if (buffer.remaining() == 0) {
      return 0;
    }

    int totalBytesRead = 0;
    int retriesAttempted = 0;

    // We read from a streaming source. We may not get all the bytes we asked for
    // in the first read. Therefore, loop till we either read the required number of
    // bytes or we reach end-of-stream.
    do {
      // Perform a lazy seek if not done already.
      performLazySeek();
      int remainingBeforeRead = buffer.remaining();
      try {
        int numBytesRead = readChannel.read(buffer);
        checkIOPrecondition(numBytesRead != 0, "Read 0 bytes without blocking");
        if (numBytesRead < 0) {
          // Check that we didn't get a premature End of Stream signal by checking the number of
          // bytes read against the stream size. Unfortunately we don't have information about the
          // actual size of the data stream when stream compression is used, so we can only ignore
          // this case here.
          checkIOPrecondition(fileEncoding == FileEncoding.GZIPPED || currentPosition == size,
              String.format(
                  "Received end of stream result before all the file data has been received; "
                  + "totalBytesRead: %s, currentPosition: %s, size: %s",
                  totalBytesRead, currentPosition, size));
          break;
        }
        totalBytesRead += numBytesRead;
        currentPosition += numBytesRead;

        if (retriesAttempted != 0) {
          LOG.info("Success after {} retries on reading '{}'",
              retriesAttempted,
              StorageResourceId.createReadableString(bucketName, objectName));
        }
        // The count of retriesAttempted is per low-level readChannel.read call; each time we make
        // progress we reset the retry counter.
        retriesAttempted = 0;
      } catch (IOException ioe) {
        // TODO(user): Refactor any reusable logic for retries into a separate RetryHelper class.
        if (retriesAttempted == maxRetries) {
          LOG.error(
              "Already attempted max of {} retries while reading '{}'; throwing exception.",
              maxRetries, StorageResourceId.createReadableString(bucketName, objectName));
          closeReadChannel();
          throw ioe;
        } else {
          if (retriesAttempted == 0) {
            // If this is the first of a series of retries, we also want to reset the backOff
            // to have fresh initial values.
            resetOrCreateReadBackOff();
          }

          ++retriesAttempted;
          LOG.warn("Got exception: {} while reading '{}'; retry # {}. Sleeping...",
              ioe.getMessage(), StorageResourceId.createReadableString(bucketName, objectName),
              retriesAttempted);
          try {
            boolean backOffSuccessful = BackOffUtils.next(sleeper, readBackOff);
            if (!backOffSuccessful) {
              LOG.error("BackOff returned false; maximum total elapsed time exhausted. Giving up "
                  + "after {} retries for '{}'", retriesAttempted,
                  StorageResourceId.createReadableString(bucketName, objectName));
              closeReadChannel();
              throw ioe;
            }
          } catch (InterruptedException ie) {
            LOG.error("Interrupted while sleeping before retry. Giving up "
                + "after {} retries for '{}'", retriesAttempted,
                StorageResourceId.createReadableString(bucketName, objectName));
            ioe.addSuppressed(ie);
            closeReadChannel();
            throw ioe;
          }
          LOG.info("Done sleeping before retry for '{}'; retry # {}.",
              StorageResourceId.createReadableString(bucketName, objectName), retriesAttempted);

          if (buffer.remaining() != remainingBeforeRead) {
            int partialRead = remainingBeforeRead - buffer.remaining();
            LOG.info("Despite exception, had partial read of {} bytes; resetting retry count.",
                partialRead);
            retriesAttempted = 0;
            totalBytesRead += partialRead;
            currentPosition += partialRead;
          }

          // Close the channel and mark it to be reopened on next performLazySeek.
          closeReadChannelAndSetLazySeekPending();
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
      checkIOPrecondition(fileEncoding == FileEncoding.GZIPPED || currentPosition == size,
          String.format("Failed to read any data before all the file data has been received; "
              + "currentPosition: %s, size: %s", currentPosition, size));
      return -1;
    } else {
      return totalBytesRead;
    }
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
      try {
        readChannel.close();
      } catch (Exception e) {
        LOG.debug("Got an exception on readChannel.close(); ignoring it.", e);
      } finally {
        readChannel = null;
      }
    }
  }

  /**
   * Closes this channel.
   *
   * @throws IOException on IO error
   */
  @Override
  public void close()
      throws IOException {
    if (!channelIsOpen) {
      LOG.warn(
          "Channel for '{}' is not open.",
          StorageResourceId.createReadableString(bucketName, objectName));
      return;
    }
    channelIsOpen = false;
    closeReadChannel();
  }

  /**
   * Returns this channel's current position.
   *
   * @return this channel's current position
   */
  @Override
  public long position()
      throws IOException {
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

    // If the position has not changed, avoid the expensive operation.
    if (newPosition == currentPosition) {
      return this;
    }

    validatePosition(newPosition);
    long seekDistance = newPosition - currentPosition;
    if (readChannel != null
        && seekDistance > 0
        && seekDistance <= readOptions.getInplaceSeekLimit()) {
      LOG.debug("Seeking forward {} bytes (limit is {}) in-place to position {}",
          seekDistance, readOptions.getInplaceSeekLimit(), newPosition);
      if (skipBuffer == null) {
        skipBuffer = new byte[SKIP_BUFFER_SIZE];
      }
      while (seekDistance > 0) {
        try {
          final int bufferSize = (int) Math.min(skipBuffer.length, seekDistance);
          int bytesRead = readChannel.read(ByteBuffer.wrap(skipBuffer, 0, bufferSize));
          if (bytesRead < 0) {
            // Shouldn't happen since we called validatePosition prior to this loop.
            LOG.info(
                "Somehow read {} bytes trying to skip {} bytes to seek to position {}, size: {}",
                bytesRead, seekDistance, newPosition, size);
            closeReadChannelAndSetLazySeekPending();
            break;
          }
          seekDistance -= bytesRead;
        } catch (IOException e) {
          LOG.info("Got an I/O exception on readChannel.read(). A lazy-seek will be pending.");
          closeReadChannelAndSetLazySeekPending();
          break;
        }
      }
    } else {
      // Close the read channel. If anything tries to read on it, it's an error and should fail.
      closeReadChannelAndSetLazySeekPending();
    }
    currentPosition = newPosition;
    return this;
  }

  private void closeReadChannelAndSetLazySeekPending() {
    closeReadChannel();
    lazySeekPending = true;
  }

  /**
   * Returns size of the object to which this channel is connected.
   *
   * @return size of the object to which this channel is connected
   * @throws IOException on IO error
   */
  @Override
  public long size()
      throws IOException {
    throwIfNotOpen();
    // Perform a lazy seek if not done already so that size of this channel is set correctly.
    performLazySeek();
    return size;
  }

  /**
   * Sets size of this channel to the given value.
   */
  protected void setSize(long size) {
    this.size = size;
  }

  /**
   * Validates that the given position is valid for this channel.
   */
  protected void validatePosition(long newPosition) throws IOException {
    // Validate: 0 <= newPosition
    if (newPosition < 0) {
      throw new EOFException(
          String.format("Invalid seek offset: position value (%d) must be >= 0", newPosition));
    }

    // Validate: newPosition < size
    // Note that we access this.size directly rather than calling size() to avoid initiating
    // lazy seek that leads to recursive error. We validate newPosition < size only when size of
    // this channel has been computed by a prior call. This means that position could be
    // potentially set to an invalid value (>= size) by position(long). However, that error
    // gets caught during lazy seek.
    // If a file is gzip encoded, the size of the response may be less than the number of bytes
    // that can be read. In this case, the new position may be a valid offset, and we proceed.
    // If not, the size of the response is the number of bytes that can be read and we throw
    // an exception for an invalid seek.
    if ((size >= 0) && (newPosition >= size) && (fileEncoding != FileEncoding.GZIPPED)) {
      throw new EOFException(
          String.format(
              "Invalid seek offset: position value (%d) must be between 0 and %d",
              newPosition, size));
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
  void performLazySeek() throws IOException {

    // Return quickly if there is no pending seek operation.
    if (!lazySeekPending) {
      return;
    }

    // Close the underlying channel if it is open.
    closeReadChannel();

    InputStream objectContentStream = openStreamAndSetMetadata(currentPosition);
    readChannel = Channels.newChannel(objectContentStream);
    lazySeekPending = false;
  }

  /**
   * Retrieve the object's metadata from GCS.
   *
   * @throws IOException on IO error.
   */
  protected StorageObject getMetadata() throws IOException {
    Storage.Objects.Get getObject = createRequest();
    try {
      return ResilientOperation.retry(
          ResilientOperation.getGoogleRequestCallable(getObject),
          new RetryBoundedBackOff(maxRetries, resetOrCreateBackOff()),
          RetryDeterminer.SOCKET_ERRORS,
          IOException.class,
          sleeper);
    } catch (IOException e) {
      if (errorExtractor.itemNotFound(e)) {
        throw GoogleCloudStorageExceptions.getFileNotFoundException(bucketName, objectName);
      }
      String msg =
          "Error reading " + StorageResourceId.createReadableString(bucketName, objectName);
      throw new IOException(msg, e);
    } catch (InterruptedException e) {  // From the sleep
      throw new IOException("Thread interrupt received.", e);
    }
  }

  /**
   * Returns the FileEncoding of a file given its metadata. Currently supports GZIPPED and OTHER.
   *
   * @param metadata the object's metadata.
   * @return FileEncoding.GZIPPED if the response from GCS will have gzip encoding or
   *        FileEncoding.OTHER otherwise.
   */
  protected static FileEncoding getEncoding(StorageObject metadata) {
    String contentEncoding = metadata.getContentEncoding();
    return contentEncoding != null && contentEncoding.contains("gzip")
        ? FileEncoding.GZIPPED : FileEncoding.OTHER;
  }

  /**
   * Set the size of the content.
   *
   * <p>First, we examine the Content-Length header. If neither exists, we then look for and parse
   * the Content-Range header. If there is no way to determine the content length, an
   * exception is thrown. If the Content-Length header is present, then the offset is added
   * to this value (i.e., offset is the number of bytes that were not requested).
   *
   * @param response response to parse.
   * @param offset the number of bytes that were not requested.
   * @throws IOException on IO error.
   */
  protected void setSize(HttpResponse response, long offset) throws IOException {
    String contentRange = response.getHeaders().getContentRange();
    if (response.getHeaders().getContentLength() != null) {
      size = response.getHeaders().getContentLength() + offset;
    } else if (contentRange != null) {
      String sizeStr = SLASH.split(contentRange)[1];
      try {
        size = Long.parseLong(sizeStr);
      } catch (NumberFormatException e) {
        throw new IOException(
            "Could not determine size from response from Content-Range: " + contentRange, e);
      }
    } else {
      throw new IOException("Could not determine size of response");
    }
  }

  /**
   * Opens the underlying stream, sets its position to the given value and sets size based on
   * stream content size.
   *
   * <p>If the file encoding in GCS is gzip (and therefore the HTTP client will attempt to
   * decompress it), the entire file is always requested and we seek to the position requested.
   * If the file encoding is not gzip, only the remaining bytes to be read are requested from GCS.
   *
   * @param newPosition position to seek into the new stream.
   * @throws IOException on IO error
   */
  protected InputStream openStreamAndSetMetadata(long newPosition)
      throws IOException {
    if (fileEncoding == FileEncoding.UNINITIALIZED) {
      StorageObject metadata = getMetadata();
      fileEncoding = getEncoding(metadata);
    }
    validatePosition(newPosition);
    Storage.Objects.Get getObject = createRequest();
    // Set the range on the existing request headers that may have been initialized with things
    // like user-agent already. If the file is gzip encoded, request the entire file.
    clientRequestHelper
        .getRequestHeaders(getObject)
        .setRange(
            fileEncoding == FileEncoding.GZIPPED
                ? "bytes=0-"
                : String.format("bytes=%d-", newPosition));
    HttpResponse response;
    try {
      response = getObject.executeMedia();
    } catch (IOException e) {
      if (errorExtractor.itemNotFound(e)) {
        throw GoogleCloudStorageExceptions.getFileNotFoundException(bucketName, objectName);
      } else if (errorExtractor.rangeNotSatisfiable(e)
                 && newPosition == 0
                 && size == -1) {
        // We don't know the size yet (size == -1) and we're seeking to byte 0, but got 'range
        // not satisfiable'; the object must be empty.
        LOG.info("Got 'range not satisfiable' for reading {} at position 0; assuming empty.",
            StorageResourceId.createReadableString(bucketName, objectName));
        size = 0;
        return new ByteArrayInputStream(new byte[0]);
      } else {
        String msg = String.format("Error reading %s at position %d",
            StorageResourceId.createReadableString(bucketName, objectName), newPosition);
        if (errorExtractor.rangeNotSatisfiable(e)) {
          throw (EOFException) (new EOFException(msg).initCause(e));
        } else {
          throw new IOException(msg, e);
        }
      }
    }
    InputStream content = null;
    try {
      content = response.getContent();

      if (readOptions.getBufferSize() > 0) {
        LOG.debug(
            "Wrapping response content in BufferedInputStream of size {}",
            readOptions.getBufferSize());
        content = new BufferedInputStream(content, readOptions.getBufferSize());
      }

      // If the file is gzip encoded, we requested the entire file and need to seek in the content
      // to the desired position. If it is not, we only requested the bytes we haven't read.
      setSize(response, fileEncoding == FileEncoding.GZIPPED ? 0 : newPosition);
      if (fileEncoding == FileEncoding.GZIPPED) {
        content.skip(newPosition);
      }
    } catch (IOException e) {
      try {
        if (content != null) {
          content.close();
        }
      } catch (IOException closeException) {  // ignore error on close
        LOG.debug("Caught exception on close after IOException thrown.", closeException);
        e.addSuppressed(closeException);
      }
      throw e;
    }
    return content;
  }

  protected Storage.Objects.Get createRequest() throws IOException {
    return gcs.objects().get(bucketName, objectName);
  }

  /**
   * Throws if this channel is not currently open.
   */
  private void throwIfNotOpen()
      throws IOException {
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
