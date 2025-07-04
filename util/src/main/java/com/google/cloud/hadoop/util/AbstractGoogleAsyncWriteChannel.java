/*
 * Copyright 2019 Google LLC
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

package com.google.cloud.hadoop.util;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.flogger.GoogleLogger;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.Pipe;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Skeletal implementation of a WritableByteChannel that executes an asynchronous upload operation
 * and optionally handles the result.
 *
 * @param <T> The type of the result of the completed upload operation.
 */
public abstract class AbstractGoogleAsyncWriteChannel<T> implements WritableByteChannel {

  protected static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  // A pipe that connects write channel used by caller to the input stream used by GCS uploader.
  // The uploader reads from input stream, which blocks till a caller writes some data to the
  // write channel (pipeSinkChannel below). The pipe is formed by connecting pipeSink to pipeSource
  protected final ExecutorService threadPool;

  protected final AsyncWriteChannelOptions channelOptions;

  // Upload operation that takes place on a separate thread.
  protected Future<T> uploadOperation;

  private boolean initialized = false;

  private WritableByteChannel pipeSink;

  private ByteBuffer uploadCache = null;

  protected final Hasher cumulativeCrc32cHasher;

  // To avoid duplicate calculations in case of reuploadFromCache.
  private boolean reuploadFromCacheInitiated = false;

  /** Construct a new channel using the given ExecutorService to run background uploads. */
  public AbstractGoogleAsyncWriteChannel(
      ExecutorService threadPool, AsyncWriteChannelOptions channelOptions) {
    this.threadPool = threadPool;
    this.channelOptions = channelOptions;
    this.cumulativeCrc32cHasher = Hashing.crc32c().newHasher();
    if (channelOptions.getUploadCacheSize() > 0) {
      this.uploadCache = ByteBuffer.allocate(channelOptions.getUploadCacheSize());
    }
  }

  /**
   * Handle the API response.
   *
   * <p>This method is invoked after the upload has completed on the same thread that invokes
   * close(). It can throw IOException if checksum matching is enabled and mismatched.
   *
   * @param response The API response object.
   */
  public void handleResponse(T response) throws IOException {}

  /** Returns true if direct media uploads are enabled. */
  public boolean isDirectUploadEnabled() {
    return channelOptions.isDirectUploadEnabled();
  }

  /**
   * Writes contents of the given buffer to this channel.
   *
   * <p>Note: The data that one writes gets written to a pipe which must not block if the pipe has
   * sufficient buffer space. A success code returned from this method does not mean that the
   * specific data was successfully written to the underlying storage. It simply means that there is
   * no error at present. The data upload may encounter an error on a separate thread. Such error is
   * not ignored; it shows up as an exception during a subsequent call to write() or close(). The
   * only way to be sure of successful upload is when the close() method returns successfully.
   *
   * @param buffer buffer to write
   * @throws IOException on IO error
   */
  @Override
  public synchronized int write(ByteBuffer buffer) throws IOException {
    checkState(initialized, "initialize() must be invoked before use.");
    if (!isOpen()) {
      throw new ClosedChannelException();
    }

    // No point in writing further if upload failed on another thread.
    if (uploadOperation.isDone()) {
      waitForCompletionAndThrowIfUploadFailed();
    }

    if (uploadCache != null && uploadCache.remaining() >= buffer.remaining()) {
      int position = buffer.position();
      uploadCache.put(buffer);
      buffer.position(position);
    } else {
      uploadCache = null;
    }
    try {
      int originalPosition = buffer.position();
      int writtenBytes = pipeSink.write(buffer);
      if (channelOptions.isRollingChecksumEnabled() && !reuploadFromCacheInitiated) {
        addBytesToCumulativeChecksum(buffer, writtenBytes, originalPosition);
      }
      return writtenBytes;
    } catch (IOException e) {
      throw new IOException(
          String.format(
              "Failed to write %d bytes in '%s'", buffer.remaining(), getResourceString()),
          e);
    }
  }

  private void addBytesToCumulativeChecksum(
      ByteBuffer src, int writtenBytes, int originalPosition) {
    ByteBuffer duplicateBuffer = src.duplicate();
    duplicateBuffer.position(originalPosition);
    // Only calculate hash for written bytes offset from the original position.
    duplicateBuffer.limit(originalPosition + writtenBytes);
    cumulativeCrc32cHasher.putBytes(duplicateBuffer);
  }

  /**
   * Tells whether this channel is open.
   *
   * @return a value indicating whether this channel is open
   */
  @Override
  public boolean isOpen() {
    return (pipeSink != null) && pipeSink.isOpen();
  }

  /**
   * Closes this channel.
   *
   * <p>Note: The method returns only after all data has been successfully written to GCS or if
   * there is a non-retry-able error.
   *
   * @throws IOException on IO error
   */
  @Override
  public void close() throws IOException {
    checkState(initialized, "initialize() must be invoked before use.");
    if (!isOpen()) {
      return;
    }
    try {
      pipeSink.close();
      handleResponse(waitForCompletionAndThrowIfUploadFailed());
    } catch (IOException e) {
      if (uploadCache == null) {
        throw e;
      }
      logger.atWarning().withCause(e).log("Reuploading using cached data");
      reuploadFromCache();
    } finally {
      closeInternal();
    }
  }

  private void reuploadFromCache() throws IOException {
    reuploadFromCacheInitiated = true;
    closeInternal();
    initialized = false;

    initialize();

    // Set cache to null so it will not be re-cached during retry.
    ByteBuffer reuploadData = uploadCache;
    uploadCache = null;

    reuploadData.flip();

    try {
      write(reuploadData);
    } finally {
      close();
    }
  }

  private void closeInternal() {
    pipeSink = null;
    if (uploadOperation != null && !uploadOperation.isDone()) {
      uploadOperation.cancel(/* mayInterruptIfRunning= */ true);
    }
    uploadOperation = null;
  }

  /** Initialize this channel object for writing. */
  public void initialize() throws IOException {
    InputStream pipeSource = initializeUploadPipe();
    startUpload(pipeSource);
    initialized = true;
  }

  // Create a pipe such that its one end is connected to the input stream used by
  // the uploader and the other end is the write channel used by the caller.
  private InputStream initializeUploadPipe() throws IOException {
    switch (channelOptions.getPipeType()) {
      case NIO_CHANNEL_PIPE:
        Pipe pipe = Pipe.open();
        pipeSink = pipe.sink();
        InputStream pipeSource = Channels.newInputStream(pipe.source());
        return channelOptions.getPipeBufferSize() > 0
            ? new BufferedInputStream(pipeSource, channelOptions.getPipeBufferSize())
            : pipeSource;
      case IO_STREAM_PIPE:
        PipedInputStream internalPipeSource =
            new PipedInputStream(channelOptions.getPipeBufferSize());
        PipedOutputStream internalPipeSink = new PipedOutputStream(internalPipeSource);
        pipeSink = Channels.newChannel(internalPipeSink);
        return internalPipeSource;
    }
    throw new IllegalStateException("Unknown PipeType: " + channelOptions.getPipeType());
  }

  /** Create a new thread which handles the upload. */
  public abstract void startUpload(InputStream pipeSource) throws IOException;

  protected abstract String getResourceString();

  /**
   * Throws if upload operation failed. Propagates any errors.
   *
   * @throws IOException on IO error
   */
  private T waitForCompletionAndThrowIfUploadFailed() throws IOException {
    try {
      return uploadOperation.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      // If we were interrupted, we need to cancel the upload operation.
      uploadOperation.cancel(true);
      IOException exception = new ClosedByInterruptException();
      exception.addSuppressed(e);
      throw exception;
    } catch (ExecutionException e) {
      if (e.getCause() instanceof Error) {
        throw (Error) e.getCause();
      }
      throw new IOException(
          String.format("Upload failed for '%s'", getResourceString()), e.getCause());
    }
  }
}
