/*
 * Copyright 2014 Google Inc. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.util;

import static com.google.common.base.Preconditions.checkState;

import com.google.api.client.googleapis.media.MediaHttpUploader;
import com.google.api.client.googleapis.services.AbstractGoogleClientRequest;
import com.google.api.client.http.InputStreamContent;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public abstract class AbstractGoogleAsyncWriteChannel<T extends AbstractGoogleClientRequest<S>, S>
    implements WritableByteChannel {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  // Default GCS upload granularity.
  public static final int GCS_UPLOAD_GRANULARITY = 8 * 1024 * 1024;

  // Chunk size to use.
  public static final int UPLOAD_CHUNK_SIZE_DEFAULT =
      Runtime.getRuntime().maxMemory() < 512 * 1024 * 1024
          ? GCS_UPLOAD_GRANULARITY
          : 8 * GCS_UPLOAD_GRANULARITY;

  private String contentType;

  // ClientRequestHelper to be used instead of calling final methods in client requests.
  private ClientRequestHelper<S> clientRequestHelper = new ClientRequestHelper<>();

  // A pipe that connects write channel used by caller to the input stream used by GCS uploader.
  // The uploader reads from input stream, which blocks till a caller writes some data to the
  // write channel (pipeSinkChannel below). The pipe is formed by connecting pipeSink to pipeSource
  private final ExecutorService threadPool;

  private boolean initialized = false;

  // Size of buffer used by upload pipe.
  private final int pipeBufferSize;

  // Chunk size to use.
  private int uploadChunkSize = UPLOAD_CHUNK_SIZE_DEFAULT;

  // A channel wrapper over pipeSink.
  private WritableByteChannel pipeSinkChannel;

  // Upload operation that takes place on a separate thread.
  private Future<S> uploadOperation;

  // When enabled, we get higher throughput for writing small files.
  private boolean directUploadEnabled = false;

  private ByteBuffer uploadCache = null;

  /** Construct a new channel using the given ExecutorService to run background uploads. */
  public AbstractGoogleAsyncWriteChannel(
      ExecutorService threadPool, AsyncWriteChannelOptions options) {
    this.threadPool = threadPool;
    this.pipeBufferSize = options.getPipeBufferSize();
    if (options.getUploadCacheSize() > 0) {
      this.uploadCache = ByteBuffer.allocate(options.getUploadCacheSize());
    }
    setUploadChunkSize(options.getUploadChunkSize());
    setDirectUploadEnabled(options.isDirectUploadEnabled());
    setContentType("application/octet-stream");
  }

  /**
   * Sets the ClientRequestHelper to be used instead of calling final methods in client requests.
   */
  @VisibleForTesting
  public void setClientRequestHelper(ClientRequestHelper<S> helper) {
    clientRequestHelper = helper;
  }

  /**
   * Create an API request to upload the given InputStreamContent.
   *
   * @return An initialized request.
   * @throws IOException
   */
  public abstract T createRequest(InputStreamContent inputStream) throws IOException;

  /**
   * Handle the API response.
   *
   * <p>This method is invoked after the upload has completed on the same thread that invokes
   * close().
   *
   * @param response The API response object.
   */
  public void handleResponse(S response) throws IOException {}

  /**
   * Derived classes may optionally intercept an IOException thrown from the execute() method of a
   * prepared request that came from {@link #createRequest}, and return a reconstituted "response"
   * object if the IOException can be handled as a success; for example, if the caller already has
   * an identifier for an object, and the response is used solely for obtaining the same identifier,
   * and the IOException is a handled "409 Already Exists" type of exception, then the derived class
   * may override this method to return the expected "identifier" response. Return null to let the
   * exception propagate through correctly.
   */
  public S createResponseFromException(IOException ioe) {
    return null;
  }

  @Deprecated
  public void setUploadBufferSize(int bufferSize) {
    setUploadChunkSize(bufferSize);
  }

  /** Sets size of upload buffer used. */
  public void setUploadChunkSize(int chunkSize) {
    Preconditions.checkArgument(chunkSize > 0, "Upload chunk size must be great than 0.");
    Preconditions.checkArgument(
        chunkSize % MediaHttpUploader.MINIMUM_CHUNK_SIZE == 0,
        "Upload chunk size must be a multiple of MediaHttpUploader.MINIMUM_CHUNK_SIZE");
    if ((chunkSize > GCS_UPLOAD_GRANULARITY) && (chunkSize % GCS_UPLOAD_GRANULARITY != 0)) {
      logger.atWarning().log(
          "Upload chunk size should be a multiple of %s for best performance, got %s",
          GCS_UPLOAD_GRANULARITY, chunkSize);
    }
    uploadChunkSize = chunkSize;
  }

  /**
   * Enables or disables direct uploads.
   *
   * @see MediaHttpUploader#setDirectUploadEnabled(boolean)
   */
  public void setDirectUploadEnabled(boolean enableDirectUpload) {
    directUploadEnabled = enableDirectUpload;
  }

  /** Returns true if direct media uploads are enabled. */
  public boolean isDirectUploadEnabled() {
    return directUploadEnabled;
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

    return pipeSinkChannel.write(buffer);
  }

  /**
   * Tells whether this channel is open.
   *
   * @return a value indicating whether this channel is open
   */
  @Override
  public boolean isOpen() {
    return (pipeSinkChannel != null) && pipeSinkChannel.isOpen();
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
      pipeSinkChannel.close();
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
    pipeSinkChannel = null;
    if (uploadOperation != null && !uploadOperation.isDone()) {
      uploadOperation.cancel(/* mayInterruptIfRunning= */ true);
    }
    uploadOperation = null;
  }

  /**
   * Initialize this channel object for writing.
   *
   * @throws IOException
   */
  public void initialize() throws IOException {
    // Create a pipe such that its one end is connected to the input stream used by
    // the uploader and the other end is the write channel used by the caller.
    PipedInputStream pipeSource = new PipedInputStream(pipeBufferSize);
    OutputStream pipeSink = new PipedOutputStream(pipeSource);
    pipeSinkChannel = Channels.newChannel(pipeSink);

    // Connect pipe-source to the stream used by uploader.
    InputStreamContent objectContentStream = new InputStreamContent(contentType, pipeSource);
    // Indicate that we do not know length of file in advance.
    objectContentStream.setLength(-1);
    objectContentStream.setCloseInputStream(false);

    T request = createRequest(objectContentStream);
    request.setDisableGZipContent(true);

    // Change chunk size from default value (10MB) to one that yields higher performance.
    clientRequestHelper.setChunkSize(request, uploadChunkSize);

    // Given that the two ends of the pipe must operate asynchronous relative
    // to each other, we need to start the upload operation on a separate thread.
    uploadOperation = threadPool.submit(new UploadOperation(request, pipeSource));

    initialized = true;
  }

  class UploadOperation implements Callable<S> {
    // Object to be uploaded. This object declared final for safe object publishing.
    private final T uploadObject;

    // Read end of the pipe. This object declared final for safe object publishing.
    private final InputStream pipeSource;

    /** Constructs an instance of UploadOperation. */
    public UploadOperation(T uploadObject, InputStream pipeSource) {
      this.uploadObject = uploadObject;
      this.pipeSource = pipeSource;
    }

    @Override
    public S call() throws Exception {
      // Try-with-resource will close this end of the pipe so that
      // the writer at the other end will not hang indefinitely.
      try (InputStream uploadPipeSource = pipeSource) {
        return uploadObject.execute();
      } catch (IOException ioe) {
        S response = createResponseFromException(ioe);
        if (response != null) {
          logger.atWarning().withCause(ioe).log(
              "Received IOException, but successfully converted to response '%s'.", response);
          return response;
        }
        throw ioe;
      }
    }
  }

  /** Sets the contentType. This must be called before {@link #initialize()} for any effect. */
  protected void setContentType(String contentType) {
    this.contentType = contentType;
  }

  /**
   * Throws if upload operation failed. Propagates any errors.
   *
   * @throws IOException on IO error
   */
  private S waitForCompletionAndThrowIfUploadFailed() throws IOException {
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
      throw new IOException("Upload failed", e.getCause());
    }
  }
}
