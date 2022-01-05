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

import com.google.api.client.googleapis.services.AbstractGoogleClientRequest;
import com.google.api.client.http.InputStreamContent;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

public abstract class AbstractGoogleAsyncWriteChannel<T extends AbstractGoogleClientRequest<S>, S>
    extends BaseAbstractGoogleAsyncWriteChannel<S> {
  // ClientRequestHelper to be used instead of calling final methods in client requests.
  private final ClientRequestHelper<S> clientRequestHelper;

  /** Construct a new channel using the given ExecutorService to run background uploads. */
  public AbstractGoogleAsyncWriteChannel(
      ClientRequestHelper<S> clientRequestHelper,
      ExecutorService threadPool,
      AsyncWriteChannelOptions options) {
    super(threadPool, options);
    this.clientRequestHelper = clientRequestHelper;
  }

  /**
   * Create an API request to upload the given InputStreamContent.
   *
   * @return An initialized request.
   */
  public abstract T createRequest(InputStreamContent inputStream) throws IOException;

  /**
   * Derived classes may optionally intercept an IOException thrown from the {@code execute()}
   * method of a prepared request that came from {@link #createRequest}, and return a reconstituted
   * "response" object if the IOException can be handled as a success; for example, if the caller
   * already has an identifier for an object, and the response is used solely for obtaining the same
   * identifier, and the IOException is a handled "409 Already Exists" type of exception, then the
   * derived class may override this method to return the expected "identifier" response. Return
   * null to let the exception propagate through correctly.
   */
  public S createResponseFromException(IOException e) {
    return null;
  }

  @Override
  public void startUpload(InputStream pipeSource) throws IOException {
    // Connect pipe-source to the stream used by uploader.
    InputStreamContent objectContentStream =
        new InputStreamContent(getContentType(), pipeSource)
            // Indicate that we do not know length of file in advance.
            .setLength(-1)
            .setCloseInputStream(false);

    T request = createRequest(objectContentStream);
    request.setDisableGZipContent(true);

    // Change chunk size from default value (10MB) to one that yields higher performance.
    clientRequestHelper.setChunkSize(request, channelOptions.getUploadChunkSize());

    // Given that the two ends of the pipe must operate asynchronous relative
    // to each other, we need to start the upload operation on a separate thread.
    uploadOperation = threadPool.submit(new UploadOperation(request, pipeSource));
  }

  protected abstract String getContentType();

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
      try (InputStream ignore = pipeSource) {
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
}
