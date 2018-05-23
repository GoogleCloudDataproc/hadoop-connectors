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

package com.google.cloud.hadoop.util;

import com.google.api.client.googleapis.services.AbstractGoogleClientRequest;
import com.google.api.client.http.HttpHeaders;

/**
 * ClientRequestHelper provides wrapper methods around final methods of AbstractGoogleClientRequest
 * to allow overriding them if necessary. Typically should be used for testing purposes only.
 */
public class ClientRequestHelper<S> {
  /**
   * Wraps AbstractGoogleClientRequest.getRequestHeaders().
   */
  public HttpHeaders getRequestHeaders(
      AbstractGoogleClientRequest<S> clientRequest) {
    return clientRequest.getRequestHeaders();
  }

  /**
   * Wraps AbstractGoogleClientRequest.getMediaHttpUploader().
   */
  public void setChunkSize(
      AbstractGoogleClientRequest<S> clientRequest, int chunkSize) {
    clientRequest.getMediaHttpUploader().setChunkSize(chunkSize);
  }

  /**
   * Configures the {@code clientRequest} to enable/disable direct (single-request) uploads
   * according to {@code enable}.
   */
  public void setDirectUploadEnabled(
      AbstractGoogleClientRequest<S> clientRequest, boolean enable) {
    clientRequest.getMediaHttpUploader().setDirectUploadEnabled(enable);
  }
}
