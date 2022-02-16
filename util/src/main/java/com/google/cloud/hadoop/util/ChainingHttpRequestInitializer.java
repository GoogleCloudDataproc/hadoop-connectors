/*
 * Copyright 2015 Google Inc. All Rights Reserved.
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

import com.google.api.client.http.HttpExecuteInterceptor;
import com.google.api.client.http.HttpIOExceptionHandler;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponseInterceptor;
import com.google.api.client.http.HttpUnsuccessfulResponseHandler;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link HttpRequestInitializer} that composes handlers and interceptors supplied by component
 * handlers. All interceptors in a chain are executed. All handlers are executed until a handler
 * returns true from its handle method. If an initializer configures other parameters on request,
 * last initializer to do so wins.
 */
public class ChainingHttpRequestInitializer implements HttpRequestInitializer {

  private final List<HttpRequestInitializer> initializers;

  public ChainingHttpRequestInitializer(HttpRequestInitializer... initializers) {
    this.initializers = ImmutableList.copyOf(initializers);
  }

  @Override
  public void initialize(HttpRequest request) throws IOException {
    List<HttpIOExceptionHandler> ioExceptionHandlers = new ArrayList<>(initializers.size());
    List<HttpUnsuccessfulResponseHandler> unsuccessfulResponseHandlers =
        new ArrayList<>(initializers.size());
    List<HttpExecuteInterceptor> interceptors = new ArrayList<>(initializers.size());
    List<HttpResponseInterceptor> responseInterceptors = new ArrayList<>(initializers.size());
    for (HttpRequestInitializer initializer : initializers) {
      initializer.initialize(request);
      if (request.getIOExceptionHandler() != null) {
        ioExceptionHandlers.add(request.getIOExceptionHandler());
        request.setIOExceptionHandler(null);
      }
      if (request.getUnsuccessfulResponseHandler() != null) {
        unsuccessfulResponseHandlers.add(request.getUnsuccessfulResponseHandler());
        request.setUnsuccessfulResponseHandler(null);
      }
      if (request.getInterceptor() != null) {
        interceptors.add(request.getInterceptor());
        request.setInterceptor(null);
      }
      if (request.getResponseInterceptor() != null) {
        responseInterceptors.add(request.getResponseInterceptor());
        request.setResponseInterceptor(null);
      }
    }
    request.setIOExceptionHandler(makeIoExceptionHandler(ioExceptionHandlers));
    request.setUnsuccessfulResponseHandler(
        makeUnsuccessfulResponseHandler(unsuccessfulResponseHandlers));
    request.setInterceptor(makeInterceptor(interceptors));
    request.setResponseInterceptor(makeResponseInterceptor(responseInterceptors));
  }

  private HttpResponseInterceptor makeResponseInterceptor(
      Iterable<HttpResponseInterceptor> responseInterceptors) {
    return response -> {
      for (HttpResponseInterceptor interceptor : responseInterceptors) {
        interceptor.interceptResponse(response);
      }
    };
  }

  private HttpExecuteInterceptor makeInterceptor(Iterable<HttpExecuteInterceptor> interceptors) {
    return request -> {
      for (HttpExecuteInterceptor interceptor : interceptors) {
        interceptor.intercept(request);
      }
    };
  }

  private HttpUnsuccessfulResponseHandler makeUnsuccessfulResponseHandler(
      Iterable<HttpUnsuccessfulResponseHandler> unsuccessfulResponseHandlers) {
    return (request, response, supportsRetry) -> {
      for (HttpUnsuccessfulResponseHandler handler : unsuccessfulResponseHandlers) {
        if (handler.handleResponse(request, response, supportsRetry)) {
          return true;
        }
      }
      return false;
    };
  }

  private HttpIOExceptionHandler makeIoExceptionHandler(
      Iterable<HttpIOExceptionHandler> ioExceptionHandlers) {
    return (request, supportsRetry) -> {
      for (HttpIOExceptionHandler handler : ioExceptionHandlers) {
        if (handler.handleIOException(request, supportsRetry)) {
          return true;
        }
      }
      return false;
    };
  }
}
