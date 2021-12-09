/*
 * Copyright 2019 Google LLC. All Rights Reserved.
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

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.ACTION_HTTP_DELETE_REQUEST;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.ACTION_HTTP_DELETE_REQUEST_FAILURES;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.ACTION_HTTP_GET_REQUEST;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.ACTION_HTTP_GET_REQUEST_FAILURES;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.ACTION_HTTP_HEAD_REQUEST;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.ACTION_HTTP_HEAD_REQUEST_FAILURES;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.ACTION_HTTP_PATCH_REQUEST;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.ACTION_HTTP_PATCH_REQUEST_FAILURES;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.ACTION_HTTP_POST_REQUEST;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.ACTION_HTTP_POST_REQUEST_FAILURES;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.ACTION_HTTP_PUT_REQUEST;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.ACTION_HTTP_PUT_REQUEST_FAILURES;

import com.google.api.client.http.HttpExecuteInterceptor;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpResponseInterceptor;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

/** To track and update the statistics related to http requests */
class GcsioTrackingHttpRequestInitializer implements HttpRequestInitializer {

  private final HttpRequestInitializer delegate;

  // To track the http statistics
  private HashMap<GoogleCloudStorageStatistics, AtomicLong> httpStatistics;

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  public GcsioTrackingHttpRequestInitializer(
      HttpRequestInitializer delegate,
      HashMap<GoogleCloudStorageStatistics, AtomicLong> httpStatistics) {
    this.delegate = delegate;
    this.httpStatistics = httpStatistics;
  }

  @Override
  public void initialize(HttpRequest request) throws IOException {
    if (delegate != null) {
      delegate.initialize(request);
    }
    HttpExecuteInterceptor executeInterceptor = request.getInterceptor();
    request.setInterceptor(
        r -> {
          if (executeInterceptor != null) {
            executeInterceptor.intercept(r);
          }
          if (r.getRequestMethod() == "GET") {
            httpStatistics.putIfAbsent(
                GoogleCloudStorageStatistics.ACTION_HTTP_GET_REQUEST, new AtomicLong(0));
            httpStatistics.get(ACTION_HTTP_GET_REQUEST).incrementAndGet();
          } else if (r.getRequestMethod() == "HEAD") {
            httpStatistics.putIfAbsent(
                GoogleCloudStorageStatistics.ACTION_HTTP_HEAD_REQUEST, new AtomicLong(0));
            httpStatistics.get(ACTION_HTTP_HEAD_REQUEST).incrementAndGet();
          } else if (r.getRequestMethod() == "PUT") {
            httpStatistics.putIfAbsent(
                GoogleCloudStorageStatistics.ACTION_HTTP_PUT_REQUEST, new AtomicLong(0));
            httpStatistics.get(ACTION_HTTP_PUT_REQUEST).incrementAndGet();
          } else if (r.getRequestMethod() == "POST") {
            httpStatistics.putIfAbsent(
                GoogleCloudStorageStatistics.ACTION_HTTP_POST_REQUEST, new AtomicLong(0));
            httpStatistics.get(ACTION_HTTP_POST_REQUEST).incrementAndGet();
          } else if (r.getRequestMethod() == "PATCH") {
            httpStatistics.putIfAbsent(
                GoogleCloudStorageStatistics.ACTION_HTTP_PATCH_REQUEST, new AtomicLong(0));
            httpStatistics.get(ACTION_HTTP_PATCH_REQUEST).incrementAndGet();
          } else if (r.getRequestMethod() == "DELETE") {
            httpStatistics.putIfAbsent(
                GoogleCloudStorageStatistics.ACTION_HTTP_DELETE_REQUEST, new AtomicLong(0));
            httpStatistics.get(ACTION_HTTP_DELETE_REQUEST).incrementAndGet();
          }
        });
    request.setResponseInterceptor(
        new HttpResponseInterceptor() {
          @Override
          public void interceptResponse(HttpResponse httpResponse) throws IOException {
            if (!(httpResponse.isSuccessStatusCode())
                && httpResponse.getRequest().getRequestMethod() == "GET") {
              httpStatistics.putIfAbsent(ACTION_HTTP_GET_REQUEST_FAILURES, new AtomicLong(0));
              httpStatistics.get(ACTION_HTTP_GET_REQUEST_FAILURES).incrementAndGet();
            } else if (!(httpResponse.isSuccessStatusCode())
                && httpResponse.getRequest().getRequestMethod() == "HEAD") {
              httpStatistics.putIfAbsent(ACTION_HTTP_HEAD_REQUEST_FAILURES, new AtomicLong(0));
              httpStatistics.get(ACTION_HTTP_HEAD_REQUEST_FAILURES).incrementAndGet();
            } else if (!(httpResponse.isSuccessStatusCode())
                && httpResponse.getRequest().getRequestMethod() == "PUT") {
              httpStatistics.putIfAbsent(ACTION_HTTP_PUT_REQUEST_FAILURES, new AtomicLong(0));
              httpStatistics.get(ACTION_HTTP_PUT_REQUEST_FAILURES).incrementAndGet();
            } else if (!(httpResponse.isSuccessStatusCode())
                && httpResponse.getRequest().getRequestMethod() == "POST") {
              httpStatistics.putIfAbsent(ACTION_HTTP_POST_REQUEST_FAILURES, new AtomicLong(0));
              httpStatistics.get(ACTION_HTTP_POST_REQUEST_FAILURES).incrementAndGet();
            } else if (!(httpResponse.isSuccessStatusCode())
                && httpResponse.getRequest().getRequestMethod() == "PATCH") {
              httpStatistics.putIfAbsent(ACTION_HTTP_PATCH_REQUEST_FAILURES, new AtomicLong(0));
              httpStatistics.get(ACTION_HTTP_PATCH_REQUEST_FAILURES).incrementAndGet();
            } else if (!(httpResponse.isSuccessStatusCode())
                && httpResponse.getRequest().getRequestMethod() == "DELETE") {
              httpStatistics.putIfAbsent(ACTION_HTTP_DELETE_REQUEST_FAILURES, new AtomicLong(0));
              httpStatistics.get(ACTION_HTTP_DELETE_REQUEST_FAILURES).incrementAndGet();
            }
          }
        });
  }

  /** clear the statistics */
  public void reset() {
    httpStatistics.clear();
  }
}
