/*
 * Copyright 2021 Google LLC. All Rights Reserved.
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

import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpResponseInterceptor;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/** To track and update the statistics related to http requests */
class GcsioTrackingHttpRequestInitializer implements HttpRequestInitializer {

  private final HttpRequestInitializer delegate;

  // To track the http statistics
  private final ConcurrentHashMap<GoogleCloudStorageStatistics, AtomicLong> httpStatistics;

  public GcsioTrackingHttpRequestInitializer(
      ConcurrentHashMap<GoogleCloudStorageStatistics, AtomicLong> httpStatistics) {
    this.httpStatistics = httpStatistics;
  }

  @Override
  public void initialize(HttpRequest request) throws IOException {
    if (delegate != null) {
      delegate.initialize(request);
    }

    request.setResponseInterceptor(
        new HttpResponseInterceptor() {
          @Override
          public void interceptResponse(HttpResponse httpResponse) throws IOException {
            sethttprequestStats(httpResponse.getRequest());
            setHttpRequestFailureStats(httpResponse);
          }
        });
  }

  /**
   * For a given key, check if it is already in the current session's list of keys. If not present,
   * add it and increment the value
   *
   * @param key GoogleCloudStorageStatistics key for tracking http-related activity in gcsio
   */
  private void increment(GoogleCloudStorageStatistics key) {
    httpStatistics.putIfAbsent(key, new AtomicLong(0));
    httpStatistics.get(key).incrementAndGet();
  }

  /**
   * set the stats for the http requests
   *
   * @param r - request
   */
  private void sethttprequestStats(HttpRequest r) {
    if (r.getRequestMethod() == "GET") {
      increment(ACTION_HTTP_GET_REQUEST);
    } else if (r.getRequestMethod() == "HEAD") {
      increment(ACTION_HTTP_HEAD_REQUEST);
    } else if (r.getRequestMethod() == "PUT") {
      increment(ACTION_HTTP_PUT_REQUEST);
    } else if (r.getRequestMethod() == "POST") {
      increment(ACTION_HTTP_POST_REQUEST);
    } else if (r.getRequestMethod() == "PATCH") {
      increment(ACTION_HTTP_PATCH_REQUEST);
    } else if (r.getRequestMethod() == "DELETE") {
      increment(ACTION_HTTP_DELETE_REQUEST);
    }
  }

  /**
   * set the stats for http request failures
   *
   * @param httpResponse
   */
  private void setHttpRequestFailureStats(HttpResponse httpResponse) {
    if (!(httpResponse.isSuccessStatusCode())) {
      if (httpResponse.getRequest().getRequestMethod() == "GET") {
        increment(ACTION_HTTP_GET_REQUEST_FAILURES);
      } else if (httpResponse.getRequest().getRequestMethod() == "HEAD") {
        increment(ACTION_HTTP_HEAD_REQUEST_FAILURES);
      } else if (httpResponse.getRequest().getRequestMethod() == "PUT") {
        increment(ACTION_HTTP_PUT_REQUEST_FAILURES);
      } else if (httpResponse.getRequest().getRequestMethod() == "POST") {
        increment(ACTION_HTTP_POST_REQUEST_FAILURES);
      } else if (httpResponse.getRequest().getRequestMethod() == "PATCH") {
        increment(ACTION_HTTP_PATCH_REQUEST_FAILURES);
      } else if (httpResponse.getRequest().getRequestMethod() == "DELETE") {
        increment(ACTION_HTTP_DELETE_REQUEST_FAILURES);
      }
    }
  }

  /** clear the statistics */
  public void reset() {
    httpStatistics.clear();
  }
}
