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

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.ACTION_HTTP_GET_REQUEST;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.ACTION_HTTP_GET_REQUEST_FAILURES;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.ACTION_HTTP_HEAD_REQUEST;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.ACTION_HTTP_HEAD_REQUEST_FAILURES;

import com.google.api.client.http.HttpExecuteInterceptor;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpResponseInterceptor;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class GcsioTrackingHttpRequestInitializer implements HttpRequestInitializer {

  private final HttpRequestInitializer delegate;

  private final List<HttpRequest> requests = Collections.synchronizedList(new ArrayList<>());

  // To track the http statistics
  private HashMap<GoogleCloudStorageStatistics, AtomicLong> httpStatistics =
      new HashMap<GoogleCloudStorageStatistics, AtomicLong>();

  public GcsioTrackingHttpRequestInitializer(HttpRequestInitializer delegate) {
    this.delegate = delegate;
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
          requests.add(r);
          if (r.getRequestMethod() == "GET") {
            httpStatistics.putIfAbsent(
                GoogleCloudStorageStatistics.ACTION_HTTP_GET_REQUEST, new AtomicLong(0));
            httpStatistics.get(ACTION_HTTP_GET_REQUEST).incrementAndGet();
          } else if (r.getRequestMethod() == "HEAD") {
            httpStatistics.putIfAbsent(
                GoogleCloudStorageStatistics.ACTION_HTTP_HEAD_REQUEST, new AtomicLong(0));
            httpStatistics.get(ACTION_HTTP_HEAD_REQUEST).incrementAndGet();
          }
        });
    request.setResponseInterceptor(
        new HttpResponseInterceptor() {
          @Override
          public void interceptResponse(HttpResponse httpResponse) throws IOException {
            if (!(httpResponse.isSuccessStatusCode())
                && httpResponse.getRequest().getRequestMethod() == "GET") {
              httpStatistics.putIfAbsent(
                  GoogleCloudStorageStatistics.ACTION_HTTP_GET_REQUEST_FAILURES, new AtomicLong(0));
              httpStatistics.get(ACTION_HTTP_GET_REQUEST_FAILURES).incrementAndGet();
            } else if (!(httpResponse.isSuccessStatusCode())
                && httpResponse.getRequest().getRequestMethod() == "HEAD") {
              httpStatistics.putIfAbsent(
                  GoogleCloudStorageStatistics.ACTION_HTTP_HEAD_REQUEST_FAILURES,
                  new AtomicLong(0));
              httpStatistics.get(ACTION_HTTP_HEAD_REQUEST_FAILURES).incrementAndGet();
            }
          }
        });
  }

  public AtomicLong getHttpStatistics(GoogleCloudStorageStatistics key) {
    return httpStatistics.get(key);
  }

  public void reset() {
    requests.clear();
  }
}
