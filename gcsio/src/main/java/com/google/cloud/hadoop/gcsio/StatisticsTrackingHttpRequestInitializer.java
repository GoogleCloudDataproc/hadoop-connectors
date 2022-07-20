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

import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponse;
import com.google.common.base.Ascii;
import java.io.IOException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/** To track and update the statistics related to http requests */
class StatisticsTrackingHttpRequestInitializer implements HttpRequestInitializer {

  private final ConcurrentMap<String, AtomicLong> statistics;

  public StatisticsTrackingHttpRequestInitializer(ConcurrentMap<String, AtomicLong> statistics) {
    this.statistics = statistics;
  }

  @Override
  public void initialize(HttpRequest request) throws IOException {
    request.setResponseInterceptor(
        httpResponse -> {
          setHttpRequestStats(httpResponse.getRequest());
          setHttpRequestFailureStats(httpResponse);
        });
  }

  /** Set HTTP request stats */
  private void setHttpRequestStats(HttpRequest request) {
    String requestMethod = Ascii.toUpperCase(request.getRequestMethod());
    increment("HTTP_" + requestMethod + "_REQUEST");
  }

  /** Set stats for the HTTP request failures */
  private void setHttpRequestFailureStats(HttpResponse response) {
    if (response.isSuccessStatusCode()) {
      return;
    }

    String requestMethod = Ascii.toUpperCase(response.getRequest().getRequestMethod());
    increment("HTTP_" + requestMethod + "_REQUEST_FAILURE");
  }

  /** Increment the value for a given key. */
  private void increment(String key) {
    statistics.computeIfAbsent(key, __ -> new AtomicLong(0)).incrementAndGet();
  }
}
