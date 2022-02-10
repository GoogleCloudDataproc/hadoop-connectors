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

import static com.google.common.collect.ImmutableMap.toImmutableMap;

import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.common.base.Ascii;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/** Tracks and updates the statistics related to HTTP requests */
class StatisticsTrackingHttpRequestInitializer implements HttpRequestInitializer {

  private final ConcurrentMap<String, AtomicLong> statistics = new ConcurrentHashMap<>();

  @Override
  public void initialize(HttpRequest request) throws IOException {
    request.setResponseInterceptor(
        httpResponse -> {
          String requestMethod = Ascii.toLowerCase(httpResponse.getRequest().getRequestMethod());
          int statusCode = httpResponse.getStatusCode();
          statistics
              .computeIfAbsent("http_" + requestMethod + "_" + statusCode, k -> new AtomicLong())
              .incrementAndGet();
        });
  }

  public ImmutableMap<String, Long> getStatistics() {
    return statistics.entrySet().stream()
        .collect(toImmutableMap(Map.Entry::getKey, e -> e.getValue().get()));
  }
}
