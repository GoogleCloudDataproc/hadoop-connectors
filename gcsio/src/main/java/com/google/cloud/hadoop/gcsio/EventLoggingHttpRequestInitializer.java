/*
 * Copyright 2022 Google Inc. All Rights Reserved.
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

package com.google.cloud.hadoop.gcsio;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponse;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.collect.MapMaker;
import com.google.common.flogger.GoogleLogger;
import com.google.common.flogger.LazyArgs;
import com.google.gson.Gson;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * Implements a HttpRequestInitializer which adds a ResponseInterceptor to each HttpRequest and
 * tracks the response time taken by the successful HttpRequests.
 */
@VisibleForTesting
public class EventLoggingHttpRequestInitializer implements HttpRequestInitializer {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();
  private final Gson gson = new Gson();

  // Using a ConcurrentMap with weak key reference to avoid potential memory leak during failure
  // scenarios. We do not get an response interceptor callback if there is some failure while
  // executing the HttpRequest.
  private final ConcurrentMap<HttpRequest, HttpRequestResponseTimeTracker> requestTracker =
      new MapMaker().weakKeys().makeMap();

  @Override
  public void initialize(HttpRequest request) throws IOException {
    request.setInterceptor(
        httpRequest -> requestTracker.put(httpRequest, new HttpRequestResponseTimeTracker()));
    request.setResponseInterceptor(this::logAndRemoveRequestFromTracking);
  }

  @VisibleForTesting
  void logAndRemoveRequestFromTracking(HttpResponse httpResponse) {
    HttpRequest request = httpResponse.getRequest();

    Map<String, Object> jsonMap = new HashMap<>();
    jsonMap.put("request_method", request.getRequestMethod());
    jsonMap.put("request_url", request.getUrl().toString());
    jsonMap.put("request_headers", request.getHeaders());
    jsonMap.put("response_headers", httpResponse.getHeaders());
    jsonMap.put("response_status_code", httpResponse.getStatusCode());
    jsonMap.put("response_status_message", httpResponse.getStatusMessage());
    jsonMap.put("thread_name", Thread.currentThread().getName());

    HttpRequestResponseTimeTracker tracker = requestTracker.remove(request);
    if (tracker != null) {
      jsonMap.put("response_time", tracker.getResponseTime());
      jsonMap.put("request_start_time_utc", tracker.getStartTimeUtc());
      jsonMap.put("request_finish_time_utc", tracker.getCurrentTimeUtc());
    } else {
      jsonMap.put("response_time", Integer.MAX_VALUE);
      jsonMap.put("unexpected_error", "Unknown request. This is unexpected.");
    }

    logDetails(jsonMap);
  }

  private void logDetails(Map<String, Object> jsonMap) {
    logger.atInfo().log("%s", LazyArgs.lazy(() -> gson.toJson(jsonMap)));
  }

  private static class HttpRequestResponseTimeTracker {
    private static final DateTimeFormatter dateTimeFormatter =
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX").withZone(ZoneOffset.UTC);

    private final Stopwatch stopwatch = Stopwatch.createStarted();
    private final Instant startTime = Instant.now();

    long getResponseTime() {
      return stopwatch.elapsed(MILLISECONDS);
    }

    String getStartTimeUtc() {
      return dateTimeFormatter.format(startTime);
    }

    String getCurrentTimeUtc() {
      return dateTimeFormatter.format(Instant.now());
    }
  }
}
