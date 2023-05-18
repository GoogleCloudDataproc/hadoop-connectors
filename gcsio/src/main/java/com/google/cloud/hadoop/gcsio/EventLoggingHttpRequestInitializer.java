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

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static com.google.common.net.HttpHeaders.COOKIE;
import static com.google.common.net.HttpHeaders.PROXY_AUTHORIZATION;
import static com.google.common.net.HttpHeaders.SET_COOKIE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponse;
import com.google.cloud.hadoop.util.interceptors.InvocationIdInterceptor;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MapMaker;
import com.google.common.flogger.GoogleLogger;
import com.google.common.flogger.LazyArgs;
import com.google.gson.Gson;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;

/**
 * Implements a HttpRequestInitializer which adds a ResponseInterceptor to each HttpRequest and
 * tracks the response time taken by the successful HttpRequests.
 */
@VisibleForTesting
public class EventLoggingHttpRequestInitializer implements HttpRequestInitializer {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();
  private final Gson gson = new Gson();

  private static final ImmutableSet<String> REDACTED_REQUEST_HEADERS =
      Stream.of(AUTHORIZATION, COOKIE, PROXY_AUTHORIZATION)
          .map(header -> header.toLowerCase(Locale.US))
          .collect(toImmutableSet());

  private static final ImmutableSet<String> REDACTED_RESPONSE_HEADERS =
      ImmutableSet.of(SET_COOKIE.toLowerCase(Locale.US));

  // Using a ConcurrentMap with weak key reference to avoid potential memory leak during failure
  // scenarios. We do not get an response interceptor callback if there is some failure while
  // executing the HttpRequest.
  private final ConcurrentMap<HttpRequest, HttpRequestResponseTimeTracker> requestTracker =
      new MapMaker().weakKeys().makeMap();
  private final long timeThresholdMs;
  private final ImmutableSet<String> filterProperties;

  public EventLoggingHttpRequestInitializer() {
    this(0L, ImmutableSet.of());
  }

  public EventLoggingHttpRequestInitializer(
      long timeThresholdMs, ImmutableSet<String> filterProperties) {

    this.timeThresholdMs = timeThresholdMs;
    this.filterProperties = filterProperties;
  }

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
    HttpRequestResponseTimeTracker tracker = requestTracker.remove(request);
    if (tracker != null) {
      long responseTime = tracker.getResponseTime();
      if (responseTime < this.timeThresholdMs) {
        return;
      }

      addLogProperty("response_time", responseTime, jsonMap);
      addLogProperty("request_start_time_utc", tracker.getStartTimeUtc(), jsonMap);
      addLogProperty("request_finish_time_utc", tracker.getCurrentTimeUtc(), jsonMap);
    } else {
      addLogProperty("response_time", Integer.MAX_VALUE, jsonMap);
      addLogProperty("unexpected_error", "Unknown request. This is unexpected.", jsonMap);
    }

    addLogProperty("request_method", request.getRequestMethod(), jsonMap);
    addLogProperty("request_url", request.getUrl().toString(), jsonMap);
    addLogProperty(
        "request_headers", filterMap(request.getHeaders(), REDACTED_REQUEST_HEADERS), jsonMap);
    addLogProperty(
        "response_headers",
        filterMap(httpResponse.getHeaders(), REDACTED_RESPONSE_HEADERS),
        jsonMap);
    addLogProperty("response_status_code", httpResponse.getStatusCode(), jsonMap);
    addLogProperty("response_status_message", httpResponse.getStatusMessage(), jsonMap);
    addLogProperty("thread_name", Thread.currentThread().getName(), jsonMap);
    addLogProperty(InvocationIdInterceptor.GOOG_API_CLIENT, getInvocationId(request), jsonMap);

    logDetails(jsonMap);
  }

  private Object getInvocationId(HttpRequest request) {
    HttpHeaders headers = request.getHeaders();
    if (headers == null) {
      return "";
    }

    return headers.get(InvocationIdInterceptor.GOOG_API_CLIENT);
  }

  private void addLogProperty(String propertyName, Object value, Map<String, Object> jsonMap) {
    if (!filterProperties.contains(propertyName.toLowerCase(Locale.US))) {
      jsonMap.put(propertyName, value);
    }
  }

  private void logDetails(Map<String, Object> jsonMap) {
    logger.atInfo().log("%s", LazyArgs.lazy(() -> gson.toJson(jsonMap)));
  }

  private static ImmutableMap<String, Object> filterMap(
      HttpHeaders httpHeaders, Set<String> filterSet) {
    return httpHeaders.entrySet().stream()
        .filter(e -> !filterSet.contains(e.getKey().toLowerCase(Locale.US)))
        .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
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
