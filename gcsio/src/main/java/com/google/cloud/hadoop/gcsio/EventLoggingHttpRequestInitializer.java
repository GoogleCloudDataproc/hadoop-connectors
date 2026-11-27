/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.gcsio;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static com.google.common.net.HttpHeaders.COOKIE;
import static com.google.common.net.HttpHeaders.LOCATION;
import static com.google.common.net.HttpHeaders.PROXY_AUTHORIZATION;
import static com.google.common.net.HttpHeaders.SET_COOKIE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponse;
import com.google.cloud.hadoop.util.LoggingUrlRedactor;
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
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 * Implements a HttpRequestInitializer which adds a ResponseInterceptor to each HttpRequest and
 * tracks the response time taken by the successful HttpRequests.
 */
@VisibleForTesting
public class EventLoggingHttpRequestInitializer implements HttpRequestInitializer {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();
  private final Gson gson = new Gson();

  private static final String UPLOAD_ID_RESPONSE_HEADER = "x-guploader-uploadid";
  private static final String UPLOAD_URL_RESPONSE_HEADER = "x-goog-upload-url";
  private static final String UPLOAD_CONTROL_URL_RESPONSE_HEADER = "x-goog-upload-control-url";

  // Request or response headers that mark an HTTP exchange as part of a resumable upload.
  private static final ImmutableSet<String> UPLOAD_INDICATOR_HEADERS =
      ImmutableSet.of(
          "x-goog-resumable",
          "x-goog-upload-protocol",
          "x-goog-upload-command",
          UPLOAD_URL_RESPONSE_HEADER,
          UPLOAD_CONTROL_URL_RESPONSE_HEADER);

  private static final ImmutableSet<String> REDACTED_REQUEST_HEADERS =
      Stream.of(AUTHORIZATION, COOKIE, PROXY_AUTHORIZATION)
          .map(header -> header.toLowerCase(Locale.US))
          .collect(toImmutableSet());

  private static final ImmutableSet<String> REDACTED_RESPONSE_HEADERS =
      ImmutableSet.of(SET_COOKIE.toLowerCase(Locale.US));

  // Response headers that carry URLs (redirect targets and upload session URLs). They are logged
  // with their query parameters redacted.
  private static final ImmutableSet<String> URL_RESPONSE_HEADERS =
      Stream.of(LOCATION, UPLOAD_URL_RESPONSE_HEADER, UPLOAD_CONTROL_URL_RESPONSE_HEADER)
          .map(header -> header.toLowerCase(Locale.US))
          .collect(toImmutableSet());

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
    HttpRequestResponseTimeTracker tracker = requestTracker.remove(request);
    if (!logger.atInfo().isEnabled()) {
      return;
    }

    Map<String, Object> jsonMap = new HashMap<>();
    jsonMap.put("request_method", request.getRequestMethod());
    jsonMap.put("request_url", LoggingUrlRedactor.redactUrl(request.getUrl()));
    jsonMap.put("request_headers", filterMap(request.getHeaders(), REDACTED_REQUEST_HEADERS));
    jsonMap.put("response_headers", filterResponseHeaders(request, httpResponse));
    jsonMap.put("response_status_code", httpResponse.getStatusCode());
    jsonMap.put("response_status_message", httpResponse.getStatusMessage());
    jsonMap.put("thread_name", Thread.currentThread().getName());

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

  private static ImmutableMap<String, Object> filterMap(
      @Nullable HttpHeaders httpHeaders, Set<String> filterSet) {
    if (httpHeaders == null) {
      return ImmutableMap.of();
    }
    return httpHeaders.entrySet().stream()
        .filter(
            e ->
                e.getKey() != null
                    && e.getValue() != null
                    && !filterSet.contains(e.getKey().toLowerCase(Locale.US)))
        .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private static ImmutableMap<String, Object> filterResponseHeaders(
      HttpRequest request, HttpResponse httpResponse) {
    HttpHeaders responseHeaders = httpResponse.getHeaders();
    if (responseHeaders == null) {
      return ImmutableMap.of();
    }
    boolean digestUploadId = isUploadExchange(request, responseHeaders);
    return responseHeaders.entrySet().stream()
        .filter(
            e ->
                e.getKey() != null
                    && e.getValue() != null
                    && !REDACTED_RESPONSE_HEADERS.contains(e.getKey().toLowerCase(Locale.US)))
        .collect(
            toImmutableMap(
                Map.Entry::getKey,
                e -> formatResponseHeaderValue(e.getKey(), e.getValue(), digestUploadId)));
  }

  private static Object formatResponseHeaderValue(
      String name, Object value, boolean digestUploadId) {
    String lowerCaseName = name.toLowerCase(Locale.US);
    if (URL_RESPONSE_HEADERS.contains(lowerCaseName)) {
      return mapHeaderValue(value, LoggingUrlRedactor::redactUrlString);
    }
    if (digestUploadId && UPLOAD_ID_RESPONSE_HEADER.equals(lowerCaseName)) {
      return digestHeaderValue(value);
    }
    return value;
  }

  // x-guploader-uploadid holds the upload session ID only on resumable upload exchanges. On other
  // requests it is a trace ID that Cloud Storage support uses to look up the request.
  private static boolean isUploadExchange(HttpRequest request, HttpHeaders responseHeaders) {
    return hasUploadQueryParam(request.getUrl())
        || hasUploadHeader(request.getHeaders())
        || hasUploadHeader(responseHeaders);
  }

  private static boolean hasUploadQueryParam(@Nullable GenericUrl url) {
    if (url == null) {
      return false;
    }
    for (String key : url.keySet()) {
      if ("upload_id".equalsIgnoreCase(key) || "uploadtype".equalsIgnoreCase(key)) {
        return true;
      }
    }
    return false;
  }

  private static boolean hasUploadHeader(@Nullable HttpHeaders headers) {
    if (headers == null) {
      return false;
    }
    for (String header : UPLOAD_INDICATOR_HEADERS) {
      if (headers.getFirstHeaderStringValue(header) != null) {
        return true;
      }
    }
    return false;
  }

  @VisibleForTesting
  @Nullable
  static Object digestHeaderValue(@Nullable Object value) {
    return mapHeaderValue(value, LoggingUrlRedactor::digestToken);
  }

  @Nullable
  private static Object mapHeaderValue(@Nullable Object value, Function<String, String> mapper) {
    if (value == null) {
      return null;
    }
    if (value instanceof Collection<?>) {
      return ((Collection<?>) value)
          .stream()
              .map(item -> item == null ? null : mapper.apply(item.toString()))
              .collect(toList());
    }
    return mapper.apply(value.toString());
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
