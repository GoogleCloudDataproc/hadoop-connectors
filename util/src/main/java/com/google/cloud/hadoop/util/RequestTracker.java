/*
 * Copyright 2024 Google Inc.
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

package com.google.cloud.hadoop.util;

import static com.google.cloud.hadoop.util.interceptors.InvocationIdInterceptor.GOOG_API_CLIENT;

import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpResponse;
import com.google.common.base.MoreObjects;
import com.google.common.base.Stopwatch;
import com.google.common.flogger.GoogleLogger;
import java.util.concurrent.TimeUnit;

public class RequestTracker {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();
  private static final long LOGGING_THRESHOLD = 200;
  private Stopwatch stopWatch;
  private Object context;
  private int retryCount;
  private long backOffTime;
  private HttpRequest request;
  private final long startTime = System.currentTimeMillis();
  private boolean initialized;

  protected RequestTracker() {}

  public static RequestTracker create(HttpRequest request) {
    return new RequestTracker();
  }

  void trackResponse(HttpResponse response) {
    // The response might have been already tracked. For eg. if we get an unsuccessful response and
    // it given up after the configured retries, RetryHttpRequestInitializer response interceptor
    // will also get called.
    if (stopWatch.isRunning()) {
      postToEventQueue(
          GcsJsonApiEvent.getResponseEvent(response, stopWatch.elapsed().toMillis(), context));
      stopTracking();
    }

    if (retryCount != 0) {
      // Change to minute
      logger.atInfo().atMostEvery(10, TimeUnit.SECONDS).log(
          "Operation completed after retries with code '%s'. %s", response.getStatusCode(), this);
    }
  }

  void trackIOException() {
    stopTracking();
    postToEventQueue(GcsJsonApiEvent.getExceptionEvent(request, context));
  }

  void trackUnsuccessfulResponseHandler(HttpResponse response) {
    stopTracking();
    postToEventQueue(
        GcsJsonApiEvent.getResponseEvent(response, stopWatch.elapsed().toMillis(), context));
  }

  void trackBackOffCompleted(long backOffStartTime) {
    long diff = System.currentTimeMillis() - backOffStartTime;
    postToEventQueue(GcsJsonApiEvent.getBackoffEvent(request, diff, retryCount, context));
    backOffTime += diff;
  }

  void trackRetryStarted() {
    stopWatch.reset();
    stopWatch.start();
    retryCount++;
  }

  void trackRetrySkipped(boolean hasResponse) {
    if (!hasResponse && this.retryCount != 0) {
      logger.atInfo().atMostEvery(10, TimeUnit.SECONDS).log(
          "Retry skipped after %s retries. context=%s", retryCount, this);
    }
  }

  protected void postToEventQueue(GcsJsonApiEvent event) {
    GoogleCloudStorageEventBus.postGcsJsonApiEvent(event);
  }

  public RequestTracker init(HttpRequest request) {
    if (this.initialized) {
      // Can be called multiple times in case of request retries.
      return this;
    }

    this.initialized = true;
    stopWatch = Stopwatch.createStarted();
    context = new RequestContext(request);
    this.request = request;

    postToEventQueue(GcsJsonApiEvent.getRequestStartedEvent(request, context));

    return this;
  }

  private void stopTracking() {
    if (stopWatch.isRunning()) {
      stopWatch.stop();

      if (stopWatch.elapsed().toMillis() > LOGGING_THRESHOLD) {
        logger.atInfo().atMostEvery(10, TimeUnit.SECONDS).log(
            "Detected high latency for %s. durationMs=%s; method=%s; thread=%s",
            context,
            stopWatch.elapsed().toMillis(),
            request.getRequestMethod(),
            Thread.currentThread().getName());
      }
    } else {
      // Control can reach here only in case of a bug. Did not want to add an assert due to huge
      // blast radius.
      logger.atWarning().atMostEvery(1, TimeUnit.MINUTES).log(
          "Can stop only an already executing request. details=%s", this);
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("retryCount", retryCount)
        .add("totalBackoffTimeMs", backOffTime)
        .add("context", context)
        .add("method", request.getRequestMethod())
        .add("elapsedMs", System.currentTimeMillis() - startTime)
        .toString();
  }

  // Having this class so that the cost of creating the string for logging purposes are paid only if
  // we actually do the logging.
  private static class RequestContext {
    private final HttpRequest request;

    public RequestContext(HttpRequest request) {
      this.request = request;
    }

    @Override
    public String toString() {
      return String.format(
          "[url=%s; invocationId=%s]", request.getUrl(), request.getHeaders().get(GOOG_API_CLIENT));
    }
  }
}
