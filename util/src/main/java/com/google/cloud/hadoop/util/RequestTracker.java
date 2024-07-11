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

import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpResponse;
import com.google.common.base.MoreObjects;
import com.google.common.base.Stopwatch;
import com.google.common.flogger.GoogleLogger;
import java.util.concurrent.TimeUnit;

class RequestTracker {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();
  private static final long LOGGING_THRESHOLD = 200;
  private Stopwatch stopWatch;
  private Object context;
  private int retryCount;
  private long backOffTime;
  private HttpRequest request;

  protected RequestTracker() {}

  public static RequestTracker create(HttpRequest request) {
    return new RequestTracker().init(request);
  }

  void trackResponse(HttpResponse response) {
    // The response might have been already tracked. For eg. if we get an unsuccessful response and
    // it given up after the configured retries, RetryHttpRequestInitializer response interceptor
    // will also get called.
    if (stopWatch.isRunning()) {
      postToEventQueue(GcsJsonApiEvent.getResponseEvent(response, stopWatch.elapsed().toMillis()));
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
    postToEventQueue(GcsJsonApiEvent.getExceptionEvent(request));
  }

  void trackUnsuccessfulResponseHandler(HttpResponse response) {
    stopTracking();
    postToEventQueue(GcsJsonApiEvent.getResponseEvent(response, stopWatch.elapsed().toMillis()));
  }

  void trackBackOffCompleted(long backOffStartTime) {
    long diff = System.currentTimeMillis() - backOffStartTime;
    postToEventQueue(GcsJsonApiEvent.getBackoffEvent(request, diff, retryCount));
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

  protected RequestTracker init(HttpRequest request) {
    stopWatch = Stopwatch.createStarted();
    context = request.getUrl();
    this.request = request;

    postToEventQueue(GcsJsonApiEvent.getRequestStartedEvent(request));

    return this;
  }

  private void stopTracking() {
    if (stopWatch.isRunning()) {
      stopWatch.stop();

      if (stopWatch.elapsed().toMillis() > LOGGING_THRESHOLD) {
        logger.atInfo().atMostEvery(10, TimeUnit.SECONDS).log(
            "Detected high latency for %s. duration=%s",
            request.getUrl(), stopWatch.elapsed().toMillis());
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
        .add("totalBackoffTime", backOffTime)
        .add("context", context)
        .toString();
  }
}
