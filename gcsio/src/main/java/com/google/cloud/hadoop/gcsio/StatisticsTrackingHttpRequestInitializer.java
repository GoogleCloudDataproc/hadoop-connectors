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

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.HTTP_DELETE_REQUEST;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.HTTP_DELETE_REQUEST_FAILURE;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.HTTP_GET_REQUEST;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.HTTP_GET_REQUEST_FAILURE;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.HTTP_HEAD_REQUEST;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.HTTP_HEAD_REQUEST_FAILURE;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.HTTP_PATCH_REQUEST;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.HTTP_PATCH_REQUEST_FAILURE;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.HTTP_POST_REQUEST;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.HTTP_POST_REQUEST_FAILURE;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.HTTP_PUT_REQUEST;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.HTTP_PUT_REQUEST_FAILURE;

import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponse;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/** To track and update the statistics related to http requests */
class StatisticsTrackingHttpRequestInitializer implements HttpRequestInitializer {

  private final ImmutableMap<GoogleCloudStorageStatistics, AtomicLong> statistics;

  public StatisticsTrackingHttpRequestInitializer(
      ImmutableMap<GoogleCloudStorageStatistics, AtomicLong> statistics) {
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
    switch (request.getRequestMethod()) {
      case "DELETE":
        increment(HTTP_DELETE_REQUEST);
        break;
      case "GET":
        increment(HTTP_GET_REQUEST);
        break;
      case "HEAD":
        increment(HTTP_HEAD_REQUEST);
        break;
      case "PATCH":
        increment(HTTP_PATCH_REQUEST);
        break;
      case "POST":
        increment(HTTP_POST_REQUEST);
        break;
      case "PUT":
        increment(HTTP_PUT_REQUEST);
        break;
      default:
        throw new IllegalStateException("Unknown request method: " + request.getRequestMethod());
    }
  }

  /** Set stats for the HTTP request failures */
  private void setHttpRequestFailureStats(HttpResponse response) {
    if (response.isSuccessStatusCode()) {
      return;
    }

    HttpRequest request = response.getRequest();
    switch (request.getRequestMethod()) {
      case "DELETE":
        increment(HTTP_DELETE_REQUEST_FAILURE);
        break;
      case "GET":
        increment(HTTP_GET_REQUEST_FAILURE);
        break;
      case "HEAD":
        increment(HTTP_HEAD_REQUEST_FAILURE);
        break;
      case "PATCH":
        increment(HTTP_PATCH_REQUEST_FAILURE);
        break;
      case "POST":
        increment(HTTP_POST_REQUEST_FAILURE);
        break;
      case "PUT":
        increment(HTTP_PUT_REQUEST_FAILURE);
        break;
      default:
        throw new IllegalStateException("Unknown request method: " + request.getRequestMethod());
    }
  }

  /** Increment the value for a given key. */
  private void increment(GoogleCloudStorageStatistics key) {
    statistics.get(key).incrementAndGet();
  }
}
