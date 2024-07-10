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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpResponse;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

class GcsJsonApiEvent {
  public enum EventType {
    BACKOFF,
    STARTED,
    RETRYSKIPPED,
    EXCEPTION,
    RESPONSE,
  }

  public static final String BACKOFFTIME = "BACKOFFTIME";
  public static final String RETRYCOUNT = "RETRYCOUNT";
  public static final String STATUS_CODE = "STATUS_CODE";
  public static final String DURATION = "DURATION";
  private final EventType eventType;

  // Setting this to object so that we do not have to create the URL string.
  private final Object context;

  private final String method;

  private Map<String, Object> properties;

  static GcsJsonApiEvent getResponseEvent(HttpResponse httpResponse, @Nonnegative long duration) {
    GcsJsonApiEvent result = new GcsJsonApiEvent(httpResponse.getRequest(), EventType.RESPONSE, 2);
    result.set(STATUS_CODE, httpResponse.getStatusCode());
    result.set(DURATION, duration);

    return result;
  }

  static GcsJsonApiEvent getRequestStartedEvent(HttpRequest request) {
    return new GcsJsonApiEvent(request, EventType.STARTED);
  }

  static GcsJsonApiEvent getExceptionEvent(HttpRequest httpRequest) {
    return new GcsJsonApiEvent(httpRequest, EventType.EXCEPTION);
  }

  static GcsJsonApiEvent getBackoffEvent(
      HttpRequest request, @Nonnegative long backOffTime, @Nonnegative int retryCount) {
    return new GcsJsonApiEvent(request, EventType.BACKOFF, 2)
        .set(BACKOFFTIME, backOffTime)
        .set(RETRYCOUNT, retryCount);
  }

  @VisibleForTesting
  GcsJsonApiEvent(@Nonnull HttpRequest request, EventType eventType) {
    this.eventType = eventType;
    this.context = request.getUrl();
    this.method = request.getRequestMethod();
  }

  EventType getEventType() {
    return eventType;
  }

  Object getContext() {
    return context;
  }

  String getMethod() {
    return method;
  }

  Object getProperty(String key) {
    return properties == null ? null : properties.get(key);
  }

  private GcsJsonApiEvent(HttpRequest request, EventType eventType, int capacity) {
    this(request, eventType);
    this.properties = new HashMap<>(capacity);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("method", method)
        .add("type", eventType)
        .add("properties", properties)
        .add("context", context)
        .toString();
  }

  private GcsJsonApiEvent set(String key, Object value) {
    checkArgument(properties != null, "properties cannot be null");

    this.properties.put(key, value);
    return this;
  }
}
