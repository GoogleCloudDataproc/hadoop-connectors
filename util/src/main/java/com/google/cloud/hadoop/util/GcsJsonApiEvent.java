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

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpMethods;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpResponse;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

public class GcsJsonApiEvent implements IGcsJsonApiEvent {
  public static final String BACKOFF_TIME = "BACKOFF_TIME";
  public static final String RETRY_COUNT = "RETRY_COUNT";
  public static final String STATUS_CODE = "STATUS_CODE";
  public static final String DURATION = "DURATION";
  public static final String REQUEST_TYPE = "REQUEST_TYPE";
  private final EventType eventType;

  // Having this as Object type so that we do not have to create the URL string.
  private final Object context;
  private final String method;
  private Map<String, Object> properties;

  @VisibleForTesting
  protected GcsJsonApiEvent(@Nonnull HttpRequest request, EventType eventType, Object context) {
    this.eventType = eventType;
    this.context = context;
    this.method = request.getRequestMethod();
  }

  private GcsJsonApiEvent(HttpRequest request, EventType eventType, int capacity, Object context) {
    this(request, eventType, context);
    this.properties = new HashMap<>(capacity, 1);
  }

  @VisibleForTesting
  public static GcsJsonApiEvent getResponseEvent(
      HttpResponse httpResponse, @Nonnegative long duration, Object context) {
    GcsJsonApiEvent result =
        new GcsJsonApiEvent(httpResponse.getRequest(), EventType.RESPONSE, 3, context);
    result.set(STATUS_CODE, httpResponse.getStatusCode());
    result.set(DURATION, duration);
    result.set(REQUEST_TYPE, getRequestType(httpResponse));

    return result;
  }

  static GcsJsonApiEvent getRequestStartedEvent(HttpRequest request, Object context) {
    return new GcsJsonApiEvent(request, EventType.STARTED, context);
  }

  static GcsJsonApiEvent getExceptionEvent(HttpRequest httpRequest, Object context) {
    return new GcsJsonApiEvent(httpRequest, EventType.EXCEPTION, context);
  }

  static GcsJsonApiEvent getBackoffEvent(
      HttpRequest request,
      @Nonnegative long backOffTime,
      @Nonnegative int retryCount,
      Object context) {
    return new GcsJsonApiEvent(request, EventType.BACKOFF, 2, context)
        .set(BACKOFF_TIME, backOffTime)
        .set(RETRY_COUNT, retryCount);
  }

  public EventType getEventType() {
    return eventType;
  }

  public Object getContext() {
    return context;
  }

  public String getMethod() {
    return method;
  }

  public Object getProperty(String key) {
    return properties == null ? null : properties.get(key);
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

  private static RequestType getRequestType(HttpResponse httpResponse) {
    HttpRequest request = httpResponse.getRequest();
    String method = request.getRequestMethod();
    if (HttpMethods.GET.equals(method)) {
      GenericUrl url = request.getUrl();

      // This not a clean way to classify the type of GET operation. Going with this approach since
      // it is simpler and get the job done.
      Object maxResult = url.getFirst("maxResults");
      if (maxResult != null) {
        String maxResultString = maxResult.toString();
        if ("1".equals(maxResultString)) {
          return RequestType.LIST_FILE;
        }

        return RequestType.LIST_DIR;
      }

      Object fields = url.getFirst("fields");
      if (fields != null && fields.toString().contains("metadata")) {
        return RequestType.GET_METADATA;
      }

      Object alt = url.getFirst("alt");
      if (alt != null && "media".equals(alt.toString())) {
        return RequestType.GET_MEDIA;
      }

      return RequestType.GET_OTHER;
    } else if (HttpMethods.DELETE.equals(method)) {
      return RequestType.DELETE;
    } else if (HttpMethods.PUT.equals(method)) {
      return RequestType.PUT;
    } else if (HttpMethods.POST.equals(method)) {
      return RequestType.POST;
    } else if (HttpMethods.PATCH.equals(method)) {
      return RequestType.PATCH;
    }

    return RequestType.OTHER;
  }

  public enum EventType {
    BACKOFF,
    EXCEPTION,
    RESPONSE,
    RETRY_SKIPPED,
    STARTED,
  }

  public enum RequestType {
    DELETE,
    GET_MEDIA,
    GET_METADATA,
    GET_OTHER,
    LIST_DIR,
    LIST_FILE,
    OTHER,
    PATCH,
    POST,
    PUT
  }
}
