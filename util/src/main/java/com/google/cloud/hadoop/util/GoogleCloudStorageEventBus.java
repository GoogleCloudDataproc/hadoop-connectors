/*
 * Copyright 2023 Google Inc.
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

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpResponse;
import com.google.common.eventbus.EventBus;
import java.io.IOException;

/** Event Bus class */
public class GoogleCloudStorageEventBus {

  /** Hold the instance of the event bus here */
  private static EventBus eventBus = new EventBus();

  private static IOException exception = new IOException();

  /**
   * Method to register an obj to event bus
   *
   * @param obj to register to event bus
   */
  public static void register(Object obj) {
    eventBus.register(obj);
  }

  /**
   * Posting GoogleJsonResponseException to invoke corresponding Subscriber method.
   *
   * @param response contains statusCode based on which metrics are updated in Subscriber method
   */
  public static void postOnGoogleJsonResponseException(GoogleJsonResponseException response) {
    eventBus.post(response);
  }

  /**
   * Posting HttpResponse to invoke corresponding Subscriber method.
   *
   * @param response contains statusCode based on which metrics are updated in Subscriber method
   */
  public static void postOnHttpResponse(HttpResponse response) {
    eventBus.post(response);
  }

  /**
   * Posting HttpRequest to invoke corresponding Subscriber method.
   *
   * @param request based on which metrics are updated in Subscriber method
   */
  public static void postOnHttpRequest(HttpRequest request) {
    eventBus.post(request);
  }

  /**
   * Posting Exception to invoke corresponding Subscriber method. Passing a dummy exception as
   * EventBus has @ElementTypesAreNonnullByDefault annotation.
   */
  public static void postOnException() {
    eventBus.post(exception);
  }
}
