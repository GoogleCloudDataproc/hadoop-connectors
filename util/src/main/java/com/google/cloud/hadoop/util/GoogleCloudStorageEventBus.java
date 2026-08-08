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

import com.google.common.eventbus.EventBus;
import io.grpc.Status;
import java.io.IOException;

/** Event Bus class */
public class GoogleCloudStorageEventBus {

  /** Hold the instance of the event bus here */
  private static EventBus eventBus = new EventBus();

  private static IOException exception = new IOException();
  private static GCSChecksumFailureEvent checksumFailureEvent = new GCSChecksumFailureEvent();

  /**
   * Method to register an obj to event bus
   *
   * @param obj to register to event bus
   */
  public static void register(Object obj) {
    eventBus.register(obj);
  }

  /**
   * Method to unregister an obj to event bus
   *
   * @param obj to unregister from event bus
   * @throws IllegalArgumentException if the object was not previously registered.
   */
  public static void unregister(Object obj) {
    eventBus.unregister(obj);
  }

  /**
   * Posting Gcs request execution event i.e. request to gcs is being initiated.
   *
   * @param event dummy event to map to request execution type.
   */
  public static void onGcsRequest(GcsRequestExecutionEvent event) {
    eventBus.post(event);
  }

  /**
   * Posting Exception to invoke corresponding Subscriber method. Passing a dummy exception as
   * EventBus has @ElementTypesAreNonnullByDefault annotation.
   */
  public static void postOnException() {
    eventBus.post(exception);
  }

  /**
   * Posting grpc Status to invoke the corresponding Subscriber method.
   *
   * @param status status object of grpc response
   */
  public static void onGrpcStatus(Status status) {
    eventBus.post(status);
  }

  public static void postGcsJsonApiEvent(IGcsJsonApiEvent gcsJsonApiEvent) {
    eventBus.post(gcsJsonApiEvent);
  }

  public static void postWriteChecksumFailure() {
    eventBus.post(checksumFailureEvent);
  }
}
