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

import static com.google.common.truth.Truth.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestRequestTracker extends RequestTracker {
  private final List<GcsJsonApiEvent> events = new ArrayList<>();

  public List<GcsJsonApiEvent> getEvents() {
    return events;
  }

  @Override
  protected void postToEventQueue(GcsJsonApiEvent event) {
    this.events.add(event);
  }

  void verifyEvents(List<ExpectedEventDetails> expectedEvents) {
    List<GcsJsonApiEvent> actualEvents = getEvents();
    assertThat(actualEvents.size()).isEqualTo(expectedEvents.size());

    for (int i = 0; i < actualEvents.size(); i++) {
      GcsJsonApiEvent actual = actualEvents.get(i);
      ExpectedEventDetails expected = expectedEvents.get(i);

      assertThat(actual.getEventType()).isEqualTo(expected.eventType);
      assertThat(actual.getContext().toString()).contains(expected.context.toString());

      GcsJsonApiEvent.EventType eventType = actual.getEventType();
      if (eventType == GcsJsonApiEvent.EventType.RESPONSE) {
        verifyNotEmpty(actual, GcsJsonApiEvent.DURATION);
      }

      if (eventType == GcsJsonApiEvent.EventType.BACKOFF) {
        verifyNotEmpty(actual, GcsJsonApiEvent.BACKOFF_TIME);
      }

      for (String key : expected.properties.keySet()) {
        if (key.equals(GcsJsonApiEvent.BACKOFF_TIME)) {
          long backOffTime = (long) actual.getProperty(key);
          int expectedBackoffTime = (int) expected.properties.get(GcsJsonApiEvent.BACKOFF_TIME);
          assertThat(backOffTime).isAtLeast(expectedBackoffTime);
          // Adding a buffer of 10 seconds. If this is not sufficient increase the threshold or
          // remove this check.
          assertThat(backOffTime).isLessThan(expectedBackoffTime + 10);
        } else {
          assertThat(actual.getProperty(key)).isEqualTo(expected.properties.get(key));
        }
      }
    }
  }

  private void verifyNotEmpty(GcsJsonApiEvent actual, String duration) {
    assertThat(actual.getProperty(duration)).isNotNull();
  }

  public static class ExpectedEventDetails {
    final GcsJsonApiEvent.EventType eventType;
    final String context;
    Map<String, Object> properties = new HashMap<>();

    private ExpectedEventDetails(GcsJsonApiEvent.EventType eventType, String url) {
      this.eventType = eventType;
      this.context = url;
    }

    public static ExpectedEventDetails getStarted(String url) {
      return new ExpectedEventDetails(GcsJsonApiEvent.EventType.STARTED, url);
    }

    public static ExpectedEventDetails getResponse(String url, int statusCode) {
      ExpectedEventDetails result =
          new ExpectedEventDetails(GcsJsonApiEvent.EventType.RESPONSE, url);
      result.properties.put(GcsJsonApiEvent.STATUS_CODE, statusCode);

      return result;
    }

    public static ExpectedEventDetails getBackoff(String url, int retryCount) {
      ExpectedEventDetails result =
          new ExpectedEventDetails(GcsJsonApiEvent.EventType.BACKOFF, url);
      result.properties.put(GcsJsonApiEvent.RETRY_COUNT, retryCount);

      return result;
    }

    public static ExpectedEventDetails getBackoff(String url, int retryCount, int backOff) {
      ExpectedEventDetails result =
          new ExpectedEventDetails(GcsJsonApiEvent.EventType.BACKOFF, url);
      result.properties.put(GcsJsonApiEvent.RETRY_COUNT, retryCount);
      result.properties.put(GcsJsonApiEvent.BACKOFF_TIME, backOff);

      return result;
    }

    public static ExpectedEventDetails getException(String url) {
      return new ExpectedEventDetails(GcsJsonApiEvent.EventType.EXCEPTION, url);
    }
  }
}
