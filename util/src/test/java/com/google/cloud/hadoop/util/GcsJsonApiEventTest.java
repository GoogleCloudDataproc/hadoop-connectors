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

import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.emptyResponse;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.mockTransport;
import static com.google.common.truth.Truth.assertThat;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpResponse;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GcsJsonApiEventTest {
  private static final String URL = "http://fake-url.com";

  @Test
  public void testGetResponseEvent() throws IOException {
    int duration = Math.abs(ThreadLocalRandom.current().nextInt(1000000));
    GcsJsonApiEvent event = GcsJsonApiEvent.getResponseEvent(getResponse(), duration, URL);

    assertThat(event.getContext().toString()).isEqualTo(URL);
    assertThat(event.getMethod()).isEqualTo("GET");
    assertThat(event.getEventType()).isEqualTo(GcsJsonApiEvent.EventType.RESPONSE);
    assertThat(event.getProperty(GcsJsonApiEvent.DURATION)).isEqualTo(duration);
  }

  @Test
  public void testGetExceptionEvent() throws IOException {
    GcsJsonApiEvent event = GcsJsonApiEvent.getExceptionEvent(getResponse().getRequest(), URL);

    assertThat(event.getContext().toString()).isEqualTo(URL);
    assertThat(event.getMethod()).isEqualTo("GET");
    assertThat(event.getEventType()).isEqualTo(GcsJsonApiEvent.EventType.EXCEPTION);
  }

  private HttpResponse getResponse() throws IOException {
    return mockTransport(emptyResponse(200))
        .createRequestFactory()
        .buildGetRequest(new GenericUrl(URL))
        .execute();
  }

  @Test
  public void testGetNonExistingProperty() throws IOException {
    GcsJsonApiEvent event = GcsJsonApiEvent.getResponseEvent(getResponse(), 1, URL);
    assertThat(event.getProperty("foo")).isNull();
  }
}
