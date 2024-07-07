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

import static com.google.cloud.hadoop.util.TestRequestTracker.ExpectedEventDetails;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.emptyResponse;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.mockTransport;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpResponse;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class RequestTrackerTest extends TestCase {
  private static final String URL = "http://fake-url.com";
  public static final int STATUS_CODE = 200;
  private TestRequestTracker tracker;
  private HttpResponse response;
  private HttpRequest request;

  @Before
  public void setup() throws IOException {
    this.request = getHttpRequest(STATUS_CODE);
    this.response = request.execute();
    this.tracker = new TestRequestTracker();
    tracker.init(response.getRequest());
  }

  @Test
  public void testOnResponse() throws IOException {
    tracker.verifyEvents(List.of(ExpectedEventDetails.getStarted(URL)));
    tracker.trackResponse(response);
    tracker.verifyEvents(
        List.of(
            ExpectedEventDetails.getStarted(URL),
            ExpectedEventDetails.getResponse(URL, STATUS_CODE)));
  }

  @Test
  public void testOnIOException() throws IOException {
    tracker.trackIOException(request);
    tracker.verifyEvents(
        List.of(ExpectedEventDetails.getStarted(URL), ExpectedEventDetails.getException(URL)));
  }

  @Test
  public void testOnUnsuccessfulResponseHandler() {
    tracker.trackUnsuccessfulResponseHandler(response);
    tracker.verifyEvents(
        List.of(
            ExpectedEventDetails.getStarted(URL),
            ExpectedEventDetails.getResponse(URL, STATUS_CODE)));
  }

  @Test
  public void testBackOff() {
    tracker.trackUnsuccessfulResponseHandler(response);
    int backOff1 = doBackOffRandom();
    tracker.verifyEvents(
        List.of(
            ExpectedEventDetails.getStarted(URL),
            ExpectedEventDetails.getResponse(URL, STATUS_CODE),
            ExpectedEventDetails.getBackoff(URL, 0, backOff1)));

    int backOff2 = doBackOffRandom();
    tracker.verifyEvents(
        List.of(
            ExpectedEventDetails.getStarted(URL),
            ExpectedEventDetails.getResponse(URL, STATUS_CODE),
            ExpectedEventDetails.getBackoff(URL, 0, backOff1),
            ExpectedEventDetails.getBackoff(URL, 0, backOff2)));
  }

  private int doBackOffRandom() {
    int backoffTime = ThreadLocalRandom.current().nextInt(1, 20000);
    tracker.trackBackOffCompleted(System.currentTimeMillis() - backoffTime, request);
    return backoffTime;
  }

  @Test
  public void testRetryEvents() {
    tracker.trackUnsuccessfulResponseHandler(response);
    tracker.trackRetryStarted();
    int backOff1 = doBackOffRandom();
    tracker.verifyEvents(
        List.of(
            ExpectedEventDetails.getStarted(URL),
            ExpectedEventDetails.getResponse(URL, STATUS_CODE),
            ExpectedEventDetails.getBackoff(URL, 1, backOff1)));

    tracker.trackUnsuccessfulResponseHandler(response);
    tracker.trackRetryStarted();
    int backOff2 = doBackOffRandom();
    tracker.verifyEvents(
        List.of(
            ExpectedEventDetails.getStarted(URL),
            ExpectedEventDetails.getResponse(URL, STATUS_CODE),
            ExpectedEventDetails.getBackoff(URL, 1, backOff1),
            ExpectedEventDetails.getResponse(URL, STATUS_CODE),
            ExpectedEventDetails.getBackoff(URL, 2, backOff2)));
  }

  @Test
  public void testRetrySkipped() {
    tracker.trackUnsuccessfulResponseHandler(response);
    tracker.trackRetryStarted();
    int backOff1 = doBackOffRandom();
    tracker.trackUnsuccessfulResponseHandler(response);
    tracker.trackRetrySkipped(true);

    tracker.verifyEvents(
        List.of(
            ExpectedEventDetails.getStarted(URL),
            ExpectedEventDetails.getResponse(URL, STATUS_CODE),
            ExpectedEventDetails.getBackoff(URL, 1, backOff1),
            ExpectedEventDetails.getResponse(URL, STATUS_CODE)));
  }

  private static HttpRequest getHttpRequest(int statusCode) throws IOException {
    return mockTransport(emptyResponse(statusCode))
        .createRequestFactory()
        .buildGetRequest(new GenericUrl(URL));
  }
}
