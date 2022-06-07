/*
 * Copyright 2022 Google Inc. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.gcsio;

import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.mockTransport;
import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpResponse;
import com.google.cloud.hadoop.util.testing.MockHttpTransportHelper;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class EventLoggingHttpRequestInitializerTest {
  private static final String requestUrl = "http://google.com";

  private EventLoggingHttpRequestInitializer requestInitializer;
  private EventLoggingHttpRequestInitializer requestInitializerSpy;
  private ArgumentCaptor<HashMap<String, Object>> logDetailsCaptor;

  @Before
  public void setUp() throws IOException {
    this.requestInitializer = new EventLoggingHttpRequestInitializer();
    this.requestInitializerSpy = spy(requestInitializer);
    this.logDetailsCaptor = ArgumentCaptor.forClass(HashMap.class);
  }

  @Test
  public void testBasicOperation() throws IOException {
    HttpRequest httpRequest =
        getHttpRequestWithResponse(MockHttpTransportHelper.emptyResponse(201));
    requestInitializer.initialize(httpRequest);
    HttpResponse res = httpRequest.execute();

    assertThat(res).isNotNull();
    assertThat(res.getStatusCode()).isEqualTo(201);
  }

  @Test
  public void testTracingDetailsSet() throws IOException {
    HttpRequest httpRequest =
        getHttpRequestWithResponse(MockHttpTransportHelper.emptyResponse(200));
    requestInitializerSpy.initialize(httpRequest);
    httpRequest.execute();

    verify(requestInitializerSpy).logDetails(logDetailsCaptor.capture());
    verify(requestInitializerSpy, times(1)).logDetails(any());
    Map<String, Object> captured = logDetailsCaptor.getValue();

    assertThat(captured.get("response_time")).isNotNull();
    assertThat(captured.get("request_url")).isEqualTo(requestUrl);
    assertThat(captured.get("response_status_code")).isEqualTo(200);
  }

  @Test
  public void testIOExceptionNoCallback() throws IOException {
    HttpRequest httpRequest = getHttpRequestWithResponse(new IOException("test IOException"));
    requestInitializerSpy.initialize(httpRequest);

    try {
      httpRequest.execute();
    } catch (IOException ioe) {
    }

    verify(requestInitializerSpy, times(0)).logDetails(any());
  }

  @Test
  public void testZombieRequestDoesNotCrash() throws IOException {
    HttpResponse httpResponse =
        getHttpRequestWithResponse(MockHttpTransportHelper.emptyResponse(200)).execute();
    requestInitializerSpy.logAndRemoveRequestFromTracking(httpResponse);

    verify(requestInitializerSpy).logDetails(logDetailsCaptor.capture());
    verify(requestInitializerSpy, times(1)).logDetails(any());

    Map<String, Object> captured = logDetailsCaptor.getValue();

    assertThat(captured.get("response_time")).isEqualTo(Integer.MAX_VALUE);
    assertThat(captured.get("request_url")).isEqualTo(requestUrl);
    assertThat(captured.get("response_status_code")).isEqualTo(200);
    assertThat(captured.get("unexpected_error")).isEqualTo("Zombie request. This is unexpected.");
  }

  private static HttpRequest getHttpRequestWithResponse(Object response) throws IOException {
    return mockTransport(response)
        .createRequestFactory()
        .buildGetRequest(new GenericUrl(requestUrl));
  }
}
