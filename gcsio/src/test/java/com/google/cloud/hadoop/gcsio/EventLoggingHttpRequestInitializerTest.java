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
import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static com.google.common.net.HttpHeaders.COOKIE;
import static com.google.common.net.HttpHeaders.PROXY_AUTHORIZATION;
import static com.google.common.net.HttpHeaders.SET_COOKIE;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.cloud.hadoop.util.testing.MockHttpTransportHelper;
import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class EventLoggingHttpRequestInitializerTest {
  private static final String REQUEST_URL = "http://google.com";
  private static final Logger LOGGER =
      Logger.getLogger(EventLoggingHttpRequestInitializer.class.getName());

  private EventLoggingHttpRequestInitializer requestInitializer;
  private AssertingLogHandler assertingHandler;

  @Before
  public void setUp() throws IOException {
    assertingHandler = new AssertingLogHandler();
    LOGGER.setUseParentHandlers(false);
    LOGGER.addHandler(assertingHandler);
    LOGGER.setLevel(Level.INFO);

    requestInitializer = new EventLoggingHttpRequestInitializer();
  }

  @After
  public void verifyAndRemoveAssertingHandler() {
    LOGGER.removeHandler(assertingHandler);
  }

  @Test
  public void testTracingDetailsSetOnSuccess() throws IOException {
    int expectedStatusCode = 201;
    HttpRequest httpRequest =
        getHttpRequestWithResponse(MockHttpTransportHelper.emptyResponse(expectedStatusCode));
    requestInitializer.initialize(httpRequest);

    HttpResponse res = httpRequest.execute();

    assertThat(res).isNotNull();
    assertThat(res.getStatusCode()).isEqualTo(expectedStatusCode);

    assertingHandler.assertLogCount(1);
    Map<String, Object> logRecord = assertingHandler.getLogRecordAtIndex(0);
    verifyFields(logRecord, expectedStatusCode);
    assertThat(logRecord.get("request_start_time_utc")).isNotNull();
    assertThat(logRecord.get("request_finish_time_utc")).isNotNull();
    assertThat(logRecord.get("unexpected_error")).isNull();
  }

  @Test
  public void testTracingHeadersSuccess() throws IOException {
    int expectedStatusCode = 201;
    MockLowLevelHttpResponse httpResponse =
        MockHttpTransportHelper.emptyResponse(expectedStatusCode)
            .addHeader("custom-response-header", "responseValue");
    HttpRequest httpRequest =
        getHttpRequestWithResponse(httpResponse)
            .setHeaders(new HttpHeaders().set("custom-request-header", "requestValue"));
    requestInitializer.initialize(httpRequest);

    HttpResponse res = httpRequest.execute();

    assertThat(res).isNotNull();
    assertThat(res.getStatusCode()).isEqualTo(expectedStatusCode);

    assertingHandler.assertLogCount(1);
    Map<String, Object> logRecord = assertingHandler.getLogRecordAtIndex(0);
    verifyFields(logRecord, expectedStatusCode);
    Object requestHeadersObj = logRecord.get("request_headers");
    assertThat(requestHeadersObj).isInstanceOf(Map.class);
    Map<?, ?> requestHeadersMap = (Map<?, ?>) requestHeadersObj;
    assertThat(requestHeadersMap.get("custom-request-header")).isNotNull();
    Object responseHeadersObj = logRecord.get("response_headers");
    assertThat(responseHeadersObj).isInstanceOf(Map.class);
    Map<?, ?> responseHeadersMap = (Map<?, ?>) responseHeadersObj;
    assertThat(responseHeadersMap.get("custom-response-header")).isNotNull();
  }

  @Test
  public void testTracingHeadersFiltered() throws IOException {
    int expectedStatusCode = 201;
    MockLowLevelHttpResponse httpResponse =
        MockHttpTransportHelper.emptyResponse(expectedStatusCode)
            .addHeader(SET_COOKIE, "cookieName=cookieValue");
    HttpRequest httpRequest =
        getHttpRequestWithResponse(httpResponse)
            .setHeaders(
                new HttpHeaders()
                    .set(AUTHORIZATION, "Bearer TOKEN")
                    .set(COOKIE, "cookieName=cookieValue")
                    .set(PROXY_AUTHORIZATION, "Basic user/pass"));
    requestInitializer.initialize(httpRequest);

    HttpResponse res = httpRequest.execute();

    assertThat(res).isNotNull();
    assertThat(res.getStatusCode()).isEqualTo(expectedStatusCode);

    assertingHandler.assertLogCount(1);
    Map<String, Object> logRecord = assertingHandler.getLogRecordAtIndex(0);
    verifyFields(logRecord, expectedStatusCode);
    Object requestHeadersObj = logRecord.get("request_headers");
    assertThat(requestHeadersObj).isInstanceOf(Map.class);
    Map<?, ?> requestHeadersMap = (Map<?, ?>) requestHeadersObj;
    assertThat(requestHeadersMap.get("authorization")).isNull();
    assertThat(requestHeadersMap.get("cookie")).isNull();
    assertThat(requestHeadersMap.get("proxy-authorization")).isNull();
    Object responseHeadersObj = logRecord.get("response_headers");
    assertThat(responseHeadersObj).isInstanceOf(Map.class);
    Map<?, ?> responseHeadersMap = (Map<?, ?>) responseHeadersObj;
    assertThat(responseHeadersMap.get("set-cookie")).isNull();
  }

  @Test
  public void testIOExceptionNoCallback() throws IOException {
    HttpRequest httpRequest = getHttpRequestWithResponse(new IOException("test IOException"));
    requestInitializer.initialize(httpRequest);

    assertThrows(IOException.class, () -> httpRequest.execute());

    assertingHandler.assertLogCount(0);
  }

  @Test
  public void testZombieRequestDoesNotCrash() throws IOException {
    HttpResponse httpResponse =
        getHttpRequestWithResponse(MockHttpTransportHelper.emptyResponse(200)).execute();

    requestInitializer.logAndRemoveRequestFromTracking(httpResponse);

    assertingHandler.assertLogCount(1);
    Map<String, Object> logRecord = assertingHandler.getLogRecordAtIndex(0);
    verifyFields(logRecord, 200);
    assertThat(logRecord.get("unexpected_error")).isEqualTo("Unknown request. This is unexpected.");
  }

  private static void verifyFields(Map<String, Object> logRecord, int expectedStatusCode) {
    assertThat(logRecord.get("response_time")).isNotNull();
    assertThat(logRecord.get("response_headers")).isNotNull();
    assertThat(logRecord.get("request_headers")).isNotNull();
    assertThat(logRecord.get("request_method")).isNotNull();
    assertThat(logRecord.get("request_url")).isEqualTo(REQUEST_URL);
    assertThat(logRecord.get("response_status_code")).isEqualTo(expectedStatusCode);
  }

  private static HttpRequest getHttpRequestWithResponse(Object response) throws IOException {
    return mockTransport(response)
        .createRequestFactory()
        .buildGetRequest(new GenericUrl(REQUEST_URL));
  }
}
