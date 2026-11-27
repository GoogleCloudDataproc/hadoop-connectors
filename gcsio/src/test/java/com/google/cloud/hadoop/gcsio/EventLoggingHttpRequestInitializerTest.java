/*
 * Copyright 2022 Google LLC
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
import com.google.gson.Gson;
import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class EventLoggingHttpRequestInitializerTest {
  private static final String REQUEST_URL = "http://google.com";
  private static final Logger LOGGER =
      Logger.getLogger(EventLoggingHttpRequestInitializer.class.getName());

  private EventLoggingHttpRequestInitializer requestInitializer;
  private AssertingLogHandler assertingHandler;
  private Level previousLevel;
  private boolean previousUseParentHandlers;

  @Before
  public void setUp() throws IOException {
    assertingHandler = new AssertingLogHandler();
    previousLevel = LOGGER.getLevel();
    previousUseParentHandlers = LOGGER.getUseParentHandlers();
    LOGGER.setUseParentHandlers(false);
    LOGGER.addHandler(assertingHandler);
    LOGGER.setLevel(Level.INFO);

    requestInitializer = new EventLoggingHttpRequestInitializer();
  }

  @After
  public void verifyAndRemoveAssertingHandler() {
    LOGGER.removeHandler(assertingHandler);
    LOGGER.setLevel(previousLevel);
    LOGGER.setUseParentHandlers(previousUseParentHandlers);
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
    String encryptionKey = "BASE64_ENCRYPTION_KEY_VALUE";
    String encryptionKeySha256 = "BASE64_ENCRYPTION_KEY_SHA256";
    String copySourceEncryptionKey = "BASE64_COPY_SOURCE_KEY_VALUE";
    String copySourceEncryptionKeySha256 = "BASE64_COPY_SOURCE_KEY_SHA256";
    String googApiKey = "SECRET_GOOG_API_KEY";
    String apiKey = "SECRET_API_KEY";
    String iamAuthToken = "SECRET_IAM_AUTH_TOKEN";
    String googAuthorization = "Bearer SECRET_GOOG_AUTHORIZATION";
    String idempotencyToken = "benign-idempotency-token-123";
    HttpRequest httpRequest =
        getHttpRequestWithResponse(httpResponse)
            .setHeaders(
                new HttpHeaders()
                    .set(AUTHORIZATION, "Bearer TOKEN")
                    .set(COOKIE, "cookieName=cookieValue")
                    .set(PROXY_AUTHORIZATION, "Basic user/pass")
                    .set("x-goog-encryption-algorithm", "AES256")
                    .set("x-goog-gcs-idempotency-token", idempotencyToken)
                    .set("X-Goog-Encryption-Key", encryptionKey)
                    .set("X-Goog-Encryption-Key-Sha256", encryptionKeySha256)
                    .set("X-GOOG-COPY-SOURCE-ENCRYPTION-KEY", copySourceEncryptionKey)
                    .set("X-GOOG-COPY-SOURCE-ENCRYPTION-KEY-SHA256", copySourceEncryptionKeySha256)
                    .set("X-Goog-Api-Key", googApiKey)
                    .set("X-API-Key", apiKey)
                    .set("X-Goog-Iam-Authorization-Token", iamAuthToken)
                    .set("X-Goog-Authorization", googAuthorization));
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
    assertThat(requestHeadersMap.get("X-Goog-Encryption-Key")).isNull();
    assertThat(requestHeadersMap.get("x-goog-encryption-key")).isNull();
    assertThat(requestHeadersMap.get("X-Goog-Encryption-Key-Sha256")).isNull();
    assertThat(requestHeadersMap.get("x-goog-encryption-key-sha256")).isNull();
    assertThat(requestHeadersMap.get("X-GOOG-COPY-SOURCE-ENCRYPTION-KEY")).isNull();
    assertThat(requestHeadersMap.get("x-goog-copy-source-encryption-key")).isNull();
    assertThat(requestHeadersMap.get("X-GOOG-COPY-SOURCE-ENCRYPTION-KEY-SHA256")).isNull();
    assertThat(requestHeadersMap.get("x-goog-copy-source-encryption-key-sha256")).isNull();
    assertThat(requestHeadersMap.get("X-Goog-Api-Key")).isNull();
    assertThat(requestHeadersMap.get("X-API-Key")).isNull();
    assertThat(requestHeadersMap.get("X-Goog-Iam-Authorization-Token")).isNull();
    assertThat(requestHeadersMap.get("X-Goog-Authorization")).isNull();
    assertThat(requestHeadersMap.get("x-goog-authorization")).isNull();
    assertThat(requestHeadersMap.get("x-goog-encryption-algorithm")).isEqualTo("AES256");
    assertThat(requestHeadersMap.get("x-goog-gcs-idempotency-token")).isEqualTo(idempotencyToken);

    String serializedLog = new Gson().toJson(logRecord);
    assertThat(serializedLog).doesNotContain(encryptionKey);
    assertThat(serializedLog).doesNotContain(encryptionKeySha256);
    assertThat(serializedLog).doesNotContain(copySourceEncryptionKey);
    assertThat(serializedLog).doesNotContain(copySourceEncryptionKeySha256);
    assertThat(serializedLog).doesNotContain(googApiKey);
    assertThat(serializedLog).doesNotContain(apiKey);
    assertThat(serializedLog).doesNotContain(iamAuthToken);
    assertThat(serializedLog).doesNotContain(googAuthorization);

    // Caller request headers must remain unmutated.
    assertThat(httpRequest.getHeaders().get("X-Goog-Encryption-Key")).isEqualTo(encryptionKey);
    assertThat(httpRequest.getHeaders().get("X-Goog-Encryption-Key-Sha256"))
        .isEqualTo(encryptionKeySha256);
    assertThat(httpRequest.getHeaders().get("X-GOOG-COPY-SOURCE-ENCRYPTION-KEY"))
        .isEqualTo(copySourceEncryptionKey);
    assertThat(httpRequest.getHeaders().get("X-GOOG-COPY-SOURCE-ENCRYPTION-KEY-SHA256"))
        .isEqualTo(copySourceEncryptionKeySha256);
    assertThat(httpRequest.getHeaders().get("X-Goog-Api-Key")).isEqualTo(googApiKey);

    Object responseHeadersObj = logRecord.get("response_headers");
    assertThat(responseHeadersObj).isInstanceOf(Map.class);
    Map<?, ?> responseHeadersMap = (Map<?, ?>) responseHeadersObj;
    assertThat(responseHeadersMap.get("set-cookie")).isNull();
  }

  @Test
  public void testNullHeaderValueFilteredCleanly() throws IOException {
    HttpHeaders headersWithNull = new HttpHeaders();
    headersWithNull.put("null-header", null);
    headersWithNull.set("custom-request-header", "keep");
    HttpRequest httpRequest =
        getHttpRequestWithResponse(MockHttpTransportHelper.emptyResponse(200))
            .setHeaders(headersWithNull);
    requestInitializer.initialize(httpRequest);
    httpRequest.execute();

    assertingHandler.assertLogCount(1);
    Map<?, ?> requestHeadersMap =
        (Map<?, ?>) assertingHandler.getLogRecordAtIndex(0).get("request_headers");
    assertThat(requestHeadersMap.get("null-header")).isNull();
    assertThat(requestHeadersMap.get("custom-request-header")).isEqualTo("keep");
  }

  @Test
  public void testInfoDisabledSkipsLogging() throws IOException {
    LOGGER.setLevel(Level.WARNING);
    HttpRequest skippedRequest =
        getHttpRequestWithResponse(MockHttpTransportHelper.emptyResponse(200));
    requestInitializer.initialize(skippedRequest);
    skippedRequest.execute();

    assertingHandler.assertLogCount(0);
  }

  @Test
  public void testIOExceptionNoCallback() throws IOException {
    HttpRequest httpRequest = getHttpRequestWithResponse(new IOException("test IOException"));
    requestInitializer.initialize(httpRequest);

    assertThrows(IOException.class, httpRequest::execute);

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
