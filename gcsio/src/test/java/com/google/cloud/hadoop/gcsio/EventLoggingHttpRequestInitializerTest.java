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
import static com.google.common.net.HttpHeaders.LOCATION;
import static com.google.common.net.HttpHeaders.PROXY_AUTHORIZATION;
import static com.google.common.net.HttpHeaders.SET_COOKIE;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.cloud.hadoop.util.LoggingUrlRedactor;
import com.google.cloud.hadoop.util.testing.MockHttpTransportHelper;
import com.google.gson.Gson;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
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

  @Test
  public void testResumableUploadRedactsUrlAndLocationAndDigestsUploadIdHeader()
      throws IOException {
    String secretUploadId = "SECRET_HTTP_TRACE_UPLOAD_ID";
    String expectedDigest = LoggingUrlRedactor.digestToken(secretUploadId);
    String sessionUrl =
        "https://storage.googleapis.com/upload/storage/v1/b/bkt/o"
            + "?uploadType=resumable&upload_id="
            + secretUploadId;

    // 1. Initiation POST returns Location + X-GUploader-UploadID
    MockLowLevelHttpResponse initMockResponse =
        MockHttpTransportHelper.emptyResponse(200)
            .addHeader(LOCATION, sessionUrl)
            .addHeader("X-GUploader-UploadID", secretUploadId)
            .addHeader("custom-header", "keep-me");
    HttpRequest initRequest =
        mockTransport(initMockResponse)
            .createRequestFactory()
            .buildPostRequest(
                new GenericUrl(
                    "https://storage.googleapis.com/upload/storage/v1/b/bkt/o"
                        + "?uploadType=resumable&name=dir/obj"),
                null);
    requestInitializer.initialize(initRequest);
    HttpResponse initResponse = initRequest.execute();

    // 2. Intermediate chunk PUT to the session URL returns 308 + X-GUploader-UploadID
    MockLowLevelHttpResponse chunkMockResponse =
        MockHttpTransportHelper.emptyResponse(308)
            .addHeader("X-GUploader-UploadID", secretUploadId);
    HttpRequest chunkRequest =
        mockTransport(chunkMockResponse)
            .createRequestFactory()
            .buildPutRequest(new GenericUrl(sessionUrl), null)
            .setThrowExceptionOnExecuteError(false);
    requestInitializer.initialize(chunkRequest);
    HttpResponse chunkResponse = chunkRequest.execute();

    assertingHandler.assertLogCount(2);

    Map<String, Object> initLog = assertingHandler.getLogRecordAtIndex(0);
    assertThat(initLog.get("request_url"))
        .isEqualTo(
            "https://storage.googleapis.com/upload/storage/v1/b/bkt/o"
                + "?uploadType=resumable&name=dir/obj");
    Map<?, ?> initRespHeaders = (Map<?, ?>) initLog.get("response_headers");
    assertThat(initRespHeaders.get("location"))
        .isEqualTo(
            Collections.singletonList(
                "https://storage.googleapis.com/upload/storage/v1/b/bkt/o"
                    + "?uploadType=resumable&upload_id="
                    + expectedDigest));
    assertThat(initRespHeaders.get("x-guploader-uploadid"))
        .isEqualTo(Collections.singletonList(expectedDigest));
    assertThat(initRespHeaders.get("custom-header"))
        .isEqualTo(Collections.singletonList("keep-me"));

    Map<String, Object> chunkLog = assertingHandler.getLogRecordAtIndex(1);
    assertThat(chunkLog.get("request_url").toString()).contains("uploadType=resumable");
    assertThat(chunkLog.get("request_url").toString()).contains("upload_id=" + expectedDigest);
    Map<?, ?> chunkRespHeaders = (Map<?, ?>) chunkLog.get("response_headers");
    assertThat(chunkRespHeaders.get("x-guploader-uploadid"))
        .isEqualTo(Collections.singletonList(expectedDigest));

    String allLogsJson = new Gson().toJson(assertingHandler.getAllLogRecords());
    assertThat(allLogsJson).doesNotContain(secretUploadId);

    // Caller objects must remain unmutated.
    assertThat(initResponse.getHeaders().getLocation()).isEqualTo(sessionUrl);
    assertThat(initResponse.getHeaders().getFirstHeaderStringValue("x-guploader-uploadid"))
        .isEqualTo(secretUploadId);
    assertThat(chunkRequest.getUrl().getFirst("upload_id")).isEqualTo(secretUploadId);
    assertThat(chunkResponse.getHeaders().getFirstHeaderStringValue("x-guploader-uploadid"))
        .isEqualTo(secretUploadId);
  }

  @Test
  public void testNonUploadRequestPreservesGUploaderUploadIdHeader() throws IOException {
    String readTraceId = "READ_SCOTTY_TRACE_ID_123";
    MockLowLevelHttpResponse mockResponse =
        MockHttpTransportHelper.emptyResponse(200).addHeader("X-GUploader-UploadID", readTraceId);
    HttpRequest readRequest =
        mockTransport(mockResponse)
            .createRequestFactory()
            .buildGetRequest(
                new GenericUrl(
                    "https://storage.googleapis.com/storage/v1/b/upload/o/dir%2Ffile?alt=media"));
    requestInitializer.initialize(readRequest);
    readRequest.execute();

    assertingHandler.assertLogCount(1);
    Map<?, ?> respHeaders =
        (Map<?, ?>) assertingHandler.getLogRecordAtIndex(0).get("response_headers");
    assertThat(respHeaders.get("x-guploader-uploadid"))
        .isEqualTo(Collections.singletonList(readTraceId));
  }

  @Test
  public void testNonUploadMutationsPreserveGUploaderUploadIdHeader() throws IOException {
    String traceId = "MUTATION_SCOTTY_TRACE_ID";
    GenericUrl objectUrl = new GenericUrl("https://storage.googleapis.com/storage/v1/b/bkt/o/obj");
    GenericUrl composeUrl =
        new GenericUrl("https://storage.googleapis.com/storage/v1/b/bkt/o/obj/compose");
    for (HttpRequest request :
        Arrays.asList(
            traceIdRequestFactory(traceId).buildDeleteRequest(objectUrl),
            traceIdRequestFactory(traceId).buildPatchRequest(objectUrl, null),
            traceIdRequestFactory(traceId).buildPostRequest(composeUrl, null))) {
      requestInitializer.initialize(request);
      request.execute();
    }

    assertingHandler.assertLogCount(3);
    for (Map<String, Object> logRecord : assertingHandler.getAllLogRecords()) {
      Map<?, ?> respHeaders = (Map<?, ?>) logRecord.get("response_headers");
      assertThat(respHeaders.get("x-guploader-uploadid"))
          .isEqualTo(Collections.singletonList(traceId));
    }
  }

  @Test
  public void testCustomEndpointUploadResponseHeadersAreRedacted() throws IOException {
    String secretUploadId = "SECRET_CUSTOM_PROXY_UPLOAD_ID";
    String expectedDigest = LoggingUrlRedactor.digestToken(secretUploadId);
    MockLowLevelHttpResponse mockResponse =
        MockHttpTransportHelper.emptyResponse(308)
            .addHeader("X-GUploader-UploadID", secretUploadId)
            .addHeader("X-Goog-Upload-URL", "https://proxy.example/session?upload_id=SECRET_URL")
            .addHeader(
                "X-Goog-Upload-Control-URL", "https://proxy.example/ctrl?upload_id=SECRET_CTRL");
    // The upload session headers in the response mark this custom endpoint PUT as an upload.
    HttpRequest putRequest =
        mockTransport(mockResponse)
            .createRequestFactory()
            .buildPutRequest(new GenericUrl("https://proxy.example/v1/session/abc"), null)
            .setThrowExceptionOnExecuteError(false);
    requestInitializer.initialize(putRequest);
    putRequest.execute();

    assertingHandler.assertLogCount(1);
    Map<?, ?> respHeaders =
        (Map<?, ?>) assertingHandler.getLogRecordAtIndex(0).get("response_headers");
    assertThat(respHeaders.get("x-goog-upload-url"))
        .isEqualTo(
            Collections.singletonList(
                "https://proxy.example/session?upload_id="
                    + LoggingUrlRedactor.digestToken("SECRET_URL")));
    assertThat(respHeaders.get("x-goog-upload-control-url"))
        .isEqualTo(
            Collections.singletonList(
                "https://proxy.example/ctrl?upload_id="
                    + LoggingUrlRedactor.digestToken("SECRET_CTRL")));
    assertThat(respHeaders.get("x-guploader-uploadid"))
        .isEqualTo(Collections.singletonList(expectedDigest));
    String allLogsJson = new Gson().toJson(assertingHandler.getAllLogRecords());
    assertThat(allLogsJson).doesNotContain(secretUploadId);
    assertThat(allLogsJson).doesNotContain("SECRET_URL");
    assertThat(allLogsJson).doesNotContain("SECRET_CTRL");
  }

  @Test
  public void testGetRedirectRedactsLocationAndPreservesReadTraceIdAndResumableHeaderDigests()
      throws IOException {
    String readTraceId = "READ_REDIRECT_TRACE_ID";
    MockLowLevelHttpResponse redirectResponse =
        MockHttpTransportHelper.emptyResponse(302)
            .addHeader(LOCATION, "https://storage.googleapis.com/o?X-Goog-Signature=SECRET_SIG")
            .addHeader("X-GUploader-UploadID", readTraceId);
    HttpRequest redirectGet =
        mockTransport(redirectResponse)
            .createRequestFactory()
            .buildGetRequest(new GenericUrl("https://storage.googleapis.com/storage/v1/b/bkt/o/x"))
            .setFollowRedirects(false)
            .setThrowExceptionOnExecuteError(false);
    requestInitializer.initialize(redirectGet);
    redirectGet.execute();

    String resumableId = "SECRET_XML_RESUMABLE_ID";
    MockLowLevelHttpResponse resumableResponse =
        MockHttpTransportHelper.emptyResponse(200).addHeader("X-GUploader-UploadID", resumableId);
    HttpRequest resumableGet =
        mockTransport(resumableResponse)
            .createRequestFactory()
            .buildGetRequest(new GenericUrl("https://storage.googleapis.com/bkt/obj"))
            .setHeaders(new HttpHeaders().set("x-goog-resumable", "start"));
    requestInitializer.initialize(resumableGet);
    resumableGet.execute();

    assertingHandler.assertLogCount(2);
    Map<?, ?> redirectRespHeaders =
        (Map<?, ?>) assertingHandler.getLogRecordAtIndex(0).get("response_headers");
    assertThat(redirectRespHeaders.get("location"))
        .isEqualTo(
            Collections.singletonList(
                "https://storage.googleapis.com/o?X-Goog-Signature="
                    + LoggingUrlRedactor.REDACTED));
    assertThat(redirectRespHeaders.get("x-guploader-uploadid"))
        .isEqualTo(Collections.singletonList(readTraceId));

    Map<?, ?> resumableRespHeaders =
        (Map<?, ?>) assertingHandler.getLogRecordAtIndex(1).get("response_headers");
    assertThat(resumableRespHeaders.get("x-guploader-uploadid"))
        .isEqualTo(Collections.singletonList(LoggingUrlRedactor.digestToken(resumableId)));
  }

  @Test
  public void testDigestHeaderValueHandlesNullAndMultiValueWithNull() {
    assertThat(EventLoggingHttpRequestInitializer.digestHeaderValue(null)).isNull();
    assertThat(
            EventLoggingHttpRequestInitializer.digestHeaderValue(
                Arrays.asList("tok-1", null, "tok-2")))
        .isEqualTo(
            Arrays.asList(
                LoggingUrlRedactor.digestToken("tok-1"),
                null,
                LoggingUrlRedactor.digestToken("tok-2")));
  }

  @Test
  public void testInfoDisabledSkipsLoggingCleanly() throws IOException {
    LOGGER.setLevel(Level.WARNING);
    HttpRequest httpRequest =
        getHttpRequestWithResponse(MockHttpTransportHelper.emptyResponse(200));
    requestInitializer.initialize(httpRequest);
    httpRequest.execute();

    assertingHandler.assertLogCount(0);
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

  private static HttpRequestFactory traceIdRequestFactory(String traceId) {
    return mockTransport(
            MockHttpTransportHelper.emptyResponse(200).addHeader("X-GUploader-UploadID", traceId))
        .createRequestFactory();
  }
}
