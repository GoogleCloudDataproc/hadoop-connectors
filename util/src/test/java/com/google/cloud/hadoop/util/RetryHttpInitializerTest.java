/*
 * Copyright 2014 Google Inc.
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
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.inputStreamResponse;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.mockTransport;
import static com.google.common.net.HttpHeaders.CONTENT_LENGTH;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpExecuteInterceptor;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.http.HttpStatusCodes;
import com.google.auth.Credentials;
import com.google.cloud.hadoop.util.interceptors.InvocationIdInterceptor;
import com.google.cloud.hadoop.util.testing.FakeCredentials;
import com.google.cloud.hadoop.util.testing.ThrowingInputStream;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Basic unittests for RetryHttpInitializer to check the proper wiring of various interceptors and
 * handlers for HttpRequests.
 */
@RunWith(JUnit4.class)
public class RetryHttpInitializerTest {
  public static final String URL = "http://fake-url.com";
  private TestRequestTracker requestTracker;

  @Before
  public void beforeTest() {
    this.requestTracker = new TestRequestTracker();
  }

  @Test
  public void testConstructorNullCredentials() {
    createRetryHttpInitializer(/* credentials= */ null);
  }

  @Test
  public void successfulRequest_authenticated() throws IOException {
    String authHeaderValue = "Bearer: y2.WAKiHahzxGS_sP30RpjNUF";
    HttpRequestFactory requestFactory =
        mockTransport(emptyResponse(200))
            .createRequestFactory(createRetryHttpInitializer(new FakeCredentials(authHeaderValue)));

    HttpRequest req = requestFactory.buildGetRequest(new GenericUrl(URL));

    assertThat(req.getHeaders())
        .containsAtLeast(
            "user-agent", ImmutableList.of("foo-user-agent"),
            "header-key", "header-value",
            "authorization", ImmutableList.of(authHeaderValue));

    HttpResponse res = req.execute();

    assertThat(res).isNotNull();
    assertThat((String) req.getHeaders().get(InvocationIdInterceptor.GOOG_API_CLIENT))
        .contains(InvocationIdInterceptor.GCCL_INVOCATION_ID_PREFIX);
    assertThat(
            req.getHeaders().containsKey(JsonIdempotencyTokenInterceptor.IDEMPOTENCY_TOKEN_HEADER))
        .isTrue();
    assertThat(res.getStatusCode()).isEqualTo(HttpStatusCodes.STATUS_CODE_OK);

    requestTracker.verifyEvents(
        List.of(ExpectedEventDetails.getStarted(URL), ExpectedEventDetails.getResponse(URL, 200)));
  }

  @Test
  public void forbiddenResponse_failsWithoutRetries() throws IOException {
    String authHeaderValue = "Bearer: y2.WAKiHahzxGS_a1b2c3d40RpjNUF";
    HttpRequestFactory requestFactory =
        mockTransport(emptyResponse(403))
            .createRequestFactory(createRetryHttpInitializer(new FakeCredentials(authHeaderValue)));

    HttpRequest req = requestFactory.buildGetRequest(new GenericUrl(URL));

    assertThat(req.getHeaders())
        .containsAtLeast(
            "user-agent", ImmutableList.of("foo-user-agent"),
            "header-key", "header-value",
            "authorization", ImmutableList.of(authHeaderValue));

    HttpResponseException thrown = assertThrows(HttpResponseException.class, req::execute);
    assertThat((String) req.getHeaders().get(InvocationIdInterceptor.GOOG_API_CLIENT))
        .contains(InvocationIdInterceptor.GCCL_INVOCATION_ID_PREFIX);
    assertThat(
            req.getHeaders().containsKey(JsonIdempotencyTokenInterceptor.IDEMPOTENCY_TOKEN_HEADER))
        .isTrue();
    assertThat(thrown.getStatusCode()).isEqualTo(HttpStatusCodes.STATUS_CODE_FORBIDDEN);

    requestTracker.verifyEvents(
        List.of(
            TestRequestTracker.ExpectedEventDetails.getStarted(URL),
            ExpectedEventDetails.getResponse(URL, 403)));
  }

  @Test
  public void serverErrorResponse_succeedsAfterRetries() throws Exception {
    errorCodeResponse_succeedsAfterRetries(503);
  }

  @Test
  public void rateLimitExceededResponse_succeedsAfterRetries() throws Exception {
    errorCodeResponse_succeedsAfterRetries(429);
  }

  /** Helper for test cases wanting to test retries kicking in for particular error codes. */
  private void errorCodeResponse_succeedsAfterRetries(int statusCode) throws Exception {
    String authHeaderValue = "Bearer: y2.WAKiHahzxGS_a1bd40RjNUF";
    HttpRequestFactory requestFactory =
        mockTransport(emptyResponse(statusCode), emptyResponse(statusCode), emptyResponse(200))
            .createRequestFactory(createRetryHttpInitializer(new FakeCredentials(authHeaderValue)));

    HttpRequest req = requestFactory.buildGetRequest(new GenericUrl(URL));
    IdempotencyHeaderRecordInterceptor testInterceptor =
        new IdempotencyHeaderRecordInterceptor(req.getInterceptor());
    req.setInterceptor(testInterceptor);

    assertThat(req.getHeaders())
        .containsAtLeast(
            "user-agent", ImmutableList.of("foo-user-agent"),
            "header-key", "header-value",
            "authorization", ImmutableList.of(authHeaderValue));

    HttpResponse res = req.execute();
    assertThat((String) req.getHeaders().get(InvocationIdInterceptor.GOOG_API_CLIENT))
        .contains(InvocationIdInterceptor.GCCL_INVOCATION_ID_PREFIX);
    assertThat(
            req.getHeaders().containsKey(JsonIdempotencyTokenInterceptor.IDEMPOTENCY_TOKEN_HEADER))
        .isTrue();
    assertThat(testInterceptor.getIdempotencyTokens().stream().distinct().count()).isEqualTo(1);
    assertThat(res).isNotNull();
    assertThat(res.getStatusCode()).isEqualTo(HttpStatusCodes.STATUS_CODE_OK);

    requestTracker.verifyEvents(
        List.of(
            ExpectedEventDetails.getStarted(URL),
            ExpectedEventDetails.getResponse(URL, statusCode),
            ExpectedEventDetails.getBackoff(URL, 0),
            ExpectedEventDetails.getResponse(URL, statusCode),
            ExpectedEventDetails.getBackoff(URL, 1),
            ExpectedEventDetails.getResponse(URL, 200)));
  }

  @Test
  public void errorCodeResponse_failsAfterMaxRetries() throws Exception {
    int statusCode = 429;
    String authHeaderValue = "Bearer: y2.WAKiHahzxGS_a1bd40RjNUF";
    HttpRequestFactory requestFactory =
        mockTransport(
                emptyResponse(statusCode),
                emptyResponse(statusCode),
                emptyResponse(statusCode),
                emptyResponse(statusCode),
                emptyResponse(statusCode),
                emptyResponse(statusCode),
                emptyResponse(statusCode))
            .createRequestFactory(createRetryHttpInitializer(new FakeCredentials(authHeaderValue)));

    HttpRequest req = requestFactory.buildGetRequest(new GenericUrl(URL));

    assertThat(req.getHeaders())
        .containsAtLeast(
            "user-agent", ImmutableList.of("foo-user-agent"),
            "header-key", "header-value",
            "authorization", ImmutableList.of(authHeaderValue));

    try {
      HttpResponse res = req.execute();
    } catch (HttpResponseException exception) {
      // Ignore. Expected.
    }

    requestTracker.verifyEvents(
        List.of(
            ExpectedEventDetails.getStarted(URL),
            ExpectedEventDetails.getResponse(URL, statusCode),
            ExpectedEventDetails.getBackoff(URL, 0),
            ExpectedEventDetails.getResponse(URL, statusCode),
            ExpectedEventDetails.getBackoff(URL, 1),
            ExpectedEventDetails.getResponse(URL, statusCode),
            ExpectedEventDetails.getBackoff(URL, 2),
            ExpectedEventDetails.getResponse(URL, statusCode),
            ExpectedEventDetails.getBackoff(URL, 3),
            ExpectedEventDetails.getResponse(URL, statusCode),
            ExpectedEventDetails.getBackoff(URL, 4),
            ExpectedEventDetails.getResponse(URL, statusCode)));
  }

  @Test
  public void ioExceptionResponse_succeedsAfterRetries() throws Exception {
    String authHeaderValue = "Bearer: y2.WAKiHahzxGS_a1bd4jNUF";
    HttpRequestFactory requestFactory =
        mockTransport(
                inputStreamResponse(
                    /* header= */ CONTENT_LENGTH,
                    /* headerValue= */ 1,
                    new ThrowingInputStream(new IOException("read IOException"))),
                emptyResponse(200))
            .createRequestFactory(createRetryHttpInitializer(new FakeCredentials(authHeaderValue)));

    HttpRequest req = requestFactory.buildGetRequest(new GenericUrl(URL));

    assertThat(req.getHeaders())
        .containsAtLeast(
            "user-agent", ImmutableList.of("foo-user-agent"),
            "header-key", "header-value",
            "authorization", ImmutableList.of(authHeaderValue));

    HttpResponse res = req.execute();
    assertThat((String) req.getHeaders().get(InvocationIdInterceptor.GOOG_API_CLIENT))
        .contains(InvocationIdInterceptor.GCCL_INVOCATION_ID_PREFIX);
    assertThat(
            req.getHeaders().containsKey(JsonIdempotencyTokenInterceptor.IDEMPOTENCY_TOKEN_HEADER))
        .isTrue();
    assertThat(res).isNotNull();
    assertThat(res.getStatusCode()).isEqualTo(HttpStatusCodes.STATUS_CODE_OK);

    // TODO: For some reason the IOException handler is not getting called. Check why that is the
    // case.
    requestTracker.verifyEvents(
        List.of(
            TestRequestTracker.ExpectedEventDetails.getStarted(URL),
            TestRequestTracker.ExpectedEventDetails.getResponse(URL, 200)));
  }

  private TestRetryHttpInitializer createRetryHttpInitializer(Credentials credentials) {
    return new TestRetryHttpInitializer(
        credentials,
        RetryHttpInitializerOptions.builder()
            .setDefaultUserAgent("foo-user-agent")
            .setHttpHeaders(ImmutableMap.of("header-key", "header-value"))
            .setMaxRequestRetries(5)
            .setConnectTimeout(Duration.ofSeconds(5))
            .setReadTimeout(Duration.ofSeconds(5))
            .build());
  }

  // Helper class which help provide a custom test implementation of RequestTracker
  private class TestRetryHttpInitializer extends RetryHttpInitializer {
    private boolean isInitialized;

    public TestRetryHttpInitializer(Credentials credentials, RetryHttpInitializerOptions build) {
      super(credentials, build);
    }

    @Override
    protected RequestTracker getRequestTracker(HttpRequest request) {
      if (!this.isInitialized) {
        requestTracker.init(request);
        this.isInitialized = true;
      }

      return requestTracker;
    }
  }

  // Helper class to capture headers during intercept
  private static class IdempotencyHeaderRecordInterceptor implements HttpExecuteInterceptor {
    private final List<String> idempotencyTokens = new java.util.ArrayList<>();
    private final HttpExecuteInterceptor chainedInterceptor;

    public IdempotencyHeaderRecordInterceptor(HttpExecuteInterceptor chainedInterceptor) {
      this.chainedInterceptor = chainedInterceptor;
    }

    @Override
    public void intercept(HttpRequest request) throws IOException {
      if (chainedInterceptor != null) {
        chainedInterceptor.intercept(request);
      }
      if (request
          .getHeaders()
          .containsKey(JsonIdempotencyTokenInterceptor.IDEMPOTENCY_TOKEN_HEADER)) {
        idempotencyTokens.add(
            (String)
                request.getHeaders().get(JsonIdempotencyTokenInterceptor.IDEMPOTENCY_TOKEN_HEADER));
      }
    }

    public List<String> getIdempotencyTokens() {
      return idempotencyTokens;
    }
  }
}
