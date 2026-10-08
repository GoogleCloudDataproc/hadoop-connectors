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

import static com.google.cloud.hadoop.util.RetryHttpInitializer.isRetryableCredentialsError;
import static com.google.cloud.hadoop.util.TestRequestTracker.ExpectedEventDetails;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.emptyResponse;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.inputStreamResponse;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.jsonDataResponse;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.mockTransport;
import static com.google.common.net.HttpHeaders.CONTENT_LENGTH;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.Assert.assertThrows;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpExecuteInterceptor;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.http.HttpStatusCodes;
import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.api.client.util.Sleeper;
import com.google.auth.Credentials;
import com.google.auth.Retryable;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.cloud.hadoop.util.interceptors.InvocationIdInterceptor;
import com.google.cloud.hadoop.util.testing.FakeCredentials;
import com.google.cloud.hadoop.util.testing.ThrowingInputStream;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Ints;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Map;
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

  private static final String MDS_TOKEN_PATH =
      "/computeMetadata/v1/instance/service-accounts/default/token";

  private static MockLowLevelHttpResponse mdsTokenResponse() throws IOException {
    return jsonDataResponse(
        ImmutableMap.of(
            "access_token", "test-access-token", "expires_in", 3600, "token_type", "Bearer"));
  }

  /**
   * Fake GCE metadata server: serves the queued responses for the token endpoint and records the
   * URLs it was asked for. Any other path fails the test.
   */
  private static MockHttpTransport fakeMetadataServer(
      List<String> requestedUrls, MockLowLevelHttpResponse... responses) {
    Deque<MockLowLevelHttpResponse> queue = new ArrayDeque<>(Arrays.asList(responses));
    return new MockHttpTransport() {
      @Override
      public LowLevelHttpRequest buildRequest(String method, String url) {
        requestedUrls.add(url);
        assertThat(url).contains(MDS_TOKEN_PATH);
        return new MockLowLevelHttpRequest(url) {
          @Override
          public LowLevelHttpResponse execute() {
            return queue.poll();
          }
        };
      }
    };
  }

  /**
   * Reproduces b/505801454 with the real {@link ComputeEngineCredentials}: the GCE metadata server
   * returns 503 for the token request. Before the fix this failed the GCS request immediately.
   */
  @Test
  public void computeEngineCredentials_metadataServer503_retriedAndRequestSucceeds()
      throws IOException {
    List<String> mdsRequests = new ArrayList<>();
    MockHttpTransport metadataServer =
        fakeMetadataServer(mdsRequests, emptyResponse(503), emptyResponse(503), mdsTokenResponse());
    ComputeEngineCredentials credentials =
        ComputeEngineCredentials.newBuilder().setHttpTransportFactory(() -> metadataServer).build();
    List<Long> sleeps = new ArrayList<>();
    HttpRequestFactory requestFactory =
        mockTransport(emptyResponse(200))
            .createRequestFactory(createRetryHttpInitializer(credentials, sleeps::add));

    HttpRequest req = requestFactory.buildGetRequest(new GenericUrl(URL));

    assertThat(req.getHeaders().getFirstHeaderStringValue("authorization"))
        .isEqualTo("Bearer test-access-token");
    assertThat(mdsRequests).hasSize(3);
    assertThat(sleeps).hasSize(2);
    assertThat(req.execute().getStatusCode()).isEqualTo(HttpStatusCodes.STATUS_CODE_OK);
  }

  @Test
  public void computeEngineCredentials_metadataServerAlways503_failsAfterMaxRetries() {
    List<String> mdsRequests = new ArrayList<>();
    MockLowLevelHttpResponse[] responses = new MockLowLevelHttpResponse[10];
    Arrays.fill(responses, emptyResponse(503));
    MockHttpTransport metadataServer = fakeMetadataServer(mdsRequests, responses);
    ComputeEngineCredentials credentials =
        ComputeEngineCredentials.newBuilder().setHttpTransportFactory(() -> metadataServer).build();
    List<Long> sleeps = new ArrayList<>();
    HttpRequestFactory requestFactory =
        mockTransport(emptyResponse(200))
            .createRequestFactory(createRetryHttpInitializer(credentials, sleeps::add));

    IOException thrown =
        assertThrows(IOException.class, () -> requestFactory.buildGetRequest(new GenericUrl(URL)));

    // Same failure signature as in the customer logs: retryable auth error caused by a 503 from
    // MDS.
    assertThat(isRetryableCredentialsError(thrown)).isTrue();
    assertThat(thrown).hasCauseThat().isInstanceOf(HttpResponseException.class);
    assertThat(((HttpResponseException) thrown.getCause()).getStatusCode()).isEqualTo(503);
    // 1 initial attempt + 5 retries (maxRequestRetries)
    assertThat(mdsRequests).hasSize(6);
    assertThat(sleeps).hasSize(5);
  }

  /**
   * Only 503 is surfaced by {@link ComputeEngineCredentials} as a {@code Retryable} exception; 429
   * is a plain {@link IOException} that carries the status code in its message only, so this test
   * also guards the message matching against google-auth-library upgrades.
   */
  @Test
  public void computeEngineCredentials_metadataServerRetryableErrors_retried() throws IOException {
    for (int statusCode : RETRYABLE_STATUS_CODES) {
      List<String> mdsRequests = new ArrayList<>();
      List<Long> sleeps = new ArrayList<>();
      HttpRequestFactory requestFactory =
          computeEngineRequestFactory(statusCode, mdsRequests, sleeps);

      HttpRequest req = requestFactory.buildGetRequest(new GenericUrl(URL));

      assertWithMessage("MDS status %s", statusCode)
          .that(req.getHeaders().getFirstHeaderStringValue("authorization"))
          .isEqualTo("Bearer test-access-token");
      assertWithMessage("MDS status %s", statusCode).that(mdsRequests).hasSize(3);
      assertWithMessage("MDS status %s", statusCode).that(sleeps).hasSize(2);
    }
  }

  @Test
  public void computeEngineCredentials_metadataServerNonRetryableErrors_failWithoutRetries()
      throws IOException {
    for (int statusCode : NON_RETRYABLE_STATUS_CODES) {
      List<String> mdsRequests = new ArrayList<>();
      List<Long> sleeps = new ArrayList<>();
      HttpRequestFactory requestFactory =
          computeEngineRequestFactory(statusCode, mdsRequests, sleeps);

      IOException thrown =
          assertThrows(
              IOException.class, () -> requestFactory.buildGetRequest(new GenericUrl(URL)));

      assertWithMessage("MDS status %s", statusCode)
          .that(isRetryableCredentialsError(thrown))
          .isFalse();
      assertWithMessage("MDS status %s", statusCode).that(mdsRequests).hasSize(1);
      assertWithMessage("MDS status %s", statusCode).that(sleeps).isEmpty();
    }
  }

  /**
   * Request factory authenticated with real {@link ComputeEngineCredentials} against a fake
   * metadata server that fails the token request twice with {@code statusCode} and then succeeds.
   */
  private HttpRequestFactory computeEngineRequestFactory(
      int statusCode, List<String> mdsRequests, List<Long> sleeps) throws IOException {
    MockHttpTransport metadataServer =
        fakeMetadataServer(
            mdsRequests, emptyResponse(statusCode), emptyResponse(statusCode), mdsTokenResponse());
    ComputeEngineCredentials credentials =
        ComputeEngineCredentials.newBuilder().setHttpTransportFactory(() -> metadataServer).build();
    return mockTransport(emptyResponse(200))
        .createRequestFactory(createRetryHttpInitializer(credentials, sleeps::add));
  }

  @Test
  public void credentialsRetryableError_succeedsAfterRetries() throws IOException {
    String authHeaderValue = "Bearer: y2.WAKiHahzxGS_a1bd4jRetry";
    FlakyCredentials credentials =
        new FlakyCredentials(
            authHeaderValue,
            metadataServerError(503),
            new RetryableIOException(/* retryable= */ true));
    List<Long> sleeps = new ArrayList<>();
    HttpRequestFactory requestFactory =
        mockTransport(emptyResponse(200))
            .createRequestFactory(createRetryHttpInitializer(credentials, sleeps::add));

    HttpRequest req = requestFactory.buildGetRequest(new GenericUrl(URL));

    assertThat(req.getHeaders())
        .containsAtLeast("authorization", ImmutableList.of(authHeaderValue));
    assertThat(credentials.getRequestMetadataCalls).isEqualTo(3);
    assertThat(sleeps).hasSize(2);
    assertThat(req.execute().getStatusCode()).isEqualTo(HttpStatusCodes.STATUS_CODE_OK);
  }

  @Test
  public void credentialsRetryableError_failsAfterMaxRetries() {
    IOException[] errors = new IOException[10];
    for (int i = 0; i < errors.length; i++) {
      errors[i] = metadataServerError(503);
    }
    FlakyCredentials credentials = new FlakyCredentials("Bearer: token", errors);
    List<Long> sleeps = new ArrayList<>();
    HttpRequestFactory requestFactory =
        mockTransport(emptyResponse(200))
            .createRequestFactory(createRetryHttpInitializer(credentials, sleeps::add));

    IOException thrown =
        assertThrows(IOException.class, () -> requestFactory.buildGetRequest(new GenericUrl(URL)));

    assertThat(thrown).isSameInstanceAs(errors[5]);
    // 1 initial attempt + 5 retries (maxRequestRetries)
    assertThat(credentials.getRequestMetadataCalls).isEqualTo(6);
    assertThat(sleeps).hasSize(5);
  }

  @Test
  public void credentialsNonRetryableError_failsWithoutRetries() {
    IOException error = metadataServerError(404);
    FlakyCredentials credentials = new FlakyCredentials("Bearer: token", error);
    List<Long> sleeps = new ArrayList<>();
    HttpRequestFactory requestFactory =
        mockTransport(emptyResponse(200))
            .createRequestFactory(createRetryHttpInitializer(credentials, sleeps::add));

    IOException thrown =
        assertThrows(IOException.class, () -> requestFactory.buildGetRequest(new GenericUrl(URL)));

    assertThat(thrown).isSameInstanceAs(error);
    assertThat(credentials.getRequestMetadataCalls).isEqualTo(1);
    assertThat(sleeps).isEmpty();
  }

  @Test
  public void credentialsRetryableError_interruptedDuringBackoff() {
    FlakyCredentials credentials = new FlakyCredentials("Bearer: token", metadataServerError(503));
    HttpRequestFactory requestFactory =
        mockTransport(emptyResponse(200))
            .createRequestFactory(
                createRetryHttpInitializer(
                    credentials,
                    millis -> {
                      throw new InterruptedException("test interrupt");
                    }));

    try {
      assertThrows(
          InterruptedIOException.class, () -> requestFactory.buildGetRequest(new GenericUrl(URL)));
      assertThat(Thread.currentThread().isInterrupted()).isTrue();
    } finally {
      // Clear interrupted flag
      Thread.interrupted();
    }
    assertThat(credentials.getRequestMetadataCalls).isEqualTo(1);
  }

  /** Status codes of a failed token request that the connector retries. */
  private static final ImmutableSet<Integer> RETRYABLE_STATUS_CODES = ImmutableSet.of(503, 429);

  private static final ImmutableSet<Integer> NON_RETRYABLE_STATUS_CODES =
      ImmutableSet.of(400, 401, 403, 404, 408, 500, 502, 504);

  /**
   * Status codes that google-auth-library marks as retryable for OAuth2 token endpoint requests;
   * the connector deliberately retries a narrower set.
   */
  private static final ImmutableSet<Integer> SDK_RETRYABLE_STATUS_CODES =
      ImmutableSet.of(500, 503, 408, 429);

  @Test
  public void isRetryableCredentialsError_classifiesErrors() {
    for (int statusCode : RETRYABLE_STATUS_CODES) {
      assertWithMessage("metadata server %s", statusCode)
          .that(isRetryableCredentialsError(metadataServerError(statusCode)))
          .isTrue();
      assertWithMessage("token endpoint %s", statusCode)
          .that(isRetryableCredentialsError(tokenEndpointError(statusCode)))
          .isTrue();
      assertWithMessage("IAM credentials %s", statusCode)
          .that(isRetryableCredentialsError(iamCredentialsError(statusCode)))
          .isTrue();
    }
    for (int statusCode : NON_RETRYABLE_STATUS_CODES) {
      assertWithMessage("metadata server %s", statusCode)
          .that(isRetryableCredentialsError(metadataServerError(statusCode)))
          .isFalse();
      // 500 and 408 are marked as retryable by google-auth-library but not retried by the connector
      assertWithMessage("token endpoint %s", statusCode)
          .that(isRetryableCredentialsError(tokenEndpointError(statusCode)))
          .isFalse();
      assertWithMessage("IAM credentials %s", statusCode)
          .that(isRetryableCredentialsError(iamCredentialsError(statusCode)))
          .isFalse();
    }

    // ImpersonatedCredentials wrap source credentials errors
    assertThat(
            isRetryableCredentialsError(
                new IOException("Unable to refresh sourceCredentials", metadataServerError(503))))
        .isTrue();
    assertThat(
            isRetryableCredentialsError(
                new IOException("Unable to refresh sourceCredentials", metadataServerError(404))))
        .isFalse();

    // Connection errors without a status code
    assertThat(isRetryableCredentialsError(new RetryableIOException(/* retryable= */ true)))
        .isTrue();
    assertThat(isRetryableCredentialsError(new RetryableIOException(/* retryable= */ false)))
        .isFalse();
    assertThat(
            isRetryableCredentialsError(
                new RetryableIOException(
                    /* retryable= */ true, new SocketTimeoutException("timeout"))))
        .isTrue();
    assertThat(isRetryableCredentialsError(new IOException(new SocketTimeoutException("timeout"))))
        .isTrue();
    assertThat(isRetryableCredentialsError(new IOException(new ConnectException("refused"))))
        .isTrue();
    assertThat(
            isRetryableCredentialsError(
                new IOException(
                    "ComputeEngineCredentials cannot find the metadata server.",
                    new UnknownHostException("metadata.google.internal"))))
        .isFalse();
    assertThat(isRetryableCredentialsError(new IOException("unknown"))).isFalse();
  }

  /**
   * Mimics the error thrown by {@code ComputeEngineCredentials} when the metadata server returns an
   * error: only 503 is wrapped into a retryable {@code GoogleAuthException}, any other status code
   * is reported as a plain {@link IOException} with the status code in the message only.
   */
  private static IOException metadataServerError(int statusCode) {
    if (statusCode == 503) {
      return new RetryableIOException(/* retryable= */ true, httpResponseException(statusCode));
    }
    if (statusCode == 404) {
      return new IOException(
          "Error code 404 trying to get security access token from Compute Engine metadata for"
              + " the default service account. This may be because the virtual machine instance"
              + " does not have permission scopes specified.");
    }
    return new IOException(
        String.format(
            "Unexpected Error code %s trying to get security access token from Compute Engine"
                + " metadata for the default service account: ",
            statusCode));
  }

  /**
   * Mimics the {@code GoogleAuthException} thrown by {@code ServiceAccountCredentials} and {@code
   * UserCredentials} when the OAuth2 token endpoint returns an error.
   */
  private static IOException tokenEndpointError(int statusCode) {
    return new RetryableIOException(
        SDK_RETRYABLE_STATUS_CODES.contains(statusCode), httpResponseException(statusCode));
  }

  /**
   * Mimics the error thrown by {@code ImpersonatedCredentials} when the IAM API returns an error.
   */
  private static IOException iamCredentialsError(int statusCode) {
    return new IOException("Error requesting access token", httpResponseException(statusCode));
  }

  private static HttpResponseException httpResponseException(int statusCode) {
    return new HttpResponseException.Builder(statusCode, "error", new HttpHeaders()).build();
  }

  private RetryHttpInitializer createRetryHttpInitializer(
      Credentials credentials, Sleeper sleeper) {
    return new RetryHttpInitializer(
        credentials,
        RetryHttpInitializerOptions.builder()
            .setDefaultUserAgent("foo-user-agent")
            .setMaxRequestRetries(5)
            .build(),
        sleeper);
  }

  /** Mimics {@code com.google.auth.oauth2.GoogleAuthException}, which is package-private. */
  private static class RetryableIOException extends IOException implements Retryable {
    private final boolean retryable;

    RetryableIOException(boolean retryable) {
      this(retryable, /* cause= */ null);
    }

    RetryableIOException(boolean retryable, Throwable cause) {
      super("Retryable: " + retryable, cause);
      this.retryable = retryable;
    }

    @Override
    public boolean isRetryable() {
      return retryable;
    }

    @Override
    public int getRetryCount() {
      return 0;
    }
  }

  /** {@link Credentials} that throw provided errors before returning request metadata. */
  private static class FlakyCredentials extends Credentials {
    private final String authHeaderValue;
    private final Deque<IOException> errors;
    private int getRequestMetadataCalls = 0;

    FlakyCredentials(String authHeaderValue, IOException... errors) {
      this.authHeaderValue = authHeaderValue;
      this.errors = new ArrayDeque<>(Arrays.asList(errors));
    }

    @Override
    public String getAuthenticationType() {
      return "test-auth";
    }

    @Override
    public Map<String, List<String>> getRequestMetadata(URI uri) throws IOException {
      getRequestMetadataCalls++;
      if (!errors.isEmpty()) {
        throw errors.poll();
      }
      return ImmutableMap.of("Authorization", ImmutableList.of(authHeaderValue));
    }

    @Override
    public boolean hasRequestMetadata() {
      return true;
    }

    @Override
    public boolean hasRequestMetadataOnly() {
      return true;
    }

    @Override
    public void refresh() {
      throw new UnsupportedOperationException();
    }
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

  @Test
  public void successfulRequest_addsTrailingChecksum() throws IOException {
    String authHeaderValue = "Bearer: test-token";
    HttpRequestFactory requestFactory =
        mockTransport(emptyResponse(200))
            .createRequestFactory(createRetryHttpInitializer(new FakeCredentials(authHeaderValue)));

    byte[] testData = {1, 2, 3, 4};
    Hasher hasher = Hashing.crc32c().newHasher();
    hasher.putBytes(testData);

    String expectedChecksum = BaseEncoding.base64().encode(Ints.toByteArray(hasher.hash().asInt()));

    ChecksumContext.setChecksumSupplier(() -> expectedChecksum);

    try {
      HttpRequest req = requestFactory.buildPutRequest(new GenericUrl(URL), null);
      req.getHeaders().setContentRange("bytes 0-3/4");

      HttpResponse res = req.execute();

      // Verify that Interceptor added the header
      assertThat(res).isNotNull();
      assertThat(res.getStatusCode()).isEqualTo(HttpStatusCodes.STATUS_CODE_OK);
      assertThat(req.getHeaders()).containsKey("x-goog-hash");
      assertThat(req.getHeaders().getFirstHeaderStringValue("x-goog-hash"))
          .isEqualTo("crc32c=" + expectedChecksum);
    } finally {
      ChecksumContext.clear();
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
