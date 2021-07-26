/*
 * Copyright 2014 Google Inc. All Rights Reserved.
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

package com.google.cloud.hadoop.util;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.util.Sleeper;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Basic unittests for RetryHttpInitializer to check the proper wiring of various interceptors
 * and handlers for HttpRequests.
 */
@RunWith(JUnit4.class)
public class RetryHttpInitializerTest {
  // Mock to capture calls delegated to an underlying Credential.
  @Mock private Credential mockCredential;

  // Mock LowLevelHttpRequest always supplied by our fake HttpTransport.
  @Mock private LowLevelHttpRequest mockLowLevelRequest;

  // Mock LowLevelHttpResponse to return when the mock request is executed.
  @Mock private LowLevelHttpResponse mockLowLevelResponse;

  // Mock sleeper for backoff handlers to check when exponential backoff retries kick in.
  @Mock private Sleeper mockSleeper;

  // A fake factory set up to use the initializer we are testing.
  private HttpRequestFactory requestFactory;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    MockHttpTransport fakeTransport =
        new MockHttpTransport() {
          // Only override the method for retrieving a LowLevelHttpRequest.
          @Override
          public LowLevelHttpRequest buildRequest(String method, String url) {
            // TODO(user): Also record and test the number of calls to this and the method/url.
            return mockLowLevelRequest;
          }
        };
    RetryHttpInitializer initializer =
        new RetryHttpInitializer(
            mockCredential,
            RetryHttpInitializerOptions.builder()
                .setDefaultUserAgent("foo-user-agent")
                .setHttpHeaders(ImmutableMap.of("header-key", "header=value"))
                .setMaxRequestRetries(HttpRequest.DEFAULT_NUMBER_OF_RETRIES)
                .setConnectTimeout(Duration.ofMillis(20_000))
                .setReadTimeout(Duration.ofMillis(20_000))
                .build());
    initializer.setSleeperOverride(mockSleeper);
    requestFactory = fakeTransport.createRequestFactory(initializer);
  }

  @After
  public void tearDown() {
    verifyNoMoreInteractions(mockCredential);
    verifyNoMoreInteractions(mockSleeper);
  }

  @Test
  public void testConstructorNullCredential() {
    new RetryHttpInitializer(/* credential= */ null, "foo-user-agent");
  }

  @Test
  public void testInitializeWithInitializerDisabled() throws IOException {
    MockHttpTransport fakeTransport =
        new MockHttpTransport() {
          // Only override the method for retrieving a LowLevelHttpRequest.
          @Override
          public LowLevelHttpRequest buildRequest(String method, String url) {
            // TODO(user): Also record and test the number of calls to this and the method/url.
            return mockLowLevelRequest;
          }
        };
    RetryHttpInitializer initializer =
        new RetryHttpInitializer(
            mockCredential,
            RetryHttpInitializerOptions.builder()
                .setIsEnabled(false)
                .build());
    initializer.setSleeperOverride(mockSleeper);
    requestFactory = fakeTransport.createRequestFactory(initializer);

    when(mockLowLevelRequest.execute())
        .thenReturn(mockLowLevelResponse);
    when(mockLowLevelResponse.getStatusCode())
        .thenReturn(200);

    final HttpRequest req = requestFactory.buildGetRequest(new GenericUrl("http://fake-url.com"));
    req.execute();

    verifyNoInteractions(mockCredential);
  }

  @Test
  public void testBasicOperation() throws IOException {
    final String authHeaderValue = "Bearer a1b2c3d4";
    final HttpRequest req = requestFactory.buildGetRequest(new GenericUrl("http://fake-url.com"));
    assertThat(req.getHeaders().getUserAgent()).isEqualTo("foo-user-agent");
    assertThat(req.getHeaders().get("header-key")).isEqualTo("header=value");
    assertThat(req.getInterceptor()).isEqualTo(mockCredential);
    assertThat(((RetryHttpInitializer) requestFactory.getInitializer()).getCredential())
        .isEqualTo(mockCredential);

    // Simulate the actual behavior of inserting a header for the credential.
    doAnswer(
            unused -> {
              req.getHeaders().setAuthorization(authHeaderValue);
              return null;
            })
        .when(mockCredential)
        .intercept(eq(req));

    when(mockLowLevelRequest.execute())
        .thenReturn(mockLowLevelResponse);
    when(mockLowLevelResponse.getStatusCode())
        .thenReturn(200);

    HttpResponse res = req.execute();
    assertThat(res).isNotNull();

    verify(mockCredential).intercept(eq(req));
    verify(mockLowLevelRequest).addHeader(eq("Authorization"), eq(authHeaderValue));
    verify(mockLowLevelRequest).execute();
    verify(mockLowLevelResponse, times(2)).getStatusCode();
  }

  @Test
  public void testErrorCodeForbidden() throws IOException {
    final String authHeaderValue = "Bearer a1b2c3d4";
    final HttpRequest req = requestFactory.buildGetRequest(new GenericUrl("http://fake-url.com"));
    assertThat(req.getHeaders().getUserAgent()).isEqualTo("foo-user-agent");
    assertThat(req.getInterceptor()).isEqualTo(mockCredential);

    // Simulate the actual behavior of inserting a header for the credential.
    doAnswer(
            unused -> {
              req.getHeaders().setAuthorization(authHeaderValue);
              return null;
            })
        .doAnswer(
            unused -> {
              req.getHeaders().setAuthorization(authHeaderValue);
              return null;
            })
        .when(mockCredential)
        .intercept(eq(req));

    when(mockLowLevelRequest.execute())
        .thenReturn(mockLowLevelResponse)
        .thenReturn(mockLowLevelResponse);
    when(mockLowLevelResponse.getStatusCode())
        .thenReturn(403)
        .thenReturn(403)
        .thenReturn(200);
    when(mockCredential.handleResponse(eq(req), any(HttpResponse.class), eq(true)))
        .thenReturn(true);

    HttpResponse res = req.execute();
    assertThat(res).isNotNull();

    verify(mockCredential, times(2)).intercept(eq(req));
    verify(mockLowLevelRequest, times(2)).addHeader(eq("Authorization"), eq(authHeaderValue));
    verify(mockLowLevelRequest, times(2)).execute();
    verify(mockLowLevelResponse, times(4)).getStatusCode();
    verify(mockCredential).handleResponse(eq(req), any(HttpResponse.class), eq(true));
  }

  @Test
  public void testErrorCodeTransientServerError() throws Exception {
    testRetriesForErrorCode(503);
  }

  @Test
  public void testErrorCodeRateLimitExceeded() throws Exception {
    testRetriesForErrorCode(429);
  }

  /** Helper for test cases wanting to test retries kicking in for particular error codes. */
  private void testRetriesForErrorCode(int code) throws Exception {
    final String authHeaderValue = "Bearer a1b2c3d4";
    final HttpRequest req = requestFactory.buildGetRequest(new GenericUrl("http://fake-url.com"));
    assertThat(req.getHeaders().getUserAgent()).isEqualTo("foo-user-agent");
    assertThat(req.getInterceptor()).isEqualTo(mockCredential);

    // Simulate the actual behavior of inserting a header for the credential.
    doAnswer(
            unused -> {
              req.getHeaders().setAuthorization(authHeaderValue);
              return null;
            })
        .doAnswer(
            unused -> {
              req.getHeaders().setAuthorization(authHeaderValue);
              return null;
            })
        .when(mockCredential)
        .intercept(eq(req));

    when(mockLowLevelRequest.execute())
        .thenReturn(mockLowLevelResponse)
        .thenReturn(mockLowLevelResponse);
    when(mockLowLevelResponse.getStatusCode())
        .thenReturn(code)
        .thenReturn(code)
        .thenReturn(200);
    when(mockCredential.handleResponse(eq(req), any(HttpResponse.class), eq(true)))
        .thenReturn(false);

    HttpResponse res = req.execute();
    assertThat(res).isNotNull();

    verify(mockCredential, times(2)).intercept(eq(req));
    verify(mockLowLevelRequest, times(2)).addHeader(eq("Authorization"), eq(authHeaderValue));
    verify(mockLowLevelRequest, times(2)).execute();
    verify(mockLowLevelResponse, times(4)).getStatusCode();
    verify(mockCredential).handleResponse(eq(req), any(HttpResponse.class), eq(true));
    verify(mockSleeper).sleep(anyLong());
  }

  @Test
  public void testThrowIOException() throws Exception {
    final String authHeaderValue = "Bearer a1b2c3d4";
    final HttpRequest req = requestFactory.buildGetRequest(new GenericUrl("http://fake-url.com"));
    assertThat(req.getHeaders().getUserAgent()).isEqualTo("foo-user-agent");
    assertThat(req.getInterceptor()).isEqualTo(mockCredential);

    // Simulate the actual behavior of inserting a header for the credential.
    doAnswer(
            unused -> {
              req.getHeaders().setAuthorization(authHeaderValue);
              return null;
            })
        .doAnswer(
            unused -> {
              req.getHeaders().setAuthorization(authHeaderValue);
              return null;
            })
        .when(mockCredential)
        .intercept(eq(req));

    when(mockLowLevelRequest.execute())
        .thenThrow(new IOException("fake IOException"))
        .thenReturn(mockLowLevelResponse);
    when(mockLowLevelResponse.getStatusCode())
        .thenReturn(200);
    when(mockCredential.handleResponse(eq(req), any(HttpResponse.class), eq(true)))
        .thenReturn(false);

    HttpResponse res = req.execute();
    assertThat(res).isNotNull();

    verify(mockCredential, times(2)).intercept(eq(req));
    verify(mockLowLevelRequest, times(2)).addHeader(eq("Authorization"), eq(authHeaderValue));
    verify(mockLowLevelRequest, times(2)).execute();
    verify(mockLowLevelResponse, times(2)).getStatusCode();
    verify(mockSleeper).sleep(anyLong());
  }
}
