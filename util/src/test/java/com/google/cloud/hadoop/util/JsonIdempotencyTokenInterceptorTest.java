/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.util;

import static com.google.common.truth.Truth.assertThat;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpExecuteInterceptor;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.testing.http.MockHttpTransport;
import java.io.IOException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class JsonIdempotencyTokenInterceptorTest {

  @Test
  public void intercept_addsIdempotencyTokenHeader_ifNotPresent() throws IOException {
    HttpTransport transport = new MockHttpTransport();
    HttpRequestFactory requestFactory = transport.createRequestFactory();
    HttpRequest request = requestFactory.buildGetRequest(new GenericUrl("http://fake.url"));

    JsonIdempotencyTokenInterceptor interceptor = new JsonIdempotencyTokenInterceptor(null);
    interceptor.intercept(request);

    assertThat(
            request
                .getHeaders()
                .containsKey(JsonIdempotencyTokenInterceptor.IDEMPOTENCY_TOKEN_HEADER))
        .isTrue();
    assertThat(
            (String)
                request.getHeaders().get(JsonIdempotencyTokenInterceptor.IDEMPOTENCY_TOKEN_HEADER))
        .isNotEmpty();
  }

  @Test
  public void intercept_doesNotOverwriteExistingToken() throws IOException {
    HttpTransport transport = new MockHttpTransport();
    HttpRequestFactory requestFactory = transport.createRequestFactory();
    HttpRequest request = requestFactory.buildGetRequest(new GenericUrl("http://fake.url"));
    String existingToken = "existing-token";
    request
        .getHeaders()
        .set(JsonIdempotencyTokenInterceptor.IDEMPOTENCY_TOKEN_HEADER, existingToken);

    JsonIdempotencyTokenInterceptor interceptor = new JsonIdempotencyTokenInterceptor(null);
    interceptor.intercept(request);

    assertThat(
            (String)
                request.getHeaders().get(JsonIdempotencyTokenInterceptor.IDEMPOTENCY_TOKEN_HEADER))
        .isEqualTo(existingToken);
  }

  @Test
  public void intercept_callsChainedInterceptor() throws IOException {
    HttpTransport transport = new MockHttpTransport();
    HttpRequestFactory requestFactory = transport.createRequestFactory();
    HttpRequest request = requestFactory.buildGetRequest(new GenericUrl("http://fake.url"));
    TestInterceptor chainedInterceptor = new TestInterceptor();

    JsonIdempotencyTokenInterceptor interceptor =
        new JsonIdempotencyTokenInterceptor(chainedInterceptor);
    interceptor.intercept(request);

    assertThat(chainedInterceptor.intercepted).isTrue();
    assertThat(
            request
                .getHeaders()
                .containsKey(JsonIdempotencyTokenInterceptor.IDEMPOTENCY_TOKEN_HEADER))
        .isTrue();
  }

  @Test
  public void intercept_tokenIsConsistentAcrossRetries() throws IOException {
    HttpTransport transport = new MockHttpTransport();
    HttpRequestFactory requestFactory = transport.createRequestFactory();
    HttpRequest request = requestFactory.buildGetRequest(new GenericUrl("http://fake.url"));

    JsonIdempotencyTokenInterceptor interceptor = new JsonIdempotencyTokenInterceptor(null);

    // First attempt
    interceptor.intercept(request);
    String token1 =
        (String) request.getHeaders().get(JsonIdempotencyTokenInterceptor.IDEMPOTENCY_TOKEN_HEADER);
    assertThat(token1).isNotEmpty();

    // Retry attempt on the same request object
    interceptor.intercept(request);
    String token2 =
        (String) request.getHeaders().get(JsonIdempotencyTokenInterceptor.IDEMPOTENCY_TOKEN_HEADER);

    assertThat(token2).isEqualTo(token1);
  }

  private static class TestInterceptor implements HttpExecuteInterceptor {
    boolean intercepted = false;

    @Override
    public void intercept(HttpRequest request) throws IOException {
      intercepted = true;
    }
  }
}
