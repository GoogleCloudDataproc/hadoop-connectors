/*
 * Copyright 2026 Google LLC
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

import static com.google.cloud.hadoop.util.interceptors.InvocationIdInterceptor.GCCL_INVOCATION_ID_PREFIX;
import static com.google.cloud.hadoop.util.interceptors.InvocationIdInterceptor.GOOG_API_CLIENT;
import static com.google.common.truth.Truth.assertThat;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.cloud.hadoop.util.interceptors.InvocationIdInterceptor;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class InvocationIdInterceptorTest {

  private static final String OBJECT_URL_1 = "https://storage.googleapis.com/b/bucket/o/obj1";
  private static final String OBJECT_URL_2 = "https://storage.googleapis.com/b/bucket/o/obj2";

  private static final Pattern INVOCATION_ID_PATTERN =
      Pattern.compile(
          Pattern.quote(GCCL_INVOCATION_ID_PREFIX)
              + "([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})");

  private final HttpTransport transport = new MockHttpTransport();

  @Before
  @After
  public void clearInvocationId() {
    InvocationIdContext.clear();
  }

  private HttpRequest buildRequest(String url) throws IOException {
    return transport.createRequestFactory().buildGetRequest(new GenericUrl(url));
  }

  private static InvocationIdInterceptor newInterceptor(HttpRequest request) {
    return new InvocationIdInterceptor(/* interceptor= */ null, RequestTracker.create(request));
  }

  private static String getInvocationId(HttpRequest request) {
    String header = (String) request.getHeaders().get(GOOG_API_CLIENT);
    assertThat(header).isNotNull();
    Matcher matcher = INVOCATION_ID_PATTERN.matcher(header);
    assertThat(matcher.find()).isTrue();
    return matcher.group(1);
  }

  @Test
  public void separateRequestsShouldNotShareInvocationId() throws IOException {
    HttpRequest req1 = buildRequest(OBJECT_URL_1);
    HttpRequest req2 = buildRequest(OBJECT_URL_2);

    try (InvocationIdContext.Scope ignored = InvocationIdContext.openScope()) {
      newInterceptor(req1).intercept(req1);
      newInterceptor(req2).intercept(req2);
    }

    assertThat(req1.getHeaders().get(GOOG_API_CLIENT))
        .isNotEqualTo(req2.getHeaders().get(GOOG_API_CLIENT));
    assertThat(getInvocationId(req1)).isNotEqualTo(getInvocationId(req2));
  }

  @Test
  public void intercept_withoutOperation_setsUniqueUuidAndDoesNotTouchContext() throws IOException {
    HttpRequest req1 = buildRequest(OBJECT_URL_1);
    HttpRequest req2 = buildRequest(OBJECT_URL_2);

    newInterceptor(req1).intercept(req1);
    newInterceptor(req2).intercept(req2);

    assertThat(getInvocationId(req1)).hasLength(36);
    assertThat(getInvocationId(req1)).isNotEqualTo(getInvocationId(req2));
    assertThat(InvocationIdContext.getInvocationId()).isEmpty();
  }

  @Test
  public void intercept_withinOperation_sharesOperationPrefixButIsUniquePerRequest()
      throws IOException {
    HttpRequest req1 = buildRequest(OBJECT_URL_1);
    HttpRequest req2 = buildRequest(OBJECT_URL_2);

    String operationPrefix;
    try (InvocationIdContext.Scope ignored = InvocationIdContext.openScope()) {
      operationPrefix =
          InvocationIdContext.getInvocationId().substring(GCCL_INVOCATION_ID_PREFIX.length());
      newInterceptor(req1).intercept(req1);
      newInterceptor(req2).intercept(req2);
    }

    String id1 = getInvocationId(req1);
    String id2 = getInvocationId(req2);
    assertThat(operationPrefix).hasLength(8);
    assertThat(id1).startsWith(operationPrefix + "-");
    assertThat(id2).startsWith(operationPrefix + "-");
    assertThat(id1).hasLength(36);
    assertThat(id1).isNotEqualTo(id2);
  }

  @Test
  public void intercept_retriedRequest_keepsInvocationId() throws IOException {
    HttpRequest request = buildRequest(OBJECT_URL_1);
    InvocationIdInterceptor interceptor = newInterceptor(request);

    try (InvocationIdContext.Scope ignored = InvocationIdContext.openScope()) {
      interceptor.intercept(request);
      String firstAttemptHeader = (String) request.getHeaders().get(GOOG_API_CLIENT);

      interceptor.intercept(request);
      interceptor.intercept(request);

      assertThat(request.getHeaders().get(GOOG_API_CLIENT)).isEqualTo(firstAttemptHeader);
    }
  }

  @Test
  public void intercept_appendsToExistingApiClientHeader() throws IOException {
    HttpRequest request = buildRequest(OBJECT_URL_1);
    request.getHeaders().set(GOOG_API_CLIENT, "gl-java/11.0.26 gdcl/2.1.1");

    newInterceptor(request).intercept(request);

    String header = (String) request.getHeaders().get(GOOG_API_CLIENT);
    assertThat(header).startsWith("gl-java/11.0.26 gdcl/2.1.1 " + GCCL_INVOCATION_ID_PREFIX);
    assertThat(getInvocationId(request)).hasLength(36);
  }

  @Test
  public void intercept_signedUrl_doesNotAddInvocationId() throws IOException {
    HttpRequest request =
        buildRequest(
            "https://storage.googleapis.com/bucket/obj?X-Goog-Algorithm=GOOG4-RSA-SHA256"
                + "&X-Goog-Signature=abcdef");

    newInterceptor(request).intercept(request);

    assertThat(request.getHeaders().get(GOOG_API_CLIENT)).isNull();
  }
}
