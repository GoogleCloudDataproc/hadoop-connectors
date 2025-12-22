/*
 * Copyright 2025 Google LLC
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

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpExecuteInterceptor;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Ints;
import java.io.IOException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ChecksumHeaderInterceptorTest {

  private ChecksumHeaderInterceptor interceptor;
  private HttpExecuteInterceptor delegate;
  private HttpRequest request;
  private HttpHeaders headers;

  @Before
  public void setUp() throws IOException {
    delegate = mock(HttpExecuteInterceptor.class);
    interceptor = new ChecksumHeaderInterceptor(delegate);
    request =
        new MockHttpTransport()
            .createRequestFactory()
            .buildRequest("PUT", new GenericUrl("http://test"), null);
    headers = new HttpHeaders();
    request.setHeaders(headers);
  }

  @After
  public void tearDown() {
    ChecksumContext.CURRENT_HASHER.remove();
  }

  @Test
  public void intercept_delegatesExecution() throws IOException {
    interceptor.intercept(request);
    verify(delegate).intercept(request);
  }

  @Test
  public void intercept_noHasher_skipsHeader() throws IOException {
    headers.setContentRange("bytes 0-100/101");

    interceptor.intercept(request);

    // header not set since no Hasher was set
    assertThat(headers.get("x-goog-hash")).isNull();
  }

  @Test
  public void intercept_notPutMethod_skipsHeader() throws IOException {
    setupHasher(new byte[] {1});
    request.setRequestMethod("POST");
    headers.setContentRange("bytes 0-100/101");

    interceptor.intercept(request);

    assertThat(headers.get("x-goog-hash")).isNull();
  }

  @Test
  public void intercept_intermediateChunk_skipsHeader() throws IOException {
    setupHasher(new byte[] {1});
    headers.setContentRange("bytes 0-100/*");

    interceptor.intercept(request);

    assertThat(headers.get("x-goog-hash")).isNull();
  }

  @Test
  public void intercept_finalChunk_addsHeader() throws IOException {
    byte[] data = {1, 2, 3, 4};
    Hasher hasher = setupHasher(data);
    String expectedChecksum = BaseEncoding.base64().encode(Ints.toByteArray(hasher.hash().asInt()));
    headers.setContentRange("bytes 0-3/4");

    interceptor.intercept(request);

    assertThat(headers.getFirstHeaderStringValue("x-goog-hash"))
        .isEqualTo("crc32c=" + expectedChecksum);
  }

  private Hasher setupHasher(byte[] data) {
    Hasher hasher = Hashing.crc32c().newHasher();
    if (data != null) {
      hasher.putBytes(data);
    }
    ChecksumContext.CURRENT_HASHER.set(hasher);
    return hasher;
  }
}
