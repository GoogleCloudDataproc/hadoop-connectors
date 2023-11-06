/*
 * Copyright 2023 Google LLC
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

package com.google.cloud.hadoop.fs.gcs;

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatusStatistics.GCS_API_TOTAL;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatusStatistics.GCS_CLIENT_RATE_LIMIT_COUNT;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.emptyResponse;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.mockTransport;
import static com.google.common.truth.Truth.assertThat;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpStatusCodes;
import com.google.auth.Credentials;
import com.google.cloud.hadoop.util.RetryHttpInitializer;
import com.google.cloud.hadoop.util.RetryHttpInitializerOptions;
import com.google.cloud.hadoop.util.interceptors.InvocationIdInterceptor;
import com.google.common.collect.ImmutableMap;
import java.net.URI;
import java.time.Duration;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link GoogleCloudStorageStatusStatistics} class. */
@RunWith(JUnit4.class)
public class GoogleHadoopStatusMetricsTest {

  private GhfsInstrumentation ghfsInstrumentation;

  @Before
  public void Before() {
    URI initUri = new Path("gs://test/").toUri();
    this.ghfsInstrumentation = new GhfsInstrumentation(initUri);
  }

  private RetryHttpInitializer createRetryHttpInitializer(Credentials credentials) {
    return new RetryHttpInitializer(
        null,
        RetryHttpInitializerOptions.builder()
            .setDefaultUserAgent("foo-user-agent")
            .setHttpHeaders(ImmutableMap.of("header-key", "header-value"))
            .setMaxRequestRetries(5)
            .setConnectTimeout(Duration.ofSeconds(5))
            .setReadTimeout(Duration.ofSeconds(5))
            .build(),
        ghfsInstrumentation);
  }

  @Test
  public void test_gcsClientRateLimitCount() throws Exception {

    RetryHttpInitializer retryHttpInitializer = createRetryHttpInitializer(null);

    HttpRequestFactory requestFactory =
        mockTransport(emptyResponse(429), emptyResponse(200))
            .createRequestFactory(retryHttpInitializer);

    HttpRequest req = requestFactory.buildGetRequest(new GenericUrl("http://fake-url.com"));

    HttpResponse res = req.execute();

    assertThat((String) req.getHeaders().get(InvocationIdInterceptor.GOOG_API_CLIENT))
        .contains(InvocationIdInterceptor.GCCL_INVOCATION_ID_PREFIX);
    assertThat(res).isNotNull();
    assertThat(res.getStatusCode()).isEqualTo(HttpStatusCodes.STATUS_CODE_OK);
    assertThat(
            (ghfsInstrumentation)
                .getIOStatistics()
                .counters()
                .get(GCS_CLIENT_RATE_LIMIT_COUNT.getSymbol()))
        .isEqualTo(1);
  }

  @Test
  public void test_gcsRequestCount() throws Exception {
    RetryHttpInitializer retryHttpInitializer = createRetryHttpInitializer(null);
    HttpRequestFactory requestFactory =
        mockTransport(emptyResponse(429)).createRequestFactory(retryHttpInitializer);

    HttpRequest req = requestFactory.buildGetRequest(new GenericUrl("http://fake-url.com"));
    assertThat(
            ((GhfsInstrumentation) ghfsInstrumentation)
                .getIOStatistics()
                .counters()
                .get(GCS_API_TOTAL.getSymbol()))
        .isEqualTo(1);
  }
}
