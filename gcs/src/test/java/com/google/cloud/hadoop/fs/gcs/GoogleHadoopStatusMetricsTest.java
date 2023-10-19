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

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatusStatistic.GCS_CLIENT_RATE_LIMIT_COUNT;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.emptyResponse;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.mockTransport;
import static com.google.common.truth.Truth.assertThat;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpStatusCodes;
import com.google.cloud.hadoop.util.RetryHttpInitializer;
import com.google.cloud.hadoop.util.RetryHttpInitializerOptions;
import com.google.cloud.hadoop.util.interceptors.InvocationIdInterceptor;
import com.google.common.collect.ImmutableMap;
import java.time.Duration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatusStatistic} class. */
@RunWith(JUnit4.class)
public class GoogleHadoopStatusMetricsTest {

  @Test
  public void gcs_client_429_status_metrics() throws Exception {

    GhfsStorageStatistics ghfsStorageStatistics = new GhfsStorageStatistics();

    String authHeaderValue = "Bearer: y2.WAKiHahzxGS_a1bd40RjNUF";

    RetryHttpInitializer retryHttpInitializer =
        new RetryHttpInitializer(
            null,
            RetryHttpInitializerOptions.builder()
                .setDefaultUserAgent("foo-user-agent")
                .setHttpHeaders(ImmutableMap.of("header-key", "header-value"))
                .setMaxRequestRetries(5)
                .setConnectTimeout(Duration.ofSeconds(5))
                .setReadTimeout(Duration.ofSeconds(5))
                .build(),
            ghfsStorageStatistics);

    HttpRequestFactory requestFactory =
        mockTransport(emptyResponse(429), emptyResponse(429), emptyResponse(200))
            .createRequestFactory(retryHttpInitializer);

    HttpRequest req = requestFactory.buildGetRequest(new GenericUrl("http://fake-url.com"));

    HttpResponse res = req.execute();

    assertThat((String) req.getHeaders().get(InvocationIdInterceptor.GOOG_API_CLIENT))
        .contains(InvocationIdInterceptor.GCCL_INVOCATION_ID_PREFIX);
    assertThat(res).isNotNull();
    assertThat(res.getStatusCode()).isEqualTo(HttpStatusCodes.STATUS_CODE_OK);
    assertThat(ghfsStorageStatistics.getLong(GCS_CLIENT_RATE_LIMIT_COUNT.getSymbol()))
        .isNotEqualTo(0);
  }
}
