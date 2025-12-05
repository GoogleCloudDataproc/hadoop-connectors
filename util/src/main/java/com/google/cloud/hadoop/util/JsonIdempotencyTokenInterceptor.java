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

import com.google.api.client.http.HttpExecuteInterceptor;
import com.google.api.client.http.HttpRequest;
import java.io.IOException;
import java.util.UUID;

/** A {@link HttpExecuteInterceptor} that adds an idempotency token header to GCS requests. */
public class JsonIdempotencyTokenInterceptor implements HttpExecuteInterceptor {

  public static final String IDEMPOTENCY_TOKEN_HEADER = "x-goog-gcs-idempotency-token";

  private final HttpExecuteInterceptor chainedInterceptor;

  public JsonIdempotencyTokenInterceptor(HttpExecuteInterceptor chainedInterceptor) {
    this.chainedInterceptor = chainedInterceptor;
  }

  @Override
  public void intercept(HttpRequest request) throws IOException {
    if (!request.getHeaders().containsKey(IDEMPOTENCY_TOKEN_HEADER)) {
      request.getHeaders().set(IDEMPOTENCY_TOKEN_HEADER, UUID.randomUUID().toString());
    }
    if (chainedInterceptor != null) {
      chainedInterceptor.intercept(request);
    }
  }
}
