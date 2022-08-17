/*
 * Copyright 2022 Google LLC. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.util.interceptors;

import com.google.api.client.http.HttpExecuteInterceptor;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import java.io.IOException;
import java.util.UUID;

/**
 * HTTP request interceptor to attach unique identifier i.e. invocationId to each new request and
 * make sure to pass in the same identifier if request is retried.
 */
public final class InvocationIdInterceptor implements HttpExecuteInterceptor {

  public static final String GCCL_INVOCATION_ID_PREFIX = "gccl-invocation-id/";

  public static final String GOOG_API_CLIENT = "x-goog-api-client";

  private final HttpExecuteInterceptor interceptor;

  public InvocationIdInterceptor(HttpExecuteInterceptor interceptor) {
    this.interceptor = interceptor;
  }

  @Override
  public void intercept(HttpRequest request) throws IOException {
    if (this.interceptor != null) {
      this.interceptor.intercept(request);
    }
    HttpHeaders headers = request.getHeaders();
    String existing = (String) headers.get(GOOG_API_CLIENT);
    if (isInvocationIdPresent(existing)) {
      // As invocationId is already present,It's a retried request.
      // TODO: add support for attempt_count
      return;
    }

    final String signatureKey = "Signature="; // For V2 and V4 signedURLs
    final String builtURL = request.getUrl().build();
    if (!builtURL.contains(signatureKey)) {
      UUID invocationId = UUID.randomUUID();
      String invocationEntry = GCCL_INVOCATION_ID_PREFIX + invocationId;
      final String newValue;
      if (existing != null && !existing.isEmpty()) {
        newValue = existing + " " + invocationEntry;
      } else {
        newValue = invocationEntry;
      }
      headers.set(GOOG_API_CLIENT, newValue);
    }
  }

  private static boolean isInvocationIdPresent(String apiClientHeader) {
    return apiClientHeader != null && apiClientHeader.contains(GCCL_INVOCATION_ID_PREFIX)
        ? true
        : false;
  }
}
