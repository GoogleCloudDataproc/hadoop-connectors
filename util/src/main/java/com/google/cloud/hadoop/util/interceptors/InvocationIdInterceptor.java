/*
 * Copyright 2022 Google LLC
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

package com.google.cloud.hadoop.util.interceptors;

import com.google.api.client.http.HttpExecuteInterceptor;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.cloud.hadoop.util.InvocationIdContext;
import com.google.cloud.hadoop.util.RequestTracker;
import com.google.cloud.hadoop.util.ThreadTrace;
import com.google.cloud.hadoop.util.TraceOperation;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;

/**
 * HTTP request interceptor to attach unique identifier i.e. invocationId to each new request and
 * make sure to pass in the same identifier if request is retried.
 */
@VisibleForTesting
public final class InvocationIdInterceptor implements HttpExecuteInterceptor {

  public static final String GCCL_INVOCATION_ID_PREFIX = "gccl-invocation-id/";

  public static final String GOOG_API_CLIENT = "x-goog-api-client";

  private final HttpExecuteInterceptor interceptor;
  private final RequestTracker tracker;

  public InvocationIdInterceptor(HttpExecuteInterceptor interceptor, RequestTracker tracker) {
    this.interceptor = interceptor;
    this.tracker = tracker;
  }

  @Override
  public void intercept(HttpRequest request) throws IOException {
    if (this.interceptor != null) {
      this.interceptor.intercept(request);
    }

    tracker.init(request);
    HttpHeaders headers = request.getHeaders();
    String existing = (String) headers.get(GOOG_API_CLIENT);
    if (isInvocationIdPresent(existing)) {
      // As invocationId is already present,It's a retried request.
      // TODO: add support for attempt_count
      return;
    }
    // Replicating the logic from `manual` client.
    // reference:
    // https://github.com/googleapis/java-storage/blob/main/google-cloud-storage/src/main/java/com/google/cloud/storage/spi/v1/HttpStorageRpc.java#L156-L177
    final String signatureKey = "Signature="; // For V2 and V4 signedURLs
    final String builtURL = request.getUrl().build();
    if (!builtURL.contains(signatureKey)) {
      // Thread-safe: InvocationIdContext uses thread-local storage, so each thread has its own
      // invocationId.
      if (InvocationIdContext.getInvocationId().isEmpty()) {
        InvocationIdContext.setInvocationId();
      }
      String invocationEntry = InvocationIdContext.getInvocationId();
      final String newValue;
      if (existing != null && !existing.isEmpty()) {
        newValue = existing + " " + invocationEntry;
      } else {
        newValue = invocationEntry;
      }

      headers.set(GOOG_API_CLIENT, newValue);

      ThreadTrace tt = TraceOperation.current();
      if (tt != null) {
        tt.annotate("invocationId", newValue);
      }
    }
  }

  private static boolean isInvocationIdPresent(String apiClientHeader) {
    return (apiClientHeader != null && apiClientHeader.contains(GCCL_INVOCATION_ID_PREFIX));
  }
}
