package com.google.cloud.hadoop.util.interceptors;

import com.google.api.client.http.HttpExecuteInterceptor;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import java.io.IOException;
import java.util.UUID;

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
    UUID invocationId = UUID.randomUUID();
    final String signatureKey = "Signature="; // For V2 and V4 signedURLs
    final String builtURL = request.getUrl().build();
    if (invocationId != null && !builtURL.contains(signatureKey)) {
      String invocationEntry = GCCL_INVOCATION_ID_PREFIX + invocationId;
      final String newValue;
      if (existing != null && !existing.isEmpty()) {
        newValue = existing + " " + invocationEntry;
      } else {
        newValue = invocationEntry;
      }
      headers.set("x-goog-api-client", newValue);
    }
  }

  private boolean isInvocationIdPresent(String apiClientHeader) {
    return apiClientHeader != null ? apiClientHeader.contains(GCCL_INVOCATION_ID_PREFIX) : false;
  }
}
