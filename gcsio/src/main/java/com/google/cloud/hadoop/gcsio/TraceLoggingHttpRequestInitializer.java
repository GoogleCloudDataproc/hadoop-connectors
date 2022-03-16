package com.google.cloud.hadoop.gcsio;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponse;
import com.google.cloud.MonitoredResource;
import com.google.cloud.logging.LogEntry;
import com.google.cloud.logging.Logging;
import com.google.cloud.logging.Payload.JsonPayload;
import com.google.cloud.logging.Severity;
import com.google.common.base.Stopwatch;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class TraceLoggingHttpRequestInitializer implements HttpRequestInitializer {

  private final Map<HttpRequest, Stopwatch> requestTracker = new ConcurrentHashMap<>();
  private final Logging logging;

  public TraceLoggingHttpRequestInitializer(Logging logging) {
    this.logging = logging;
  }

  @Override
  public void initialize(HttpRequest request) throws IOException {
    request.setInterceptor(
        httpRequest -> requestTracker.put(httpRequest, Stopwatch.createStarted()));
    request.setResponseInterceptor(this::log);
  }

  private void log(HttpResponse httpResponse) {
    HttpRequest request = httpResponse.getRequest();
    Stopwatch stopwatch = requestTracker.get(request);
    requestTracker.remove(request);
    Long latency = (stopwatch == null) ? null : stopwatch.elapsed(MILLISECONDS);

    Map<String, Object> jsonMap = new HashMap<>();
    jsonMap.put("request_method", request.getRequestMethod());
    jsonMap.put("request_url", request.getUrl().toString());
    jsonMap.put("request_headers", request.getHeaders());
    jsonMap.put("response_headers", httpResponse.getHeaders());
    jsonMap.put("response_status_code", httpResponse.getStatusCode());
    jsonMap.put("response_status_message", httpResponse.getStatusMessage());
    jsonMap.put("latency", latency);
    jsonMap.put("thread_name", Thread.currentThread().getName());
    try {
      jsonMap.put("hostname", InetAddress.getLocalHost().getHostName());
    } catch (UnknownHostException e) {
      // ignore
    }
    jsonMap.put("protocol", "json");
    LogEntry logEntry =
        LogEntry.newBuilder(JsonPayload.of(jsonMap))
            .setSeverity(Severity.INFO)
            .setLogName("dataproc-gcs-performance")
            .setResource(
                MonitoredResource.newBuilder("global").addLabel("library", "gcsio").build())
            .build();

    logging.write(Collections.singleton(logEntry));
  }
}
