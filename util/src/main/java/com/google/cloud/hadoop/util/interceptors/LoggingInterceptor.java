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

package com.google.cloud.hadoop.util.interceptors;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.logging.LogEntry;
import com.google.cloud.logging.Logging;
import com.google.cloud.logging.LoggingOptions;
import com.google.cloud.logging.Payload.StringPayload;
import com.google.cloud.logging.Severity;
import java.util.Collections;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;

/**
 * A logging interceptor that publishes log records to Google Cloud Logging. This class extends
 * {@link Handler} to integrate with the Java logging framework.
 */
public class LoggingInterceptor extends Handler {

  private final Logging cloudLogging;
  private final String logNameSuffix;
  private static final String LOG_NAME_PREFIX = "gcs-connector";

  /**
   * Constructs a new {@code LoggingInterceptor}.
   *
   * @param credentials the Google Cloud credentials used to authenticate with the Logging service
   * @param logNameSuffix the suffix to append to the log name
   */
  public LoggingInterceptor(GoogleCredentials credentials, String logNameSuffix) {
    this.cloudLogging = createLoggingService(credentials);
    this.logNameSuffix = logNameSuffix;
  }

  /**
   * Creates a Google Cloud Logging service instance.
   *
   * @param credentials the Google Cloud credentials used to authenticate with the Logging service
   * @return a {@link Logging} instance
   */
  protected Logging createLoggingService(GoogleCredentials credentials) {
    return LoggingOptions.newBuilder().setCredentials(credentials).build().getService();
  }

  /**
   * Publishes a log record to Google Cloud Logging.
   *
   * @param record the log record to publish
   */
  @Override
  public void publish(LogRecord record) {
    if (!isLoggable(record)) {
      return;
    }
    String logName = String.join("-", LOG_NAME_PREFIX, logNameSuffix).replaceAll("-$", "");

    LogEntry entry =
        LogEntry.newBuilder(StringPayload.of(record.getMessage()))
            .setSeverity(mapToCloudSeverity(record.getLevel()))
            .setLogName(logName)
            .addLabel("class", record.getSourceClassName())
            .addLabel("method", record.getSourceMethodName())
            .build();

    cloudLogging.write(Collections.singleton(entry));
  }

  /** Flushes any buffered log entries to Google Cloud Logging. */
  @Override
  public void flush() {
    cloudLogging.flush();
  }

  /**
   * Closes the Google Cloud Logging service.
   *
   * @throws SecurityException if an error occurs while closing the service
   */
  @Override
  public void close() throws SecurityException {
    try {
      cloudLogging.close();
    } catch (Exception e) {
      throw new RuntimeException("Failed to close the Google Cloud Logging service", e);
    }
  }

  /**
   * Maps a {@link Level} to a corresponding Google Cloud Logging {@link Severity}.
   *
   * @param level the Java logging level
   * @return the corresponding Google Cloud Logging severity
   */
  private Severity mapToCloudSeverity(Level level) {
    switch (level.getName()) {
      case "SEVERE":
        return Severity.ERROR;
      case "WARNING":
        return Severity.WARNING;
      case "INFO":
        return Severity.INFO;
      case "FINE":
      case "FINER":
      case "FINEST":
        return Severity.DEBUG;
      default:
        return Severity.DEFAULT;
    }
  }
}
