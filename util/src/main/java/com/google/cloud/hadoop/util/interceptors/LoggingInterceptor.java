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

public class LoggingInterceptor extends Handler {

  private final Logging cloudLogging;
  private final String logNameSuffix;

  public LoggingInterceptor(GoogleCredentials credentials, String logNameSuffix) {
    this.cloudLogging = createLoggingService(credentials);
    this.logNameSuffix = logNameSuffix;
  }

  protected Logging createLoggingService(GoogleCredentials credentials) {
    return LoggingOptions.newBuilder().setCredentials(credentials).build().getService();
  }

  @Override
  public void publish(LogRecord record) {
    if (!isLoggable(record)) {
      return;
    }
    String logNamePrefix = "gcs-connector";
    String logName = String.join("-", logNamePrefix, logNameSuffix).replaceAll("-$", "");

    LogEntry entry =
        LogEntry.newBuilder(StringPayload.of(record.getMessage()))
            .setSeverity(mapToCloudSeverity(record.getLevel()))
            .setLogName(logName)
            .addLabel("class", record.getSourceClassName())
            .addLabel("method", record.getSourceMethodName())
            .build();

    cloudLogging.write(Collections.singleton(entry));
  }

  @Override
  public void flush() {
    cloudLogging.flush();
  }

  @Override
  public void close() throws SecurityException {
    try {
      cloudLogging.close();
    } catch (Exception e) {
      throw new RuntimeException("Failed to close the Google Cloud Logging service", e);
    }
  }

  private Severity mapToCloudSeverity(Level level) {
    if (level == Level.SEVERE) {
      return Severity.ERROR;
    } else if (level == Level.WARNING) {
      return Severity.WARNING;
    } else if (level == Level.INFO) {
      return Severity.INFO;
    } else if (level == Level.FINE || level == Level.FINER || level == Level.FINEST) {
      return Severity.DEBUG;
    } else {
      return Severity.DEFAULT;
    }
  }
}
