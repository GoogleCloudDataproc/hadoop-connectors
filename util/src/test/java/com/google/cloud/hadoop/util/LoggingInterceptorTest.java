package com.google.cloud.hadoop.util;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.*;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.hadoop.util.interceptors.LoggingInterceptor;
import com.google.cloud.logging.LogEntry;
import com.google.cloud.logging.Logging;
import com.google.cloud.logging.Payload.StringPayload;
import com.google.cloud.logging.Severity;
import java.util.Collections;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;

@RunWith(JUnit4.class)
public class LoggingInterceptorTest {

  private Logging mockLogging;
  private LoggingInterceptor loggingInterceptor;

  @Before
  public void setUp() {
    mockLogging = mock(Logging.class);
    loggingInterceptor =
        new LoggingInterceptor(mock(GoogleCredentials.class), "") {
          @Override
          protected Logging createLoggingService(GoogleCredentials credentials) {
            return mockLogging;
          }
        };
  }

  @Test
  public void publishesLogEntryWithCorrectSeverity() {
    LogRecord record = new LogRecord(Level.SEVERE, "Critical error occurred");
    record.setSourceClassName("com.example.MyClass");
    record.setSourceMethodName("myMethod");
    loggingInterceptor.publish(record);

    LogEntry expectedEntry =
        LogEntry.newBuilder(StringPayload.of("Critical error occurred"))
            .setSeverity(Severity.ERROR)
            .setLogName("gcs-connector")
            .addLabel("class", "com.example.MyClass")
            .addLabel("method", "myMethod")
            .build();

    verify(mockLogging).write(Collections.singleton(expectedEntry));
  }

  @Test
  public void publishesLogEntryWithSuffixedLogName() {
    LoggingInterceptor customloggingInterceptor =
        new LoggingInterceptor(mock(GoogleCredentials.class), "suffix") {
          @Override
          protected Logging createLoggingService(GoogleCredentials credentials) {
            return mockLogging;
          }
        };
    LogRecord record = new LogRecord(Level.INFO, "Information message");
    record.setSourceClassName("com.example.MyClass");
    record.setSourceMethodName("myMethod");
    customloggingInterceptor.publish(record);

    LogEntry expectedEntry =
        LogEntry.newBuilder(StringPayload.of("Information message"))
            .setSeverity(Severity.INFO)
            .setLogName("gcs-connector-suffix")
            .addLabel("class", "com.example.MyClass")
            .addLabel("method", "myMethod")
            .build();

    verify(mockLogging).write(Collections.singleton(expectedEntry));
  }

  @Test
  public void doesNotPublishNonLoggableRecord() {
    LoggingInterceptor nonLoggableInterceptor =
        new LoggingInterceptor(mock(GoogleCredentials.class), "") {
          @Override
          public boolean isLoggable(LogRecord record) {
            return false; // Force isLoggable() to return false
          }

          @Override
          protected Logging createLoggingService(GoogleCredentials credentials) {
            return mockLogging;
          }
        };

    LogRecord record = new LogRecord(Level.FINE, "Debug message");
    nonLoggableInterceptor.publish(record);

    verify(mockLogging, never()).write(any());
  }

  @Test
  public void flushesLoggingService() {
    loggingInterceptor.flush();
    verify(mockLogging).flush();
  }

  @Test
  public void mapsUnknownLogLevelToDefaultSeverity() {
    LogRecord record = new LogRecord(Level.CONFIG, "Configuration message");
    record.setSourceClassName("com.example.MyClass");
    record.setSourceMethodName("myMethod");
    loggingInterceptor.publish(record);

    LogEntry expectedEntry =
        LogEntry.newBuilder(StringPayload.of("Configuration message"))
            .setSeverity(Severity.DEFAULT)
            .setLogName("gcs-connector")
            .addLabel("class", "com.example.MyClass")
            .addLabel("method", "myMethod")
            .build();

    verify(mockLogging).write(Collections.singleton(expectedEntry));
  }

  @Test
  public void publish_withFormatter_usesFormattedMessage() {
    // Arrange
    String originalMessage = "Original message";
    String formattedMessage = "Formatted: Original message\n";
    LogRecord record = new LogRecord(Level.INFO, originalMessage);
    record.setSourceClassName("com.example.MyClass");
    record.setSourceMethodName("myMethod");

    Formatter testFormatter =
        new Formatter() {
          @Override
          public String format(LogRecord r) {
            return "Formatted: " + r.getMessage() + "\n";
          }
        };
    loggingInterceptor.setFormatter(testFormatter);

    // Act
    loggingInterceptor.publish(record);

    // Assert
    ArgumentCaptor<Iterable<LogEntry>> captor = ArgumentCaptor.forClass(Iterable.class);
    verify(mockLogging).write(captor.capture());
    LogEntry entry = captor.getValue().iterator().next();
    StringPayload payload = (StringPayload) entry.getPayload();
    assertThat(payload.getData()).isEqualTo(formattedMessage);
  }

  @Test
  public void publish_withoutFormatter_usesOriginalMessage() {
    // Arrange
    String originalMessage = "Original message";
    LogRecord record = new LogRecord(Level.INFO, originalMessage);
    record.setSourceClassName("com.example.MyClass");
    record.setSourceMethodName("myMethod");
    loggingInterceptor.setFormatter(
        new Formatter() {
          @Override
          public String format(LogRecord record) {
            return record.getMessage();
          }
        }); // Ensure no formatter is set

    // Act
    loggingInterceptor.publish(record);

    // Assert
    ArgumentCaptor<Iterable<LogEntry>> captor = ArgumentCaptor.forClass(Iterable.class);
    verify(mockLogging).write(captor.capture());
    LogEntry entry = captor.getValue().iterator().next();
    StringPayload payload = (StringPayload) entry.getPayload();
    assertThat(payload.getData()).isEqualTo(originalMessage);
  }
}
