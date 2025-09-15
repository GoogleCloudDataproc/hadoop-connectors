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

import static org.mockito.Mockito.*;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.hadoop.util.interceptors.LoggingInterceptor;
import com.google.cloud.logging.LogEntry;
import com.google.cloud.logging.Logging;
import com.google.cloud.logging.Payload.StringPayload;
import com.google.cloud.logging.Severity;
import java.util.Collections;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import org.junit.Before;
import org.junit.Test;

public class LoggingInterceptorTest {

  private Logging mockLogging;
  private LoggingInterceptor loggingInterceptor;

  @Before
  public void setUp() {
    mockLogging = mock(Logging.class);
    loggingInterceptor =
        new LoggingInterceptor(GoogleCredentials.newBuilder().build(), "") {
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
        new LoggingInterceptor(GoogleCredentials.newBuilder().build(), "suffix") {
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
        new LoggingInterceptor(GoogleCredentials.newBuilder().build(), "") {
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
}
