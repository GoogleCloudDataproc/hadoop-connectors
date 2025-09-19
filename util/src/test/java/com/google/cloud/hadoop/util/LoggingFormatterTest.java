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

import static org.junit.Assert.*;

import java.util.UUID;
import java.util.logging.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class LoggingFormatterTest {

  private static final String GCS_CONNECTOR_LOGGER_NAME = "com.google.cloud.hadoop.test.logger";
  private static final String OTHER_LOGGER_NAME = "org.apache.hadoop.test.logger";

  private LoggingFormatter formatter;

  @Before
  public void setUp() {
    formatter =
        new LoggingFormatter(
            new Formatter() {
              @Override
              public String format(LogRecord record) {
                return String.format("%s%n", record.getMessage());
              }
            });
    // Ensure a fresh Invocation ID is set for each test
    InvocationIdContext.setInvocationId();
  }

  @After
  public void tearDown() {
    // Clean up the ThreadLocal after each test
    InvocationIdContext.clear();
  }

  @Test
  public void format_withMatchingLoggerAndInvocationId_prefixesMessageWithId() {
    String invocationId = InvocationIdContext.getInvocationId();
    LogRecord record = new LogRecord(java.util.logging.Level.INFO, "Test log message");
    record.setLoggerName(GCS_CONNECTOR_LOGGER_NAME);

    String formattedMessage = formatter.format(record);

    assertEquals("[" + invocationId + "]: Test log message\n", formattedMessage);
  }

  @Test
  public void format_withNonMatchingLoggerName_doesNotPrefixMessage() {
    LogRecord record = new LogRecord(java.util.logging.Level.INFO, "Test log message");
    record.setLoggerName(OTHER_LOGGER_NAME);

    String formattedMessage = formatter.format(record);

    // Should not be prefixed because the logger name doesn't match
    assertEquals("Test log message\n", formattedMessage);
  }

  @Test
  public void format_withJsonMessage_doesNotPrefixMessage() {
    LogRecord record = new LogRecord(java.util.logging.Level.INFO, "{\"key\":\"value\"}");
    record.setLoggerName(GCS_CONNECTOR_LOGGER_NAME);

    String formattedMessage = formatter.format(record);

    // Should not be prefixed because the message is JSON
    assertEquals("{\"key\":\"value\"}\n", formattedMessage);
  }

  @Test
  public void format_withEmptyInvocationId_doesNotPrefixMessage() {
    InvocationIdContext.clear();
    LogRecord record = new LogRecord(java.util.logging.Level.INFO, "Test log message");
    record.setLoggerName(GCS_CONNECTOR_LOGGER_NAME);

    String formattedMessage = formatter.format(record);

    // Should not be prefixed because the invocation ID is empty
    assertEquals("Test log message\n", formattedMessage);
  }

  @Test
  public void format_withNullLoggerName_doesNotThrowNpeOrPrefixMessage() {
    // This record will have a null logger name by default
    LogRecord record = new LogRecord(java.util.logging.Level.INFO, "Test log message");

    String formattedMessage = formatter.format(record);

    // Should not throw an NPE and should not be prefixed
    assertEquals("Test log message\n", formattedMessage);
  }

  @Test
  public void addFormatter_toLogger_addsFormatterToAllHandlers() {
    Logger logger = Logger.getLogger("testLogger");

    LoggingFormatter.addFormatter(logger);

    // Verify that the formatter is added to all handlers
    for (var handler : logger.getHandlers()) {
      assertEquals(LoggingFormatter.class, handler.getFormatter().getClass());
    }
  }

  @Test
  public void addFormatter_withLoggerWithNoHandlers_doesNotThrowException() {
    // Get a logger that doesn't exist yet to ensure it has no handlers.
    // A unique name prevents interference from other tests.
    Logger loggerWithNoHandlers = Logger.getLogger("logger.with.no.handlers." + UUID.randomUUID());
    loggerWithNoHandlers.setUseParentHandlers(false);
    assertEquals(0, loggerWithNoHandlers.getHandlers().length);

    // This call should not throw any exception.
    LoggingFormatter.addFormatter(loggerWithNoHandlers);
  }

  @Test
  public void addFormatterToHandler_isIdempotent() {
    Handler handler = new StreamHandler();
    // Set a simple formatter to be wrapped.
    handler.setFormatter(
        new Formatter() {
          @Override
          public String format(LogRecord record) {
            return record.getMessage();
          }
        });

    // First call to wrap the formatter.
    LoggingFormatter.addFormatterToHandler(handler);
    Formatter formatter1 = handler.getFormatter();

    // Second call should be a no-op.
    LoggingFormatter.addFormatterToHandler(handler);
    Formatter formatter2 = handler.getFormatter();

    // Assert first call wraps the formatter and the second formatter instance is the same, meaning
    // it wasn't wrapped again.
    assertTrue(formatter1 instanceof LoggingFormatter);
    assertSame(formatter1, formatter2);
  }
}
