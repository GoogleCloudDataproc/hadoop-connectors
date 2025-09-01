package com.google.cloud.hadoop.util;

import static org.junit.Assert.assertEquals;

import java.util.logging.Formatter;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
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
}
