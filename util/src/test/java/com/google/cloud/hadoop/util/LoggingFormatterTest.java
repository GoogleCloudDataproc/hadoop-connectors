package com.google.cloud.hadoop.util;

import static org.junit.Assert.assertEquals;

import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.Before;
import org.junit.Test;

public class LoggingFormatterTest {

  private static String invocationId;
  private LoggingFormatter formatter;

  @Before
  public void setUp() {
    formatter = new LoggingFormatter();
    InvocationIdContext.setInvocationId();
    invocationId = InvocationIdContext.getInvocationId();
  }

  @Test
  public void testFormat_withInvocationId() {
    LogRecord record = new LogRecord(java.util.logging.Level.INFO, "Test log message");

    String formattedMessage = formatter.format(record);

    assertEquals("[" + invocationId + "]: Test log message\n", formattedMessage);
  }

  @Test
  public void testFormat_withJsonMessage() {
    LogRecord record = new LogRecord(java.util.logging.Level.INFO, "{\"key\":\"value\"}");

    String formattedMessage = formatter.format(record);

    assertEquals("{\"key\":\"value\"}\n", formattedMessage);
  }

  @Test
  public void testFormat_withEmptyInvocationId() {
    InvocationIdContext.clear();
    invocationId = InvocationIdContext.getInvocationId();
    LogRecord record = new LogRecord(java.util.logging.Level.INFO, "Test log message");

    String formattedMessage = formatter.format(record);

    assertEquals("Test log message\n", formattedMessage);
  }

  @Test
  public void testAddFormatter() {
    Logger logger = Logger.getLogger("testLogger");

    LoggingFormatter.addFormatter(logger);

    // Verify that the formatter is added to all handlers
    for (var handler : logger.getHandlers()) {
      assertEquals(LoggingFormatter.class, handler.getFormatter().getClass());
    }
  }
}
