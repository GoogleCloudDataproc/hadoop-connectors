package com.google.cloud.hadoop.util;

import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

/**
 * A custom logging formatter that prefixes log messages with an invocation ID. If the log message
 * is in JSON format, the invocation ID is not prefixed.
 */
public class LoggingFormatter extends Formatter {

  /**
   * Formats the given log record by prefixing it with the invocation ID.
   *
   * @param record the log record to be formatted
   * @return the formatted log message
   */
  @Override
  public String format(LogRecord record) {
    String invocationId = InvocationIdContext.getInvocationId();
    String message = record.getMessage();
    if (invocationId.isEmpty() || isJson(message)) {
      return String.format("%s%n", message);
    }
    // Prefix the invocation ID to the log message
    return String.format("[%s]: %s%n", invocationId, record.getMessage());
  }

  /**
   * Adds the custom logging formatter to all handlers of the given logger.
   *
   * @param logger the logger to which the formatter will be added
   */
  public static void addFormatter(Logger logger) {
    // Set the custom formatter on all handlers of the logger
    for (Handler handler : logger.getHandlers()) {
      handler.setFormatter(new LoggingFormatter());
    }
  }

  /**
   * Checks if the given message is in JSON format.
   *
   * @param message the log message to check
   * @return true if the message is in JSON format, false otherwise
   */
  private boolean isJson(String message) {
    // Check if the message is a trace log.
    return message != null && (message.startsWith("{") || message.startsWith("["));
  }
}
