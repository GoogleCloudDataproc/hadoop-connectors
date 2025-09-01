package com.google.cloud.hadoop.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import java.util.Optional;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

/**
 * A custom logging formatter that prefixes log messages with an invocation ID. If the log message
 * is in JSON format, the invocation ID is not prefixed.
 */
public class LoggingFormatter extends Formatter {

  /** The base package for all loggers in this repository that should be formatted. */
  private static final String GCS_CONNECTOR_LOGGER_PREFIX = "com.google.cloud.hadoop";
  private final Formatter existingFormatter;

  /**
   * Constructs a new LoggingFormatter that wraps an existing formatter.
   *
   * @param existingFormatter The formatter to be decorated.
   */
  public LoggingFormatter(Formatter existingFormatter) {
    this.existingFormatter = existingFormatter;
  }
  /**
   * Formats the given log record by prefixing it with the invocation ID.
   *
   * @param record the log record to be formatted
   * @return the formatted log message
   */
  @Override
  public String format(LogRecord record) {
    String originalMessage = record.getMessage();
    Optional<String> optLoggerName = Optional.ofNullable(record.getLoggerName());

    // A log should be formatted if its logger name matches the connector's prefix,
    // it has an invocation ID, and the message is not structured JSON.
    String invocationId = InvocationIdContext.getInvocationId();
    boolean shouldFormat =
        optLoggerName.map(name -> name.startsWith(GCS_CONNECTOR_LOGGER_PREFIX)).orElse(false)
            && !invocationId.isEmpty()
            && !isJson(originalMessage);

    String messageToFormat = originalMessage;
    if (shouldFormat) {
      // Prefix the invocation ID to the log message.
      messageToFormat =
          String.format("[%s]: %s", invocationId, originalMessage == null ? "" : originalMessage);
    }

    if (existingFormatter != null) {
      try {
        record.setMessage(messageToFormat);
        return existingFormatter.format(record);
      } finally {
        // Restore original message to avoid side-effects on the LogRecord.
        record.setMessage(originalMessage);
      }
    }
    // Otherwise, return the message as-is with a newline.
    return String.format("%s%n", messageToFormat == null ? "" : messageToFormat);
  }

  /**
   * Adds the custom logging formatter to all handlers of the given logger.
   *
   * @param logger the logger to which the formatter will be added
   */
  public static void addFormatter(Logger logger) {
    // Set the custom formatter on all handlers of the logger
    for (Handler handler : logger.getHandlers()) {
      addFormatterToHandler(handler);
    }
  }

  @VisibleForTesting
  static void addFormatterToHandler(Handler handler) {
    Formatter existingFormatter = handler.getFormatter();
    if (existingFormatter instanceof LoggingFormatter) {
      // To prevent re-wrapping, do nothing if already a LoggingFormatter.
      return;
    }
    handler.setFormatter(new LoggingFormatter(existingFormatter));
  }

  /**
   * Checks if the given message is in JSON format.
   *
   * @param message the log message to check
   * @return true if the message is in JSON format, false otherwise
   */
  private boolean isJson(String message) {
    // Check if the message is a JSON string.
    if (message == null) {
      return false;
    }
    try {
      JsonParser.parseString(message);
      return true; // If parsing succeeds, it's valid JSON.
    } catch (JsonSyntaxException e) {
      return false;
    }
  }
}
