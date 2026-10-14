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

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import java.io.IOException;
import java.io.StringReader;
import java.util.Objects;
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

  private static final TypeAdapter<JsonElement> JSON_ELEMENT_ADAPTER =
      new Gson().getAdapter(JsonElement.class);

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
          String.format("[%s]: %s", invocationId, Objects.toString(originalMessage, ""));
    }

    if (existingFormatter != null) {
      record.setMessage(messageToFormat);
      return existingFormatter.format(record);
    }
    // Otherwise, return the message as-is with a newline.
    return String.format("%s%n", Objects.toString(messageToFormat, ""));
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
   * Checks if the given message is a JSON object or non-empty array of JSON objects.
   *
   * @param message the log message to check
   * @return true if the message is a JSON object or array of JSON objects, false otherwise
   */
  private boolean isJson(String message) {
    if (message == null) {
      return false;
    }
    String trimmed = message.trim();
    if (!hasJsonDelimiters(trimmed)) {
      return false;
    }
    try (JsonReader reader = new JsonReader(new StringReader(trimmed))) {
      reader.setLenient(false);
      JsonElement element = JSON_ELEMENT_ADAPTER.read(reader);
      if (reader.peek() != JsonToken.END_DOCUMENT) {
        return false;
      }
      return element.isJsonObject() || isJsonObjectArray(element);
    } catch (IOException | JsonParseException | NumberFormatException e) {
      return false;
    }
  }

  private static boolean hasJsonDelimiters(String trimmed) {
    return (trimmed.startsWith("{") && trimmed.endsWith("}"))
        || (trimmed.startsWith("[") && trimmed.endsWith("]"));
  }

  private static boolean isJsonObjectArray(JsonElement element) {
    if (!element.isJsonArray()) {
      return false;
    }
    JsonArray array = element.getAsJsonArray();
    if (array.isEmpty()) {
      return false;
    }
    for (JsonElement item : array) {
      if (!item.isJsonObject()) {
        return false;
      }
    }
    return true;
  }
}
