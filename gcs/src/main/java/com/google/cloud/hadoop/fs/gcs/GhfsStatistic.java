/*
 * Copyright 2021 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.fs.gcs;

import static com.google.cloud.hadoop.fs.gcs.GhfsStatisticTypeEnum.*;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.statistics.StoreStatisticNames;
import org.apache.hadoop.fs.statistics.StreamStatisticNames;

/**
 * Statistics which are collected in GCS. Counter and duration statistics are published in {@link
 * GoogleHadoopFileSystemBase} and as metrics in {@link GhfsInstrumentation}.
 *
 * <p>Where possible, stream names come from {@link StreamStatisticNames} and {@link
 * StoreStatisticNames}
 */
@InterfaceStability.Unstable
public enum GhfsStatistic {

  /** FileSystem Level statistics */
  FILES_CREATED(
      "files_created", "Total number of files created through the object store.", TYPE_COUNTER),
  FILES_DELETED(
      "files_deleted", "Total number of files deleted from the object store.", TYPE_COUNTER),
  INVOCATION_COPY_FROM_LOCAL_FILE(
      StoreStatisticNames.OP_COPY_FROM_LOCAL_FILE, "Calls of copyFromLocalFile()", TYPE_COUNTER),
  INVOCATION_CREATE(StoreStatisticNames.OP_CREATE, "Calls of create()", TYPE_COUNTER),
  INVOCATION_CREATE_NON_RECURSIVE(
      StoreStatisticNames.OP_CREATE_NON_RECURSIVE, "Calls of createNonRecursive()", TYPE_COUNTER),
  INVOCATION_DELETE(StoreStatisticNames.OP_DELETE, "Calls of delete()", TYPE_COUNTER),
  INVOCATION_EXISTS(StoreStatisticNames.OP_EXISTS, "Calls of exists()", TYPE_COUNTER),
  INVOCATION_GET_DELEGATION_TOKEN(
      StoreStatisticNames.OP_GET_DELEGATION_TOKEN, "Calls of getDelegationToken()", TYPE_COUNTER),
  INVOCATION_GET_FILE_CHECKSUM(
      StoreStatisticNames.OP_GET_FILE_CHECKSUM, "Calls of getFileChecksum()", TYPE_COUNTER),
  INVOCATION_GET_FILE_STATUS(
      StoreStatisticNames.OP_GET_FILE_STATUS, "Calls of getFileStatus()", TYPE_COUNTER),
  INVOCATION_GLOB_STATUS(StoreStatisticNames.OP_GLOB_STATUS, "Calls of globStatus()", TYPE_COUNTER),
  INVOCATION_LIST_FILES(StoreStatisticNames.OP_LIST_FILES, "Calls of listFiles()", TYPE_COUNTER),
  INVOCATION_LIST_STATUS(StoreStatisticNames.OP_LIST_STATUS, "Calls of listStatus()", TYPE_COUNTER),
  INVOCATION_MKDIRS(StoreStatisticNames.OP_MKDIRS, "Calls of mkdirs()", TYPE_COUNTER),
  INVOCATION_OPEN(StoreStatisticNames.OP_OPEN, "Calls of open()", TYPE_COUNTER),
  INVOCATION_RENAME(StoreStatisticNames.OP_RENAME, "Calls of rename()", TYPE_COUNTER),

  /** Object IO */
  OBJECT_DELETE_OBJECTS(
      StoreStatisticNames.OBJECT_DELETE_OBJECTS,
      "Objects deleted in delete requests",
      TYPE_COUNTER),

  /** Stream Reads */
  STREAM_READ_BYTES(
      StreamStatisticNames.STREAM_READ_BYTES,
      "Bytes read from an input stream in read() calls",
      TYPE_COUNTER),
  STREAM_READ_CLOSE_OPERATIONS(
      StreamStatisticNames.STREAM_READ_CLOSE_OPERATIONS,
      "Total count of times an attempt to close an input stream was made",
      TYPE_COUNTER),
  STREAM_READ_EXCEPTIONS(
      StreamStatisticNames.STREAM_READ_EXCEPTIONS,
      "Count of exceptions raised during input stream reads",
      TYPE_COUNTER),
  STREAM_READ_OPERATIONS(
      StreamStatisticNames.STREAM_READ_OPERATIONS,
      "Count of read() operations in an input stream",
      TYPE_COUNTER),
  STREAM_READ_OPERATIONS_INCOMPLETE(
      StreamStatisticNames.STREAM_READ_OPERATIONS_INCOMPLETE,
      "Count of incomplete read() operations in an input stream",
      TYPE_COUNTER),
  STREAM_READ_SEEK_BACKWARD_OPERATIONS(
      StreamStatisticNames.STREAM_READ_SEEK_BACKWARD_OPERATIONS,
      "Count of executed seek operations which went backwards in a stream",
      TYPE_COUNTER),
  STREAM_READ_SEEK_BYTES_BACKWARDS(
      StreamStatisticNames.STREAM_READ_SEEK_BYTES_BACKWARDS,
      "Count of bytes moved backwards during seek operations" + " in an input stream",
      TYPE_COUNTER),
  STREAM_READ_SEEK_BYTES_SKIPPED(
      StreamStatisticNames.STREAM_READ_SEEK_BYTES_SKIPPED,
      "Count of bytes skipped during forward seek operations" + " an input stream",
      TYPE_COUNTER),
  STREAM_READ_SEEK_FORWARD_OPERATIONS(
      StreamStatisticNames.STREAM_READ_SEEK_FORWARD_OPERATIONS,
      "Count of executed seek operations which went forward in" + " an input stream",
      TYPE_COUNTER),
  STREAM_READ_SEEK_OPERATIONS(
      StreamStatisticNames.STREAM_READ_SEEK_OPERATIONS,
      "Count of seek operations in an input stream",
      TYPE_COUNTER),
  STREAM_READ_TOTAL_BYTES(
      StreamStatisticNames.STREAM_READ_TOTAL_BYTES,
      "Total count of bytes read from an input stream",
      TYPE_COUNTER),

  /** Stream Write statistics */
  STREAM_WRITE_EXCEPTIONS(
      StreamStatisticNames.STREAM_WRITE_EXCEPTIONS,
      "Count of stream write failures reported",
      TYPE_COUNTER),
  STREAM_WRITE_BYTES(
      StreamStatisticNames.STREAM_WRITE_BYTES,
      "Count of bytes written to output stream" + " (including all not yet uploaded)",
      TYPE_COUNTER),

  /** The XAttr API metrics are all durations */
  INVOCATION_XATTR_GET_MAP(
      StoreStatisticNames.OP_XATTR_GET_MAP, "Calls of getXAttrs(Path path)", TYPE_DURATION),
  INVOCATION_XATTR_GET_NAMED(
      StoreStatisticNames.OP_XATTR_GET_NAMED, "Calls of getXAttr(Path, String)", TYPE_DURATION),
  INVOCATION_XATTR_GET_NAMED_MAP(
      StoreStatisticNames.OP_XATTR_GET_NAMED_MAP, "Calls of xattr()", TYPE_DURATION),
  INVOCATION_OP_XATTR_LIST(
      StoreStatisticNames.OP_XATTR_LIST,
      "Calls of getXAttrs(Path path, List<String> names)",
      TYPE_DURATION),

  /** Delegation Token Operations. */
  DELEGATION_TOKENS_ISSUED(
      StoreStatisticNames.DELEGATION_TOKENS_ISSUED,
      "Count of delegation tokens issued",
      TYPE_DURATION);

  /** A map used to support the {@link #fromSymbol(String)} call. */
  private static final Map<String, GhfsStatistic> SYMBOL_MAP =
      new HashMap<>(GhfsStatistic.values().length);

  static {
    for (GhfsStatistic stat : values()) {
      SYMBOL_MAP.put(stat.getSymbol(), stat);
    }
  }

  /**
   * Statistic definition.
   *
   * @param symbol name
   * @param description description.
   * @param type type
   */
  GhfsStatistic(String symbol, String description, GhfsStatisticTypeEnum type) {
    this.symbol = symbol;
    this.description = description;
    this.type = type;
  }

  /** Statistic name. */
  private final String symbol;

  /** Statistic description. */
  private final String description;

  /** Statistic type. */
  private final GhfsStatisticTypeEnum type;

  public String getSymbol() {
    return symbol;
  }

  /**
   * Get a statistic from a symbol.
   *
   * @param symbol statistic to look up
   * @return the value or null.
   */
  public static GhfsStatistic fromSymbol(String symbol) {
    return SYMBOL_MAP.get(symbol);
  }

  public String getDescription() {
    return description;
  }

  /**
   * The string value is simply the symbol. This makes this operation very low cost.
   *
   * @return the symbol of this statistic.
   */
  @Override
  public String toString() {
    return symbol;
  }

  /**
   * What type is this statistic?
   *
   * @return the type.
   */
  public GhfsStatisticTypeEnum getType() {
    return type;
  }
}
