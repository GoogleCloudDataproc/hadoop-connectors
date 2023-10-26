/*
 * Copyright 2023 Google Inc. All Rights Reserved.
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

import static com.google.cloud.hadoop.gcsio.StatisticTypeEnum.TYPE_COUNTER;
import static com.google.cloud.hadoop.gcsio.StatisticTypeEnum.TYPE_DURATION;

import com.google.cloud.hadoop.gcsio.StatisticTypeEnum;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import java.util.EnumSet;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public enum GhfsStatistic {
  ACTION_HTTP_HEAD_REQUEST_FAILURE(
      "action_http_head_request_failures", "HEAD request failure.", TYPE_COUNTER),
  ACTION_HTTP_GET_REQUEST_FAILURE(
      "action_http_get_request_failures", "GET request failure.", TYPE_COUNTER),
  ACTION_HTTP_PUT_REQUEST_FAILURE(
      "action_http_put_request_failures", "PUT request failure.", TYPE_COUNTER),
  ACTION_HTTP_PATCH_REQUEST_FAILURE(
      "action_http_patch_request_failures", "PATCH request failure.", TYPE_COUNTER),
  ACTION_HTTP_POST_REQUEST_FAILURE(
      "action_http_post_request_failures", "POST request failure.", TYPE_COUNTER),
  ACTION_HTTP_DELETE_REQUEST_FAILURE(
      "action_http_delete_request_failures", "DELETE request failure.", TYPE_COUNTER),

  /** FileSystem-level statistics */
  DIRECTORIES_CREATED(
      "directories_created",
      "Total number of directories created through the object store.",
      TYPE_COUNTER),
  DIRECTORIES_DELETED(
      "directories_deleted",
      "Total number of directories deleted through the object store.",
      TYPE_COUNTER),
  FILES_CREATED(
      "files_created", "Total number of files created through the object store.", TYPE_COUNTER),
  FILES_DELETED(
      "files_deleted", "Total number of files deleted from the object store.", TYPE_COUNTER),
  FILES_DELETE_REJECTED(
      "files_delete_rejected",
      "Total number of files whose delete request was rejected",
      TYPE_COUNTER),
  INVOCATION_CREATE("op_create", "Calls of create()", TYPE_DURATION),
  INVOCATION_CREATE_NON_RECURSIVE(
      "op_create_non_recursive", "Calls of createNonRecursive()", TYPE_DURATION),
  INVOCATION_DELETE("op_delete", "Calls of delete()", TYPE_DURATION),
  INVOCATION_EXISTS("op_exists", "Calls of exists()", TYPE_COUNTER),
  INVOCATION_GET_FILE_STATUS("op_get_file_status", "Calls of getFileStatus()", TYPE_DURATION),

  INVOCATION_GET_FILE_CHECKSUM("op_get_file_checksum", "Calls of getFileChecksum()", TYPE_COUNTER),
  INVOCATION_GLOB_STATUS("op_glob_status", "Calls of globStatus()", TYPE_DURATION),
  INVOCATION_HFLUSH("op_hflush", "Calls of hflush()", TYPE_DURATION),
  INVOCATION_HSYNC("op_hsync", "Calls of hsync()", TYPE_DURATION),
  INVOCATION_LIST_FILES("op_list_files", "Calls of listFiles()", TYPE_COUNTER),
  INVOCATION_LIST_STATUS("op_list_status", "Calls of listStatus()", TYPE_COUNTER),
  INVOCATION_MKDIRS("op_mkdirs", "Calls of mkdirs()", TYPE_DURATION),
  INVOCATION_OPEN("op_open", "Calls of open()", TYPE_DURATION),
  INVOCATION_RENAME("op_rename", "Calls of rename()", TYPE_DURATION),

  /** Stream reads */
  STREAM_READ_BYTES(
      "stream_read_bytes", "Bytes read from an input stream in read() calls", TYPE_COUNTER),
  STREAM_READ_CLOSE_OPERATIONS(
      "stream_read_close_operations", "Calls of read stream close()", TYPE_DURATION),
  STREAM_READ_EXCEPTIONS(
      "stream_read_exceptions",
      "Count of exceptions raised during input stream reads",
      TYPE_COUNTER),
  STREAM_READ_OPERATIONS("stream_read_operations", "Calls of read()", TYPE_DURATION),

  STREAM_READ_OPERATIONS_INCOMPLETE(
      "stream_read_operations_incomplete",
      "Count of incomplete read() operations in an input stream",
      TYPE_COUNTER),
  STREAM_READ_SEEK_BACKWARD_OPERATIONS(
      "stream_read_seek_backward_operations",
      "Count of executed seek operations which went backwards in a stream",
      TYPE_COUNTER),
  STREAM_READ_SEEK_BYTES_BACKWARDS(
      "stream_read_bytes_backwards_on_seek",
      "Count of bytes moved backwards during seek operations" + " in an input stream",
      TYPE_COUNTER),
  STREAM_READ_SEEK_BYTES_SKIPPED(
      "stream_read_seek_bytes_skipped",
      "Count of bytes skipped during forward seek operations" + " an input stream",
      TYPE_COUNTER),
  STREAM_READ_SEEK_FORWARD_OPERATIONS(
      "stream_read_seek_forward_operations",
      "Count of executed seek operations which went forward in" + " an input stream",
      TYPE_COUNTER),
  STREAM_READ_SEEK_OPERATIONS("stream_read_seek_operations", "Calls of seek()", TYPE_DURATION),
  STREAM_READ_TOTAL_BYTES(
      "stream_read_total_bytes", "Total count of bytes read from an input stream", TYPE_COUNTER),

  /** Stream writes */
  STREAM_WRITE_EXCEPTIONS(
      "stream_write_exceptions", "Count of stream write failures reported", TYPE_COUNTER),
  STREAM_WRITE_BYTES(
      "stream_write_bytes",
      "Count of bytes written to output stream" + " (including all not yet uploaded)",
      TYPE_COUNTER),
  STREAM_WRITE_CLOSE_OPERATIONS(
      "stream_write_close_operations", "Calls of write stream close()", TYPE_DURATION),
  STREAM_WRITE_OPERATIONS("stream_write_operations", "Calls of write()", TYPE_DURATION),

  /** The XAttr API statistics */
  INVOCATION_XATTR_GET_MAP("op_xattr_get_map", "Calls of getXAttrs(Path path)", TYPE_DURATION),
  INVOCATION_XATTR_GET_NAMED(
      "op_xattr_get_named", "Calls of getXAttr(Path, String)", TYPE_DURATION),
  INVOCATION_XATTR_GET_NAMED_MAP("op_xattr_get_named_map", "Calls of xattr()", TYPE_DURATION),
  INVOCATION_OP_XATTR_LIST(
      "op_xattr_list", "Calls of getXAttrs(Path path, List<String> names)", TYPE_DURATION),

  /** Delegation token operations */
  DELEGATION_TOKENS_ISSUED(
      "delegation_tokens_issued", "Count of delegation tokens issued", TYPE_DURATION);

  public static final ImmutableSet<GhfsStatistic> VALUES =
      ImmutableSet.copyOf(EnumSet.allOf(GhfsStatistic.class));

  /** A map used to support the {@link #fromSymbol(String)} call. */
  private static final ImmutableMap<String, GhfsStatistic> SYMBOL_MAP =
      Maps.uniqueIndex(Iterators.forArray(values()), GhfsStatistic::getSymbol);

  /**
   * Statistic definition.
   *
   * @param symbol name
   * @param description description.
   * @param type type
   */
  GhfsStatistic(String symbol, String description, StatisticTypeEnum type) {
    this.symbol = symbol;
    this.description = description;
    this.type = type;
  }

  /** Statistic name. */
  private final String symbol;

  /** Statistic description. */
  private final String description;

  /** Statistic type. */
  private final StatisticTypeEnum type;

  /** the name of the statistic */
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

  /** The description of the Statistic */
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
  public StatisticTypeEnum getType() {
    return type;
  }
}
