/*
 * Copyright 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.hadoop.gcsio;

import static com.google.cloud.hadoop.gcsio.StatisticTypeEnum.TYPE_COUNTER;
import static com.google.cloud.hadoop.gcsio.StatisticTypeEnum.TYPE_DURATION_TOTAL;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import java.util.EnumSet;

/** Statistics which are collected in GCS Connector. */
public enum GoogleCloudStorageStatistics {
  EXCEPTION_COUNT("exception_count", "Counts the number of exceptions encountered", TYPE_COUNTER),
  WRITE_CHECKSUM_FAILURE_COUNT(
      "write_checksum_failure_count",
      "Counts the number of checksum failures during write",
      TYPE_COUNTER),

  /** Status Code Counters for JSON Path */
  GCS_API_REQUEST_COUNT(
      "gcs_api_total_request_count", "Counts the total number of gcs requests made", TYPE_COUNTER),

  GCS_API_CLIENT_SIDE_ERROR_COUNT(
      "gcs_api_client_side_error_count",
      "Counts the occurrence of client side error status code",
      TYPE_COUNTER),

  GCS_API_SERVER_SIDE_ERROR_COUNT(
      "gcs_api_server_side_error_count",
      "Counts the occurrence of server side error status code",
      TYPE_COUNTER),

  GCS_API_CLIENT_RATE_LIMIT_COUNT(
      "gcs_api_client_rate_limit_error_count", "Counts the occurence of rate limit", TYPE_COUNTER),

  GCS_API_CLIENT_BAD_REQUEST_COUNT(
      "gcs_api_client_bad_request_count", "Counts the occurence of 400 status code", TYPE_COUNTER),

  GCS_API_CLIENT_UNAUTHORIZED_RESPONSE_COUNT(
      "gcs_api_client_unauthorized_response_count",
      "Counts the occurence of 401 status code",
      TYPE_COUNTER),

  GCS_API_CLIENT_NOT_FOUND_RESPONSE_COUNT(
      "gcs_api_client_non_found_response_count",
      "Counts the occurence of 404 status code",
      TYPE_COUNTER),

  GCS_API_CLIENT_REQUEST_TIMEOUT_COUNT(
      "gcs_api_client_request_timeout_count",
      "Counts the occurence of 408 status code",
      TYPE_COUNTER),

  GCS_API_CLIENT_GONE_RESPONSE_COUNT(
      "gcs_api_client_gone_response_count",
      "Counts the occurence of 410 status code",
      TYPE_COUNTER),

  GCS_API_CLIENT_PRECONDITION_FAILED_RESPONSE_COUNT(
      "gcs_api_client_precondition_failed_response_count",
      "Counts the occurence of 412 status code",
      TYPE_COUNTER),

  GCS_API_CLIENT_REQUESTED_RANGE_NOT_SATISFIABLE_COUNT(
      "gcs_api_client_requested_range_not_statisfiable_count",
      "Counts the occurence of 416 status code",
      TYPE_COUNTER),

  GCS_API_SERVER_INTERNAL_ERROR_COUNT(
      "gcs_api_server_internal_error_count",
      "Counts the occurrence of server side 500 error status code",
      TYPE_COUNTER),

  GCS_API_SERVER_BAD_GATEWAY_COUNT(
      "gcs_api_server_bad_gateway_count",
      "Counts the occurrence of server side 502 error status code",
      TYPE_COUNTER),

  GCS_API_SERVER_SERVICE_UNAVAILABLE_COUNT(
      "gcs_api_server_unavailable_count",
      "Counts the occurrence of server side 503 error status code",
      TYPE_COUNTER),

  GCS_API_SERVER_TIMEOUT_COUNT(
      "gcs_api_server_timeout_count",
      "Counts the occurrence of server side 504 error status code",
      TYPE_COUNTER),

  GCS_BACKOFF_TIME("gcs_backoff_time", "Total back off time in milliseconds", TYPE_COUNTER),

  GCS_BACKOFF_COUNT(
      "gcs_backoff_count",
      "Counts the number of times a request was retried after backing off due to some retriable response",
      TYPE_COUNTER),

  GCS_EXCEPTION_COUNT(
      "gcs_exception_count", "Counts the occurence of exception from GCS APIs", TYPE_COUNTER),

  GCS_API_TIME(
      "gcs_api_time", "Tracks the amount of time spend while calling GCS APIs.", TYPE_COUNTER),

  GCS_METADATA_REQUEST(
      "gcs_metadata_request", "Tracks GCS GET metadata API calls.", TYPE_DURATION_TOTAL),

  GCS_GET_MEDIA_REQUEST(
      "gcs_get_media_request", "Tracks GCS GET data API calls", TYPE_DURATION_TOTAL),

  GCS_LIST_DIR_REQUEST(
      "gcs_list_dir_request", "Tracks GCS GET list prefix API calls", TYPE_DURATION_TOTAL),

  GCS_LIST_FILE_REQUEST(
      "gcs_list_file_request",
      "Tracks GCS GET list file API calls. This is usually called with '/' as delimiter with some prefix and maxResults as 1",
      TYPE_DURATION_TOTAL),
  GS_FILESYSTEM_CREATE(
      "gs_filesystem_create", "Number of FileSystem objects created for 'gs' scheme", TYPE_COUNTER),
  GCS_GET_OTHER_REQUEST(
      "gcs_get_other_request", "Catch all metric of GCS GET API calls.", TYPE_COUNTER),
  GS_FILESYSTEM_INITIALIZE(
      "gs_filesystem_initialize", "Counts the filesystem initialize()", TYPE_COUNTER);

  public static final ImmutableSet<GoogleCloudStorageStatistics> VALUES =
      ImmutableSet.copyOf(EnumSet.allOf(GoogleCloudStorageStatistics.class));

  /** A map used to support the {@link #fromSymbol(String)} call. */
  private static final ImmutableMap<String, GoogleCloudStorageStatistics> SYMBOL_MAP =
      Maps.uniqueIndex(Iterators.forArray(values()), GoogleCloudStorageStatistics::getSymbol);

  /**
   * Statistic definition.
   *
   * @param symbol name
   * @param description description.
   * @param type type
   */
  GoogleCloudStorageStatistics(String symbol, String description, StatisticTypeEnum type) {
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
  public static GoogleCloudStorageStatistics fromSymbol(String symbol) {
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
