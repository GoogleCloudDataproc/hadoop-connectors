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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import java.util.EnumSet;

/** Statistics which are collected in GCS Connector */
public enum GoogleCloudStorageStatistics {

  /** GCS connector specific statistics */
  GCS_REQUEST_COUNT(
      "gcs_total_request_count", "Counts the total number of gcs requests made", TYPE_COUNTER),

  EXCEPTION_COUNT("exception_count", "Counts the number of exceptions encountered", TYPE_COUNTER),

  GCS_CLIENT_SIDE_ERROR_COUNT(
      "gcs_client_side_error_count",
      "Counts the occurrence of client side error status code",
      TYPE_COUNTER),

  GCS_SERVER_SIDE_ERROR_COUNT(
      "gcs_server_side_error_count",
      "Counts the occurrence of server side error status code",
      TYPE_COUNTER),

  GCS_CLIENT_RATE_LIMIT_COUNT(
      "gcs_client_rate_limit_error_count", "Counts the occurence of 429 status code", TYPE_COUNTER);

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
