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

import static com.google.cloud.hadoop.fs.gcs.GhfsStatisticTypeEnum.TYPE_DURATION;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.statistics.StoreStatisticNames;
import org.apache.hadoop.fs.statistics.StreamStatisticNames;

/**
 * Statistic which are collected in GCS. Counter and duration statistics are published in {@link
 * GoogleHadoopFileSystemBase}. and as metrics in {@link GhfsInstrumentation}.
 *
 * <p>Where possible, stream names come from {@link StreamStatisticNames} and {@link
 * StoreStatisticNames}
 */
@InterfaceStability.Unstable
enum GhfsStatistic {
  /** Low-level duration counters */
  ACTION_HTTP_HEAD_REQUEST(
      StoreStatisticNames.ACTION_HTTP_HEAD_REQUEST, "HEAD request.", TYPE_DURATION),
  ACTION_HTTP_GET_REQUEST(
      StoreStatisticNames.ACTION_HTTP_GET_REQUEST, "GET request.", TYPE_DURATION);

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
