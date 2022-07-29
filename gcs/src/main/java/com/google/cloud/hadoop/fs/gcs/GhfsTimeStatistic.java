/*
 * Copyright 2022 Google Inc. All Rights Reserved.
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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import java.util.EnumSet;
import org.apache.hadoop.classification.InterfaceStability;

/** 
 * Time Statistics which are collected in GCS. 
 * TODO : {@link https://b.corp.google.com/issues/240645618} 
*/
@InterfaceStability.Unstable
public enum GhfsTimeStatistic {

  /** Time Statistic for functions in milliseconds */
  SEEK("timeElapsedSeekMillisec", "total time required for seek in milliseconds", TYPE_DURATION),
  DELETE("timeElapsedDeleteMillisec", "total time required for delete in milliseconds", TYPE_DURATION),
  CREATE("timeElapsedCreateMillisec", "total time required for create in milliseconds", TYPE_DURATION),
  RENAME("timeElapsedRenameMillisec", "total time required for rename in milliseconds", TYPE_DURATION),
  OPEN("timeElapsedOpenMillisec", "total time required for open in milliseconds", TYPE_DURATION),
  HFLUSH("timeElapsedHflushMillisec", "total time required for hflush in milliseconds", TYPE_DURATION),
  HSYNC("timeElapsedHsyncMillisec", "total time required for hsync in milliseconds", TYPE_DURATION),
  CLOSE("timeElapsedCloseMillisec", "total time required for close in milliseconds", TYPE_DURATION);

  public static final ImmutableSet<GhfsTimeStatistic> VALUES =
      ImmutableSet.copyOf(EnumSet.allOf(GhfsTimeStatistic.class));

  /* A map used to support the {@link #fromSymbol(String)} call. */
  private static final ImmutableMap<String, GhfsTimeStatistic> SYMBOL_MAP =
      Maps.uniqueIndex(Iterators.forArray(values()), GhfsTimeStatistic::getSymbol);

  /** Statistic name. */
  private final String symbol;

  /** Statistic description. */
  private final String description;

  /** Statistic type. */
  private final GhfsStatisticTypeEnum type;

  /**
   * Statistic definition.
   *
   * @param symbol name
   * @param description description.
   */
  private GhfsTimeStatistic(String symbol, String description, GhfsStatisticTypeEnum type) {
    this.symbol = symbol;
    this.description = description;
    this.type = type;
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
   * Gets the symbol of the Statistic
   *
   * @returns the symbol.
   */
  public String getSymbol() {
    return symbol;
  }

  /**
   * Gets the description of the Statistic
   *
   * @return the description.
   */
  public String getDescription() {
    return description;
  }

  /**
   * What type is this statistic?
   *
   * @return the type.
   */
  public GhfsStatisticTypeEnum getType() {
    return type;
  }

  /**
   * Get a statistic from a symbol.
   *
   * @param symbol statistic to look up
   * @return the value or null.
   */
  public static GhfsTimeStatistic fromSymbol(String symbol) {
    return SYMBOL_MAP.get(symbol);
  }
}
