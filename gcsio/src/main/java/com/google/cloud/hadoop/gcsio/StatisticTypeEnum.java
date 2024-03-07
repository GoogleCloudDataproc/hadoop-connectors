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

/** Enum of statistic types. */
public enum StatisticTypeEnum {

  /** Counter. Counts the number of occurrences of each operation */
  TYPE_COUNTER,

  /** Duration. Duration for the execution of opertaion */
  TYPE_DURATION,

  /** Gauge. */
  TYPE_GAUGE
}
========

>>>>>>>> b0c7fb09 (Metrics master (#1117)):gcs/src/main/java/com/google/cloud/hadoop/fs/gcs/GhfsStatisticTypeEnum.java
