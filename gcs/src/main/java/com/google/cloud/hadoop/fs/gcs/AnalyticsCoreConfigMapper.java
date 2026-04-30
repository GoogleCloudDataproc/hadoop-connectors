/*
 * Copyright 2026 Google LLC
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
package com.google.cloud.hadoop.fs.gcs;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;

/** Maps GCS Hadoop Connector configurations to GCS Analytics Core configurations. */
final class AnalyticsCoreConfigMapper {

  static final String PROJECT_ID_KEY = "project-id";
  static final String USER_PROJECT_KEY = "user-project";
  static final String READ_THREAD_COUNT_KEY = "analytics-core.read.thread.count";
  static final String MAX_MERGE_GAP_KEY = "analytics-core.read.vectored.range.merge-gap.max-bytes";
  static final String MAX_MERGE_SIZE_KEY =
      "analytics-core.read.vectored.range.merged-size.max-bytes";

  private AnalyticsCoreConfigMapper() {
    // Utility class
  }

  /**
   * Maps configurations from Hadoop Configuration to a map suitable for Analytics Core.
   *
   * @param config The Hadoop configuration.
   * @param prefix The prefix used for Analytics Core properties (e.g., "fs.gs.").
   * @return A map containing the mapped properties.
   */
  static Map<String, String> mapConfigs(Configuration config, String prefix) {
    Map<String, String> properties = config.getValByRegex("^" + prefix.replace(".", "\\."));
    Map<String, String> mappedProperties = new HashMap<>(properties);

    // Direct 1:1 mappings from Connector to Analytics Core
    mapAndRemoveSource(
        config,
        GoogleHadoopFileSystemConfiguration.GCS_PROJECT_ID.getKey(),
        mappedProperties,
        prefix + PROJECT_ID_KEY);
    mapAndRemoveSource(
        config,
        GoogleHadoopFileSystemConfiguration.GCS_REQUESTER_PAYS_PROJECT_ID.getKey(),
        mappedProperties,
        prefix + USER_PROJECT_KEY);
    mapAndRemoveSource(
        config,
        GoogleHadoopFileSystemConfiguration.GCS_VECTORED_READ_THREADS.getKey(),
        mappedProperties,
        prefix + READ_THREAD_COUNT_KEY);
    mapAndRemoveSource(
        config,
        GoogleHadoopFileSystemConfiguration.GCS_VECTORED_READ_RANGE_MIN_SEEK.getKey(),
        mappedProperties,
        prefix + MAX_MERGE_GAP_KEY);
    mapAndRemoveSource(
        config,
        GoogleHadoopFileSystemConfiguration.GCS_VECTORED_READ_MERGED_RANGE_MAX_SIZE.getKey(),
        mappedProperties,
        prefix + MAX_MERGE_SIZE_KEY);

    return mappedProperties;
  }

  private static void mapAndRemoveSource(
      Configuration config, String hadoopKey, Map<String, String> map, String analyticsCoreKey) {
    String value = config.get(hadoopKey);
    if (value != null) {
      map.put(analyticsCoreKey, value);
      map.remove(hadoopKey);
    }
  }
}
