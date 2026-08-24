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

import com.google.cloud.hadoop.util.RequesterPaysOptions.RequesterPaysMode;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;

/** Maps GCS Hadoop Connector configurations to GCS Analytics Core configurations. */
final class AnalyticsCoreConfigMapper {

  static final String PROJECT_ID_KEY = "project-id";
  static final String USER_PROJECT_KEY = "user-project";
  static final String SERVICE_HOST_KEY = "service.host";
  static final String READ_THREAD_COUNT_KEY = "analytics-core.read.thread.count";
  static final String MAX_MERGE_GAP_KEY = "analytics-core.read.vectored.range.merge-gap.max-bytes";
  static final String MAX_MERGE_SIZE_KEY =
      "analytics-core.read.vectored.range.merged-size.max-bytes";
  static final String USER_AGENT_KEY = "user-agent";
  static final String FILE_ACCESS_PATTERN_KEY = "analytics-core.read.file-access-pattern";
  static final String INPLACE_SEEK_LIMIT_KEY = "analytics-core.read.inplace-seek-limit-bytes";
  static final String RANDOM_READ_MIN_REQ_SIZE_KEY = "analytics-core.random-read.min-request-size";
  static final String ADAPTIVE_READ_SEQ_THRESHOLD_KEY =
      "analytics-core.adaptive-read.sequential-read-threshold";
  static final String UPLOAD_CHUNK_SIZE_KEY = "channel.write.chunk-size-bytes";
  static final String UPLOAD_TYPE_KEY = "channel.write.upload-type";
  static final String TEMPORARY_PATHS_KEY = "channel.write.temporary-paths";
  static final String PCU_BUFFER_COUNT_KEY = "channel.write.pcu.buffer.count";
  static final String PCU_BUFFER_CAPACITY_KEY = "channel.write.pcu.buffer.capacity-bytes";
  static final String PCU_PART_FILE_CLEANUP_TYPE_KEY = "channel.write.pcu.part-file.cleanup-type";
  static final String PCU_PART_FILE_NAME_PREFIX_KEY = "channel.write.pcu.part-file.name-prefix";
  static final String ENCRYPTION_KEY_KEY = "encryption-key";
  static final String CHECKSUM_VALIDATION_ENABLED_KEY = "channel.write.checksum-validation.enabled";

  private static final ImmutableMap<String, String> HADOOP_TO_ANALYTICS_CORE_KEY_MAPPINGS =
      ImmutableMap.<String, String>builder()
          .put(GoogleHadoopFileSystemConfiguration.GCS_PROJECT_ID.getKey(), PROJECT_ID_KEY)
          .put(GoogleHadoopFileSystemConfiguration.GCS_ROOT_URL.getKey(), SERVICE_HOST_KEY)
          .put(
              GoogleHadoopFileSystemConfiguration.GCS_REQUESTER_PAYS_PROJECT_ID.getKey(),
              USER_PROJECT_KEY)
          .put(
              GoogleHadoopFileSystemConfiguration.GCS_VECTORED_READ_THREADS.getKey(),
              READ_THREAD_COUNT_KEY)
          .put(
              GoogleHadoopFileSystemConfiguration.GCS_VECTORED_READ_RANGE_MIN_SEEK.getKey(),
              MAX_MERGE_GAP_KEY)
          .put(
              GoogleHadoopFileSystemConfiguration.GCS_VECTORED_READ_MERGED_RANGE_MAX_SIZE.getKey(),
              MAX_MERGE_SIZE_KEY)
          .put(
              GoogleHadoopFileSystemConfiguration.GCS_INPUT_STREAM_INPLACE_SEEK_LIMIT.getKey(),
              INPLACE_SEEK_LIMIT_KEY)
          .put(
              GoogleHadoopFileSystemConfiguration.GCS_INPUT_STREAM_MIN_RANGE_REQUEST_SIZE.getKey(),
              RANDOM_READ_MIN_REQ_SIZE_KEY)
          .put(
              GoogleHadoopFileSystemConfiguration.GCS_FADVISE_REQUEST_TRACK_COUNT.getKey(),
              ADAPTIVE_READ_SEQ_THRESHOLD_KEY)
          .put(
              GoogleHadoopFileSystemConfiguration.GCS_INPUT_STREAM_FADVISE.getKey(),
              FILE_ACCESS_PATTERN_KEY)
          .put(
              GoogleHadoopFileSystemConfiguration.GCS_OUTPUT_STREAM_UPLOAD_CHUNK_SIZE.getKey(),
              UPLOAD_CHUNK_SIZE_KEY)
          .put(GoogleHadoopFileSystemConfiguration.GCS_CLIENT_UPLOAD_TYPE.getKey(), UPLOAD_TYPE_KEY)
          .put(
              GoogleHadoopFileSystemConfiguration.GCS_PCU_BUFFER_COUNT.getKey(),
              PCU_BUFFER_COUNT_KEY)
          .put(
              GoogleHadoopFileSystemConfiguration.GCS_PCU_BUFFER_CAPACITY.getKey(),
              PCU_BUFFER_CAPACITY_KEY)
          .put(
              GoogleHadoopFileSystemConfiguration.GCS_PCU_PART_FILE_CLEANUP_TYPE.getKey(),
              PCU_PART_FILE_CLEANUP_TYPE_KEY)
          .put(
              GoogleHadoopFileSystemConfiguration.GCS_PCU_PART_FILE_NAME_PREFIX.getKey(),
              PCU_PART_FILE_NAME_PREFIX_KEY)
          .put(GoogleHadoopFileSystemConfiguration.GCS_ENCRYPTION_KEY.getKey(), ENCRYPTION_KEY_KEY)
          .put(
              GoogleHadoopFileSystemConfiguration.GCS_WRITE_ROLLING_CHECKSUM_ENABLE.getKey(),
              CHECKSUM_VALIDATION_ENABLED_KEY)
          .build();

  private AnalyticsCoreConfigMapper() {
    // Utility class
  }

  /**
   * Maps configurations from Hadoop Configuration to a map suitable for Analytics Core. If a
   * configuration flag is not set by the user, Analytics Core defaults will be used.
   *
   * @param config The Hadoop configuration.
   * @param prefix The prefix used for Analytics Core properties (e.g., "fs.gs.").
   * @return A map containing the mapped properties.
   */
  static Map<String, String> mapConfigs(Configuration config, String prefix) {
    Map<String, String> mappedProperties = config.getValByRegex("^" + prefix.replace(".", "\\."));

    // Direct mappings from Connector to Analytics Core
    HADOOP_TO_ANALYTICS_CORE_KEY_MAPPINGS.forEach(
        (hadoopKey, analyticsKey) ->
            mapAndRemoveSource(hadoopKey, mappedProperties, prefix + analyticsKey));

    // Handle requester pays project ID: when enabled, use fs.gs.requester.pays.project.id if set,
    // otherwise fallback to fs.gs.project.id
    String requesterPaysMode =
        config.get(
            GoogleHadoopFileSystemConfiguration.GCS_REQUESTER_PAYS_MODE.getKey(),
            GoogleHadoopFileSystemConfiguration.GCS_REQUESTER_PAYS_MODE.getDefault().name());
    String requesterPaysProjectId =
        mappedProperties.remove(
            GoogleHadoopFileSystemConfiguration.GCS_REQUESTER_PAYS_PROJECT_ID.getKey());
    if (!RequesterPaysMode.DISABLED.name().equalsIgnoreCase(requesterPaysMode)) {
      if (requesterPaysProjectId == null || requesterPaysProjectId.isEmpty()) {
        requesterPaysProjectId =
            config.get(GoogleHadoopFileSystemConfiguration.GCS_PROJECT_ID.getKey());
      }
      if (requesterPaysProjectId != null && !requesterPaysProjectId.isEmpty()) {
        mappedProperties.put(prefix + USER_PROJECT_KEY, requesterPaysProjectId);
      }
    }

    // Handle temporary paths: use fs.gs.write.temporary.dirs if set, otherwise fallback to
    // hadoop.tmp.dir
    String tempPaths =
        mappedProperties.remove(
            GoogleHadoopFileSystemConfiguration.GCS_WRITE_TEMPORARY_FILES_PATH.getKey());
    if (tempPaths == null || tempPaths.isEmpty()) {
      tempPaths = config.get("hadoop.tmp.dir");
    }
    if (tempPaths != null && !tempPaths.isEmpty()) {
      mappedProperties.put(prefix + TEMPORARY_PATHS_KEY, tempPaths);
    }

    // User agent is computed from GHFS_ID and an optional suffix, not a simple 1:1 mapping.
    mappedProperties.put(
        prefix + USER_AGENT_KEY, GoogleHadoopFileSystemConfiguration.getApplicationName(config));

    // Ensure client.type is explicitly removed from mapped properties to prevent crashes
    mappedProperties.remove(GoogleHadoopFileSystemConfiguration.GCS_CLIENT_TYPE.getKey());

    return mappedProperties;
  }

  private static void mapAndRemoveSource(
      String hadoopKey, Map<String, String> map, String analyticsCoreKey) {
    String value = map.remove(hadoopKey);
    if (value != null) {
      if (hadoopKey.equals(GoogleHadoopFileSystemConfiguration.GCS_INPUT_STREAM_FADVISE.getKey())) {
        value = toFileAccessPattern(value);
      } else if (hadoopKey.equals(
          GoogleHadoopFileSystemConfiguration.GCS_CLIENT_UPLOAD_TYPE.getKey())) {
        value = toUploadType(value);
      } else if (hadoopKey.equals(
          GoogleHadoopFileSystemConfiguration.GCS_PCU_PART_FILE_CLEANUP_TYPE.getKey())) {
        value = toPartFileCleanupType(value);
      }
      map.put(analyticsCoreKey, value);
    }
  }

  private static String toUploadType(String uploadType) {
    String normalized = uploadType.replace('-', '_').toUpperCase();
    switch (normalized) {
      case "CHUNK_UPLOAD":
      case "WRITE_TO_DISK_THEN_UPLOAD":
      case "JOURNALING":
      case "PARALLEL_COMPOSITE_UPLOAD":
        return normalized;
      default:
        return GoogleHadoopFileSystemConfiguration.GCS_CLIENT_UPLOAD_TYPE.getDefault().name();
    }
  }

  private static String toPartFileCleanupType(String cleanupType) {
    String normalized = cleanupType.replace('-', '_').toUpperCase();
    switch (normalized) {
      case "ALWAYS":
      case "NEVER":
      case "ON_SUCCESS":
        return normalized;
      default:
        return GoogleHadoopFileSystemConfiguration.GCS_PCU_PART_FILE_CLEANUP_TYPE
            .getDefault()
            .name();
    }
  }

  private static String toFileAccessPattern(String fadvise) {
    switch (fadvise.toUpperCase()) {
      case "AUTO":
        return "AUTO_SEQUENTIAL";
      case "AUTO_RANDOM":
        return "AUTO_RANDOM";
      case "SEQUENTIAL":
        return "SEQUENTIAL";
      case "RANDOM":
        return "RANDOM";
      default:
        return "AUTO_SEQUENTIAL";
    }
  }
}
