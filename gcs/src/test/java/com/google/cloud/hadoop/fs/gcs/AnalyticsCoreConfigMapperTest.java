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

import static com.google.common.truth.Truth.assertThat;

import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link AnalyticsCoreConfigMapper}. */
@RunWith(JUnit4.class)
public class AnalyticsCoreConfigMapperTest {

  private static final Map<String, String> EXPECTED_MANDATORY_MAPPINGS =
      Map.of("fs.gs." + AnalyticsCoreConfigMapper.USER_AGENT_KEY, GoogleHadoopFileSystem.GHFS_ID);

  @Test
  public void mapConfigs_mapsConnectorPropertiesToAnalyticsCore() {
    Configuration config = createTestConfiguration();

    Map<String, String> mapped = AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.");

    assertThat(mapped.get("fs.gs." + AnalyticsCoreConfigMapper.PROJECT_ID_KEY))
        .isEqualTo("my-project");
    assertThat(mapped.get("fs.gs." + AnalyticsCoreConfigMapper.USER_PROJECT_KEY))
        .isEqualTo("user-project");
    assertThat(mapped.get("fs.gs." + AnalyticsCoreConfigMapper.READ_THREAD_COUNT_KEY))
        .isEqualTo("10");
    assertThat(mapped.get("fs.gs." + AnalyticsCoreConfigMapper.MAX_MERGE_GAP_KEY))
        .isEqualTo("1024");
    assertThat(mapped.get("fs.gs." + AnalyticsCoreConfigMapper.MAX_MERGE_SIZE_KEY))
        .isEqualTo("2048");
    assertThat(mapped.get("fs.gs." + AnalyticsCoreConfigMapper.FILE_ACCESS_PATTERN_KEY))
        .isEqualTo("RANDOM");
    assertThat(mapped.get("fs.gs." + AnalyticsCoreConfigMapper.INPLACE_SEEK_LIMIT_KEY))
        .isEqualTo("50");
    assertThat(mapped.get("fs.gs." + AnalyticsCoreConfigMapper.RANDOM_READ_MIN_REQ_SIZE_KEY))
        .isEqualTo("100");
    assertThat(mapped.get("fs.gs." + AnalyticsCoreConfigMapper.ADAPTIVE_READ_SEQ_THRESHOLD_KEY))
        .isEqualTo("5");
    assertThat(mapped.get("fs.gs." + AnalyticsCoreConfigMapper.UPLOAD_CHUNK_SIZE_KEY))
        .isEqualTo("33554432");
    assertThat(mapped.get("fs.gs." + AnalyticsCoreConfigMapper.UPLOAD_TYPE_KEY))
        .isEqualTo("CHUNK_UPLOAD");
    assertThat(mapped.get("fs.gs." + AnalyticsCoreConfigMapper.PCU_BUFFER_COUNT_KEY))
        .isEqualTo("5");
    assertThat(mapped.get("fs.gs." + AnalyticsCoreConfigMapper.PCU_BUFFER_CAPACITY_KEY))
        .isEqualTo("16777216");
    assertThat(mapped.get("fs.gs." + AnalyticsCoreConfigMapper.PCU_PART_FILE_CLEANUP_TYPE_KEY))
        .isEqualTo("ALWAYS");
    assertThat(mapped.get("fs.gs." + AnalyticsCoreConfigMapper.PCU_PART_FILE_NAME_PREFIX_KEY))
        .isEqualTo("prefix-");
    assertThat(mapped.get("fs.gs." + AnalyticsCoreConfigMapper.ENCRYPTION_KEY_KEY))
        .isEqualTo("my-csek-key");
    assertThat(mapped.get("fs.gs." + AnalyticsCoreConfigMapper.CHECKSUM_VALIDATION_ENABLED_KEY))
        .isEqualTo("true");
    assertThat(mapped).containsAtLeastEntriesIn(EXPECTED_MANDATORY_MAPPINGS);
  }

  @Test
  public void mapConfigs_removesMappedConnectorPropertiesFromResult() {
    Configuration config = createTestConfiguration();

    Map<String, String> mapped = AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.");

    assertThat(mapped.containsKey(GoogleHadoopFileSystemConfiguration.GCS_PROJECT_ID.getKey()))
        .isFalse();
    assertThat(
            mapped.containsKey(
                GoogleHadoopFileSystemConfiguration.GCS_REQUESTER_PAYS_PROJECT_ID.getKey()))
        .isFalse();
    assertThat(
            mapped.containsKey(
                GoogleHadoopFileSystemConfiguration.GCS_VECTORED_READ_THREADS.getKey()))
        .isFalse();
    assertThat(
            mapped.containsKey(
                GoogleHadoopFileSystemConfiguration.GCS_VECTORED_READ_RANGE_MIN_SEEK.getKey()))
        .isFalse();
    assertThat(
            mapped.containsKey(
                GoogleHadoopFileSystemConfiguration.GCS_VECTORED_READ_MERGED_RANGE_MAX_SIZE
                    .getKey()))
        .isFalse();
    assertThat(
            mapped.containsKey(
                GoogleHadoopFileSystemConfiguration.GCS_INPUT_STREAM_FADVISE.getKey()))
        .isFalse();
    assertThat(
            mapped.containsKey(
                GoogleHadoopFileSystemConfiguration.GCS_INPUT_STREAM_INPLACE_SEEK_LIMIT.getKey()))
        .isFalse();
    assertThat(
            mapped.containsKey(
                GoogleHadoopFileSystemConfiguration.GCS_INPUT_STREAM_MIN_RANGE_REQUEST_SIZE
                    .getKey()))
        .isFalse();
    assertThat(
            mapped.containsKey(
                GoogleHadoopFileSystemConfiguration.GCS_FADVISE_REQUEST_TRACK_COUNT.getKey()))
        .isFalse();
    assertThat(
            mapped.containsKey(
                GoogleHadoopFileSystemConfiguration.GCS_OUTPUT_STREAM_UPLOAD_CHUNK_SIZE.getKey()))
        .isFalse();
    assertThat(
            mapped.containsKey(GoogleHadoopFileSystemConfiguration.GCS_CLIENT_UPLOAD_TYPE.getKey()))
        .isFalse();
    assertThat(
            mapped.containsKey(GoogleHadoopFileSystemConfiguration.GCS_PCU_BUFFER_COUNT.getKey()))
        .isFalse();
    assertThat(
            mapped.containsKey(
                GoogleHadoopFileSystemConfiguration.GCS_PCU_BUFFER_CAPACITY.getKey()))
        .isFalse();
    assertThat(
            mapped.containsKey(
                GoogleHadoopFileSystemConfiguration.GCS_PCU_PART_FILE_CLEANUP_TYPE.getKey()))
        .isFalse();
    assertThat(
            mapped.containsKey(
                GoogleHadoopFileSystemConfiguration.GCS_PCU_PART_FILE_NAME_PREFIX.getKey()))
        .isFalse();
    assertThat(mapped.containsKey(GoogleHadoopFileSystemConfiguration.GCS_ENCRYPTION_KEY.getKey()))
        .isFalse();
    assertThat(
            mapped.containsKey(
                GoogleHadoopFileSystemConfiguration.GCS_WRITE_ROLLING_CHECKSUM_ENABLE.getKey()))
        .isFalse();
    assertThat(mapped.containsKey(GoogleHadoopFileSystemConfiguration.GCS_CLIENT_TYPE.getKey()))
        .isFalse();
  }

  @Test
  public void mapConfigs_mapsFadviseModesCorrectly() {
    Configuration config = new Configuration();

    config.set(GoogleHadoopFileSystemConfiguration.GCS_INPUT_STREAM_FADVISE.getKey(), "SEQUENTIAL");
    assertThat(
            AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.")
                .get("fs.gs." + AnalyticsCoreConfigMapper.FILE_ACCESS_PATTERN_KEY))
        .isEqualTo("SEQUENTIAL");

    config.set(GoogleHadoopFileSystemConfiguration.GCS_INPUT_STREAM_FADVISE.getKey(), "RANDOM");
    assertThat(
            AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.")
                .get("fs.gs." + AnalyticsCoreConfigMapper.FILE_ACCESS_PATTERN_KEY))
        .isEqualTo("RANDOM");

    config.set(GoogleHadoopFileSystemConfiguration.GCS_INPUT_STREAM_FADVISE.getKey(), "AUTO");
    assertThat(
            AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.")
                .get("fs.gs." + AnalyticsCoreConfigMapper.FILE_ACCESS_PATTERN_KEY))
        .isEqualTo("AUTO_SEQUENTIAL");

    config.set(
        GoogleHadoopFileSystemConfiguration.GCS_INPUT_STREAM_FADVISE.getKey(), "AUTO_RANDOM");
    assertThat(
            AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.")
                .get("fs.gs." + AnalyticsCoreConfigMapper.FILE_ACCESS_PATTERN_KEY))
        .isEqualTo("AUTO_RANDOM");
  }

  @Test
  public void mapConfigs_mapsUserAgentWithSuffix() {
    Configuration config = new Configuration();
    config.set("fs.gs.application.name.suffix", "-my-suffix");

    Map<String, String> mapped = AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.");

    assertThat(mapped.get("fs.gs." + AnalyticsCoreConfigMapper.USER_AGENT_KEY))
        .isEqualTo(GoogleHadoopFileSystem.GHFS_ID + "-my-suffix");
  }

  @Test
  public void mapConfigs_preservesUnmappedPropertiesWithPrefix() {
    Configuration config = new Configuration();
    config.set("fs.gs.some.other.prop", "val");

    Map<String, String> mapped = AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.");

    assertThat(mapped.get("fs.gs.some.other.prop")).isEqualTo("val");
  }

  @Test
  public void mapConfigs_returnsOnlyMandatoryMappingsWhenNoMatchingPrefix() {
    Configuration config = new Configuration(false);
    config.set("other.prefix.prop", "val");

    Map<String, String> mapped = AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.");

    assertThat(mapped).containsExactlyEntriesIn(EXPECTED_MANDATORY_MAPPINGS);
  }

  @Test
  public void mapConfigs_returnsOnlyMandatoryMappingsWhenConfigIsEmpty() {
    Configuration config = new Configuration(false);

    Map<String, String> mapped = AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.");

    assertThat(mapped).containsExactlyEntriesIn(EXPECTED_MANDATORY_MAPPINGS);
  }

  private Configuration createTestConfiguration() {
    Configuration config = new Configuration();
    config.set(GoogleHadoopFileSystemConfiguration.GCS_PROJECT_ID.getKey(), "my-project");
    config.set(
        GoogleHadoopFileSystemConfiguration.GCS_REQUESTER_PAYS_PROJECT_ID.getKey(), "user-project");
    config.set(GoogleHadoopFileSystemConfiguration.GCS_VECTORED_READ_THREADS.getKey(), "10");
    config.set(
        GoogleHadoopFileSystemConfiguration.GCS_VECTORED_READ_RANGE_MIN_SEEK.getKey(), "1024");
    config.set(
        GoogleHadoopFileSystemConfiguration.GCS_VECTORED_READ_MERGED_RANGE_MAX_SIZE.getKey(),
        "2048");
    config.set(GoogleHadoopFileSystemConfiguration.GCS_INPUT_STREAM_FADVISE.getKey(), "RANDOM");
    config.set(
        GoogleHadoopFileSystemConfiguration.GCS_INPUT_STREAM_INPLACE_SEEK_LIMIT.getKey(), "50");
    config.set(
        GoogleHadoopFileSystemConfiguration.GCS_INPUT_STREAM_MIN_RANGE_REQUEST_SIZE.getKey(),
        "100");
    config.set(GoogleHadoopFileSystemConfiguration.GCS_FADVISE_REQUEST_TRACK_COUNT.getKey(), "5");
    config.set(
        GoogleHadoopFileSystemConfiguration.GCS_OUTPUT_STREAM_UPLOAD_CHUNK_SIZE.getKey(),
        "33554432");
    config.set(GoogleHadoopFileSystemConfiguration.GCS_CLIENT_UPLOAD_TYPE.getKey(), "CHUNK_UPLOAD");
    config.set(GoogleHadoopFileSystemConfiguration.GCS_PCU_BUFFER_COUNT.getKey(), "5");
    config.set(GoogleHadoopFileSystemConfiguration.GCS_PCU_BUFFER_CAPACITY.getKey(), "16777216");
    config.set(
        GoogleHadoopFileSystemConfiguration.GCS_PCU_PART_FILE_CLEANUP_TYPE.getKey(), "ALWAYS");
    config.set(
        GoogleHadoopFileSystemConfiguration.GCS_PCU_PART_FILE_NAME_PREFIX.getKey(), "prefix-");
    config.set(GoogleHadoopFileSystemConfiguration.GCS_ENCRYPTION_KEY.getKey(), "my-csek-key");
    config.set(
        GoogleHadoopFileSystemConfiguration.GCS_WRITE_ROLLING_CHECKSUM_ENABLE.getKey(), "true");
    config.set("fs.gs.client.type", "STORAGE_CLIENT");
    return config;
  }

  @Test
  public void mapConfigs_mapsChecksumValidationEnabled() {
    Configuration config = new Configuration();

    config.set(
        GoogleHadoopFileSystemConfiguration.GCS_WRITE_ROLLING_CHECKSUM_ENABLE.getKey(), "false");
    assertThat(
            AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.")
                .get("fs.gs." + AnalyticsCoreConfigMapper.CHECKSUM_VALIDATION_ENABLED_KEY))
        .isEqualTo("false");

    config.set(
        GoogleHadoopFileSystemConfiguration.GCS_WRITE_ROLLING_CHECKSUM_ENABLE.getKey(), "true");
    assertThat(
            AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.")
                .get("fs.gs." + AnalyticsCoreConfigMapper.CHECKSUM_VALIDATION_ENABLED_KEY))
        .isEqualTo("true");
  }

  @Test
  public void mapConfigs_mapsTemporaryPathsOrFallback() {
    Configuration config = new Configuration();

    // Fallback to hadoop.tmp.dir
    config.set("hadoop.tmp.dir", "/hadoop/tmp");
    assertThat(
            AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.")
                .get("fs.gs." + AnalyticsCoreConfigMapper.TEMPORARY_PATHS_KEY))
        .isEqualTo("/hadoop/tmp");

    // Override with fs.gs.write.temporary.dirs
    config.set(
        GoogleHadoopFileSystemConfiguration.GCS_WRITE_TEMPORARY_FILES_PATH.getKey(),
        "/gcs/tmp1,/gcs/tmp2");
    assertThat(
            AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.")
                .get("fs.gs." + AnalyticsCoreConfigMapper.TEMPORARY_PATHS_KEY))
        .isEqualTo("/gcs/tmp1,/gcs/tmp2");
  }
}
