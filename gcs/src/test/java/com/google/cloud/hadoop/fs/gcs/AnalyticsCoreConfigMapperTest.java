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
  }

  @Test
  public void mapConfigs_preservesUnmappedPropertiesWithPrefix() {
    Configuration config = new Configuration();
    config.set("fs.gs.some.other.prop", "val");

    Map<String, String> mapped = AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.");

    assertThat(mapped.get("fs.gs.some.other.prop")).isEqualTo("val");
  }

  @Test
  public void mapConfigs_returnsEmptyWhenNoMatchingPrefix() {
    Configuration config = new Configuration(false);
    config.set("other.prefix.prop", "val");

    Map<String, String> mapped = AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.");

    assertThat(mapped).isEmpty();
  }

  @Test
  public void mapConfigs_returnsEmptyWhenConfigIsEmpty() {
    Configuration config = new Configuration(false);

    Map<String, String> mapped = AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.");

    assertThat(mapped).isEmpty();
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
    return config;
  }
}
