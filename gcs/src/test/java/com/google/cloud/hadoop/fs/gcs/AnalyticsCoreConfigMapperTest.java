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
    Configuration config = new Configuration();
    config.set("fs.gs.project.id", "my-project");
    config.set("fs.gs.requester.pays.project.id", "user-project");
    config.set("fs.gs.vectored.read.threads", "10");
    config.set("fs.gs.vectored.read.min.range.seek.size", "1024");
    config.set("fs.gs.vectored.read.merged.range.max.size", "2048");

    Map<String, String> mapped = AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.");

    assertThat(mapped.get("fs.gs.project-id")).isEqualTo("my-project");
    assertThat(mapped.get("fs.gs.user-project")).isEqualTo("user-project");
    assertThat(mapped.get("fs.gs.analytics-core.read.thread.count")).isEqualTo("10");
    assertThat(mapped.get("fs.gs.analytics-core.read.vectored.range.merge-gap.max-bytes"))
        .isEqualTo("1024");
    assertThat(mapped.get("fs.gs.analytics-core.read.vectored.range.merged-size.max-bytes"))
        .isEqualTo("2048");
  }

  @Test
  public void mapConfigs_removesMappedConnectorPropertiesFromResult() {
    Configuration config = new Configuration();
    config.set("fs.gs.project.id", "my-project");
    config.set("fs.gs.requester.pays.project.id", "user-project");
    config.set("fs.gs.vectored.read.threads", "10");
    config.set("fs.gs.vectored.read.min.range.seek.size", "1024");
    config.set("fs.gs.vectored.read.merged.range.max.size", "2048");

    Map<String, String> mapped = AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.");

    assertThat(mapped.containsKey("fs.gs.project.id")).isFalse();
    assertThat(mapped.containsKey("fs.gs.requester.pays.project.id")).isFalse();
    assertThat(mapped.containsKey("fs.gs.vectored.read.threads")).isFalse();
    assertThat(mapped.containsKey("fs.gs.vectored.read.min.range.seek.size")).isFalse();
    assertThat(mapped.containsKey("fs.gs.vectored.read.merged.range.max.size")).isFalse();
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
    Configuration config = new Configuration();
    config.set("other.prefix.prop", "val");

    Map<String, String> mapped = AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.");
    mapped.remove("fs.gs.impl");

    assertThat(mapped).isEmpty();
  }

  @Test
  public void mapConfigs_returnsEmptyWhenConfigIsEmpty() {
    Configuration config = new Configuration();

    Map<String, String> mapped = AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.");
    mapped.remove("fs.gs.impl");

    assertThat(mapped).isEmpty();
  }
}
