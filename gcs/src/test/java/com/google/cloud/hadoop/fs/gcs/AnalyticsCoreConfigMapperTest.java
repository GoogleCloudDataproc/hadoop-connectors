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
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_CREDENTIAL_PROVIDER_PATH;

import com.google.cloud.gcs.analyticscore.client.GcsClientOptions;
import com.google.cloud.gcs.analyticscore.client.GcsClientOptions.Protocol;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystemOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions.ClientType;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link AnalyticsCoreConfigMapper}. */
@RunWith(JUnit4.class)
public class AnalyticsCoreConfigMapperTest {

  @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

  /**
   * Mappings emitted for every configuration: the user agent, and the transport selection, which is
   * always derived from {@code fs.gs.client.type} / {@code fs.gs.bidi.enable}.
   */
  private static final Map<String, String> EXPECTED_MANDATORY_MAPPINGS =
      Map.of(
          "fs.gs." + AnalyticsCoreConfigMapper.USER_AGENT_KEY,
          GoogleHadoopFileSystem.GHFS_ID,
          "fs.gs." + AnalyticsCoreConfigMapper.CLIENT_PROTOCOL_KEY,
          Protocol.HTTP.name(),
          "fs.gs." + AnalyticsCoreConfigMapper.BIDI_WRITE_ENABLED_KEY,
          "false");

  @Test
  public void mapConfigs_mapsConnectorPropertiesToAnalyticsCore() {
    Configuration config = createTestConfiguration();

    Map<String, String> mapped = AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.");

    assertThat(mapped.get("fs.gs." + AnalyticsCoreConfigMapper.PROJECT_ID_KEY))
        .isEqualTo("my-project");
    assertThat(mapped.get("fs.gs." + AnalyticsCoreConfigMapper.USER_PROJECT_KEY))
        .isEqualTo("user-project");
    assertThat(mapped.get("fs.gs." + AnalyticsCoreConfigMapper.SERVICE_HOST_KEY))
        .isEqualTo("http://emulator:8080/");
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
    assertThat(mapped.get("fs.gs." + AnalyticsCoreConfigMapper.USER_AGENT_KEY))
        .isEqualTo(GoogleHadoopFileSystem.GHFS_ID);
    // createTestConfiguration() selects STORAGE_CLIENT without bidi.
    assertThat(mapped.get("fs.gs." + AnalyticsCoreConfigMapper.CLIENT_PROTOCOL_KEY))
        .isEqualTo(Protocol.GRPC.name());
    assertThat(mapped.get("fs.gs." + AnalyticsCoreConfigMapper.BIDI_WRITE_ENABLED_KEY))
        .isEqualTo("false");
  }

  @Test
  public void mapConfigs_removesMappedConnectorPropertiesFromResult() {
    Configuration config = createTestConfiguration();

    Map<String, String> mapped = AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.");

    assertThat(mapped.containsKey(GoogleHadoopFileSystemConfiguration.GCS_PROJECT_ID.getKey()))
        .isFalse();
    assertThat(mapped.containsKey(GoogleHadoopFileSystemConfiguration.GCS_ROOT_URL.getKey()))
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

  @Test
  public void mapConfigs_mapsEncryptionKeyToEncryptionAndDecryptionKeys() {
    Configuration config = new Configuration();
    config.set(GoogleHadoopFileSystemConfiguration.GCS_ENCRYPTION_KEY.getKey(), "csek-value");

    Map<String, String> mapped = AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.");

    assertThat(mapped)
        .containsAtLeast(
            "fs.gs." + AnalyticsCoreConfigMapper.ENCRYPTION_KEY_KEY,
            "csek-value",
            "fs.gs." + AnalyticsCoreConfigMapper.DECRYPTION_KEY_KEY,
            "csek-value");
    assertThat(mapped)
        .doesNotContainKey(GoogleHadoopFileSystemConfiguration.GCS_ENCRYPTION_KEY.getKey());
  }

  @Test
  public void mapConfigs_resolvesEncryptionKeyFromCredentialProvider() throws Exception {
    Configuration config = new Configuration();
    config.set(
        HADOOP_SECURITY_CREDENTIAL_PROVIDER_PATH,
        "jceks://file" + tempFolder.getRoot().toPath().resolve("secrets.jceks").toUri().getPath());
    CredentialProvider credentialProvider = CredentialProviderFactory.getProviders(config).get(0);
    credentialProvider.createCredentialEntry(
        GoogleHadoopFileSystemConfiguration.GCS_ENCRYPTION_KEY.getKey(),
        "jceks-csek-value".toCharArray());
    credentialProvider.flush();

    Map<String, String> mapped = AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.");

    assertThat(mapped)
        .containsAtLeast(
            "fs.gs." + AnalyticsCoreConfigMapper.ENCRYPTION_KEY_KEY,
            "jceks-csek-value",
            "fs.gs." + AnalyticsCoreConfigMapper.DECRYPTION_KEY_KEY,
            "jceks-csek-value");
  }

  @Test
  public void mapConfigs_encryptionKeySet_overridesAnalyticsCoreSpelledKeys() {
    Configuration config = new Configuration();
    config.set(GoogleHadoopFileSystemConfiguration.GCS_ENCRYPTION_KEY.getKey(), "connector-value");
    config.set("fs.gs." + AnalyticsCoreConfigMapper.ENCRYPTION_KEY_KEY, "analytics-core-value");
    config.set("fs.gs." + AnalyticsCoreConfigMapper.DECRYPTION_KEY_KEY, "analytics-core-value");

    Map<String, String> mapped = AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.");

    assertThat(mapped)
        .containsAtLeast(
            "fs.gs." + AnalyticsCoreConfigMapper.ENCRYPTION_KEY_KEY,
            "connector-value",
            "fs.gs." + AnalyticsCoreConfigMapper.DECRYPTION_KEY_KEY,
            "connector-value");
  }

  @Test
  public void mapConfigs_encryptionKeyUnset_omitsEncryptionAndDecryptionKeys() {
    Configuration config = new Configuration();

    Map<String, String> mapped = AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.");

    assertThat(mapped).doesNotContainKey("fs.gs." + AnalyticsCoreConfigMapper.ENCRYPTION_KEY_KEY);
    assertThat(mapped).doesNotContainKey("fs.gs." + AnalyticsCoreConfigMapper.DECRYPTION_KEY_KEY);
  }

  private Configuration createTestConfiguration() {
    Configuration config = new Configuration();
    config.set(GoogleHadoopFileSystemConfiguration.GCS_PROJECT_ID.getKey(), "my-project");
    config.set(
        GoogleHadoopFileSystemConfiguration.GCS_REQUESTER_PAYS_PROJECT_ID.getKey(), "user-project");
    config.set(GoogleHadoopFileSystemConfiguration.GCS_REQUESTER_PAYS_MODE.getKey(), "AUTO");
    config.set(GoogleHadoopFileSystemConfiguration.GCS_ROOT_URL.getKey(), "http://emulator:8080/");
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

  @Test
  public void mapConfigs_normalizesUploadType() {
    Configuration config = new Configuration();

    config.set(GoogleHadoopFileSystemConfiguration.GCS_CLIENT_UPLOAD_TYPE.getKey(), "chunk-upload");
    assertThat(
            AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.")
                .get("fs.gs." + AnalyticsCoreConfigMapper.UPLOAD_TYPE_KEY))
        .isEqualTo("CHUNK_UPLOAD");

    config.set(
        GoogleHadoopFileSystemConfiguration.GCS_CLIENT_UPLOAD_TYPE.getKey(),
        "parallel-composite-upload");
    assertThat(
            AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.")
                .get("fs.gs." + AnalyticsCoreConfigMapper.UPLOAD_TYPE_KEY))
        .isEqualTo("PARALLEL_COMPOSITE_UPLOAD");

    // Invalid value falls back to default CHUNK_UPLOAD
    config.set(GoogleHadoopFileSystemConfiguration.GCS_CLIENT_UPLOAD_TYPE.getKey(), "invalid-type");
    assertThat(
            AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.")
                .get("fs.gs." + AnalyticsCoreConfigMapper.UPLOAD_TYPE_KEY))
        .isEqualTo("CHUNK_UPLOAD");
  }

  @Test
  public void mapConfigs_mapsRequesterPaysProjectIdConditionally() {
    Configuration config = new Configuration();
    config.set(
        GoogleHadoopFileSystemConfiguration.GCS_REQUESTER_PAYS_PROJECT_ID.getKey(), "user-project");

    // Mode is not set (defaults to DISABLED) -> should NOT map project ID
    Map<String, String> mappedDefault = AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.");
    assertThat(mappedDefault.containsKey("fs.gs." + AnalyticsCoreConfigMapper.USER_PROJECT_KEY))
        .isFalse();
    // But it should still remove the source key from result if it was there
    assertThat(
            mappedDefault.containsKey(
                GoogleHadoopFileSystemConfiguration.GCS_REQUESTER_PAYS_PROJECT_ID.getKey()))
        .isFalse();

    // Mode is DISABLED explicitly -> should NOT map project ID
    config.set(GoogleHadoopFileSystemConfiguration.GCS_REQUESTER_PAYS_MODE.getKey(), "DISABLED");
    Map<String, String> mappedDisabled = AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.");
    assertThat(mappedDisabled.containsKey("fs.gs." + AnalyticsCoreConfigMapper.USER_PROJECT_KEY))
        .isFalse();

    // Mode is AUTO -> should map project ID
    config.set(GoogleHadoopFileSystemConfiguration.GCS_REQUESTER_PAYS_MODE.getKey(), "AUTO");
    Map<String, String> mappedAuto = AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.");
    assertThat(mappedAuto.get("fs.gs." + AnalyticsCoreConfigMapper.USER_PROJECT_KEY))
        .isEqualTo("user-project");

    // Mode is ENABLED -> should map project ID
    config.set(GoogleHadoopFileSystemConfiguration.GCS_REQUESTER_PAYS_MODE.getKey(), "ENABLED");
    Map<String, String> mappedEnabled = AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.");
    assertThat(mappedEnabled.get("fs.gs." + AnalyticsCoreConfigMapper.USER_PROJECT_KEY))
        .isEqualTo("user-project");

    // Fallback to fs.gs.project.id when fs.gs.requester.pays.project.id is not set
    Configuration fallbackConfig = new Configuration();
    fallbackConfig.set(GoogleHadoopFileSystemConfiguration.GCS_PROJECT_ID.getKey(), "my-project");

    fallbackConfig.set(
        GoogleHadoopFileSystemConfiguration.GCS_REQUESTER_PAYS_MODE.getKey(), "AUTO");
    Map<String, String> mappedFallbackAuto =
        AnalyticsCoreConfigMapper.mapConfigs(fallbackConfig, "fs.gs.");
    assertThat(mappedFallbackAuto.get("fs.gs." + AnalyticsCoreConfigMapper.USER_PROJECT_KEY))
        .isEqualTo("my-project");

    fallbackConfig.set(
        GoogleHadoopFileSystemConfiguration.GCS_REQUESTER_PAYS_MODE.getKey(), "ENABLED");
    Map<String, String> mappedFallbackEnabled =
        AnalyticsCoreConfigMapper.mapConfigs(fallbackConfig, "fs.gs.");
    assertThat(mappedFallbackEnabled.get("fs.gs." + AnalyticsCoreConfigMapper.USER_PROJECT_KEY))
        .isEqualTo("my-project");

    fallbackConfig.set(
        GoogleHadoopFileSystemConfiguration.GCS_REQUESTER_PAYS_MODE.getKey(), "DISABLED");
    Map<String, String> mappedFallbackDisabled =
        AnalyticsCoreConfigMapper.mapConfigs(fallbackConfig, "fs.gs.");
    assertThat(
            mappedFallbackDisabled.containsKey(
                "fs.gs." + AnalyticsCoreConfigMapper.USER_PROJECT_KEY))
        .isFalse();
  }

  @Test
  public void mapConfigs_normalizesPcuPartFileCleanupType() {
    Configuration config = new Configuration();

    config.set(
        GoogleHadoopFileSystemConfiguration.GCS_PCU_PART_FILE_CLEANUP_TYPE.getKey(), "on-success");
    assertThat(
            AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.")
                .get("fs.gs." + AnalyticsCoreConfigMapper.PCU_PART_FILE_CLEANUP_TYPE_KEY))
        .isEqualTo("ON_SUCCESS");

    config.set(
        GoogleHadoopFileSystemConfiguration.GCS_PCU_PART_FILE_CLEANUP_TYPE.getKey(), "always");
    assertThat(
            AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.")
                .get("fs.gs." + AnalyticsCoreConfigMapper.PCU_PART_FILE_CLEANUP_TYPE_KEY))
        .isEqualTo("ALWAYS");

    // Invalid value falls back to default ALWAYS
    config.set(
        GoogleHadoopFileSystemConfiguration.GCS_PCU_PART_FILE_CLEANUP_TYPE.getKey(),
        "invalid-cleanup");
    assertThat(
            AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.")
                .get("fs.gs." + AnalyticsCoreConfigMapper.PCU_PART_FILE_CLEANUP_TYPE_KEY))
        .isEqualTo("ALWAYS");
  }

  @Test
  public void mapConfigs_defaultClientType_mapsToHttpProtocol() {
    Configuration config = new Configuration();

    Map<String, String> mapped = AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.");

    assertThat(mapped)
        .containsAtLeast(
            "fs.gs." + AnalyticsCoreConfigMapper.CLIENT_PROTOCOL_KEY,
            Protocol.HTTP.name(),
            "fs.gs." + AnalyticsCoreConfigMapper.BIDI_WRITE_ENABLED_KEY,
            "false");
  }

  @Test
  public void mapConfigs_httpApiClient_ignoresBidiAndMapsToHttpProtocol() {
    Configuration config = new Configuration();
    config.setEnum(
        GoogleHadoopFileSystemConfiguration.GCS_CLIENT_TYPE.getKey(), ClientType.HTTP_API_CLIENT);
    config.setBoolean(
        GoogleHadoopFileSystemConfiguration.GCS_OPERATION_BIDI_API_ENABLE.getKey(), true);
    config.setBoolean(GoogleHadoopFileSystemConfiguration.GCS_GRPC_WRITE_ENABLE.getKey(), true);

    Map<String, String> mapped = AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.");

    // Bidi is only effective with STORAGE_CLIENT, matching the legacy client.
    assertThat(mapped)
        .containsAtLeast(
            "fs.gs." + AnalyticsCoreConfigMapper.CLIENT_PROTOCOL_KEY,
            Protocol.HTTP.name(),
            "fs.gs." + AnalyticsCoreConfigMapper.BIDI_WRITE_ENABLED_KEY,
            "false");
  }

  @Test
  public void mapConfigs_storageClient_mapsToGrpcProtocol() {
    Configuration config = new Configuration();
    config.setEnum(
        GoogleHadoopFileSystemConfiguration.GCS_CLIENT_TYPE.getKey(), ClientType.STORAGE_CLIENT);

    Map<String, String> mapped = AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.");

    assertThat(mapped)
        .containsAtLeast(
            "fs.gs." + AnalyticsCoreConfigMapper.CLIENT_PROTOCOL_KEY,
            Protocol.GRPC.name(),
            "fs.gs." + AnalyticsCoreConfigMapper.BIDI_WRITE_ENABLED_KEY,
            "false");
  }

  @Test
  public void mapConfigs_storageClientWithBidi_mapsToBidiProtocol() {
    Configuration config = new Configuration();
    config.setEnum(
        GoogleHadoopFileSystemConfiguration.GCS_CLIENT_TYPE.getKey(), ClientType.STORAGE_CLIENT);
    config.setBoolean(
        GoogleHadoopFileSystemConfiguration.GCS_OPERATION_BIDI_API_ENABLE.getKey(), true);

    Map<String, String> mapped = AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.");

    assertThat(mapped.get("fs.gs." + AnalyticsCoreConfigMapper.CLIENT_PROTOCOL_KEY))
        .isEqualTo(Protocol.BIDI.name());
    // Appendable writes additionally require fs.gs.grpc.write.enable, as in the legacy client.
    assertThat(mapped.get("fs.gs." + AnalyticsCoreConfigMapper.BIDI_WRITE_ENABLED_KEY))
        .isEqualTo("false");
  }

  @Test
  public void mapConfigs_storageClientWithBidiAndGrpcWrite_enablesBidiWrites() {
    Configuration config = new Configuration();
    config.setEnum(
        GoogleHadoopFileSystemConfiguration.GCS_CLIENT_TYPE.getKey(), ClientType.STORAGE_CLIENT);
    config.setBoolean(
        GoogleHadoopFileSystemConfiguration.GCS_OPERATION_BIDI_API_ENABLE.getKey(), true);
    config.setBoolean(GoogleHadoopFileSystemConfiguration.GCS_GRPC_WRITE_ENABLE.getKey(), true);

    Map<String, String> mapped = AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.");

    assertThat(mapped)
        .containsAtLeast(
            "fs.gs." + AnalyticsCoreConfigMapper.CLIENT_PROTOCOL_KEY,
            Protocol.BIDI.name(),
            "fs.gs." + AnalyticsCoreConfigMapper.BIDI_WRITE_ENABLED_KEY,
            "true");
  }

  @Test
  public void mapConfigs_mapsBidiTimeoutAndFinalizeOnClose() {
    Configuration config = new Configuration();
    config.setEnum(
        GoogleHadoopFileSystemConfiguration.GCS_CLIENT_TYPE.getKey(), ClientType.STORAGE_CLIENT);
    config.setBoolean(
        GoogleHadoopFileSystemConfiguration.GCS_OPERATION_BIDI_API_ENABLE.getKey(), true);
    config.setBoolean(GoogleHadoopFileSystemConfiguration.GCS_GRPC_WRITE_ENABLE.getKey(), true);
    config.setInt(
        GoogleHadoopFileSystemConfiguration.GCS_BIDI_CLIENT_INITIALIZATION_TIMEOUT.getKey(), 45);
    config.setBoolean(
        GoogleHadoopFileSystemConfiguration.GCS_APPENDABLE_OBJECTS_FINALIZE_BEFORE_CLOSE.getKey(),
        true);

    Map<String, String> mapped = AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.");

    assertThat(mapped)
        .containsAtLeast(
            "fs.gs." + AnalyticsCoreConfigMapper.BIDI_READ_TIMEOUT_SECONDS_KEY,
            "45",
            "fs.gs." + AnalyticsCoreConfigMapper.BIDI_WRITE_FINALIZE_ON_CLOSE_KEY,
            "true");
    assertThat(mapped)
        .doesNotContainKey(
            GoogleHadoopFileSystemConfiguration.GCS_BIDI_CLIENT_INITIALIZATION_TIMEOUT.getKey());
    assertThat(mapped)
        .doesNotContainKey(
            GoogleHadoopFileSystemConfiguration.GCS_APPENDABLE_OBJECTS_FINALIZE_BEFORE_CLOSE
                .getKey());
  }

  @Test
  public void mapConfigs_bidiOptionsUnset_leavesAnalyticsCoreDefaults() {
    Configuration config = new Configuration();
    config.setEnum(
        GoogleHadoopFileSystemConfiguration.GCS_CLIENT_TYPE.getKey(), ClientType.STORAGE_CLIENT);
    config.setBoolean(
        GoogleHadoopFileSystemConfiguration.GCS_OPERATION_BIDI_API_ENABLE.getKey(), true);
    config.setBoolean(GoogleHadoopFileSystemConfiguration.GCS_GRPC_WRITE_ENABLE.getKey(), true);

    Map<String, String> mapped = AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.");

    assertThat(mapped)
        .doesNotContainKey("fs.gs." + AnalyticsCoreConfigMapper.BIDI_READ_TIMEOUT_SECONDS_KEY);
    assertThat(mapped)
        .doesNotContainKey("fs.gs." + AnalyticsCoreConfigMapper.BIDI_WRITE_FINALIZE_ON_CLOSE_KEY);
  }

  @Test
  public void mapConfigs_bidiWritesDisabled_dropsFinalizeOnClose() {
    Configuration config = new Configuration();
    config.setEnum(
        GoogleHadoopFileSystemConfiguration.GCS_CLIENT_TYPE.getKey(), ClientType.STORAGE_CLIENT);
    config.setBoolean(
        GoogleHadoopFileSystemConfiguration.GCS_APPENDABLE_OBJECTS_FINALIZE_BEFORE_CLOSE.getKey(),
        true);

    Map<String, String> mapped = AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.");

    assertThat(mapped.get("fs.gs." + AnalyticsCoreConfigMapper.CLIENT_PROTOCOL_KEY))
        .isEqualTo(Protocol.GRPC.name());
    assertThat(mapped)
        .doesNotContainKey("fs.gs." + AnalyticsCoreConfigMapper.BIDI_WRITE_FINALIZE_ON_CLOSE_KEY);
  }

  @Test
  public void mapConfigs_removesTransportSourceKeys() {
    Configuration config = new Configuration();
    config.setEnum(
        GoogleHadoopFileSystemConfiguration.GCS_CLIENT_TYPE.getKey(), ClientType.STORAGE_CLIENT);
    config.setBoolean(
        GoogleHadoopFileSystemConfiguration.GCS_OPERATION_BIDI_API_ENABLE.getKey(), true);
    config.setBoolean(GoogleHadoopFileSystemConfiguration.GCS_GRPC_WRITE_ENABLE.getKey(), true);

    Map<String, String> mapped = AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.");

    assertThat(mapped)
        .doesNotContainKey(GoogleHadoopFileSystemConfiguration.GCS_CLIENT_TYPE.getKey());
    assertThat(mapped)
        .doesNotContainKey(
            GoogleHadoopFileSystemConfiguration.GCS_OPERATION_BIDI_API_ENABLE.getKey());
    assertThat(mapped)
        .doesNotContainKey(GoogleHadoopFileSystemConfiguration.GCS_GRPC_WRITE_ENABLE.getKey());
  }

  @Test
  public void mapConfigs_connectorTransportOverridesPassedThroughProtocol() {
    Configuration config = new Configuration();
    config.set("fs.gs." + AnalyticsCoreConfigMapper.CLIENT_PROTOCOL_KEY, "GRPC");
    config.set("fs.gs." + AnalyticsCoreConfigMapper.BIDI_WRITE_ENABLED_KEY, "true");

    Map<String, String> mapped = AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.");

    // fs.gs.client.type / fs.gs.bidi.enable are the single source of truth for the transport.
    assertThat(mapped)
        .containsAtLeast(
            "fs.gs." + AnalyticsCoreConfigMapper.CLIENT_PROTOCOL_KEY,
            Protocol.HTTP.name(),
            "fs.gs." + AnalyticsCoreConfigMapper.BIDI_WRITE_ENABLED_KEY,
            "false");
  }

  @Test
  public void mapConfigs_transportMapping_isParsedByAnalyticsCore() {
    Configuration config = new Configuration();
    config.setEnum(
        GoogleHadoopFileSystemConfiguration.GCS_CLIENT_TYPE.getKey(), ClientType.STORAGE_CLIENT);
    config.setBoolean(
        GoogleHadoopFileSystemConfiguration.GCS_OPERATION_BIDI_API_ENABLE.getKey(), true);
    config.setBoolean(GoogleHadoopFileSystemConfiguration.GCS_GRPC_WRITE_ENABLE.getKey(), true);
    config.setInt(
        GoogleHadoopFileSystemConfiguration.GCS_BIDI_CLIENT_INITIALIZATION_TIMEOUT.getKey(), 45);
    config.setBoolean(
        GoogleHadoopFileSystemConfiguration.GCS_APPENDABLE_OBJECTS_FINALIZE_BEFORE_CLOSE.getKey(),
        true);

    GcsFileSystemOptions options =
        GcsFileSystemOptions.createFromOptions(
            AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs."), "fs.gs.");

    GcsClientOptions clientOptions = options.getGcsClientOptions();
    assertThat(clientOptions.getProtocol()).isEqualTo(Protocol.BIDI);
    assertThat(clientOptions.getGcsReadOptions().getBidiTimeout()).isEqualTo(45);
    assertThat(clientOptions.getGcsWriteOptions().isBidiWriteEnabled()).isTrue();
    assertThat(clientOptions.getGcsWriteOptions().isBidiFinalizeOnClose()).isTrue();
  }

  @Test
  public void toProtocol_resolvesProtocolFromClientTypeAndBidiFlag() {
    Configuration config = new Configuration();
    assertThat(AnalyticsCoreConfigMapper.toProtocol(config)).isEqualTo(Protocol.HTTP);

    config.setEnum(
        GoogleHadoopFileSystemConfiguration.GCS_CLIENT_TYPE.getKey(), ClientType.STORAGE_CLIENT);
    assertThat(AnalyticsCoreConfigMapper.toProtocol(config)).isEqualTo(Protocol.GRPC);

    config.setBoolean(
        GoogleHadoopFileSystemConfiguration.GCS_OPERATION_BIDI_API_ENABLE.getKey(), true);
    assertThat(AnalyticsCoreConfigMapper.toProtocol(config)).isEqualTo(Protocol.BIDI);
  }
}
