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

import com.google.cloud.hadoop.fs.gcs.auth.TestDelegationTokenBindingImpl;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
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

  private static final Map<String, String> EXPECTED_MANDATORY_MAPPINGS =
      Map.of(
          "fs.gs." + AnalyticsCoreConfigMapper.USER_AGENT_KEY,
          GoogleHadoopFileSystem.GHFS_ID,
          "fs.gs." + AnalyticsCoreConfigMapper.AUTH_TYPE_KEY,
          "COMPUTE_ENGINE",
          "fs.gs." + AnalyticsCoreConfigMapper.HTTP_CONNECT_TIMEOUT_KEY,
          "5000",
          "fs.gs." + AnalyticsCoreConfigMapper.HTTP_READ_TIMEOUT_KEY,
          "5000");

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
    assertThat(mapped).containsAtLeastEntriesIn(EXPECTED_MANDATORY_MAPPINGS);
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

  @Test
  public void mapConfigs_mapsUniverseDomainAndProxySettings() {
    Configuration config = createTransportConfiguration();

    Map<String, String> mapped = AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.");

    assertThat(mapped)
        .containsAtLeast(
            "fs.gs." + AnalyticsCoreConfigMapper.UNIVERSE_DOMAIN_KEY,
            "my-universe.test",
            "fs.gs." + AnalyticsCoreConfigMapper.PROXY_ADDRESS_KEY,
            "proxy-host:1234",
            "fs.gs." + AnalyticsCoreConfigMapper.PROXY_USERNAME_KEY,
            "proxy-user",
            "fs.gs." + AnalyticsCoreConfigMapper.PROXY_PASSWORD_KEY,
            "proxy-password");
  }

  @Test
  public void mapConfigs_resolvesProxyPasswordFromCredentialProvider() throws Exception {
    Configuration config = new Configuration();
    config.set(
        HADOOP_SECURITY_CREDENTIAL_PROVIDER_PATH,
        "jceks://file"
            + tempFolder.getRoot().toPath().resolve("proxy-secrets.jceks").toUri().getPath());
    CredentialProvider credentialProvider = CredentialProviderFactory.getProviders(config).get(0);
    credentialProvider.createCredentialEntry(
        "fs.gs.proxy.password", "jceks-proxy-password".toCharArray());
    credentialProvider.flush();

    Map<String, String> mapped = AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.");

    assertThat(mapped.get("fs.gs." + AnalyticsCoreConfigMapper.PROXY_PASSWORD_KEY))
        .isEqualTo("jceks-proxy-password");
  }

  @Test
  public void mapConfigs_mapsHttpTimeoutsInMillis() {
    Configuration config = new Configuration();
    config.set("fs.gs.http.connect-timeout", "30s");
    config.set("fs.gs.http.read-timeout", "0");

    Map<String, String> mapped = AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.");

    assertThat(mapped)
        .containsAtLeast(
            "fs.gs." + AnalyticsCoreConfigMapper.HTTP_CONNECT_TIMEOUT_KEY,
            "30000",
            "fs.gs." + AnalyticsCoreConfigMapper.HTTP_READ_TIMEOUT_KEY,
            "0");
  }

  @Test
  public void mapConfigs_transportSettingsUnset_omitsAnalyticsCoreKeys() {
    Configuration config = new Configuration();

    Map<String, String> mapped = AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.");

    assertThat(mapped.keySet())
        .containsNoneOf(
            "fs.gs." + AnalyticsCoreConfigMapper.SERVICE_HOST_KEY,
            "fs.gs." + AnalyticsCoreConfigMapper.UNIVERSE_DOMAIN_KEY,
            "fs.gs." + AnalyticsCoreConfigMapper.PROXY_ADDRESS_KEY,
            "fs.gs." + AnalyticsCoreConfigMapper.PROXY_USERNAME_KEY,
            "fs.gs." + AnalyticsCoreConfigMapper.PROXY_PASSWORD_KEY);
  }

  @Test
  public void mapConfigs_removesConnectorSpelledAuthAndProxyKeys() {
    Configuration config = createTransportConfiguration();
    config.set("fs.gs.auth.type", "SERVICE_ACCOUNT_JSON_KEYFILE");
    config.set("fs.gs.auth.service.account.json.keyfile", "/path/to/key.json");
    config.set("fs.gs.auth.impersonation.service.account", "sa@proj.iam.gserviceaccount.com");
    config.set("fs.gs.token.server.url", "https://token.example/token");

    Map<String, String> mapped = AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.");

    assertThat(mapped.keySet())
        .containsNoneOf(
            GoogleHadoopFileSystemConfiguration.GCS_UNIVERSE_DOMAIN.getKey(),
            "fs.gs.proxy.address",
            "fs.gs.proxy.username",
            "fs.gs.proxy.password",
            "fs.gs.auth.type",
            "fs.gs.auth.service.account.json.keyfile",
            "fs.gs.auth.impersonation.service.account",
            "fs.gs.token.server.url");
  }

  @Test
  public void mapConfigs_mapsServiceAccountKeyfileAuth() {
    Configuration config = new Configuration();
    config.set("fs.gs.auth.type", "SERVICE_ACCOUNT_JSON_KEYFILE");
    config.set("fs.gs.auth.service.account.json.keyfile", "/path/to/sa.json");
    config.set("fs.gs.token.server.url", "https://token.example/token");

    Map<String, String> mapped = AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.");

    assertThat(mapped)
        .containsAtLeast(
            "fs.gs." + AnalyticsCoreConfigMapper.AUTH_TYPE_KEY,
            "SERVICE_ACCOUNT_JSON_KEYFILE",
            "fs.gs." + AnalyticsCoreConfigMapper.SERVICE_ACCOUNT_JSON_KEYFILE_KEY,
            "/path/to/sa.json",
            "fs.gs." + AnalyticsCoreConfigMapper.TOKEN_SERVER_URI_KEY,
            "https://token.example/token");
  }

  @Test
  public void mapConfigs_mapsWorkloadIdentityFederationAuthAndRenamesEnumValue() {
    Configuration config = new Configuration();
    config.set("fs.gs.auth.type", "WORKLOAD_IDENTITY_FEDERATION_CREDENTIAL_CONFIG_FILE");
    config.set("fs.gs.auth.workload.identity.federation.credential.config.file", "/path/wif.json");

    Map<String, String> mapped = AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.");

    assertThat(mapped)
        .containsAtLeast(
            "fs.gs." + AnalyticsCoreConfigMapper.AUTH_TYPE_KEY,
            "WORKLOAD_IDENTITY_FEDERATION",
            "fs.gs." + AnalyticsCoreConfigMapper.WORKLOAD_IDENTITY_CREDENTIAL_CONFIG_FILE_KEY,
            "/path/wif.json");
  }

  @Test
  public void mapConfigs_mapsUserCredentialsFromCredentialProvider() throws Exception {
    Configuration config = new Configuration();
    config.set("fs.gs.auth.type", "USER_CREDENTIALS");
    config.set("fs.gs.auth.client.id", "my-client-id");
    config.set("fs.gs.token.server.url", "https://token.example/token");
    config.set(
        HADOOP_SECURITY_CREDENTIAL_PROVIDER_PATH,
        "jceks://file"
            + tempFolder.getRoot().toPath().resolve("user-creds.jceks").toUri().getPath());
    CredentialProvider credentialProvider = CredentialProviderFactory.getProviders(config).get(0);
    credentialProvider.createCredentialEntry(
        "fs.gs.auth.client.secret", "jceks-client-secret".toCharArray());
    credentialProvider.createCredentialEntry(
        "fs.gs.auth.refresh.token", "jceks-refresh-token".toCharArray());
    credentialProvider.flush();

    Map<String, String> mapped = AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.");

    assertThat(mapped)
        .containsAtLeast(
            "fs.gs." + AnalyticsCoreConfigMapper.AUTH_TYPE_KEY,
            "USER_CREDENTIALS",
            "fs.gs." + AnalyticsCoreConfigMapper.CLIENT_ID_KEY,
            "my-client-id",
            "fs.gs." + AnalyticsCoreConfigMapper.CLIENT_SECRET_KEY,
            "jceks-client-secret",
            "fs.gs." + AnalyticsCoreConfigMapper.REFRESH_TOKEN_KEY,
            "jceks-refresh-token",
            "fs.gs." + AnalyticsCoreConfigMapper.TOKEN_SERVER_URI_KEY,
            "https://token.example/token");
  }

  @Test
  public void mapConfigs_mapsTokenServerUrlForApplicationDefault() {
    Configuration config = new Configuration();
    config.set("fs.gs.auth.type", "APPLICATION_DEFAULT");
    config.set("fs.gs.token.server.url", "https://token.example/token");

    Map<String, String> mapped = AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.");

    assertThat(mapped.get("fs.gs." + AnalyticsCoreConfigMapper.TOKEN_SERVER_URI_KEY))
        .isEqualTo("https://token.example/token");
  }

  @Test
  public void mapConfigs_inactiveAuthTypeKeys_areNotForwarded() {
    Configuration config = new Configuration();
    config.set("fs.gs.auth.type", "COMPUTE_ENGINE");
    config.set("fs.gs.auth.service.account.json.keyfile", "/path/to/sa.json");
    config.set("fs.gs.auth.client.id", "my-client-id");
    config.set("fs.gs.auth.client.secret", "my-client-secret");
    config.set("fs.gs.auth.refresh.token", "my-refresh-token");
    config.set("fs.gs.token.server.url", "https://token.example/token");

    Map<String, String> mapped = AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.");

    assertThat(mapped.keySet())
        .containsNoneOf(
            "fs.gs." + AnalyticsCoreConfigMapper.SERVICE_ACCOUNT_JSON_KEYFILE_KEY,
            "fs.gs." + AnalyticsCoreConfigMapper.CLIENT_ID_KEY,
            "fs.gs." + AnalyticsCoreConfigMapper.CLIENT_SECRET_KEY,
            "fs.gs." + AnalyticsCoreConfigMapper.REFRESH_TOKEN_KEY,
            "fs.gs." + AnalyticsCoreConfigMapper.TOKEN_SERVER_URI_KEY);
  }

  @Test
  public void mapConfigs_resolvesPerUserImpersonationIntoTargetPrincipal() throws Exception {
    Configuration config = new Configuration();
    String currentUser = UserGroupInformation.getCurrentUser().getShortUserName();
    config.set(
        "fs.gs.auth.impersonation.service.account.for.user." + currentUser,
        "user-sa@proj.iam.gserviceaccount.com");
    config.set(
        "fs.gs.auth.impersonation.service.account", "static-sa@proj.iam.gserviceaccount.com");

    Map<String, String> mapped = AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.");

    assertThat(mapped.get("fs.gs." + AnalyticsCoreConfigMapper.IMPERSONATION_SERVICE_ACCOUNT_KEY))
        .isEqualTo("user-sa@proj.iam.gserviceaccount.com");
  }

  @Test
  public void mapConfigs_fallsBackToGoogleCloudPrefixWhenFsGsUnset() {
    Configuration config = new Configuration();
    config.set("google.cloud.auth.type", "APPLICATION_DEFAULT");
    config.set("google.cloud.proxy.address", "base-proxy:8080");
    config.set("google.cloud.http.read-timeout", "20s");

    Map<String, String> mapped = AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.");

    assertThat(mapped)
        .containsAtLeast(
            "fs.gs." + AnalyticsCoreConfigMapper.AUTH_TYPE_KEY,
            "APPLICATION_DEFAULT",
            "fs.gs." + AnalyticsCoreConfigMapper.PROXY_ADDRESS_KEY,
            "base-proxy:8080",
            "fs.gs." + AnalyticsCoreConfigMapper.HTTP_READ_TIMEOUT_KEY,
            "20000");
  }

  @Test
  public void mapConfigs_connectorKeySet_overridesAnalyticsCoreSpelledKeys() {
    Configuration config = new Configuration();
    config.set("fs.gs.auth.type", "UNAUTHENTICATED");
    config.set("fs.gs.proxy.address", "connector-proxy:3128");
    config.set("fs.gs.http.read-timeout", "10s");
    config.set("fs.gs." + AnalyticsCoreConfigMapper.AUTH_TYPE_KEY, "SERVICE_ACCOUNT_JSON_KEYFILE");
    config.set("fs.gs." + AnalyticsCoreConfigMapper.PROXY_ADDRESS_KEY, "analytics-core-proxy:9090");
    config.set("fs.gs." + AnalyticsCoreConfigMapper.HTTP_READ_TIMEOUT_KEY, "1");

    Map<String, String> mapped = AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.");

    assertThat(mapped)
        .containsAtLeast(
            "fs.gs." + AnalyticsCoreConfigMapper.AUTH_TYPE_KEY,
            "UNAUTHENTICATED",
            "fs.gs." + AnalyticsCoreConfigMapper.PROXY_ADDRESS_KEY,
            "connector-proxy:3128",
            "fs.gs." + AnalyticsCoreConfigMapper.HTTP_READ_TIMEOUT_KEY,
            "10000");
  }

  @Test
  public void mapConfigs_analyticsCoreSpelledAuthKeysSetDirectly_areDropped() {
    Configuration config = new Configuration();
    config.set("fs.gs.auth.type", "ACCESS_TOKEN_PROVIDER");
    config.set("fs.gs." + AnalyticsCoreConfigMapper.AUTH_TYPE_KEY, "SERVICE_ACCOUNT_JSON_KEYFILE");
    config.set(
        "fs.gs." + AnalyticsCoreConfigMapper.SERVICE_ACCOUNT_JSON_KEYFILE_KEY, "/path/to/sa.json");
    config.set("fs.gs." + AnalyticsCoreConfigMapper.PROXY_PASSWORD_KEY, "raw-proxy-password");

    Map<String, String> mapped = AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.");

    assertThat(mapped.keySet())
        .containsNoneOf(
            "fs.gs." + AnalyticsCoreConfigMapper.AUTH_TYPE_KEY,
            "fs.gs." + AnalyticsCoreConfigMapper.SERVICE_ACCOUNT_JSON_KEYFILE_KEY,
            "fs.gs." + AnalyticsCoreConfigMapper.PROXY_PASSWORD_KEY);
  }

  @Test
  public void mapConfigs_accessTokenProvider_omitsAnalyticsCoreIdentityKeys() {
    Configuration config = new Configuration();
    config.set("fs.gs.auth.type", "ACCESS_TOKEN_PROVIDER");
    config.set("fs.gs.auth.impersonation.service.account", "sa@proj.iam.gserviceaccount.com");
    config.set("fs.gs.token.server.url", "https://token.example/token");

    Map<String, String> mapped = AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.");

    assertThat(mapped.keySet())
        .containsNoneOf(
            "fs.gs." + AnalyticsCoreConfigMapper.AUTH_TYPE_KEY,
            "fs.gs." + AnalyticsCoreConfigMapper.IMPERSONATION_SERVICE_ACCOUNT_KEY,
            "fs.gs." + AnalyticsCoreConfigMapper.TOKEN_SERVER_URI_KEY);
  }

  @Test
  public void mapConfigs_delegationTokenBindingSet_omitsAnalyticsCoreIdentityKeys() {
    Configuration config = new Configuration();
    config.set(
        GoogleHadoopFileSystemConfiguration.DELEGATION_TOKEN_BINDING_CLASS.getKey(),
        TestDelegationTokenBindingImpl.class.getName());
    config.set("fs.gs.auth.impersonation.service.account", "sa@proj.iam.gserviceaccount.com");

    Map<String, String> mapped = AnalyticsCoreConfigMapper.mapConfigs(config, "fs.gs.");

    assertThat(mapped.keySet())
        .containsNoneOf(
            "fs.gs." + AnalyticsCoreConfigMapper.AUTH_TYPE_KEY,
            "fs.gs." + AnalyticsCoreConfigMapper.IMPERSONATION_SERVICE_ACCOUNT_KEY);
  }

  private static Configuration createTransportConfiguration() {
    Configuration config = new Configuration();
    config.set(
        GoogleHadoopFileSystemConfiguration.GCS_UNIVERSE_DOMAIN.getKey(), "my-universe.test");
    config.set("fs.gs.proxy.address", "proxy-host:1234");
    config.set("fs.gs.proxy.username", "proxy-user");
    config.set("fs.gs.proxy.password", "proxy-password");
    return config;
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
}
