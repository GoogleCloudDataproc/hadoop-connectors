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

import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.DELEGATION_TOKEN_BINDING_CLASS;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_CONFIG_PREFIX;
import static com.google.common.base.Strings.isNullOrEmpty;

import com.google.cloud.gcs.analyticscore.client.GcsClientOptions.Protocol;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions.ClientType;
import com.google.cloud.hadoop.util.HadoopConfigurationProperty;
import com.google.cloud.hadoop.util.HadoopCredentialsConfiguration;
import com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.AuthenticationType;
import com.google.cloud.hadoop.util.RedactedString;
import com.google.cloud.hadoop.util.RequesterPaysOptions.RequesterPaysMode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;

/** Maps GCS Hadoop Connector configurations to GCS Analytics Core configurations. */
final class AnalyticsCoreConfigMapper {

  static final String PROJECT_ID_KEY = "project-id";
  static final String USER_PROJECT_KEY = "user-project";
  static final String SERVICE_HOST_KEY = "service.host";
  static final String CLIENT_PROTOCOL_KEY = "client.protocol";
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
  static final String BIDI_WRITE_ENABLED_KEY = "channel.write.bidi.enabled";
  static final String BIDI_WRITE_FINALIZE_ON_CLOSE_KEY = "channel.write.bidi.finalize-on-close";
  static final String ENCRYPTION_KEY_KEY = "encryption-key";
  static final String DECRYPTION_KEY_KEY = "decryption-key";
  static final String CHECKSUM_VALIDATION_ENABLED_KEY = "channel.write.checksum-validation.enabled";
  static final String UNIVERSE_DOMAIN_KEY = "universe-domain";
  static final String AUTH_TYPE_KEY = "analytics-core.auth.type";
  static final String SERVICE_ACCOUNT_JSON_KEYFILE_KEY =
      "analytics-core.auth.service-account-json-keyfile";
  static final String WORKLOAD_IDENTITY_CREDENTIAL_CONFIG_FILE_KEY =
      "analytics-core.auth.workload-identity-federation.credential-config-file";
  static final String CLIENT_ID_KEY = "analytics-core.auth.client-id";
  static final String CLIENT_SECRET_KEY = "analytics-core.auth.client-secret";
  static final String REFRESH_TOKEN_KEY = "analytics-core.auth.refresh-token";
  static final String IMPERSONATION_SERVICE_ACCOUNT_KEY =
      "analytics-core.auth.impersonation-service-account";
  static final String TOKEN_SERVER_URI_KEY = "analytics-core.auth.token-server-uri";
  static final String PROXY_ADDRESS_KEY = "analytics-core.auth.proxy.address";
  static final String PROXY_USERNAME_KEY = "analytics-core.auth.proxy.username";
  static final String PROXY_PASSWORD_KEY = "analytics-core.auth.proxy.password";

  private static final String WORKLOAD_IDENTITY_FEDERATION_VALUE = "WORKLOAD_IDENTITY_FEDERATION";

  /** Auth types whose credentials can use a custom token server URL. */
  private static final ImmutableSet<AuthenticationType> TOKEN_SERVER_URL_AUTH_TYPES =
      Sets.immutableEnumSet(
          AuthenticationType.APPLICATION_DEFAULT,
          AuthenticationType.SERVICE_ACCOUNT_JSON_KEYFILE,
          AuthenticationType.USER_CREDENTIALS);

  private static final ImmutableList<String> KEY_PREFIXES =
      ImmutableList.copyOf(HadoopCredentialsConfiguration.getConfigKeyPrefixes(GCS_CONFIG_PREFIX));

  /**
   * Connector-spelled auth and proxy properties bound to {@code ["fs.gs", "google.cloud"]}.
   *
   * <p>Constructed as fresh instances rather than reusing the shared suffix constants in {@link
   * HadoopCredentialsConfiguration}, because {@code withPrefixes} mutates those shared instances.
   */
  private static final HadoopConfigurationProperty<AuthenticationType> AUTHENTICATION_TYPE =
      new HadoopConfigurationProperty<>(
              HadoopCredentialsConfiguration.AUTHENTICATION_TYPE_SUFFIX.getKey(),
              HadoopCredentialsConfiguration.AUTHENTICATION_TYPE_SUFFIX.getDefault())
          .withPrefixes(KEY_PREFIXES);

  private static final HadoopConfigurationProperty<String> SERVICE_ACCOUNT_JSON_KEYFILE =
      new HadoopConfigurationProperty<String>(
              HadoopCredentialsConfiguration.SERVICE_ACCOUNT_JSON_KEYFILE_SUFFIX.getKey())
          .withPrefixes(KEY_PREFIXES);

  private static final HadoopConfigurationProperty<String>
      WORKLOAD_IDENTITY_FEDERATION_CREDENTIAL_CONFIG_FILE =
          new HadoopConfigurationProperty<String>(
                  HadoopCredentialsConfiguration
                      .WORKLOAD_IDENTITY_FEDERATION_CREDENTIAL_CONFIG_FILE_SUFFIX
                      .getKey())
              .withPrefixes(KEY_PREFIXES);

  private static final HadoopConfigurationProperty<String> AUTH_CLIENT_ID =
      new HadoopConfigurationProperty<String>(
              HadoopCredentialsConfiguration.AUTH_CLIENT_ID_SUFFIX.getKey())
          .withPrefixes(KEY_PREFIXES);

  private static final HadoopConfigurationProperty<RedactedString> AUTH_CLIENT_SECRET =
      new HadoopConfigurationProperty<RedactedString>(
              HadoopCredentialsConfiguration.AUTH_CLIENT_SECRET_SUFFIX.getKey())
          .withPrefixes(KEY_PREFIXES);

  private static final HadoopConfigurationProperty<RedactedString> AUTH_REFRESH_TOKEN =
      new HadoopConfigurationProperty<RedactedString>(
              HadoopCredentialsConfiguration.AUTH_REFRESH_TOKEN_SUFFIX.getKey())
          .withPrefixes(KEY_PREFIXES);

  private static final HadoopConfigurationProperty<String> TOKEN_SERVER_URL =
      new HadoopConfigurationProperty<String>(
              HadoopCredentialsConfiguration.TOKEN_SERVER_URL_SUFFIX.getKey())
          .withPrefixes(KEY_PREFIXES);

  private static final HadoopConfigurationProperty<String> PROXY_ADDRESS =
      new HadoopConfigurationProperty<String>(
              HadoopCredentialsConfiguration.PROXY_ADDRESS_SUFFIX.getKey())
          .withPrefixes(KEY_PREFIXES);

  private static final HadoopConfigurationProperty<RedactedString> PROXY_USERNAME =
      new HadoopConfigurationProperty<RedactedString>(
              HadoopCredentialsConfiguration.PROXY_USERNAME_SUFFIX.getKey())
          .withPrefixes(KEY_PREFIXES);

  private static final HadoopConfigurationProperty<RedactedString> PROXY_PASSWORD =
      new HadoopConfigurationProperty<RedactedString>(
              HadoopCredentialsConfiguration.PROXY_PASSWORD_SUFFIX.getKey())
          .withPrefixes(KEY_PREFIXES);

  private static final ImmutableMap<String, String> HADOOP_TO_ANALYTICS_CORE_KEY_MAPPINGS =
      ImmutableMap.<String, String>builder()
          .put(GoogleHadoopFileSystemConfiguration.GCS_PROJECT_ID.getKey(), PROJECT_ID_KEY)
          .put(GoogleHadoopFileSystemConfiguration.GCS_ROOT_URL.getKey(), SERVICE_HOST_KEY)
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
          .put(
              GoogleHadoopFileSystemConfiguration.GCS_APPENDABLE_OBJECTS_FINALIZE_BEFORE_CLOSE
                  .getKey(),
              BIDI_WRITE_FINALIZE_ON_CLOSE_KEY)
          .put(
              GoogleHadoopFileSystemConfiguration.GCS_WRITE_ROLLING_CHECKSUM_ENABLE.getKey(),
              CHECKSUM_VALIDATION_ENABLED_KEY)
          .put(
              GoogleHadoopFileSystemConfiguration.GCS_UNIVERSE_DOMAIN.getKey(), UNIVERSE_DOMAIN_KEY)
          .build();

  private AnalyticsCoreConfigMapper() {
    // Utility class
  }

  /**
   * Maps configurations from Hadoop Configuration to a map suitable for Analytics Core. If a
   * configuration flag is not set by the user, Analytics Core defaults will be used.
   *
   * <p>Auth and proxy settings are translated from their connector spelling ({@code fs.gs.auth.*},
   * {@code fs.gs.proxy.*}, {@code fs.gs.token.server.url}) to the {@code analytics-core.auth.*}
   * keys, resolving JCEKS secrets and impersonation the same way the connector does. Auth keys set
   * directly in the Analytics Core spelling are ignored.
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

    mapTransportProtocol(config, mappedProperties, prefix);

    mapEncryptionKey(config, mappedProperties, prefix);

    removeSweptAuthKeys(mappedProperties, prefix);
    mapProxyConfigs(config, prefix, mappedProperties);
    mapAuthIdentityConfigs(config, prefix, mappedProperties);

    return mappedProperties;
  }

  /**
   * Maps the connector's transport selection to Analytics Core's {@code client.protocol}, a single
   * transport for every operation issued through Analytics Core:
   *
   * <ul>
   *   <li>{@code fs.gs.client.type=HTTP_API_CLIENT} (default) maps to {@link Protocol#HTTP}.
   *   <li>{@code fs.gs.client.type=STORAGE_CLIENT} maps to {@link Protocol#GRPC}.
   *   <li>{@code fs.gs.client.type=STORAGE_CLIENT} with {@code fs.gs.bidi.enable=true} maps to
   *       {@link Protocol#BIDI} (bidi reads and appendable writes).
   * </ul>
   *
   * <p>{@code fs.gs.grpc.write.enable} is legacy-only: Analytics Core has no per-operation
   * fallback, so appendable writes are enabled iff the protocol is {@link Protocol#BIDI}.
   *
   * <p>Pass-through {@code client.protocol} / {@code channel.write.bidi.enabled} values are kept
   * only when neither {@code fs.gs.client.type} nor {@code fs.gs.bidi.enable} is set.
   */
  private static void mapTransportProtocol(
      Configuration config, Map<String, String> map, String prefix) {
    boolean transportConfigured =
        config.get(GoogleHadoopFileSystemConfiguration.GCS_CLIENT_TYPE.getKey()) != null
            || config.get(
                    GoogleHadoopFileSystemConfiguration.GCS_OPERATION_BIDI_API_ENABLE.getKey())
                != null;

    // Consumed here rather than passed through: the connector's ClientType values are not valid
    // Analytics Core values, and fs.gs.bidi.enable is folded into client.protocol.
    map.remove(GoogleHadoopFileSystemConfiguration.GCS_CLIENT_TYPE.getKey());
    map.remove(GoogleHadoopFileSystemConfiguration.GCS_OPERATION_BIDI_API_ENABLE.getKey());

    String protocolKey = prefix + CLIENT_PROTOCOL_KEY;
    Protocol protocol = toProtocol(config);
    if (transportConfigured || !map.containsKey(protocolKey)) {
      map.put(protocolKey, protocol.name());
    } else {
      protocol = parseProtocol(map.get(protocolKey)).orElse(protocol);
    }

    String bidiWriteKey = prefix + BIDI_WRITE_ENABLED_KEY;
    if (transportConfigured || !map.containsKey(bidiWriteKey)) {
      map.put(bidiWriteKey, String.valueOf(protocol == Protocol.BIDI));
    }
    if (!Boolean.parseBoolean(map.get(bidiWriteKey))) {
      // Only meaningful for appendable writes; drop it so Analytics Core never sees a dangling
      // finalize option for a transport that cannot honor it.
      map.remove(prefix + BIDI_WRITE_FINALIZE_ON_CLOSE_KEY);
    }
  }

  /** Resolves the Analytics Core {@link Protocol} for the connector's transport configuration. */
  @VisibleForTesting
  static Protocol toProtocol(Configuration config) {
    ClientType clientType =
        GoogleHadoopFileSystemConfiguration.GCS_CLIENT_TYPE.get(config, config::getEnum);
    if (clientType != ClientType.STORAGE_CLIENT) {
      return Protocol.HTTP;
    }
    boolean bidiEnabled =
        GoogleHadoopFileSystemConfiguration.GCS_OPERATION_BIDI_API_ENABLE.get(
            config, config::getBoolean);
    return bidiEnabled ? Protocol.BIDI : Protocol.GRPC;
  }

  /** Parses a pass-through {@code client.protocol}; unknown values are left for Analytics Core. */
  private static Optional<Protocol> parseProtocol(String value) {
    if (isNullOrEmpty(value)) {
      return Optional.empty();
    }
    try {
      return Optional.of(Protocol.valueOf(value.trim().toUpperCase(Locale.ROOT)));
    } catch (IllegalArgumentException e) {
      return Optional.empty();
    }
  }

  /**
   * Maps {@code fs.gs.encryption.key} via {@code getPassword} so JCEKS secrets are resolved for
   * both CSEK reads and writes.
   */
  private static void mapEncryptionKey(
      Configuration config, Map<String, String> map, String prefix) {
    map.remove(GoogleHadoopFileSystemConfiguration.GCS_ENCRYPTION_KEY.getKey());
    RedactedString encryptionKey =
        GoogleHadoopFileSystemConfiguration.GCS_ENCRYPTION_KEY.getPassword(config);
    if (encryptionKey != null) {
      map.put(prefix + ENCRYPTION_KEY_KEY, encryptionKey.value());
      map.put(prefix + DECRYPTION_KEY_KEY, encryptionKey.value());
    }
  }

  private static void mapProxyConfigs(
      Configuration config, String prefix, Map<String, String> map) {
    mapStringProperty(config, map, PROXY_ADDRESS, prefix + PROXY_ADDRESS_KEY);
    mapSecretProperty(config, map, PROXY_USERNAME, prefix + PROXY_USERNAME_KEY);
    mapSecretProperty(config, map, PROXY_PASSWORD, prefix + PROXY_PASSWORD_KEY);
  }

  /**
   * Maps the auth type and only the keys that type uses, so secrets of other auth types left in a
   * shared cluster config are not forwarded. {@code ACCESS_TOKEN_PROVIDER} and delegation tokens
   * are Hadoop-only plugins and map to nothing; the connector keeps building those credentials
   * itself.
   */
  private static void mapAuthIdentityConfigs(
      Configuration config, String prefix, Map<String, String> map) {
    AuthenticationType authType = AUTHENTICATION_TYPE.get(config, config::getEnum);
    if (authType == AuthenticationType.ACCESS_TOKEN_PROVIDER
        || !isNullOrEmpty(DELEGATION_TOKEN_BINDING_CLASS.get(config, config::get))) {
      return;
    }
    map.put(prefix + AUTH_TYPE_KEY, toAnalyticsCoreAuthType(authType));

    switch (authType) {
      case SERVICE_ACCOUNT_JSON_KEYFILE:
        mapStringProperty(
            config, map, SERVICE_ACCOUNT_JSON_KEYFILE, prefix + SERVICE_ACCOUNT_JSON_KEYFILE_KEY);
        break;
      case WORKLOAD_IDENTITY_FEDERATION_CREDENTIAL_CONFIG_FILE:
        mapStringProperty(
            config,
            map,
            WORKLOAD_IDENTITY_FEDERATION_CREDENTIAL_CONFIG_FILE,
            prefix + WORKLOAD_IDENTITY_CREDENTIAL_CONFIG_FILE_KEY);
        break;
      case USER_CREDENTIALS:
        mapStringProperty(config, map, AUTH_CLIENT_ID, prefix + CLIENT_ID_KEY);
        mapSecretProperty(config, map, AUTH_CLIENT_SECRET, prefix + CLIENT_SECRET_KEY);
        mapSecretProperty(config, map, AUTH_REFRESH_TOKEN, prefix + REFRESH_TOKEN_KEY);
        break;
      default:
        break;
    }

    mapImpersonationServiceAccount(config, prefix, map);
    if (TOKEN_SERVER_URL_AUTH_TYPES.contains(authType)) {
      mapStringProperty(config, map, TOKEN_SERVER_URL, prefix + TOKEN_SERVER_URI_KEY);
    }
  }

  private static void mapImpersonationServiceAccount(
      Configuration config, String prefix, Map<String, String> map) {
    try {
      Optional<String> impersonationServiceAccount =
          HadoopCredentialsConfiguration.getImpersonationServiceAccount(config, GCS_CONFIG_PREFIX);
      impersonationServiceAccount.ifPresent(
          account -> map.put(prefix + IMPERSONATION_SERVICE_ACCOUNT_KEY, account));
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to resolve impersonation service account", e);
    }
  }

  /**
   * Removes the auth and proxy keys swept in from the Hadoop configuration, in both the connector
   * and the Analytics Core spelling, so that only the keys mapped from connector config remain.
   */
  private static void removeSweptAuthKeys(Map<String, String> map, String prefix) {
    String connectorAuthPrefix = prefix + "auth.";
    String connectorProxyPrefix = prefix + "proxy.";
    String connectorTokenServerUrlKey = prefix + "token.server.url";
    String analyticsCoreAuthPrefix = prefix + "analytics-core.auth.";
    map.keySet()
        .removeIf(
            key ->
                key.startsWith(connectorAuthPrefix)
                    || key.startsWith(connectorProxyPrefix)
                    || key.equals(connectorTokenServerUrlKey)
                    || key.startsWith(analyticsCoreAuthPrefix));
  }

  private static String toAnalyticsCoreAuthType(AuthenticationType authType) {
    if (authType == AuthenticationType.WORKLOAD_IDENTITY_FEDERATION_CREDENTIAL_CONFIG_FILE) {
      return WORKLOAD_IDENTITY_FEDERATION_VALUE;
    }
    return authType.name();
  }

  /** Maps a plain property when it is set to a non-empty value, as the connector treats it. */
  private static void mapStringProperty(
      Configuration config,
      Map<String, String> map,
      HadoopConfigurationProperty<String> property,
      String analyticsCoreKey) {
    String value = property.get(config, config::get);
    if (value != null && !value.isEmpty()) {
      map.put(analyticsCoreKey, value);
    }
  }

  /** Maps a secret via {@code getPassword}, so values held in a JCEKS provider are resolved. */
  private static void mapSecretProperty(
      Configuration config,
      Map<String, String> map,
      HadoopConfigurationProperty<RedactedString> property,
      String analyticsCoreKey) {
    RedactedString secret = property.getPassword(config);
    if (secret != null) {
      map.put(analyticsCoreKey, secret.value());
    }
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
