/*
 * Copyright 2014 Google Inc. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;

/**
 * The Hadoop credentials configuration.
 *
 * <p>When reading configuration this class makes use of a list of key prefixes that are each
 * applied to key suffixes to create a complete configuration key. There is a base prefix of
 * 'google.cloud.' that is included by the builder for each configuration key suffix. When
 * constructing, other prefixes can be specified. Prefixes specified later can be used to override
 * the values of previously set values. In this way a set of global credentials can be specified for
 * most connectors with an override specified for any connectors that need different credentials.
 */
public class HadoopCredentialsConfiguration {

  /**
   * All instances constructed using the builder will use {@code google.cloud} as the first prefix
   * checked. Other prefixes can be added and will override values in the google.cloud prefix.
   */
  public static final String BASE_KEY_PREFIX = "google.cloud";

  /**
   * Key suffix for enabling GCE service account authentication. A value of {@code false} will
   * disable the use of the service accounts for authentication. The default value is {@code true} -
   * use a service account for authentication.
   */
  public static final HadoopConfigurationProperty<Boolean> ENABLE_SERVICE_ACCOUNTS_SUFFIX =
      new HadoopConfigurationProperty<>(
          ".auth.service.account.enable",
          CredentialsOptions.SERVICE_ACCOUNT_ENABLED_DEFAULT,
          ".enable.service.account.auth");

  /**
   * Key suffix used to indicate the path to a JSON file containing a Service Account key and
   * identifier (email). Technically, this could be a JSON containing a non-service account user,
   * but this setting is only used in the service account flow and is namespaced as such.
   */
  public static final HadoopConfigurationProperty<String> SERVICE_ACCOUNT_JSON_KEYFILE_SUFFIX =
      new HadoopConfigurationProperty<>(".auth.service.account.json.keyfile");

  /**
   * The key suffix allowing null to be returned from credentials creation to allow unauthenticated
   * access.
   */
  public static final HadoopConfigurationProperty<Boolean> ENABLE_NULL_CREDENTIALS_SUFFIX =
      new HadoopConfigurationProperty<>(
          ".auth.null.enable", CredentialsOptions.NULL_CREDENTIALS_ENABLED_DEFAULT);

  /** Configuration key for setting a token server URL to use to refresh OAuth token. */
  public static final HadoopConfigurationProperty<String> TOKEN_SERVER_URL_SUFFIX =
      new HadoopConfigurationProperty<>(".token.server.url");

  /**
   * Configuration key for setting a proxy for the connector to use to connect to GCS. The proxy
   * must be an HTTP proxy of the form "host:port".
   */
  public static final HadoopConfigurationProperty<String> PROXY_ADDRESS_SUFFIX =
      new HadoopConfigurationProperty<>(".proxy.address");

  /**
   * Configuration key for setting a proxy username for the connector to use to authenticate with
   * proxy used to connect to GCS.
   */
  public static final HadoopConfigurationProperty<String> PROXY_USERNAME_SUFFIX =
      new HadoopConfigurationProperty<>(".proxy.username");

  /**
   * Configuration key for setting a proxy password for the connector to use to authenticate with
   * proxy used to connect to GCS.
   */
  public static final HadoopConfigurationProperty<String> PROXY_PASSWORD_SUFFIX =
      new HadoopConfigurationProperty<>(".proxy.password");

  /** Configuration key for the name of the AccessTokenProvider to use to generate AccessTokens. */
  public static final HadoopConfigurationProperty<Class<? extends AccessTokenProvider>>
      ACCESS_TOKEN_PROVIDER_IMPL_SUFFIX =
          new HadoopConfigurationProperty<>(".auth.access.token.provider.impl");

  /**
   * Key suffix specifying the impersonating service account with which to call GCS API to get
   * access token.
   */
  public static final HadoopConfigurationProperty<String> IMPERSONATION_SERVICE_ACCOUNT_SUFFIX =
      new HadoopConfigurationProperty<>(".auth.impersonation.service.account");

  /**
   * Key prefix for the user identifier associated with the service account to impersonate when
   * accessing GCS.
   */
  public static final HadoopConfigurationProperty<Map<String, String>>
      USER_IMPERSONATION_SERVICE_ACCOUNT_SUFFIX =
          new HadoopConfigurationProperty<>(
              ".auth.impersonation.service.account.for.user.", ImmutableMap.of());

  /**
   * Key prefix for the group identifier associated with the service account to impersonate when
   * accessing GCS.
   */
  public static final HadoopConfigurationProperty<Map<String, String>>
      GROUP_IMPERSONATION_SERVICE_ACCOUNT_SUFFIX =
          new HadoopConfigurationProperty<>(
              ".auth.impersonation.service.account.for.group.", ImmutableMap.of());

  public static CredentialsFactory getCredentialsFactory(
      Configuration config, String... keyPrefixesVararg) {
    CredentialsOptions credentialsOptions = getCredentialsOptions(config, keyPrefixesVararg);
    return new CredentialsFactory(credentialsOptions);
  }

  @VisibleForTesting
  static CredentialsOptions getCredentialsOptions(
      Configuration config, String... keyPrefixesVararg) {
    List<String> keyPrefixes = getConfigKeyPrefixes(keyPrefixesVararg);
    return CredentialsOptions.builder()
        .setServiceAccountEnabled(
            ENABLE_SERVICE_ACCOUNTS_SUFFIX
                .withPrefixes(keyPrefixes)
                .get(config, config::getBoolean))
        .setServiceAccountJsonKeyFile(
            SERVICE_ACCOUNT_JSON_KEYFILE_SUFFIX.withPrefixes(keyPrefixes).get(config, config::get))
        .setNullCredentialsEnabled(
            ENABLE_NULL_CREDENTIALS_SUFFIX
                .withPrefixes(keyPrefixes)
                .get(config, config::getBoolean))
        .setTokenServerUrl(
            TOKEN_SERVER_URL_SUFFIX.withPrefixes(keyPrefixes).get(config, config::get))
        .setProxyAddress(PROXY_ADDRESS_SUFFIX.withPrefixes(keyPrefixes).get(config, config::get))
        .setProxyUsername(PROXY_USERNAME_SUFFIX.withPrefixes(keyPrefixes).getPassword(config))
        .setProxyPassword(PROXY_PASSWORD_SUFFIX.withPrefixes(keyPrefixes).getPassword(config))
        .build();
  }

  /** Creates an {@link AccessTokenProvider} based on the configuration. */
  public static AccessTokenProvider getAccessTokenProvider(
      Configuration config, List<String> keyPrefixes) throws IOException {
    Class<? extends AccessTokenProvider> clazz =
        getAccessTokenProviderImplClass(config, keyPrefixes.toArray(new String[0]));
    if (clazz == null) {
      return null;
    }
    try {
      return clazz.getDeclaredConstructor().newInstance();
    } catch (ReflectiveOperationException ex) {
      throw new IOException("Can't instantiate " + clazz.getName(), ex);
    }
  }

  public static Class<? extends AccessTokenProvider> getAccessTokenProviderImplClass(
      Configuration config, String... keyPrefixes) {
    return ACCESS_TOKEN_PROVIDER_IMPL_SUFFIX
        .withPrefixes(getConfigKeyPrefixes(keyPrefixes))
        .get(config, (k, d) -> config.getClass(k, d, AccessTokenProvider.class));
  }

  /**
   * Returns full list of config prefixes that will be resolved based on the order in returned list.
   */
  public static ImmutableList<String> getConfigKeyPrefixes(String... keyPrefixes) {
    return ImmutableList.<String>builder().add(keyPrefixes).add(BASE_KEY_PREFIX).build();
  }

  protected HadoopCredentialsConfiguration() {}
}
