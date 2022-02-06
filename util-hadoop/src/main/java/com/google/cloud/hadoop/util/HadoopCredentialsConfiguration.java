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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;

import com.google.api.client.http.HttpTransport;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.auth.oauth2.UserCredentials;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.flogger.GoogleLogger;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

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

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  /**
   * All instances constructed using the builder will use {@code google.cloud} as the first prefix
   * checked. Other prefixes can be added and will override values in the {@code google.cloud}
   * prefix.
   */
  public static final String BASE_KEY_PREFIX = "google.cloud";

  public static final String CLOUD_PLATFORM_SCOPE =
      "https://www.googleapis.com/auth/cloud-platform";

  /**
   * Key suffix for enabling GCE service account authentication. A value of {@code false} will
   * disable the use of the service accounts for authentication. The default value is {@code true} -
   * use a service account for authentication.
   */
  public static final HadoopConfigurationProperty<AuthenticationType> AUTHENTICATION_TYPE_SUFFIX =
      new HadoopConfigurationProperty<>(".auth.type", AuthenticationType.GCE_METADATA_SERVICE);

  /**
   * Key suffix used to indicate the path to a JSON file containing a Service Account key and
   * identifier (email). Technically, this could be a JSON containing a non-service account user,
   * but this setting is only used in the service account flow and is namespaced as such.
   */
  public static final HadoopConfigurationProperty<String> SERVICE_ACCOUNT_JSON_KEYFILE_SUFFIX =
      new HadoopConfigurationProperty<>(".auth.service.account.json.keyfile");

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
      ACCESS_TOKEN_PROVIDER_SUFFIX =
          new HadoopConfigurationProperty<>(".auth.access.token.provider");

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

  /**
   * Get the credentials as configured.
   *
   * <p>The following is the order in which properties are applied to create the Credentials:
   *
   * <ol>
   *   <li>If service accounts are enabled and no service account keyfile or service account
   *       parameters are set, use the metadata service.
   *   <li>If service accounts are enabled and a service-account json keyfile is provided, use
   *       service account authentication.
   *   <li>If service accounts are disabled and client id, client secret and OAuth credentials file
   *       is provided, use the Installed App authentication flow.
   *   <li>If service accounts are disabled and null credentials are enabled for unit testing,
   *       return null
   * </ol>
   *
   * @throws IllegalStateException if none of the above conditions are met and a Credentials cannot
   *     be created
   */
  public static GoogleCredentials getCredentials(Configuration config, String... keyPrefixesVararg)
      throws IOException {
    List<String> keyPrefixes = getConfigKeyPrefixes(keyPrefixesVararg);
    return getCredentials(getHttpTransport(config, keyPrefixes), config, keyPrefixes);
  }

  @VisibleForTesting
  static GoogleCredentials getCredentials(
      Supplier<HttpTransport> transport, Configuration config, List<String> keyPrefixes)
      throws IOException {
    GoogleCredentials credentials = getCredentialsInternal(transport, config, keyPrefixes);
    return credentials == null ? null : configureCredentials(config, keyPrefixes, credentials);
  }

  private static GoogleCredentials getCredentialsInternal(
      Supplier<HttpTransport> transport, Configuration config, List<String> keyPrefixes)
      throws IOException {
    AuthenticationType authenticationType =
        AUTHENTICATION_TYPE_SUFFIX.withPrefixes(keyPrefixes).get(config, config::getEnum);
    switch (authenticationType) {
      case ACCESS_TOKEN_PROVIDER:
        Class<? extends AccessTokenProvider> clazz =
            ACCESS_TOKEN_PROVIDER_SUFFIX
                .withPrefixes(keyPrefixes)
                .get(config, (k, d) -> config.getClass(k, d, AccessTokenProvider.class));
        AccessTokenProvider accessTokenProvider;
        try {
          accessTokenProvider = clazz.getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
          throw new IOException("Can't instantiate " + clazz.getName(), e);
        }
        accessTokenProvider.setConf(config);
        return new AccessTokenProviderCredentials(accessTokenProvider)
            .createScoped(CLOUD_PLATFORM_SCOPE);
      case APPLICATION_DEFAULT:
        return GoogleCredentials.getApplicationDefault(transport::get)
            .createScoped(CLOUD_PLATFORM_SCOPE);
      case GCE_METADATA_SERVICE:
        return ComputeEngineCredentials.newBuilder()
            .setHttpTransportFactory(transport::get)
            .setScopes(ImmutableList.of(CLOUD_PLATFORM_SCOPE))
            .build();
      case SERVICE_ACCOUNT_JSON_KEYFILE:
        String keyFile =
            SERVICE_ACCOUNT_JSON_KEYFILE_SUFFIX.withPrefixes(keyPrefixes).get(config, config::get);
        try (FileInputStream fis = new FileInputStream(keyFile)) {
          return ServiceAccountCredentials.fromStream(fis, transport::get)
              .createScoped(CLOUD_PLATFORM_SCOPE);
        }
      case UNAUTHENTICATED:
        return null;
      default:
        throw new IllegalArgumentException("Unknown authentication type: " + authenticationType);
    }
  }

  /**
   * Generate a {@link GoogleCredentials} from the internal access token provider based on the
   * service account to impersonate.
   */
  public static GoogleCredentials getImpersonatedCredentials(
      Configuration config, GoogleCredentials sourceCredentials, String... keyPrefixesVararg)
      throws IOException {
    List<String> keyPrefixes = getConfigKeyPrefixes(keyPrefixesVararg);
    Map<String, String> userImpersonationServiceAccounts =
        USER_IMPERSONATION_SERVICE_ACCOUNT_SUFFIX
            .withPrefixes(keyPrefixes)
            .getPropsWithPrefix(config);
    Map<String, String> groupImpersonationServiceAccounts =
        GROUP_IMPERSONATION_SERVICE_ACCOUNT_SUFFIX
            .withPrefixes(keyPrefixes)
            .getPropsWithPrefix(config);
    String impersonationServiceAccount =
        IMPERSONATION_SERVICE_ACCOUNT_SUFFIX.withPrefixes(keyPrefixes).get(config, config::get);

    // Exit early if impersonation is not configured
    if (userImpersonationServiceAccounts.isEmpty()
        && groupImpersonationServiceAccounts.isEmpty()
        && isNullOrEmpty(impersonationServiceAccount)) {
      return null;
    }

    checkNotNull(sourceCredentials, "credentials can not be null");
    UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
    Optional<String> serviceAccountToImpersonate =
        Stream.of(
                () ->
                    getServiceAccountToImpersonateForUserGroup(
                        userImpersonationServiceAccounts,
                        ImmutableList.of(currentUser.getShortUserName())),
                () ->
                    getServiceAccountToImpersonateForUserGroup(
                        groupImpersonationServiceAccounts,
                        ImmutableList.copyOf(currentUser.getGroupNames())),
                (Supplier<Optional<String>>) () -> Optional.ofNullable(impersonationServiceAccount))
            .map(Supplier::get)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .filter(sa -> !isNullOrEmpty(sa))
            .findFirst();

    if (serviceAccountToImpersonate.isPresent()) {
      Supplier<HttpTransport> transport = getHttpTransport(config, keyPrefixes);
      ImpersonatedCredentials impersonatedCredentials =
          ImpersonatedCredentials.newBuilder()
              .setSourceCredentials(sourceCredentials)
              .setTargetPrincipal(serviceAccountToImpersonate.get())
              .setScopes(ImmutableList.of(CLOUD_PLATFORM_SCOPE))
              .setHttpTransportFactory(transport::get)
              .build();
      logger.atFine().log(
          "Impersonating '%s' service account for '%s' user",
          serviceAccountToImpersonate.get(), currentUser);
      return impersonatedCredentials;
    }

    return null;
  }

  private static Optional<String> getServiceAccountToImpersonateForUserGroup(
      Map<String, String> serviceAccountMapping, List<String> userGroups) {
    return serviceAccountMapping.entrySet().stream()
        .filter(e -> userGroups.contains(e.getKey()))
        .map(Map.Entry::getValue)
        .findFirst();
  }

  /**
   * Returns full list of config prefixes that will be resolved based on the order in returned list.
   */
  public static List<String> getConfigKeyPrefixes(String... keyPrefixes) {
    return ImmutableList.<String>builder().add(keyPrefixes).add(BASE_KEY_PREFIX).build();
  }

  private static Supplier<HttpTransport> getHttpTransport(
      Configuration config, List<String> keyPrefixes) {
    return Suppliers.memoize(
        () -> {
          try {
            return HttpTransportFactory.createHttpTransport(
                PROXY_ADDRESS_SUFFIX.withPrefixes(keyPrefixes).get(config, config::get),
                PROXY_USERNAME_SUFFIX.withPrefixes(keyPrefixes).getPassword(config),
                PROXY_PASSWORD_SUFFIX.withPrefixes(keyPrefixes).getPassword(config));
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        });
  }

  private static GoogleCredentials configureCredentials(
      Configuration config, List<String> keyPrefixes, GoogleCredentials credentials) {
    String tokenServerUrl =
        TOKEN_SERVER_URL_SUFFIX.withPrefixes(keyPrefixes).get(config, config::get);
    if (tokenServerUrl == null) {
      return credentials;
    }
    if (credentials instanceof ServiceAccountCredentials) {
      return ((ServiceAccountCredentials) credentials)
          .toBuilder().setTokenServerUri(URI.create(tokenServerUrl)).build();
    }
    if (credentials instanceof UserCredentials) {
      return ((UserCredentials) credentials)
          .toBuilder().setTokenServerUri(URI.create(tokenServerUrl)).build();
    }
    return credentials;
  }

  public static final class AccessTokenProviderCredentials extends GoogleCredentials {
    private final AccessTokenProvider accessTokenProvider;

    public AccessTokenProviderCredentials(AccessTokenProvider accessTokenProvider) {
      super(convertAccessToken(accessTokenProvider.getAccessToken()));
      this.accessTokenProvider = accessTokenProvider;
    }

    private static AccessToken convertAccessToken(AccessTokenProvider.AccessToken accessToken) {
      checkNotNull(accessToken, "AccessToken cannot be null!");
      String token = checkNotNull(accessToken.getToken(), "AccessToken value cannot be null!");
      Instant expirationTime = accessToken.getExpirationTime();
      return new AccessToken(token, expirationTime == null ? null : Date.from(expirationTime));
    }

    public AccessTokenProvider getAccessTokenProvider() {
      return accessTokenProvider;
    }

    @Override
    public AccessToken refreshAccessToken() throws IOException {
      accessTokenProvider.refresh();
      return convertAccessToken(accessTokenProvider.getAccessToken());
    }
  }

  public enum AuthenticationType {
    ACCESS_TOKEN_PROVIDER,
    APPLICATION_DEFAULT,
    GCE_METADATA_SERVICE,
    SERVICE_ACCOUNT_JSON_KEYFILE,
    UNAUTHENTICATED,
  }

  protected HadoopCredentialsConfiguration() {}
}