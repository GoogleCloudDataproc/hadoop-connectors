/*
 * Copyright 2013 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.hadoop.util;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.client.http.HttpTransport;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.auth.oauth2.UserCredentials;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.common.flogger.GoogleLogger;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.time.Instant;
import java.util.Date;
import java.util.Map;
import java.util.function.Supplier;

/** Miscellaneous helper methods for getting a {@code Credentials} from various sources. */
public class CredentialsFactory {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  public static final String CLOUD_PLATFORM_SCOPE =
      "https://www.googleapis.com/auth/cloud-platform";

  private final CredentialsOptions options;

  private final Iterable<Map.Entry<String, String>> config;

  private final Supplier<HttpTransport> transport;

  public CredentialsFactory(
      CredentialsOptions options, Iterable<Map.Entry<String, String>> config) {
    this(
        options,
        config,
        Suppliers.memoize(
            () -> {
              try {
                return HttpTransportFactory.createHttpTransport(
                    options.getProxyAddress(),
                    options.getProxyUsername(),
                    options.getProxyPassword());
              } catch (IOException e) {
                throw new UncheckedIOException(e);
              }
            }));
  }

  @VisibleForTesting
  CredentialsFactory(
      CredentialsOptions options,
      Iterable<Map.Entry<String, String>> config,
      Supplier<HttpTransport> transport) {
    this.options = options;
    this.config = checkNotNull(config, "config can not be null");
    this.transport = transport;
  }

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
  public GoogleCredentials getCredentials() throws IOException {
    GoogleCredentials credentials = getCredentialsInternal();
    return credentials == null ? null : configureCredentials(credentials);
  }

  private GoogleCredentials getCredentialsInternal() throws IOException {
    switch (options.getAuthenticationType()) {
      case ACCESS_TOKEN_PROVIDER:
        Class<? extends AccessTokenProvider> clazz = options.getAccessTokenProviderClass();
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
        logger.atFine().log("getApplicationDefaultCredentials()");
        return GoogleCredentials.getApplicationDefault(transport::get);
      case GCE_METADATA_SERVICE:
        logger.atFine().log("Getting service account credentials from metadata service.");
        return ComputeEngineCredentials.newBuilder()
            .setHttpTransportFactory(transport::get)
            .build();
      case SERVICE_ACCOUNT_JSON_KEYFILE:
        logger.atFine().log(
            "getCredentialsFromJsonKeyFile() from '%s'", options.getServiceAccountJsonKeyFile());
        try (FileInputStream fis = new FileInputStream(options.getServiceAccountJsonKeyFile())) {
          return ServiceAccountCredentials.fromStream(fis, transport::get)
              .createScoped(CLOUD_PLATFORM_SCOPE);
        }
      case UNAUTHENTICATED:
        return null;
      default:
        throw new IllegalArgumentException(
            "Unknown authentication type: " + options.getAuthenticationType());
    }
  }

  private GoogleCredentials configureCredentials(GoogleCredentials credentials) {
    if (options.getTokenServerUrl() == null) {
      return credentials;
    }
    if (credentials instanceof ServiceAccountCredentials) {
      return ((ServiceAccountCredentials) credentials)
          .toBuilder().setTokenServerUri(URI.create(options.getTokenServerUrl())).build();
    }
    if (credentials instanceof UserCredentials) {
      return ((UserCredentials) credentials)
          .toBuilder().setTokenServerUri(URI.create(options.getTokenServerUrl())).build();
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
}
