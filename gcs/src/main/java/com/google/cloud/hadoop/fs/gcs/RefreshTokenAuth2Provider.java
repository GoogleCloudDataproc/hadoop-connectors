/*
 * Copyright 2022 Google Inc. All Rights Reserved.
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
package com.google.cloud.hadoop.fs.gcs;

import static com.google.api.client.util.Preconditions.checkNotNull;
import static com.google.common.flogger.LazyArgs.lazy;

import com.google.api.client.auth.oauth2.ClientParametersAuthentication;
import com.google.api.client.auth.oauth2.RefreshTokenRequest;
import com.google.api.client.auth.oauth2.TokenRequest;
import com.google.api.client.auth.oauth2.TokenResponse;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.util.AccessTokenProvider;
import com.google.cloud.hadoop.util.HttpTransportFactory;
import com.google.cloud.hadoop.util.RedactedString;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;

/**
 * Retrieve an access token using the OAuth2 refresh token grant flow. See <a
 * href="https://datatracker.ietf.org/doc/html/rfc6749#section-1.5">RFC 6749</a>.
 */
public class RefreshTokenAuth2Provider implements AccessTokenProvider {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();
  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

  private static final DateTimeFormatter dateFormat =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneId.systemDefault());
  private static final AccessToken EXPIRED_TOKEN = new AccessToken("", -1L);

  private Configuration config;
  private Optional<AccessToken> accessToken = Optional.empty();
  private HttpTransport httpTransport;
  private Optional<RedactedString> previousRefreshToken = Optional.empty();

  @Override
  public AccessToken getAccessToken() {
    if (!accessToken.isPresent()) {
      refresh();
    }
    return accessToken.get();
  }

  @Override
  public void refresh() {
    logger.atFine().log(
        "Refreshing access-token based token. Our token is set to expire at '%s' and it is now '%s'",
        lazy(
            () ->
                dateFormat.format(
                    Instant.ofEpochMilli(
                        accessToken.orElse(EXPIRED_TOKEN).getExpirationTimeMilliSeconds()))),
        lazy(() -> dateFormat.format(Instant.now())));

    GoogleCloudStorageOptions gcsOptions =
        GoogleHadoopFileSystemConfiguration.getGcsOptionsBuilder(config).build();
    String tokenServerUrl = gcsOptions.getTokenServerUrl();
    RedactedString refreshToken = gcsOptions.getRefreshToken();
    String clientId = gcsOptions.getClientId();
    RedactedString clientSecret = gcsOptions.getClientSecret();
    checkNotNull(refreshToken, "Must provide a refresh token");
    checkNotNull(clientSecret, "Must provide a client secret");

    logger.atFine().log(
        "Refresh token calling endpoint '%s' with client id '%s'", tokenServerUrl, clientId);

    try {
      HttpTransport httpTransport = getTransport();
      accessToken =
          Optional.of(
              getAccessToken(
                  tokenServerUrl,
                  clientId,
                  clientSecret,
                  previousRefreshToken.orElse(refreshToken),
                  httpTransport));

      logger.atFine().log(
          "New access token expires at '%s'",
          lazy(
              () ->
                  dateFormat.format(
                      Instant.ofEpochMilli(accessToken.get().getExpirationTimeMilliSeconds()))));

    } catch (IOException e) {
      logger.atSevere().withCause(e).log("Couldn't refresh token");
    }
  }

  private AccessTokenProvider.AccessToken getAccessToken(
      String tokenServerUrl,
      String clientId,
      RedactedString clientSecret,
      RedactedString refreshToken,
      HttpTransport httpTransport)
      throws IOException {

    logger.atFine().log("Get a new access token using the refresh token grant flow");

    TokenRequest request =
        new RefreshTokenRequest(
                httpTransport, JSON_FACTORY, new GenericUrl(tokenServerUrl), refreshToken.value())
            .setClientAuthentication(
                new ClientParametersAuthentication(clientId, clientSecret.value()));

    TokenResponse tokenResponse = request.execute();
    previousRefreshToken =
        Optional.ofNullable(RedactedString.create(tokenResponse.getRefreshToken()));

    long expirationTimeMilliSeconds;
    if (tokenResponse.getExpiresInSeconds() != null) {
      expirationTimeMilliSeconds =
          System.currentTimeMillis() + (tokenResponse.getExpiresInSeconds() * 1000L);
    } else {
      logger.atWarning().log(
          "The OAuth2 provider has returned an access token without a defined expiration (ie `expires_in` was null). "
              + "We will consider the access token as expired.");
      expirationTimeMilliSeconds = 0L;
    }
    return new AccessToken(tokenResponse.getAccessToken(), expirationTimeMilliSeconds);
  }

  @Override
  public Configuration getConf() {
    return config;
  }

  @Override
  public void setConf(Configuration config) {
    this.config = config;
  }

  private HttpTransport getTransport() throws IOException {
    if (httpTransport == null) {
      GoogleCloudStorageOptions gcsOptions =
          GoogleHadoopFileSystemConfiguration.getGcsOptionsBuilder(config).build();
      String proxyAddress = gcsOptions.getProxyAddress();
      RedactedString proxyUsername = gcsOptions.getProxyUsername();
      RedactedString proxyPassword = gcsOptions.getProxyPassword();
      logger.atFine().log("Proxy setup: '%s' with username = '%s'", proxyAddress, proxyUsername);
      httpTransport =
          HttpTransportFactory.createHttpTransport(proxyAddress, proxyUsername, proxyPassword);
    }
    return httpTransport;
  }

  @VisibleForTesting
  void setTransport(HttpTransport httpTransport) {
    this.httpTransport = httpTransport;
  }

  @VisibleForTesting
  Optional<RedactedString> getPreviousRefreshToken() {
    return previousRefreshToken;
  }
}
