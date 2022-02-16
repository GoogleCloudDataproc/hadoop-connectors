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
package com.google.cloud.hadoop.util;

import static com.google.api.client.util.Preconditions.checkNotNull;

import static com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.AUTH_CLIENT_ID_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.AUTH_CLIENT_SECRET_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.AUTH_REFRESH_TOKEN_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.PROXY_ADDRESS_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.PROXY_PASSWORD_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.PROXY_USERNAME_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.TOKEN_SERVER_URL_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.getConfigKeyPrefixes;
import static com.google.common.flogger.LazyArgs.lazy;

import com.google.api.client.auth.oauth2.ClientParametersAuthentication;
import com.google.api.client.auth.oauth2.RefreshTokenRequest;
import com.google.api.client.auth.oauth2.TokenRequest;
import com.google.api.client.auth.oauth2.TokenResponse;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;

import com.google.api.client.json.gson.GsonFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.time.Instant;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

/**
 * Retrieve an access token using the OAuth2 refresh token grant flow. See <a
 * href="https://datatracker.ietf.org/doc/html/rfc6749#section-1.5">RFC 6749</a>.
 */
public class RefreshTokenAuth2Provider implements AccessTokenProvider {
  public static final String GCS_CONFIG_PREFIX = "fs.gs";

  public static final List<String> CONFIG_KEY_PREFIXES =
          ImmutableList.copyOf(getConfigKeyPrefixes(GCS_CONFIG_PREFIX));

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();
  private static final JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();

  private static final AccessToken EXPIRED_TOKEN = new AccessToken("", Instant.MIN);

  private String tokenServerUrl;
  private String clientId;
  private RedactedString clientSecret;
  private RedactedString refreshToken;
  private AccessToken accessToken = null;
  private Configuration config;
  private HttpTransport httpTransport;

  @Override
  public AccessToken getAccessToken() {
    if (accessToken == null) {
      accessToken = EXPIRED_TOKEN;
      refresh();
    }
    return accessToken;
  }

  @Override
  public void refresh() {
    logger.atFine().log(
        "Refreshing access token using the refresh token grant flow. Our current token is set to expire at '%s' and it is now '%s'",
        lazy(() -> accessToken.getExpirationTime()),
        lazy(Instant::now));

    tokenServerUrl =
        TOKEN_SERVER_URL_SUFFIX.withPrefixes(CONFIG_KEY_PREFIXES).get(config, config::get);
    refreshToken = AUTH_REFRESH_TOKEN_SUFFIX.withPrefixes(CONFIG_KEY_PREFIXES).getPassword(config);
    clientId = AUTH_CLIENT_ID_SUFFIX.withPrefixes(CONFIG_KEY_PREFIXES).get(config, config::get);
    clientSecret = AUTH_CLIENT_SECRET_SUFFIX.withPrefixes(CONFIG_KEY_PREFIXES).getPassword(config);

    checkNotNull(refreshToken, "Must provide a refresh token");
    checkNotNull(clientSecret, "Must provide a client secret");

    logger.atFine().log(
        "Refresh token provider setup with token server url '%s', refresh token '%s', client id='%s' and client secret '%s'",
        tokenServerUrl, refreshToken, clientId, clientSecret);

    try {
      if (httpTransport == null) {
        String proxyAddress =
            PROXY_ADDRESS_SUFFIX.withPrefixes(CONFIG_KEY_PREFIXES).get(config, config::get);
        RedactedString proxyUsername =
            PROXY_USERNAME_SUFFIX.withPrefixes(CONFIG_KEY_PREFIXES).getPassword(config);
        RedactedString proxyPassword =
            PROXY_PASSWORD_SUFFIX.withPrefixes(CONFIG_KEY_PREFIXES).getPassword(config);
        httpTransport =
            HttpTransportFactory.createHttpTransport(proxyAddress, proxyUsername, proxyPassword);
      }
      TokenRequest request =
          new RefreshTokenRequest(
                  httpTransport, JSON_FACTORY, new GenericUrl(tokenServerUrl), refreshToken.value())
              .setClientAuthentication(
                  new ClientParametersAuthentication(clientId, clientSecret.value()));

      TokenResponse tokenResponse = request.execute();
      if (tokenResponse.getRefreshToken() != null) {
        // Some OAuth2 provider may not issue a new refresh token
        refreshToken = RedactedString.create(tokenResponse.getRefreshToken());
      }

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
      accessToken = new AccessToken(tokenResponse.getAccessToken(), Instant.ofEpochMilli(expirationTimeMilliSeconds));

      logger.atFine().log(
          "New access token expires at '%s'",
          lazy(() -> accessToken.getExpirationTime()));

    } catch (IOException e) {
      logger.atSevere().withCause(e).log("Couldn't refresh token");
    }
  }

  @Override
  public Configuration getConf() {
    return config;
  }

  @Override
  public void setConf(Configuration config) {
    this.config = config;
  }

  @VisibleForTesting
  void setTransport(HttpTransport httpTransport) {
    this.httpTransport = httpTransport;
  }

  @VisibleForTesting
  RedactedString getRefreshToken() {
    return refreshToken;
  }
}
