package com.google.cloud.hadoop.util;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;

import com.google.api.client.auth.oauth2.TokenResponse;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.Clock;
import com.google.api.services.iamcredentials.v1.IAMCredentials;
import com.google.api.services.iamcredentials.v1.IAMCredentials.Projects.ServiceAccounts.GenerateAccessToken;
import com.google.api.services.iamcredentials.v1.model.GenerateAccessTokenRequest;
import com.google.api.services.iamcredentials.v1.model.GenerateAccessTokenResponse;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.time.Instant;

/** A {@code Credential} to generate or refresh IAM access token. */
public class GoogleCredentialWithIamAccessToken extends GoogleCredential {

  private static final String DEFAULT_ACCESS_TOKEN_LIFETIME = "3600s";
  private static final String DEFAULT_SERVICE_ACCOUNT_NAME_PREFIX = "projects/-/serviceAccounts/";
  private static final JsonFactory JSON_FACTORY = new JacksonFactory();

  private final String fullServiceAccountName;
  private final HttpRequestInitializer initializer;
  private final HttpTransport transport;
  private final ImmutableList<String> scopes;
  private final Clock clock;

  public GoogleCredentialWithIamAccessToken(
      String serviceAccountName,
      HttpRequestInitializer initializer,
      HttpTransport transport,
      ImmutableList<String> scopes,
      Clock clock)
      throws IOException {
    this.fullServiceAccountName = DEFAULT_SERVICE_ACCOUNT_NAME_PREFIX + serviceAccountName;
    this.initializer = initializer;
    this.transport = transport;
    this.scopes = scopes;
    this.clock = clock;
    initialize();
  }

  private void initialize() throws IOException {
    GenerateAccessTokenResponse accessTokenResponse = generateAccessToken();
    if (!isNullOrEmpty(accessTokenResponse.getExpireTime())) {
      Instant expirationTimeInInstant = Instant.parse(accessTokenResponse.getExpireTime());
      setExpirationTimeMilliseconds(expirationTimeInInstant.toEpochMilli());
    }
    setAccessToken(accessTokenResponse.getAccessToken());
  }

  @Override
  protected TokenResponse executeRefreshToken() throws IOException {
    GenerateAccessTokenResponse accessTokenResponse = generateAccessToken();
    TokenResponse tokenResponse =
        new TokenResponse().setAccessToken(accessTokenResponse.getAccessToken());

    if (isNullOrEmpty(accessTokenResponse.getExpireTime())) {
      return tokenResponse;
    }

    Instant expirationTimeInInstant = Instant.parse(accessTokenResponse.getExpireTime());
    Long expirationTimeMilliSeconds = expirationTimeInInstant.toEpochMilli();
    return tokenResponse.setExpiresInSeconds(
        expirationTimeMilliSeconds - clock.currentTimeMillis() / 1000);
  }

  private GenerateAccessTokenResponse generateAccessToken() throws IOException {
    IAMCredentials IAMCredentials = new IAMCredentials(transport, JSON_FACTORY, initializer);
    GenerateAccessTokenRequest content = new GenerateAccessTokenRequest();
    content.setScope(scopes);
    content.setLifetime(DEFAULT_ACCESS_TOKEN_LIFETIME);
    GenerateAccessToken generateAccessTokenReq =
        IAMCredentials.projects()
            .serviceAccounts()
            .generateAccessToken(fullServiceAccountName, content);
    GenerateAccessTokenResponse response = generateAccessTokenReq.execute();
    checkNotNull(response.getAccessToken(), "Access Token cannot be null!");
    return response;
  }
}
