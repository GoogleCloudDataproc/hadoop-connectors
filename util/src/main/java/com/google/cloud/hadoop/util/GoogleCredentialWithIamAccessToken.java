package com.google.cloud.hadoop.util;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.client.auth.oauth2.TokenResponse;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.Clock;
import com.google.api.services.iamcredentials.v1.IAMCredentials;
import com.google.api.services.iamcredentials.v1.model.GenerateAccessTokenRequest;
import com.google.api.services.iamcredentials.v1.model.GenerateAccessTokenResponse;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.time.Instant;

public class GoogleCredentialWithIamAccessToken extends GoogleCredential {
  private final String serviceAccountName;
  private final HttpRequestInitializer initializer;
  private final HttpTransport transport;
  private final Clock clock;

  private static final String DEFAULT_ACCESS_TOKEN_LIFETIME = "3600s";
  private static final String DEFAULT_SERVICE_ACCOUNT_NAME_PREFIX = "projects/-/serviceAccounts/";

  public GoogleCredentialWithIamAccessToken(
      String serviceAccountName,
      HttpRequestInitializer initializer,
      HttpTransport transport,
      Clock clock)
      throws IOException, GeneralSecurityException {
    this.serviceAccountName = serviceAccountName;
    this.initializer = initializer;
    this.transport = transport;
    this.clock = clock;
    initialize();
  }

  private void initialize() throws IOException, GeneralSecurityException {
    GenerateAccessTokenResponse accessTokenResponse = generateAccessToken();
    Instant expirationTimeInInstant = Instant.parse(accessTokenResponse.getExpireTime());
    setAccessToken(accessTokenResponse.getAccessToken());
    setExpirationTimeMilliseconds(expirationTimeInInstant.toEpochMilli());
  }

  @Override
  protected TokenResponse executeRefreshToken() throws IOException {
    GenerateAccessTokenResponse accessTokenResponse = null;
    try {
      accessTokenResponse = generateAccessToken();
    } catch (GeneralSecurityException e) {
      throw new RuntimeException("Failed to get access token", e);
    }
    String accessToken =
        checkNotNull(accessTokenResponse.getAccessToken(), "Access Token cannot be null!");
    Instant expirationTimeInInstant = Instant.parse(accessTokenResponse.getExpireTime());
    Long expirationTimeMilliSeconds = expirationTimeInInstant.toEpochMilli();
    return new TokenResponse()
        .setAccessToken(accessToken)
        .setExpiresInSeconds(
            expirationTimeMilliSeconds == null
                ? null
                : (expirationTimeMilliSeconds - clock.currentTimeMillis()) / 1000);
  }

  private GenerateAccessTokenResponse generateAccessToken()
      throws IOException, GeneralSecurityException {
    IAMCredentials IAMCredentials =
        new IAMCredentials(transport, new JacksonFactory(), initializer);
    GenerateAccessTokenRequest content = new GenerateAccessTokenRequest();
    content.setScope(CredentialFactory.GCS_SCOPES);
    content.setLifetime(DEFAULT_ACCESS_TOKEN_LIFETIME);
    IAMCredentials.Projects.ServiceAccounts.GenerateAccessToken generateAccessTokenReq =
        IAMCredentials.projects()
            .serviceAccounts()
            .generateAccessToken(DEFAULT_SERVICE_ACCOUNT_NAME_PREFIX + serviceAccountName, content);
    GenerateAccessTokenResponse response = generateAccessTokenReq.execute();
    return response;
  }
}
