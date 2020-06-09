package com.google.cloud.hadoop.util;

import static com.google.common.base.Preconditions.checkNotNull;

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
import com.google.api.services.storage.StorageScopes;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.time.Instant;

/** A {@code Credential} to generate or refresh IAM access token. */
public class GoogleCredentialWithIamAccessToken extends GoogleCredential {

  private static final String DEFAULT_ACCESS_TOKEN_LIFETIME = "3600s";
  private static final String DEFAULT_SERVICE_ACCOUNT_NAME_PREFIX = "projects/-/serviceAccounts/";
  private static final ImmutableList<String> CLOUD_PLATFORM_SCOPE =
      ImmutableList.of(StorageScopes.CLOUD_PLATFORM);
  private static final JsonFactory JSON_FACTORY = new JacksonFactory();

  private final String fullServiceAccountName;
  private final HttpRequestInitializer initializer;
  private final HttpTransport transport;
  private final Clock clock;

  public GoogleCredentialWithIamAccessToken(
      String serviceAccountName,
      HttpRequestInitializer initializer,
      HttpTransport transport,
      Clock clock)
      throws IOException {
    this.fullServiceAccountName = DEFAULT_SERVICE_ACCOUNT_NAME_PREFIX + serviceAccountName;
    this.initializer = initializer;
    this.transport = transport;
    this.clock = clock;
    initialize();
  }

  private void initialize() throws IOException {
    GenerateAccessTokenResponse accessTokenResponse = generateAccessToken();
    Instant expirationTimeInInstant = Instant.parse(accessTokenResponse.getExpireTime());
    setAccessToken(accessTokenResponse.getAccessToken());
    setExpirationTimeMilliseconds(expirationTimeInInstant.toEpochMilli());
  }

  @Override
  protected TokenResponse executeRefreshToken() throws IOException {
    GenerateAccessTokenResponse accessTokenResponse = generateAccessToken();
    Instant expirationTimeInInstant = Instant.parse(accessTokenResponse.getExpireTime());
    Long expirationTimeMilliSeconds = expirationTimeInInstant.toEpochMilli();
    return new TokenResponse()
        .setAccessToken(accessTokenResponse.getAccessToken())
        .setExpiresInSeconds(
            expirationTimeMilliSeconds == null
                ? null
                : (expirationTimeMilliSeconds - clock.currentTimeMillis()) / 1000);
  }

  private GenerateAccessTokenResponse generateAccessToken() throws IOException {
    IAMCredentials IAMCredentials = new IAMCredentials(transport, JSON_FACTORY, initializer);
    GenerateAccessTokenRequest content = new GenerateAccessTokenRequest();
    content.setScope(CLOUD_PLATFORM_SCOPE);
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
