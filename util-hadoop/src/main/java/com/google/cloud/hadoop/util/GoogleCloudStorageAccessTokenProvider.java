package com.google.cloud.hadoop.util;

import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.iamcredentials.v1.IAMCredentials;
import com.google.api.services.iamcredentials.v1.model.GenerateAccessTokenRequest;
import com.google.api.services.iamcredentials.v1.model.GenerateAccessTokenResponse;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.time.Instant;
import org.apache.hadoop.conf.Configuration;

public class GoogleCloudStorageAccessTokenProvider implements AccessTokenProvider {

  private final String serviceAccoutName;
  private final HttpRequestInitializer initializer;
  private final HttpTransport transport;
  private final ImmutableList<String> scope;
  private Configuration configuration;
  private AccessToken accessToken;

  private static final String DEFAULT_ACCESS_TOKEN_LIFETIME = "3600s";
  private static final String DEFAULT_SERVICE_ACCOUNT_NAME_PREFIX = "projects/-/serviceAccounts/";

  public GoogleCloudStorageAccessTokenProvider(
      String serviceAccountName,
      HttpRequestInitializer initializer,
      HttpTransport transport,
      ImmutableList<String> scope) {
    this.serviceAccoutName = serviceAccountName;
    this.initializer = initializer;
    this.transport = transport;
    this.scope = scope;
  }

  @Override
  public AccessToken getAccessToken() {
    if (accessToken != null) {
      return accessToken;
    }

    try {
      refresh();
    } catch (IOException e) {
      throw new RuntimeException("Failed to get access token", e);
    }
    return accessToken;
  }

  @Override
  public void refresh() throws IOException {
    IAMCredentials IAMCredentials =
        new IAMCredentials(transport, new JacksonFactory(), initializer);
    GenerateAccessTokenRequest content = new GenerateAccessTokenRequest();
    content.setScope(scope);
    content.setLifetime(DEFAULT_ACCESS_TOKEN_LIFETIME);
    IAMCredentials.Projects.ServiceAccounts.GenerateAccessToken generateAccessTokenReq =
        IAMCredentials.projects()
            .serviceAccounts()
            .generateAccessToken(DEFAULT_SERVICE_ACCOUNT_NAME_PREFIX + serviceAccoutName, content);
    GenerateAccessTokenResponse response = generateAccessTokenReq.execute();
    Instant expirationTimeInInstant = Instant.parse(response.getExpireTime());
    accessToken =
        new AccessToken(response.getAccessToken(), expirationTimeInInstant.toEpochMilli());
  }

  @Override
  public void setConf(Configuration configuration) {
    this.configuration = configuration;
  }

  @Override
  public Configuration getConf() {
    return configuration;
  }
}
