package com.google.cloud.hadoop.fs.gcs;

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

public class RefreshTokenAuth2Provider implements AccessTokenProvider {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();
  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

  private static final AccessToken EXPIRED_TOKEN = new AccessToken("", -1L);
  private static DateTimeFormatter dateFormat =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneId.systemDefault());
  private Configuration config;
  private AccessToken accessToken = EXPIRED_TOKEN;
  private HttpTransport httpTransport;
  private Optional<RedactedString> previousRefreshToken = Optional.empty();

  @Override
  public AccessToken getAccessToken() {
    return this.accessToken;
  }

  @Override
  public void refresh() {
    if (logger.atFine().isEnabled()) {
      String expireAt =
          accessToken.getExpirationTimeMilliSeconds() == null
              ? "NEVER_EXPIRE"
              : dateFormat.format(
                  Instant.ofEpochMilli(accessToken.getExpirationTimeMilliSeconds()));
      logger.atFine().log(
          "Our token is set to expire at '"
              + expireAt
              + "' and it is now '"
              + dateFormat.format(Instant.now())
              + "'");
      logger.atFine().log("Refreshing access-token based token");
    }

    GoogleCloudStorageOptions gcsOptions =
        GoogleHadoopFileSystemConfiguration.getGcsOptionsBuilder(this.config).build();
    String tokenServerUrl = gcsOptions.getTokenServerUrl();
    RedactedString refreshToken = gcsOptions.getRefreshToken();
    String clientId = gcsOptions.getClientId();
    RedactedString clientSecret = gcsOptions.getClientSecret();
    assert refreshToken != null;
    assert clientSecret != null;

    logger.atFine().log(
        "Refresh token calling endpoint '"
            + tokenServerUrl
            + "' with client id '"
            + clientId
            + "'");

    try {
      HttpTransport httpTransport = getTransport();
      this.accessToken =
          getAccessToken(
              tokenServerUrl,
              clientId,
              clientSecret,
              previousRefreshToken.orElse(refreshToken),
              httpTransport);

      if (logger.atFine().isEnabled()) {
        if (this.accessToken.getExpirationTimeMilliSeconds() != null) {
          logger.atFine().log(
              "New access token expires at '"
                  + dateFormat.format(
                      Instant.ofEpochMilli(this.accessToken.getExpirationTimeMilliSeconds()))
                  + "'");
        } else {
          logger.atFine().log("New access token never expires.");
        }
      }
    } catch (IOException e) {
      logger.atSevere().log("Couldn't refresh token", e);
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
    this.previousRefreshToken =
        Optional.ofNullable(RedactedString.create(tokenResponse.getRefreshToken()));

    return new AccessToken(
        tokenResponse.getAccessToken(),
        tokenResponse.getExpiresInSeconds() == null
            ? null
            : System.currentTimeMillis() + (tokenResponse.getExpiresInSeconds() * 1000L));
  }

  @Override
  public Configuration getConf() {
    return this.config;
  }

  @Override
  public void setConf(Configuration config) {
    this.config = config;
  }

  private HttpTransport getTransport() throws IOException {
    if (httpTransport == null) {
      GoogleCloudStorageOptions gcsOptions =
          GoogleHadoopFileSystemConfiguration.getGcsOptionsBuilder(this.config).build();
      String proxyAddress = gcsOptions.getProxyAddress();
      RedactedString proxyUsername = gcsOptions.getProxyUsername();
      RedactedString proxyPassword = gcsOptions.getProxyPassword();
      logger.atFine().log(
          "Proxy setup: '" + proxyAddress + "' with username = '" + proxyUsername + "'");
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
    return this.previousRefreshToken;
  }
}
