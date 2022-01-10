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
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import org.apache.hadoop.conf.Configuration;

public class RefreshTokenAuth2Provider implements AccessTokenProvider {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();
  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

  private static final AccessToken EXPIRED_TOKEN = new AccessToken("", -1L);
  private static DateTimeFormatter dateFormat =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneId.systemDefault());
  private Configuration config;
  private AccessToken accessToken = EXPIRED_TOKEN;

  @Override
  public AccessToken getAccessToken() {
    return this.accessToken;
  }

  @Override
  public void refresh() {
    logger.atFine().log(
        "Our token is set to expire at '"
            + dateFormat.format(Instant.ofEpochMilli(accessToken.getExpirationTimeMilliSeconds()))
            + "' and it is now '"
            + dateFormat.format(Instant.now())
            + "'");
    logger.atFine().log("Refreshing access-token based token");

    GoogleCloudStorageOptions gcsOptions =
        GoogleHadoopFileSystemConfiguration.getGcsOptionsBuilder(this.config).build();
    String tokenServerUrl = gcsOptions.getTokenServerUrl();
    RedactedString refreshToken = gcsOptions.getRefreshToken();
    String clientId = gcsOptions.getClientId();
    RedactedString clientSecret = gcsOptions.getClientSecret();
    String proxyAddress = gcsOptions.getProxyAddress();
    RedactedString proxyUsername = gcsOptions.getProxyUsername();
    RedactedString proxyPassword = gcsOptions.getProxyPassword();

    logger.atFine().log(
        "Refresh token calling endpoint '"
            + tokenServerUrl
            + "' with client id '"
            + clientId
            + "'");
    logger.atFine().log(
        "Proxy setup: '" + proxyAddress + "' with username = '" + proxyUsername + "'");

    try {
      this.accessToken =
          getAccessToken(
              tokenServerUrl,
              clientId,
              clientSecret,
              refreshToken,
              proxyAddress,
              proxyUsername,
              proxyPassword);
      logger.atFine().log(
          "New token expires at '"
              + dateFormat.format(
                  Instant.ofEpochMilli(this.accessToken.getExpirationTimeMilliSeconds()))
              + "'");
    } catch (IOException e) {
      logger.atSevere().log("Couldn't refresh token", e);
    }
  }

  private AccessTokenProvider.AccessToken getAccessToken(
      String tokenServerUrl,
      String clientId,
      RedactedString clientSecret,
      RedactedString refreshToken,
      String proxyAddress,
      RedactedString proxyUsername,
      RedactedString proxyPassword)
      throws IOException {

    logger.atFine().log("Get a new access token using the refresh token grant flow");
    HttpTransport httpTransport =
        HttpTransportFactory.createHttpTransport(proxyAddress, proxyUsername, proxyPassword);
    TokenRequest request =
        new RefreshTokenRequest(
                httpTransport, JSON_FACTORY, new GenericUrl(tokenServerUrl), refreshToken.value())
            .setClientAuthentication(
                new ClientParametersAuthentication(clientId, clientSecret.value()));

    TokenResponse execute = request.execute();
    return new AccessToken(
        execute.getAccessToken(),
        System.currentTimeMillis() + (execute.getExpiresInSeconds() * 1000L));
  }

  @Override
  public Configuration getConf() {
    return this.config;
  }

  @Override
  public void setConf(Configuration config) {
    this.config = config;
  }
}
