package com.google.cloud.hadoop.util;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.flogger.GoogleLogger;
import com.google.common.io.BaseEncoding;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import org.apache.hadoop.conf.Configuration;
import org.apache.http.HttpHeaders;

public class RefreshTokenAuth2Provider implements AccessTokenProvider {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();
  private static DateTimeFormatter dateFormat =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneId.systemDefault());

  private static final int CONNECT_TIMEOUT = 30 * 1000;
  private static final int READ_TIMEOUT = 30 * 1000;

  public static final String CONFIG_REFRESH_TOKEN = "fs.gs.auth.refresh.token";
  public static final String CONFIG_TOKEN_ENDPOINT = "fs.gs.auth.token.endpoint";
  public static final String CONFIG_CLIENT_ID = "fs.gs.auth.client.id";
  public static final String CONFIG_CLIENT_SECRET = "fs.gs.auth.client.secret";
  public static final String CONFIG_PROXY_ADDRESS = "fs.gs.proxy.address";
  public static final String CONFIG_PROXY_USERNAME = "fs.gs.proxy.username";
  public static final String CONFIG_PROXY_PASSWORD = "fs.gs.proxy.password";
  private static final AccessToken EXPIRED_TOKEN = new AccessToken("", -1L);

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
    String tokenEndpoint = this.config.get(CONFIG_TOKEN_ENDPOINT);
    String refreshToken = this.config.get(CONFIG_REFRESH_TOKEN);
    String clientId = this.config.get(CONFIG_CLIENT_ID);
    String clientSecret = this.config.get(CONFIG_CLIENT_SECRET);
    String proxyAddress = this.config.get(CONFIG_PROXY_ADDRESS);
    String proxyUsername = this.config.get(CONFIG_PROXY_USERNAME);
    String proxyPassword = this.config.get(CONFIG_PROXY_PASSWORD);
    logger.atFine().log(
        "Refresh token calling endpoint '" + tokenEndpoint + "' with client id '" + clientId + "'");
    logger.atFine().log(
        "Proxy setup: '" + proxyAddress + "' with username = '" + proxyUsername + "'");

    try {
      this.accessToken =
          getAccessToken(
              tokenEndpoint,
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

  public AccessTokenProvider.AccessToken getAccessToken(
      String tokenEndpoint,
      String clientId,
      String clientSecret,
      String refreshToken,
      String proxyAddress,
      String proxyUsername,
      String proxyPassword)
      throws IOException {

    AccessToken token = null;
    HttpURLConnection conn = null;
    logger.atFine().log("Get a new access token using the refresh token grant flow");
    try {
      URL url = new URL(tokenEndpoint);
      conn = (HttpURLConnection) url.openConnection(getProxy(proxyAddress));
      conn.setRequestMethod("POST");
      conn.setReadTimeout(READ_TIMEOUT);
      conn.setConnectTimeout(CONNECT_TIMEOUT);
      conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
      conn.setRequestProperty("Accept", "application/json");
      if (proxyUsername != null) {
        logger.atFine().log("Setup the request with the proxy auth");
        conn.setRequestProperty(
            HttpHeaders.PROXY_AUTHORIZATION,
            "Basic "
                + BaseEncoding.base64()
                    .encode(
                        (proxyUsername + ":" + proxyPassword).getBytes(StandardCharsets.UTF_8)));
      }
      conn.setDoOutput(true);
      String payload =
          "grant_type=refresh_token&refresh_token="
              + refreshToken
              + "&client_id="
              + clientId
              + "&client_secret="
              + clientSecret;
      conn.getOutputStream().write(payload.getBytes("UTF-8"));

      int httpResponseCode = conn.getResponseCode();
      logger.atFine().log("Response " + httpResponseCode);

      String responseContentType = conn.getHeaderField("Content-Type");

      if (httpResponseCode == HttpURLConnection.HTTP_OK
          && responseContentType.startsWith("application/json")) {
        logger.atFine().log("Received a 200 with presumably a new access token");
        InputStream httpResponseStream = conn.getInputStream();
        token = parseTokenFromStream(httpResponseStream);
      } else {
        logger.atFine().log("Refresh token grant flow failed");
        InputStream stream = conn.getErrorStream();
        if (stream == null) {
          // no error stream, try the original input stream
          stream = conn.getInputStream();
        }
        String responseBody = consumeInputStream(stream);
        logger.atSevere().log(
            "Received following response '"
                + responseBody
                + "' with code '"
                + httpResponseCode
                + "'");
      }
    } finally {
      if (conn != null) {
        conn.disconnect();
      }
    }
    return token;
  }

  private String consumeInputStream(InputStream stream) throws IOException {
    BufferedReader br = new BufferedReader(new InputStreamReader(stream, "utf-8"));
    StringBuilder response = new StringBuilder();
    String responseLine;
    while ((responseLine = br.readLine()) != null) {
      response.append(responseLine.trim());
    }
    return response.toString();
  }

  private AccessToken parseTokenFromStream(InputStream httpResponseStream) throws IOException {
    AccessToken token;
    try {
      int expirationPeriodInSecs = 0;

      JsonFactory jf = new JsonFactory();
      JsonParser jp = jf.createJsonParser(httpResponseStream);
      String fieldName, fieldValue;
      jp.nextToken();
      String accessTokenFromResponse = "";
      while (jp.hasCurrentToken()) {
        if (jp.getCurrentToken() == JsonToken.FIELD_NAME) {
          fieldName = jp.getCurrentName();
          jp.nextToken(); // field value
          fieldValue = jp.getText();

          if (fieldName.equals("access_token")) {
            accessTokenFromResponse = fieldValue;
          }

          if (fieldName.equals("expires_in")) {
            expirationPeriodInSecs = Integer.parseInt(fieldValue);
          }
        }
        jp.nextToken();
      }

      token =
          new AccessToken(
              accessTokenFromResponse,
              System.currentTimeMillis() + (expirationPeriodInSecs * 1000L));
      jp.close();
    } finally {
      httpResponseStream.close();
    }
    return token;
  }

  public Proxy getProxy(String proxyAddress) {
    if (proxyAddress == null || "".equals(proxyAddress)) {
      return Proxy.NO_PROXY;
    }
    logger.atFine().log("The proxy is defined");
    String[] proxyAddressParsed = proxyAddress.split(":");
    return new Proxy(
        Proxy.Type.HTTP,
        new InetSocketAddress(proxyAddressParsed[0], Integer.parseInt(proxyAddressParsed[1])));
  }

  @Override
  public void setConf(Configuration config) {
    this.config = config;
  }

  @Override
  public Configuration getConf() {
    return this.config;
  }
}
