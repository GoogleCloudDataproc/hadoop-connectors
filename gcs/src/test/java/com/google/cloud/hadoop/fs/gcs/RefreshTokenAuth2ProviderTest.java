package com.google.cloud.hadoop.fs.gcs;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.hadoop.util.AccessTokenProvider;
import com.google.cloud.hadoop.util.RedactedString;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.BaseEncoding;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import junit.framework.TestCase;
import net.jadler.Jadler;
import net.jadler.stubbing.server.jdk.JdkStubHttpServer;
import org.apache.hadoop.shaded.net.minidev.json.JSONObject;
import org.apache.http.HttpHeaders;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class RefreshTokenAuth2ProviderTest extends TestCase {

  @Before
  public void before() {
    Jadler.initJadlerUsing(new JdkStubHttpServer());
  }

  @After
  public void after() {
    Jadler.closeJadler();
  }

  @Test
  public void testGetAccessTokenWithoutProxy() throws IOException {
    // GIVEN
    RefreshTokenAuth2Provider refreshTokenAuth2Provider = new RefreshTokenAuth2Provider();

    String baseURL = "http://localhost:" + Jadler.port();

    String tokenEndpoint = baseURL + "/token";
    RedactedString refreshToken = RedactedString.create("REFRESH_TOKEN");
    String clientId = "FAKE_CLIENT_ID";
    RedactedString clientSecret = RedactedString.create("FAKE_CLIENT_SECRET");
    String proxyAddress = null;
    RedactedString proxyUsername = RedactedString.create(null);
    RedactedString proxyPassword = RedactedString.create(null);

    int expireInSec = 300;
    String accessTokenAsString = "SlAV32hkKG";
    JSONObject tokenResponse = new JSONObject();
    tokenResponse.put("access_token", accessTokenAsString);
    tokenResponse.put("token_type", "Bearer");
    tokenResponse.put("expires_in", 300);

    Jadler.onRequest()
        .havingMethodEqualTo("POST")
        .havingPathEqualTo("/token")
        .havingHeaderEqualTo("Content-Type", "application/x-www-form-urlencoded")
        .havingBody(
            new BodyFormMatcher(
                ImmutableMap.of(
                    // credentials are in the body of the POST request
                    "client_id",
                    clientId,
                    "client_secret",
                    clientSecret.value(),
                    "refresh_token",
                    refreshToken.value(),
                    "grant_type",
                    "refresh_token")))
        .respond()
        .withStatus(200)
        .withHeader("Content-Type", "application/json")
        .withBody(tokenResponse.toString());

    // WHEN
    AccessTokenProvider.AccessToken accessToken =
        refreshTokenAuth2Provider.getAccessToken(
            tokenEndpoint,
            clientId,
            clientSecret,
            refreshToken,
            proxyAddress,
            proxyUsername,
            proxyPassword);

    // THEN
    assertThat(accessToken).isNotNull();
    assertThat(accessToken.getToken()).isEqualTo(accessTokenAsString);
    long now = System.currentTimeMillis();
    // To avoid any timebase issue, we test a time range instead
    assertThat(accessToken.getExpirationTimeMilliSeconds())
        .isGreaterThan(now + ((expireInSec - 10) * 1000L));
    assertThat(accessToken.getExpirationTimeMilliSeconds())
        .isLessThan(now + ((expireInSec + 10) * 1000L));
  }

  @Test
  public void testGetAccessTokenWithProxy() throws IOException {
    // GIVEN
    RefreshTokenAuth2Provider refreshTokenAuth2Provider = new RefreshTokenAuth2Provider();

    String tokenEndpoint = "http://myawesomeIAM.com/token";
    RedactedString refreshToken = RedactedString.create("REFRESH_TOKEN");
    String clientId = "FAKE_CLIENT_ID";
    RedactedString clientSecret = RedactedString.create("FAKE_CLIENT_SECRET");
    String proxyAddress = "localhost:" + Jadler.port();
    RedactedString proxyUsername = RedactedString.create("admin");
    RedactedString proxyPassword = RedactedString.create("changeit");

    int expireInSec = 300;
    String accessTokenAsString = "SlAV32hkKG";
    JSONObject tokenResponse = new JSONObject();
    tokenResponse.put("access_token", accessTokenAsString);
    tokenResponse.put("token_type", "Bearer");
    tokenResponse.put("expires_in", expireInSec);

    Jadler.onRequest()
        .havingMethodEqualTo("POST")
        .havingPathEqualTo("/token")
        .havingHeaderEqualTo("Content-Type", "application/x-www-form-urlencoded")
        .havingHeaderEqualTo(
            HttpHeaders.PROXY_AUTHORIZATION,
            "Basic "
                + BaseEncoding.base64()
                    .encode((proxyUsername.value() + ":" + proxyPassword.value()).getBytes(StandardCharsets.UTF_8)))
        .havingBody(
            new BodyFormMatcher(
                ImmutableMap.of(
                    // credentials are in the body of the POST request
                    "client_id",
                    clientId,
                    "client_secret",
                    clientSecret.value(),
                    "refresh_token",
                    refreshToken.value(),
                    "grant_type",
                    "refresh_token")))
        .respond()
        .withStatus(200)
        .withHeader("Content-Type", "application/json")
        .withBody(tokenResponse.toString());

    // WHEN
    AccessTokenProvider.AccessToken accessToken =
        refreshTokenAuth2Provider.getAccessToken(
            tokenEndpoint,
            clientId,
            clientSecret,
            refreshToken,
            proxyAddress,
            proxyUsername,
            proxyPassword);

    // THEN
    assertThat(accessToken).isNotNull();
    assertThat(accessToken.getToken()).isEqualTo(accessTokenAsString);
    long now = System.currentTimeMillis();
    // To avoid any timebase issue, we test a time range instead
    assertThat(accessToken.getExpirationTimeMilliSeconds())
        .isGreaterThan(now + ((expireInSec - 10) * 1000L));
    assertThat(accessToken.getExpirationTimeMilliSeconds())
        .isLessThan(now + ((expireInSec + 10) * 1000L));
  }
}
