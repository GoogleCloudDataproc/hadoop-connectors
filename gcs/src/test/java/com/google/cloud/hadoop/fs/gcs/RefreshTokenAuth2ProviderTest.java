package com.google.cloud.hadoop.fs.gcs;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.hadoop.util.AccessTokenProvider;
import com.google.cloud.hadoop.util.RedactedString;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import junit.framework.TestCase;
import net.jadler.Jadler;
import net.jadler.stubbing.server.jdk.JdkStubHttpServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.shaded.net.minidev.json.JSONObject;
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
  public void testGetAccessToken() throws IOException {
    // GIVEN
    String baseURL = "http://localhost:" + Jadler.port();

    String tokenServerUrl = baseURL + "/token";
    RedactedString refreshToken = RedactedString.create("REFRESH_TOKEN");
    String clientId = "FAKE_CLIENT_ID";
    RedactedString clientSecret = RedactedString.create("FAKE_CLIENT_SECRET");

    Configuration config = new Configuration();
    config.set("fs.gs.token.server.url", tokenServerUrl);
    config.set("fs.gs.auth.refresh.token", refreshToken.value());
    config.set("fs.gs.auth.client.id", clientId);
    config.set("fs.gs.auth.client.secret", clientSecret.value());

    RefreshTokenAuth2Provider refreshTokenAuth2Provider = new RefreshTokenAuth2Provider();

    int expireInSec = 300;
    String accessTokenAsString = "SlAV32hkKG";
    JSONObject tokenResponse = new JSONObject();
    tokenResponse.put("access_token", accessTokenAsString);
    tokenResponse.put("token_type", "Bearer");
    tokenResponse.put("expires_in", 300);

    Jadler.onRequest()
        .havingMethodEqualTo("POST")
        .havingPathEqualTo("/token")
        .havingHeaderEqualTo("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
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
    refreshTokenAuth2Provider.setConf(config);
    refreshTokenAuth2Provider.refresh();
    AccessTokenProvider.AccessToken accessToken = refreshTokenAuth2Provider.getAccessToken();

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
