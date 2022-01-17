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
package com.google.cloud.hadoop.fs.gcs;

import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.jsonDataResponse;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.mockTransport;
import static com.google.common.truth.Truth.assertThat;

import com.google.api.client.auth.oauth2.TokenResponse;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.cloud.hadoop.util.AccessTokenProvider;
import com.google.cloud.hadoop.util.RedactedString;
import java.io.IOException;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for RefreshTokenAuth2Provider class. */
@RunWith(JUnit4.class)
public class RefreshTokenAuth2ProviderTest {

  @Test
  public void getAccessTokenWithNoNewRefreshToken() throws IOException {
    // GIVEN
    String tokenServerUrl = "http://localhost/token";
    RedactedString refreshToken = RedactedString.create("REFRESH_TOKEN");
    String clientId = "FAKE_CLIENT_ID";
    RedactedString clientSecret = RedactedString.create("FAKE_CLIENT_SECRET");

    Configuration config = new Configuration();
    config.set("fs.gs.token.server.url", tokenServerUrl);
    config.set("fs.gs.auth.refresh.token", refreshToken.value());
    config.set("fs.gs.auth.client.id", clientId);
    config.set("fs.gs.auth.client.secret", clientSecret.value());

    RefreshTokenAuth2Provider refreshTokenAuth2Provider = new RefreshTokenAuth2Provider();

    long expireInSec = 300L;
    String accessTokenAsString = "SlAV32hkKG";

    TokenResponse tokenResponse =
        new TokenResponse().setAccessToken(accessTokenAsString).setExpiresInSeconds(expireInSec);

    MockHttpTransport transport = mockTransport(jsonDataResponse(tokenResponse));

    // WHEN
    refreshTokenAuth2Provider.setConf(config);
    refreshTokenAuth2Provider.setTransport(transport);
    refreshTokenAuth2Provider.refresh();
    AccessTokenProvider.AccessToken accessToken = refreshTokenAuth2Provider.getAccessToken();
    Optional<RedactedString> previousRefreshToken =
        refreshTokenAuth2Provider.getPreviousRefreshToken();

    // THEN
    assertThat(accessToken).isNotNull();
    long now = System.currentTimeMillis();
    // To avoid any timebase issue, we test a time range instead
    assertThat(accessToken.getExpirationTimeMilliSeconds())
        .isGreaterThan(now + ((expireInSec - 10) * 1000L));
    assertThat(accessToken.getExpirationTimeMilliSeconds())
        .isLessThan(now + ((expireInSec + 10) * 1000L));
    assertThat(previousRefreshToken.isPresent()).isFalse();
  }

  @Test
  public void getAccessTokenWithNewRefreshToken() throws IOException {
    // GIVEN
    String tokenServerUrl = "http://localhost/token";
    RedactedString refreshToken = RedactedString.create("REFRESH_TOKEN");
    String clientId = "FAKE_CLIENT_ID";
    RedactedString clientSecret = RedactedString.create("FAKE_CLIENT_SECRET");

    Configuration config = new Configuration();
    config.set("fs.gs.token.server.url", tokenServerUrl);
    config.set("fs.gs.auth.refresh.token", refreshToken.value());
    config.set("fs.gs.auth.client.id", clientId);
    config.set("fs.gs.auth.client.secret", clientSecret.value());

    RefreshTokenAuth2Provider refreshTokenAuth2Provider = new RefreshTokenAuth2Provider();

    long expireInSec = 300L;
    String accessTokenAsString = "SlAV32hkKG";
    String newRefreshToken = "NEW_REFRESH_TOKEN";

    TokenResponse tokenResponse =
        new TokenResponse()
            .setTokenType("bearer")
            .setAccessToken(accessTokenAsString)
            .setExpiresInSeconds(expireInSec)
            .setRefreshToken(newRefreshToken);

    MockHttpTransport transport = mockTransport(jsonDataResponse(tokenResponse));

    // WHEN
    refreshTokenAuth2Provider.setConf(config);
    refreshTokenAuth2Provider.setTransport(transport);
    refreshTokenAuth2Provider.refresh();
    AccessTokenProvider.AccessToken accessToken = refreshTokenAuth2Provider.getAccessToken();
    Optional<RedactedString> previousRefreshToken =
        refreshTokenAuth2Provider.getPreviousRefreshToken();

    // THEN
    assertThat(accessToken).isNotNull();
    assertThat(accessToken.getToken()).isEqualTo(accessTokenAsString);
    long now = System.currentTimeMillis();
    // To avoid any timebase issue, we test a time range instead
    assertThat(accessToken.getExpirationTimeMilliSeconds())
        .isGreaterThan(now + ((expireInSec - 10) * 1000L));
    assertThat(accessToken.getExpirationTimeMilliSeconds())
        .isLessThan(now + ((expireInSec + 10) * 1000L));
    assertThat(previousRefreshToken.isPresent()).isTrue();
    assertThat(previousRefreshToken.get()).isEqualTo(RedactedString.create(newRefreshToken));
  }
}
