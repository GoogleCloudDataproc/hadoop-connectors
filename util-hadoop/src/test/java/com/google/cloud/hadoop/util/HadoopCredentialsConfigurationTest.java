/*
 * Copyright 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.util;

import static com.github.stefanbirkner.systemlambda.SystemLambda.withEnvironmentVariable;
import static com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.ACCESS_TOKEN_PROVIDER_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.AUTHENTICATION_TYPE_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.AUTH_CLIENT_ID_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.AUTH_CLIENT_SECRET_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.AUTH_REFRESH_TOKEN_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.SERVICE_ACCOUNT_JSON_KEYFILE_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.TOKEN_SERVER_URL_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.WORKLOAD_IDENTITY_FEDERATION_CREDENTIAL_CONFIG_FILE_SUFFIX;
import static com.google.cloud.hadoop.util.testing.HadoopConfigurationUtils.getDefaultProperties;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.jsonDataResponse;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.mockTransport;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import com.google.api.client.auth.oauth2.TokenResponse;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.json.Json;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.ExternalAccountCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.auth.oauth2.UserCredentials;
import com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.AuthenticationType;
import com.google.cloud.hadoop.util.testing.TestingAccessTokenProvider;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class HadoopCredentialsConfigurationTest {

  @SuppressWarnings("DoubleBraceInitialization")
  private static final Map<String, Object> expectedDefaultConfiguration =
      new HashMap<>() {
        {
          put(".auth.access.token.provider", null);
          put(".auth.client.id", null);
          put(".auth.client.secret", null);
          put(".auth.impersonation.service.account", null);
          put(".auth.impersonation.service.account.for.group.", ImmutableMap.of());
          put(".auth.impersonation.service.account.for.user.", ImmutableMap.of());
          put(".auth.refresh.token", null);
          put(".auth.service.account.json.keyfile", null);
          put(".auth.workload.identity.federation.credential.config.file", null);
          put(".auth.type", AuthenticationType.COMPUTE_ENGINE);
          put(".http.read-timeout", 5_000L);
          put(".proxy.address", null);
          put(".proxy.password", null);
          put(".proxy.username", null);
          put(".token.server.url", null);
        }
      };

  private Configuration configuration;

  private static String getConfigKey(HadoopConfigurationProperty<?> suffixProperty) {
    return HadoopCredentialsConfiguration.BASE_KEY_PREFIX + suffixProperty.getKey();
  }

  @Before
  public void setUp() {
    configuration = new Configuration();
  }

  private GoogleCredentials getCredentials() throws IOException {
    return getCredentials(new MockHttpTransport());
  }

  private GoogleCredentials getCredentials(HttpTransport transport) throws IOException {
    return HadoopCredentialsConfiguration.getCredentials(
        Suppliers.ofInstance(transport),
        configuration,
        ImmutableList.of(HadoopCredentialsConfiguration.BASE_KEY_PREFIX));
  }

  @Test
  public void nullCredentialsAreCreatedForTesting() throws Exception {
    configuration.setEnum(
        getConfigKey(AUTHENTICATION_TYPE_SUFFIX), AuthenticationType.UNAUTHENTICATED);

    GoogleCredentials credentials = getCredentials();

    assertThat(credentials).isNull();
  }

  @Test
  public void invalidAuthType_exceptionIsThrown() {
    configuration.set(getConfigKey(AUTHENTICATION_TYPE_SUFFIX), "INVALID_TEST");

    IllegalArgumentException thrown =
        assertThrows(IllegalArgumentException.class, this::getCredentials);

    assertThat(thrown).hasMessageThat().startsWith("No enum constant ");
    assertThat(thrown).hasMessageThat().contains("AuthenticationType.INVALID_TEST");
  }

  @Test
  public void metadataServiceIsUsedByDefault() throws Exception {
    // RESPONSE: The access token response.
    LowLevelHttpResponse tokenResponse =
        new MockLowLevelHttpResponse()
            .setContentType(Json.MEDIA_TYPE)
            .setContent(
                "{"
                    + "\"access_token\": \"metadata-test-token\","
                    + "\"expires_in\": 100,"
                    + "\"token_type\": \"Bearer\""
                    + "}");

    // Use your utility to create transport that will serve the responses in order.
    MockHttpTransport transport = mockTransport(tokenResponse);

    GoogleCredentials credentials = getCredentials(transport);
    assertNotNull(credentials);

    // This call will now succeed because the first response has the expected structure.
    credentials.refreshIfExpired();

    // The final assertion, which will now pass.
    assertThat(credentials.getAccessToken().getTokenValue()).isEqualTo("metadata-test-token");
  }

  @Test
  public void applicationDefaultServiceAccountWhenConfigured() throws Exception {
    configuration.setEnum(
        getConfigKey(AUTHENTICATION_TYPE_SUFFIX), AuthenticationType.APPLICATION_DEFAULT);

    ServiceAccountCredentials credentials =
        (ServiceAccountCredentials)
            withEnvironmentVariable(
                    "GOOGLE_APPLICATION_CREDENTIALS", getStringPath("test-credentials.json"))
                .execute(() -> getCredentials());

    assertThat(credentials.getClientEmail()).isEqualTo("test-email@gserviceaccount.com");
    assertThat(credentials.getPrivateKeyId()).isEqualTo("test-key-id");
  }

  @Test
  public void jsonKeyFileUsedWhenConfigured() throws Exception {
    configuration.setEnum(
        getConfigKey(AUTHENTICATION_TYPE_SUFFIX), AuthenticationType.SERVICE_ACCOUNT_JSON_KEYFILE);
    configuration.set(
        getConfigKey(SERVICE_ACCOUNT_JSON_KEYFILE_SUFFIX), getStringPath("test-credentials.json"));

    ServiceAccountCredentials credentials = (ServiceAccountCredentials) getCredentials();

    assertThat(credentials.getClientEmail()).isEqualTo("test-email@gserviceaccount.com");
    assertThat(credentials.getPrivateKeyId()).isEqualTo("test-key-id");
  }

  @Test
  public void wipConfigFileUsedWhenConfigured() throws Exception {
    configuration.setEnum(
        getConfigKey(AUTHENTICATION_TYPE_SUFFIX),
        AuthenticationType.WORKLOAD_IDENTITY_FEDERATION_CREDENTIAL_CONFIG_FILE);
    configuration.set(
        getConfigKey(WORKLOAD_IDENTITY_FEDERATION_CREDENTIAL_CONFIG_FILE_SUFFIX),
        getStringPath("test-wip-config.json"));

    ExternalAccountCredentials credentials = (ExternalAccountCredentials) getCredentials();

    assertThat(credentials.getAuthenticationType()).isEqualTo("OAuth2");
    assertThat(credentials.getAudience())
        .isEqualTo(
            "//iam.googleapis.com/projects/test/locations/global/workloadIdentityPools/test-pool/providers/tester");
    assertThat(credentials.getTokenUrl()).isEqualTo("https://sts.googleapis.com/v1/token");
  }

  @Test
  public void accessTokenProviderCredentials_credentialFactory() throws IOException {
    configuration.setEnum(
        getConfigKey(AUTHENTICATION_TYPE_SUFFIX), AuthenticationType.ACCESS_TOKEN_PROVIDER);
    configuration.setClass(
        getConfigKey(ACCESS_TOKEN_PROVIDER_SUFFIX),
        TestingAccessTokenProvider.class,
        AccessTokenProvider.class);

    GoogleCredentials credentials = getCredentials();

    AccessToken accessToken = credentials.getAccessToken();

    assertThat(accessToken).isNotNull();
    assertThat(accessToken.getTokenValue()).isEqualTo(TestingAccessTokenProvider.FAKE_ACCESS_TOKEN);
    assertThat(accessToken.getExpirationTime())
        .isEqualTo(Date.from(TestingAccessTokenProvider.EXPIRATION_TIME));
  }

  @Test
  public void userCredentials_credentialFactory_noNewRefreshToken() throws IOException {
    // GIVEN
    String initialRefreshToken = "FAKE_REFRESH_TOKEN";
    String tokenServerUrl = "http://localhost/token";
    configuration.set(getConfigKey(TOKEN_SERVER_URL_SUFFIX), tokenServerUrl);
    configuration.setEnum(
        getConfigKey(AUTHENTICATION_TYPE_SUFFIX), AuthenticationType.USER_CREDENTIALS);
    configuration.set(getConfigKey(AUTH_REFRESH_TOKEN_SUFFIX), initialRefreshToken);
    configuration.set(getConfigKey(AUTH_CLIENT_ID_SUFFIX), "FAKE_CLIENT_ID");
    configuration.set(getConfigKey(AUTH_CLIENT_SECRET_SUFFIX), "FAKE_CLIENT_SECRET");

    long expireInSec = 300L;
    String accessTokenAsString = "SlAV32hkKG";

    TokenResponse tokenResponse =
        new TokenResponse().setAccessToken(accessTokenAsString).setExpiresInSeconds(expireInSec);

    MockHttpTransport transport = mockTransport(jsonDataResponse(tokenResponse));

    // WHEN
    GoogleCredentials credentials = getCredentials(transport);
    credentials.refresh();

    // THEN
    assertThat(credentials).isInstanceOf(UserCredentials.class);

    UserCredentials userCredentials = (UserCredentials) credentials;

    assertThat(userCredentials.getClientId()).isEqualTo("FAKE_CLIENT_ID");
    assertThat(userCredentials.getClientSecret()).isEqualTo("FAKE_CLIENT_SECRET");

    AccessToken accessToken = userCredentials.getAccessToken();

    assertThat(accessToken).isNotNull();
    // To avoid any timebase issue, we test a time range instead
    assertThat(accessToken.getExpirationTime())
        .isGreaterThan(Date.from(Instant.now().plusSeconds(expireInSec - 10)));
    assertThat(accessToken.getExpirationTime())
        .isLessThan(Date.from(Instant.now().plusSeconds(expireInSec + 10)));

    String refreshToken = userCredentials.getRefreshToken();

    assertThat(refreshToken).isEqualTo(initialRefreshToken);
  }

  @Test
  public void customTokenServerUrl() throws Exception {
    configuration.setEnum(
        getConfigKey(AUTHENTICATION_TYPE_SUFFIX), AuthenticationType.SERVICE_ACCOUNT_JSON_KEYFILE);
    configuration.set(
        getConfigKey(SERVICE_ACCOUNT_JSON_KEYFILE_SUFFIX), getStringPath("test-credentials.json"));
    configuration.set(getConfigKey(TOKEN_SERVER_URL_SUFFIX), "https://test.oauth.com/token");

    ServiceAccountCredentials credentials = (ServiceAccountCredentials) getCredentials();

    assertThat(credentials.getTokenServerUri()).isEqualTo(new URI("https://test.oauth.com/token"));
  }

  @Test
  public void defaultPropertiesValues() {
    assertThat(getDefaultProperties(HadoopCredentialsConfiguration.class))
        .containsExactlyEntriesIn(expectedDefaultConfiguration);
  }

  private static String getStringPath(String resource) {
    return getPath(resource).toString();
  }

  private static Path getPath(String resource) {
    String filePath = Resources.getResource(resource).getFile();
    return Paths.get(
        System.getProperty("os.name").toLowerCase().contains("win") && filePath.startsWith("/")
            ? filePath.substring(1)
            : filePath);
  }
}
