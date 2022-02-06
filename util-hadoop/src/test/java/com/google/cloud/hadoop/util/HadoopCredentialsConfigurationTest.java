/*
 * Copyright 2014 Google Inc. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.util;

import static com.github.stefanbirkner.systemlambda.SystemLambda.withEnvironmentVariable;
import static com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.ACCESS_TOKEN_PROVIDER_IMPL_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.AUTHENTICATION_TYPE_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.SERVICE_ACCOUNT_JSON_KEYFILE_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.TOKEN_SERVER_URL_SUFFIX;
import static com.google.cloud.hadoop.util.testing.HadoopConfigurationUtils.getDefaultProperties;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.jsonDataResponse;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.mockTransport;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.api.client.auth.oauth2.TokenResponse;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
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
      new HashMap<String, Object>() {
        {
          put(".auth.access.token.provider.impl", null);
          put(".auth.impersonation.service.account", null);
          put(".auth.impersonation.service.account.for.group.", ImmutableMap.of());
          put(".auth.impersonation.service.account.for.user.", ImmutableMap.of());
          put(".auth.service.account.json.keyfile", null);
          put(".auth.type", AuthenticationType.GCE_METADATA_SERVICE);
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
    TokenResponse token =
        new TokenResponse().setAccessToken("metadata-test-token").setExpiresInSeconds(100L);
    MockHttpTransport transport = mockTransport(jsonDataResponse(token));

    GoogleCredentials credentials = getCredentials(transport);

    credentials.refreshIfExpired();

    assertThat(credentials).isInstanceOf(ComputeEngineCredentials.class);
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
  public void accessTokenProviderCredentials_credentialFactory() throws IOException {
    configuration.setEnum(
        getConfigKey(AUTHENTICATION_TYPE_SUFFIX), AuthenticationType.ACCESS_TOKEN_PROVIDER);
    configuration.setClass(
        getConfigKey(ACCESS_TOKEN_PROVIDER_IMPL_SUFFIX),
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
