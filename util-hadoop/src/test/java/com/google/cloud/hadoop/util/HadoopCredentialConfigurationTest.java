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
import static com.google.cloud.hadoop.util.CredentialsFactory.CREDENTIAL_ENV_VAR;
import static com.google.cloud.hadoop.util.HadoopCredentialConfiguration.ENABLE_NULL_CREDENTIAL_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialConfiguration.ENABLE_SERVICE_ACCOUNTS_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialConfiguration.SERVICE_ACCOUNT_JSON_KEYFILE_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialConfiguration.TOKEN_SERVER_URL_SUFFIX;
import static com.google.cloud.hadoop.util.testing.HadoopConfigurationUtils.getDefaultProperties;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.jsonDataResponse;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.mockTransport;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.api.client.auth.oauth2.TokenResponse;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class HadoopCredentialConfigurationTest {

  @SuppressWarnings("DoubleBraceInitialization")
  private static final Map<String, Object> expectedDefaultConfiguration =
      new HashMap<String, Object>() {
        {
          put(".auth.access.token.provider.impl", null);
          put(".auth.null.enable", false);
          put(".auth.service.account.enable", true);
          put(".enable.service.account.auth", true);
          put(".auth.service.account.json.keyfile", null);
          put(".token.server.url", null);
          put(".proxy.address", null);
          put(".proxy.password", null);
          put(".proxy.username", null);
          put(".auth.impersonation.service.account", null);
          put(".auth.impersonation.service.account.for.user.", ImmutableMap.of());
          put(".auth.impersonation.service.account.for.group.", ImmutableMap.of());
        }
      };

  private Configuration configuration;

  private static String getConfigKey(HadoopConfigurationProperty<?> suffixProperty) {
    return HadoopCredentialConfiguration.BASE_KEY_PREFIX + suffixProperty.getKey();
  }

  @Before
  public void setUp() {
    configuration = new Configuration();
  }

  private CredentialsFactory getCredentialFactory() {
    return getCredentialFactory(new MockHttpTransport());
  }

  private CredentialsFactory getCredentialFactory(HttpTransport transport) {
    CredentialOptions options = HadoopCredentialConfiguration.getCredentialsOptions(configuration);
    return new CredentialsFactory(options, Suppliers.ofInstance(transport));
  }

  @Test
  public void nullCredentialsAreCreatedForTesting() throws Exception {
    configuration.setBoolean(getConfigKey(ENABLE_SERVICE_ACCOUNTS_SUFFIX), false);
    configuration.setBoolean(getConfigKey(ENABLE_NULL_CREDENTIAL_SUFFIX), true);

    CredentialsFactory credentialsFactory = getCredentialFactory();

    assertThat(credentialsFactory.getCredentials()).isNull();
  }

  @Test
  public void exceptionIsThrownForNoCredentialOptions() {
    configuration.setBoolean(getConfigKey(ENABLE_SERVICE_ACCOUNTS_SUFFIX), false);

    IllegalArgumentException thrown =
        assertThrows(IllegalArgumentException.class, this::getCredentialFactory);

    assertThat(thrown).hasMessageThat().startsWith("No valid credential configuration discovered:");
  }

  @Test
  public void metadataServiceIsUsedByDefault() throws Exception {
    TokenResponse token =
        new TokenResponse().setAccessToken("metadata-test-token").setExpiresInSeconds(100L);

    MockHttpTransport transport = mockTransport(jsonDataResponse(token));

    CredentialsFactory credentialsFactory = getCredentialFactory(transport);
    GoogleCredentials credentials = credentialsFactory.getCredentials();

    credentials.refreshIfExpired();

    assertThat(credentials).isInstanceOf(ComputeEngineCredentials.class);
    assertThat(credentials.getAccessToken().getTokenValue()).isEqualTo("metadata-test-token");
  }

  @Test
  public void applicationDefaultServiceAccountWhenConfigured() throws Exception {
    CredentialsFactory credentialsFactory = getCredentialFactory();

    ServiceAccountCredentials credentials =
        (ServiceAccountCredentials)
            withEnvironmentVariable(CREDENTIAL_ENV_VAR, getStringPath("test-credential.json"))
                .execute(credentialsFactory::getCredentials);

    assertThat(credentials.getClientEmail()).isEqualTo("test-email@gserviceaccount.com");
    assertThat(credentials.getPrivateKeyId()).isEqualTo("test-key-id");
  }

  @Test
  public void jsonKeyFileUsedWhenConfigured() throws Exception {
    configuration.set(
        getConfigKey(SERVICE_ACCOUNT_JSON_KEYFILE_SUFFIX), getStringPath("test-credential.json"));

    CredentialsFactory credentialsFactory = getCredentialFactory();

    ServiceAccountCredentials credentials =
        (ServiceAccountCredentials) credentialsFactory.getCredentials();

    assertThat(credentials.getClientEmail()).isEqualTo("test-email@gserviceaccount.com");
    assertThat(credentials.getPrivateKeyId()).isEqualTo("test-key-id");
  }

  @Test
  public void customTokenServerUrl() throws Exception {
    configuration.set(
        getConfigKey(SERVICE_ACCOUNT_JSON_KEYFILE_SUFFIX), getStringPath("test-credential.json"));
    configuration.set(getConfigKey(TOKEN_SERVER_URL_SUFFIX), "https://test.oauth.com/token");

    CredentialsFactory credentialsFactory = getCredentialFactory();

    ServiceAccountCredentials credentials =
        (ServiceAccountCredentials) credentialsFactory.getCredentials();

    assertThat(credentials.getTokenServerUri()).isEqualTo(new URI("https://test.oauth.com/token"));
  }

  @Test
  public void defaultPropertiesValues() {
    assertThat(getDefaultProperties(HadoopCredentialConfiguration.class))
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
