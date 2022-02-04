/*
 * Copyright 2018 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.util;

import static com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.ACCESS_TOKEN_PROVIDER_IMPL_SUFFIX;
import static com.google.common.truth.Truth.assertThat;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.hadoop.util.AccessTokenProviderCredentialsFactory.AccessTokenProviderCredentials;
import com.google.cloud.hadoop.util.testing.TestingAccessTokenProvider;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class AccessTokenProviderCredentialsFactoryTest {

  private static final String TEST_PROPERTY_PREFIX = "test.prefix";

  private Configuration config;

  @Before
  public void setUp() {
    config = new Configuration();
    config.setClass(
        TEST_PROPERTY_PREFIX + ACCESS_TOKEN_PROVIDER_IMPL_SUFFIX.getKey(),
        TestingAccessTokenProvider.class,
        AccessTokenProvider.class);
  }

  @Test
  public void testCreateCredentialsFromProviderClassFactory() throws IOException {
    GoogleCredentials credentials =
        AccessTokenProviderCredentialsFactory.credentials(
            config, ImmutableList.of(TEST_PROPERTY_PREFIX));

    AccessToken accessToken = credentials.getAccessToken();

    assertThat(accessToken).isNotNull();
    assertThat(accessToken.getTokenValue()).isEqualTo(TestingAccessTokenProvider.FAKE_ACCESS_TOKEN);
    assertThat(accessToken.getExpirationTime())
        .isEqualTo(Date.from(TestingAccessTokenProvider.EXPIRATION_TIME));
  }

  @Test
  public void testCreateCredentialsFromAccessTokenProvider() {
    AccessTokenProvider accessTokenProvider = new TestingAccessTokenProvider();
    GoogleCredentials credentials = new AccessTokenProviderCredentials(accessTokenProvider);

    AccessToken accessToken = credentials.getAccessToken();
    assertThat(accessToken).isNotNull();
    assertThat(accessToken.getTokenValue()).isEqualTo(TestingAccessTokenProvider.FAKE_ACCESS_TOKEN);
    assertThat(accessToken.getExpirationTime())
        .isEqualTo(Date.from(TestingAccessTokenProvider.EXPIRATION_TIME));
  }
}
