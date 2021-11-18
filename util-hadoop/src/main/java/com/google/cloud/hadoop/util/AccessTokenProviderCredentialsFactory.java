/*
 * Copyright 2018 Google Inc. All Rights Reserved.
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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import java.io.IOException;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import org.apache.hadoop.conf.Configuration;

/**
 * Given an {@link HadoopCredentialConfiguration#getAccessTokenProviderImplClass(Configuration,
 * String...)} and a Hadoop {@link Configuration}, generate a {@link GoogleCredentials}.
 */
public final class AccessTokenProviderCredentialsFactory {

  /**
   * A wrapper class that exposes a {@link GoogleCredentials} interface using an {@link
   * AccessTokenProvider}.
   */
  static final class AccessTokenProviderCredentials extends GoogleCredentials {
    private final AccessTokenProvider accessTokenProvider;

    AccessTokenProviderCredentials(AccessTokenProvider accessTokenProvider) {
      super(convertAccessToken(accessTokenProvider.getAccessToken()));
      this.accessTokenProvider = accessTokenProvider;
    }

    private static AccessToken convertAccessToken(AccessTokenProvider.AccessToken accessToken) {
      checkNotNull(accessToken, "AccessToken cannot be null!");
      String token = checkNotNull(accessToken.getToken(), "AccessToken value cannot be null!");
      Instant expirationTime = accessToken.getExpirationTime();
      return new AccessToken(token, expirationTime == null ? null : Date.from(expirationTime));
    }

    @Override
    public AccessToken refreshAccessToken() throws IOException {
      accessTokenProvider.refresh();
      return convertAccessToken(accessTokenProvider.getAccessToken());
    }
  }

  /** Generate the credential from the {@link AccessTokenProvider}. */
  public static GoogleCredentials credentials(AccessTokenProvider provider) {
    return provider == null
        ? null
        : new AccessTokenProviderCredentials(provider)
            .createScoped("https://www.googleapis.com/auth/cloud-platform");
  }

  /**
   * Generate the credential.
   *
   * <p>If the {@link HadoopCredentialConfiguration#getAccessTokenProviderImplClass(Configuration,
   * String...)} generates no Class for the provider, return {@code null}.
   */
  public static GoogleCredentials credentials(Configuration config, List<String> keyPrefixes)
      throws IOException {
    return credentials(HadoopCredentialConfiguration.getAccessTokenProvider(config, keyPrefixes));
  }
}
