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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.auth.oauth2.TokenResponse;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.util.Clock;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.conf.Configuration;

/**
 * Given an {@link HadoopCredentialConfiguration#getAccessTokenProviderImplClass(Configuration,
 * String...)} and a Hadoop {@link Configuration}, generate a {@link Credential}.
 */
public final class CredentialFromAccessTokenProviderClassFactory {

  /**
   * A wrapper class that exposes a {@link GoogleCredential} interface using an {@link
   * AccessTokenProvider}.
   */
  static final class GoogleCredentialWithAccessTokenProvider extends GoogleCredential {
    private final Clock clock;
    private final AccessTokenProvider accessTokenProvider;

    private GoogleCredentialWithAccessTokenProvider(
        Clock clock, AccessTokenProvider accessTokenProvider) {
      this.clock = clock;
      this.accessTokenProvider = accessTokenProvider;
    }

    static GoogleCredential fromAccessTokenProvider(
        Clock clock, AccessTokenProvider accessTokenProvider) {
      AccessTokenProvider.AccessToken accessToken =
          checkNotNull(accessTokenProvider.getAccessToken(), "Access Token cannot be null!");

      // TODO: This should be setting the refresh token as well.
      return new GoogleCredentialWithAccessTokenProvider(clock, accessTokenProvider)
          .setAccessToken(accessToken.getToken())
          .setExpirationTimeMilliseconds(accessToken.getExpirationTimeMilliSeconds());
    }

    @Override
    protected TokenResponse executeRefreshToken() throws IOException {
      accessTokenProvider.refresh();
      AccessTokenProvider.AccessToken accessToken =
          checkNotNull(accessTokenProvider.getAccessToken(), "Access Token cannot be null!");

      String token = checkNotNull(accessToken.getToken(), "Access Token cannot be null!");
      Long expirationTimeMilliSeconds = accessToken.getExpirationTimeMilliSeconds();
      return new TokenResponse()
          .setAccessToken(token)
          .setExpiresInSeconds(
              expirationTimeMilliSeconds == null
                  ? null
                  : (expirationTimeMilliSeconds - clock.currentTimeMillis()) / 1000);
    }
  }

  /** Generate the credential from the {@link AccessTokenProvider}. */
  public static Credential credential(
      AccessTokenProvider accessTokenProvider, Collection<String> scopes) {
    if (accessTokenProvider != null) {
      return getCredentialFromAccessTokenProvider(accessTokenProvider, scopes);
    }
    return null;
  }

  /**
   * Generate the credential.
   *
   * <p>If the {@link HadoopCredentialConfiguration#getAccessTokenProviderImplClass(Configuration,
   * String...)} generates no Class for the provider, return {@code null}.
   */
  public static Credential credential(
      Configuration config, List<String> keyPrefixes, Collection<String> scopes)
      throws IOException {
    AccessTokenProvider accessTokenProvider =
        HadoopCredentialConfiguration.getAccessTokenProvider(config, keyPrefixes);
    return credential(accessTokenProvider, scopes);
  }

  /** Creates a {@link Credential} based on information from the access token provider. */
  private static Credential getCredentialFromAccessTokenProvider(
      AccessTokenProvider accessTokenProvider, Collection<String> scopes) {
    checkArgument(accessTokenProvider.getAccessToken() != null, "Access Token cannot be null!");
    GoogleCredential credential =
        GoogleCredentialWithAccessTokenProvider.fromAccessTokenProvider(
            Clock.SYSTEM, accessTokenProvider);
    return credential.createScoped(scopes);
  }
}
