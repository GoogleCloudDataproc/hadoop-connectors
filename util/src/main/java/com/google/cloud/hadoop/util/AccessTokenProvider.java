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

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/** A provider to provide access token, and upon access token expiration, the utility to refresh. */
public interface AccessTokenProvider {

  /** Supported access token types. */
  enum AccessTokenType {
    /** The same access token generated for all the GCS requests. */
    GENERIC,

    /** A downscoped access token generated for each request. */
    DOWNSCOPED
  }

  /** An access token and its expiration time. */
  class AccessToken {

    private final String token;
    private final Instant expirationTime;

    public AccessToken(String token, Instant expirationTime) {
      this.token = token;
      this.expirationTime = expirationTime;
    }

    /** @return the Access Token string. */
    public String getToken() {
      return token;
    }

    /** @return the Time when the token will expire, expressed in milliseconds. */
    public Instant getExpirationTime() {
      return expirationTime;
    }
  }

  /** @return an access token type. */
  default AccessTokenType getAccessTokenType() {
    return AccessTokenType.GENERIC;
  }

  /** @return an access token. */
  AccessToken getAccessToken();

  /**
   * @param accessBoundaries access boundaries used to generate a downscoped access token.
   * @return an access token.
   */
  default AccessToken getAccessToken(List<AccessBoundary> accessBoundaries) {
    throw new UnsupportedOperationException("Downscoped access tokens are not supported");
  }

  /**
   * Force this provider to refresh its access token.
   *
   * @throws IOException when refresh fails.
   */
  void refresh() throws IOException;

  /**
   * Set the configuration to be used by this object.
   *
   * @param conf configuration to be used
   */
  void setConf(Iterable<Map.Entry<String, String>> conf);

  /**
   * Return the configuration used by this object.
   *
   * @return Configuration
   */
  Iterable<Map.Entry<String, String>> getConf();
}
