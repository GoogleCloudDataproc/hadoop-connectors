package com.google.cloud.hadoop.gcsio.authorization;

import java.io.IOException;
import java.util.List;

public interface StorageAccessTokenProvider {

  /** An access token and its expiration time. */
  class AccessToken {

    private final String token;
    private final Long expirationTimeMilliSeconds;

    public AccessToken(String token, Long expirationTimeMillis) {
      this.token = token;
      this.expirationTimeMilliSeconds = expirationTimeMillis;
    }

    /** @return the Access Token string. */
    public String getToken() {
      return token;
    }

    /** @return the Time when the token will expire, expressed in milliseconds. */
    public Long getExpirationTimeMilliSeconds() {
      return expirationTimeMilliSeconds;
    }
  }

  /**
   * @param storageRequest a storage request holding the context.
   * @return an access token.
   */
  AccessToken getAccessToken(List<GcsResourceAndAction> storageRequest);

  /**
   * Force this provider to refresh its access token.
   *
   * @param storageRequest a storage request holding the context.
   * @throws IOException when refresh fails.
   */
  void refresh(List<GcsResourceAndAction> storageRequest) throws IOException;
}
