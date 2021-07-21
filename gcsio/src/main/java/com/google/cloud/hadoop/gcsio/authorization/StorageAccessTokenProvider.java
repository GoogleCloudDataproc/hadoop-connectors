package com.google.cloud.hadoop.gcsio.authorization;

import com.google.api.services.storage.StorageRequest;
import java.io.IOException;

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
  <RequestT extends StorageRequest<?>> AccessToken getAccessToken(RequestT storageRequest);

  /**
   * Force this provider to refresh its access token.
   *
   * @param storageRequest a storage request holding the context.
   * @throws IOException when refresh fails.
   */
  <RequestT extends StorageRequest<?>> void refresh(RequestT storageRequest) throws IOException;
}
