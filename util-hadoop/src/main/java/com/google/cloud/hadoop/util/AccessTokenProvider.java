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

import com.google.cloud.hadoop.gcsio.authorization.GcsResourceAndAction;
import com.google.cloud.hadoop.gcsio.authorization.StorageAccessTokenProvider;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configurable;

/** A provider to provide access token, and upon access token expiration, the utility to refresh. */
public interface AccessTokenProvider extends Configurable, StorageAccessTokenProvider {

  /** @return an access token. */
  AccessToken getAccessToken();

  /**
   * @param storageRequest a storage request holding the context.
   * @return an access token.
   */
  default AccessToken getAccessToken(List<GcsResourceAndAction> storageRequest) {
    return this.getAccessToken();
  }

  /**
   * Force this provider to refresh its access token.
   *
   * @throws IOException when refresh fails.
   */
  void refresh() throws IOException;

  /**
   * Force this provider to refresh its access token.
   *
   * @param storageRequest a storage request holding the context.
   * @throws IOException when refresh fails.
   */
  default void refresh(List<GcsResourceAndAction> storageRequest) throws IOException {
    this.refresh();
  }
}
