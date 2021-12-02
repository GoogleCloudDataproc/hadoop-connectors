/*
 * Copyright 2021 Google LLC. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.util;

import com.google.api.client.auth.oauth2.Credential;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.OAuth2Credentials;
import java.io.IOException;
import java.util.Date;

/** A wrapper for using {@link Credential} with the Google API Client Libraries for Java. */
public class CredentialAdapter extends OAuth2Credentials {

  private final Credential delegate;

  public CredentialAdapter(Credential delegate) {
    this.delegate = delegate;
  }

  @Override
  public AccessToken refreshAccessToken() throws IOException {
    delegate.refreshToken();
    return new AccessToken(
        delegate.getAccessToken(), new Date(delegate.getExpirationTimeMilliseconds()));
  }
}
