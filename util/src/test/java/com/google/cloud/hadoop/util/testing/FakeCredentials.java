/*
 * Copyright 2022 Google LLC. All Rights Reserved.
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

package com.google.cloud.hadoop.util.testing;

import com.google.auth.Credentials;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.net.URI;
import java.util.List;
import java.util.Map;

/** Fake {@link Credentials} to use in unit tests */
public class FakeCredentials extends Credentials {

  private final String authHeaderValue;

  public FakeCredentials() {
    this(/* authHeaderValue= */ null);
  }

  public FakeCredentials(String authHeaderValue) {
    this.authHeaderValue = authHeaderValue;
  }

  @Override
  public String getAuthenticationType() {
    return "test-auth";
  }

  @Override
  public Map<String, List<String>> getRequestMetadata(URI uri) {
    return authHeaderValue == null
        ? ImmutableMap.of()
        : ImmutableMap.of("Authorization", ImmutableList.of(authHeaderValue));
  }

  @Override
  public boolean hasRequestMetadata() {
    return true;
  }

  @Override
  public boolean hasRequestMetadataOnly() {
    return true;
  }

  @Override
  public void refresh() {
    throw new UnsupportedOperationException();
  }
}
