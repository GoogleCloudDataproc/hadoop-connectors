/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.gcsio;

import static com.google.common.truth.Truth.assertThat;

import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.services.storage.Storage;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unit tests for universe-domain handling in {@link GoogleCloudStorageImpl#createApiaryStorage}.
 */
@RunWith(JUnit4.class)
public class GoogleCloudStorageImplUniverseDomainTest {

  private static final String APP_NAME = "gcs-connector-test";

  private static GoogleCloudStorageOptions.Builder optionsBuilder() {
    return GoogleCloudStorageOptions.builder().setAppName(APP_NAME);
  }

  private static Storage createStorage(GoogleCloudStorageOptions options) {
    return GoogleCloudStorageImpl.createApiaryStorage(
        options, new MockHttpTransport(), /* httpRequestInitializer= */ null);
  }

  @Test
  public void noUniverseDomain_usesDefaultGoogleEndpoint() {
    Storage storage = createStorage(optionsBuilder().build());

    assertThat(storage.getUniverseDomain()).isEqualTo("googleapis.com");
    assertThat(storage.getBaseUrl()).isEqualTo("https://storage.googleapis.com/storage/v1/");
  }

  @Test
  public void universeDomain_derivesEndpointFromUniverseDomain() {
    Storage storage = createStorage(optionsBuilder().setUniverseDomain("my-universe.com").build());

    assertThat(storage.getUniverseDomain()).isEqualTo("my-universe.com");
    assertThat(storage.getRootUrl()).isEqualTo("https://storage.my-universe.com/");
    assertThat(storage.getBaseUrl()).isEqualTo("https://storage.my-universe.com/storage/v1/");
  }

  @Test
  public void customRootUrl_overridesUniverseDomainEndpoint() {
    Storage storage =
        createStorage(
            optionsBuilder()
                .setUniverseDomain("my-universe.com")
                .setStorageRootUrl("https://custom-endpoint.example.com/")
                .build());

    // An explicitly configured endpoint takes precedence over the universe-domain-derived one,
    // while the universe domain is still reported (and used for credential validation).
    assertThat(storage.getUniverseDomain()).isEqualTo("my-universe.com");
    assertThat(storage.getRootUrl()).isEqualTo("https://custom-endpoint.example.com/");
  }

  @Test
  public void customRootUrl_withoutUniverseDomain_isHonored() {
    Storage storage =
        createStorage(
            optionsBuilder().setStorageRootUrl("https://custom-endpoint.example.com/").build());

    assertThat(storage.getRootUrl()).isEqualTo("https://custom-endpoint.example.com/");
  }
}
