/*
 * Copyright 2022 Google LLC
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

import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.cloud.hadoop.util.RetryHttpInitializer;
import com.google.cloud.hadoop.util.RetryHttpInitializerOptions;
import com.google.cloud.hadoop.util.testing.FakeCredentials;
import com.google.cloud.storage.Storage;
import java.io.IOException;

public class MockGoogleCloudStorageImplFactory {

  public static GoogleCloudStorageImpl mockedGcsImpl(HttpTransport transport) throws IOException {
    return mockedGcsImpl(getDefaultOptions(), transport);
  }

  public static GoogleCloudStorageImpl mockedGcsImpl(
      GoogleCloudStorageOptions options, HttpTransport transport) throws IOException {
    return mockedGcsImpl(options, transport, /* httpRequestInitializer= */ null);
  }

  public static GoogleCloudStorageImpl mockedGcsImpl(
      GoogleCloudStorageOptions options,
      HttpTransport transport,
      HttpRequestInitializer httpRequestInitializer)
      throws IOException {
    return GoogleCloudStorageImpl.builder()
        .setFeatureUsageHeader(
            new FeatureUsageHeader(
                GoogleCloudStorageFileSystemOptions.builder()
                    .setCloudStorageOptions(options)
                    .build()))
        .setOptions(options)
        .setCredentials(new FakeCredentials())
        .setHttpTransport(transport)
        .setHttpRequestInitializer(httpRequestInitializer)
        .build();
  }

  public static GoogleCloudStorageClientImpl mockedGcsClientImpl(
      HttpTransport transport, Storage storage) throws IOException {
    return mockedGcsClientImpl(getDefaultOptions(), transport, storage);
  }

  public static GoogleCloudStorageClientImpl mockedGcsClientImpl(
      GoogleCloudStorageOptions options, HttpTransport transport, Storage storage)
      throws IOException {
    FakeCredentials fakeCredentials = new FakeCredentials();
    return GoogleCloudStorageClientImpl.builder()
        .setOptions(options)
        .setHttpTransport(transport)
        .setHttpRequestInitializer(
            new RetryHttpInitializer(
                fakeCredentials,
                RetryHttpInitializerOptions.builder()
                    .setDefaultUserAgent("gcsio-unit-test")
                    .build()))
        .setClientLibraryStorage(storage)
        .setFeatureUsageHeader(
            new FeatureUsageHeader(
                GoogleCloudStorageFileSystemOptions.builder()
                    .setCloudStorageOptions(options)
                    .build()))
        .build();
  }

  private static GoogleCloudStorageOptions getDefaultOptions() {
    return GoogleCloudStorageOptions.builder()
        .setAppName("gcsio-unit-test")
        .setProjectId("google.com:foo-project")
        .build();
  }
}
