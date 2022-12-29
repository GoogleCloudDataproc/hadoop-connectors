/*
 * Copyright 2022 Google Inc. All Rights Reserved.
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

package com.google.cloud.hadoop.gcsio;

import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.cloud.hadoop.util.RetryHttpInitializer;
import com.google.cloud.hadoop.util.RetryHttpInitializerOptions;
import com.google.cloud.hadoop.util.testing.FakeCredentials;
import java.io.IOException;

public class MockGoogleCloudStorageImplFactory {

  public static GoogleCloudStorageImpl mockedGcs(HttpTransport transport) throws IOException {
    return mockedGcs(getDefaultOptions(), transport);
  }

  public static GoogleCloudStorageImpl mockedGcs(
      GoogleCloudStorageOptions options, HttpTransport transport) throws IOException {
    return mockedGcs(options, transport, /* httpRequestInitializer= */ null);
  }

  public static GoogleCloudStorageImpl mockedGcs(
      GoogleCloudStorageOptions options,
      HttpTransport transport,
      HttpRequestInitializer httpRequestInitializer)
      throws IOException {
    return GoogleCloudStorageImpl.builder()
        .setOptions(options)
        .setCredentials(new FakeCredentials())
        .setHttpTransport(transport)
        .setHttpRequestInitializer(httpRequestInitializer)
        .build();
  }

  public static GcsJavaClientImpl mockedJavaClientGcs(HttpTransport transport) throws IOException {
    return mockedJavaClientGcs(getDefaultOptions(), transport);
  }

  public static GcsJavaClientImpl mockedJavaClientGcs(
      GoogleCloudStorageOptions options, HttpTransport transport) throws IOException {
    return GcsJavaClientImpl.builder()
        .setOptions(options)
        .setHttpTransport(transport)
        .setHttpRequestInitializer(
            new RetryHttpInitializer(
                new FakeCredentials(),
                RetryHttpInitializerOptions.builder()
                    .setDefaultUserAgent("gcsio-unit-test")
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
