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

package com.google.cloud.hadoop.gcsio.testing;

import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.storage.Storage;
import com.google.auth.Credentials;
import com.google.cloud.hadoop.gcsio.GcsJavaClientImpl;
import com.google.cloud.hadoop.gcsio.GcsJavaClientImpl.GcsJavaClientImplBuilder;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer;
import com.google.cloud.hadoop.util.RetryHttpInitializer;
import com.google.cloud.hadoop.util.RetryHttpInitializerOptions;
import com.google.cloud.hadoop.util.testing.FakeCredentials;
import java.io.IOException;

public class MockGoogleCloudStorageImplFactory {

  private static final String PROJECT_ID = "google.com:foo-project";

  public static GoogleCloudStorageImpl mockedGcs(HttpTransport transport) {
    return mockedGcs(
        GoogleCloudStorageOptions.builder()
            .setAppName("gcsio-unit-test")
            .setProjectId(PROJECT_ID)
            .build(),
        transport);
  }

  public static GoogleCloudStorageImpl mockedGcs(
      GoogleCloudStorageOptions options, HttpTransport transport) {
    Storage storage =
        new Storage(
            transport,
            GsonFactory.getDefaultInstance(),
            new TrackingHttpRequestInitializer(
                new RetryHttpInitializer(
                    new FakeCredentials(),
                    RetryHttpInitializerOptions.builder()
                        .setDefaultUserAgent("gcs-io-unit-test")
                        .build()),
                false));
    return new GoogleCloudStorageImpl(options, storage, null);
  }

  public static GcsJavaClientImpl mockGcsJavaStorage(
      HttpTransport transport, com.google.cloud.storage.Storage javaClientStorage)
      throws IOException {
    return mockGcsJavaStorage(
        GoogleCloudStorageOptions.builder()
            .setAppName("gcsio-unit-test")
            .setProjectId(PROJECT_ID)
            .setGrpcEnabled(true)
            .build(),
        transport,
        javaClientStorage);
  }

  public static GcsJavaClientImpl mockGcsJavaStorage(
      GoogleCloudStorageOptions options,
      HttpTransport transport,
      com.google.cloud.storage.Storage javaClientStorage)
      throws IOException {
    Credentials fakeCredential = new FakeCredentials();
    Storage storage =
        new Storage(
            transport,
            GsonFactory.getDefaultInstance(),
            new RetryHttpInitializer(
                fakeCredential,
                RetryHttpInitializerOptions.builder()
                    .setDefaultUserAgent("gcs-io-unit-test")
                    .build()));
    return new GcsJavaClientImplBuilder(options, fakeCredential, null)
        .withApairyClientStorage(storage)
        .withJavaClientStorage(javaClientStorage)
        .build();
  }
}
