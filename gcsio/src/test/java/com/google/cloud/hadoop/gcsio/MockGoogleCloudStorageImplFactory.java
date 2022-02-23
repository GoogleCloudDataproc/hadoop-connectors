package com.google.cloud.hadoop.gcsio;

import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.storage.Storage;
import com.google.cloud.hadoop.util.RetryHttpInitializer;
import com.google.cloud.hadoop.util.RetryHttpInitializerOptions;
import com.google.cloud.hadoop.util.testing.FakeCredentials;

class MockGoogleCloudStorageImplFactory {

  private static final String PROJECT_ID = "google.com:foo-project";

  static GoogleCloudStorageImpl mockedGcs(HttpTransport transport) {
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
    return new GoogleCloudStorageImpl(
        GoogleCloudStorageOptions.builder()
            .setAppName("gcsio-unit-test")
            .setProjectId(PROJECT_ID)
            .build(),
        storage,
        null);
  }
}
