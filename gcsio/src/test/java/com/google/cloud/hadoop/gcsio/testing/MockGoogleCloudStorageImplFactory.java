package com.google.cloud.hadoop.gcsio.testing;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.testing.auth.oauth2.MockGoogleCredential;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.storage.Storage;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageClientImpl;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer;
import com.google.cloud.hadoop.util.RetryHttpInitializer;
import com.google.cloud.hadoop.util.RetryHttpInitializerOptions;
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
                    new MockGoogleCredential.Builder().build(),
                    RetryHttpInitializerOptions.builder()
                        .setDefaultUserAgent("gcs-io-unit-test")
                        .build()),
                false));
    return new GoogleCloudStorageImpl(options, storage, null);
  }

  public static GoogleCloudStorageClientImpl mockedGcsClientImpl(
      HttpTransport transport, com.google.cloud.storage.Storage storage) throws IOException {
    return mockedGcsClientImpl(
        GoogleCloudStorageOptions.builder()
            .setAppName("gcsio-unit-test")
            .setProjectId(PROJECT_ID)
            .build(),
        transport,
        storage);
  }

  public static GoogleCloudStorageClientImpl mockedGcsClientImpl(
      GoogleCloudStorageOptions options,
      HttpTransport transport,
      com.google.cloud.storage.Storage storage)
      throws IOException {
    Credential fakeCredential = new MockGoogleCredential.Builder().build();
    com.google.api.services.storage.Storage apiaryStorage =
        new com.google.api.services.storage.Storage(
            transport,
            GsonFactory.getDefaultInstance(),
            new TrackingHttpRequestInitializer(
                new RetryHttpInitializer(
                    fakeCredential,
                    RetryHttpInitializerOptions.builder()
                        .setDefaultUserAgent("gcs-io-unit-test")
                        .build()),
                false));
    return GoogleCloudStorageClientImpl.builder()
        .setOptions(options)
        .setApiaryClientStorage(apiaryStorage)
        .setClientLibraryStorage(storage)
        .build();
  }
}
