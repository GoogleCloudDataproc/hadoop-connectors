package com.google.cloud.hadoop.gcsio.testing;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.testing.auth.oauth2.MockGoogleCredential;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.storage.Storage;
import com.google.cloud.hadoop.gcsio.GcsJavaClientImpl;
import com.google.cloud.hadoop.gcsio.GcsJavaClientImpl.GcsJavaClientImplBuilder;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer;
import com.google.cloud.hadoop.util.RetryHttpInitializer;
import com.google.cloud.hadoop.util.RetryHttpInitializerOptions;
import java.io.IOException;

public class MockGoogleCloudStorageImplFactory {

  private static final String PROJECT_ID = "google.com:foo-project";

  public static GoogleCloudStorageImpl mockedGcs(HttpTransport transport) {
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
    return new GoogleCloudStorageImpl(
        GoogleCloudStorageOptions.builder()
            .setAppName("gcsio-unit-test")
            .setProjectId(PROJECT_ID)
            .build(),
        storage,
        null);
  }

  public static GcsJavaClientImpl mockedJavaClientGcs(HttpTransport transport) throws IOException {
    return mockedJavaClientGcs(
        GoogleCloudStorageOptions.builder()
            .setAppName("gcsio-unit-test")
            .setProjectId(PROJECT_ID)
            .build(),
        transport);
  }

  public static GcsJavaClientImpl mockedJavaClientGcs(
      GoogleCloudStorageOptions options, HttpTransport transport) throws IOException {
    Credential fakeCredential = null;
    Storage storage =
        new Storage(
            transport,
            GsonFactory.getDefaultInstance(),
            new TrackingHttpRequestInitializer(
                new RetryHttpInitializer(
                    fakeCredential,
                    RetryHttpInitializerOptions.builder()
                        .setDefaultUserAgent("gcs-io-unit-test")
                        .build()),
                false));
    return new GcsJavaClientImplBuilder(options, fakeCredential, null)
        .withApairyClientStorage(storage)
        .build();
  }
}
