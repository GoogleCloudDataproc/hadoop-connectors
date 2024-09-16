package com.google.cloud.hadoop.gcsio;

import static com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.getCredential;
import static com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.getCredentials;
import static com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.getStandardOptionBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import com.google.api.services.storage.StorageScopes;
import com.google.auth.Credentials;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.hadoop.gcsio.testing.TestConfiguration;
import com.google.common.collect.ImmutableMap;
import java.io.FileInputStream;
import java.io.IOException;
import javax.annotation.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for GoogleCloudStorageClientImpl caching experiment. */
@RunWith(JUnit4.class)
public class GoogleCloudStorageJavaStorageClientCachingTest {

  @Test
  public void reusesCachedStorageClient_experimentEnabled() throws IOException {
    GoogleCloudStorageClientImpl test_gcs1 = createGcsClient(null, true);
    GoogleCloudStorageClientImpl test_gcs2 = createGcsClient(null, true);

    assertEquals(test_gcs1.storage, test_gcs2.storage);
  }

  @Test
  public void createsNewClient_experimentEnabled() throws IOException {
    GoogleCloudStorageOptions.Builder testOptionsBuilder =
        getStandardOptionBuilder()
            .setTraceLogEnabled(true)
            .setHttpRequestHeaders((ImmutableMap.of("header-key", "header-value")));
    GoogleCloudStorageClientImpl test_gcs1 = createGcsClient(testOptionsBuilder, true);
    GoogleCloudStorageClientImpl test_gcs2 = createGcsClient(null, true);

    assertNotEquals(test_gcs1.storage, test_gcs2.storage);
  }

  @Test
  public void createsNewClient_experimentDisabled() throws IOException {
    GoogleCloudStorageClientImpl test_gcs1 = createGcsClient(null, false);
    GoogleCloudStorageClientImpl test_gcs2 = createGcsClient(null, false);

    assertNotEquals(test_gcs1.storage, test_gcs2.storage);
  }

  private GoogleCloudStorageClientImpl createGcsClient(
      @Nullable GoogleCloudStorageOptions.Builder optionsBuilder, boolean enableCachingExperiment)
      throws IOException {
    if (optionsBuilder == null) {
      optionsBuilder = getStandardOptionBuilder();
    }
    optionsBuilder.setStorageClientCachingExperimentEnabled(enableCachingExperiment);
    return GoogleCloudStorageClientImpl.builder()
        .setOptions(optionsBuilder.build())
        .setCredential(getCredential())
        .setCredentials(getCredentials())
        .build();
  }

  public static Credentials getCredentials() throws IOException {
    String serviceAccountJsonKeyFile =
        TestConfiguration.getInstance().getServiceAccountJsonKeyFile();
    if (serviceAccountJsonKeyFile == null) {
      return ComputeEngineCredentials.create().createScoped(StorageScopes.CLOUD_PLATFORM);
    }
    try (FileInputStream fis = new FileInputStream(serviceAccountJsonKeyFile)) {
      return ServiceAccountCredentials.fromStream(fis).createScoped(StorageScopes.CLOUD_PLATFORM);
    }
  }
}
