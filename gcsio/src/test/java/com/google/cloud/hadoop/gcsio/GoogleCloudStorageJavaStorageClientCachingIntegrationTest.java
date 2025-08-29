package com.google.cloud.hadoop.gcsio;

import static com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.getStandardOptionBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import javax.annotation.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for GoogleCloudStorageClientImpl caching. */
@RunWith(JUnit4.class)
public class GoogleCloudStorageJavaStorageClientCachingIntegrationTest {

  @Test
  public void reusesCachedStorageClient_cacheEnabled() throws IOException {
    GoogleCloudStorageClientImpl test_gcs1 = createGcsClient(null, true);
    GoogleCloudStorageClientImpl test_gcs2 = createGcsClient(null, true);

    assertEquals(test_gcs1.storage, test_gcs2.storage);
  }

  @Test
  public void createsNewClient_cacheEnabled() throws IOException {
    GoogleCloudStorageOptions.Builder testOptionsBuilder =
        getStandardOptionBuilder()
            .setTraceLogEnabled(true)
            .setHttpRequestHeaders((ImmutableMap.of("header-key", "header-value")));
    GoogleCloudStorageClientImpl test_gcs1 = createGcsClient(testOptionsBuilder, true);
    GoogleCloudStorageClientImpl test_gcs2 = createGcsClient(null, true);

    assertNotEquals(test_gcs1.storage, test_gcs2.storage);
  }

  @Test
  public void createsNewClient_cacheDisabled() throws IOException {
    GoogleCloudStorageClientImpl test_gcs1 = createGcsClient(null, false);
    GoogleCloudStorageClientImpl test_gcs2 = createGcsClient(null, false);

    assertNotEquals(test_gcs1.storage, test_gcs2.storage);
  }

  private GoogleCloudStorageClientImpl createGcsClient(
      @Nullable GoogleCloudStorageOptions.Builder optionsBuilder, boolean enableCache)
      throws IOException {
    if (optionsBuilder == null) {
      optionsBuilder = getStandardOptionBuilder();
    }
    optionsBuilder.setStorageClientCachingEnabled(enableCache);
    return GoogleCloudStorageClientImpl.builder()
        .setOptions(optionsBuilder.build())
        .setCredentials(null)
        .setDownscopedAccessTokenFn(ignore -> "testDownscopedAccessToken")
        .build();
  }
}
