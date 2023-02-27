package com.google.cloud.hadoop.gcsio;

import static com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.getStandardOptionBuilder;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.truth.Truth.assertThat;

import com.google.auth.Credentials;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions.ClientType;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper;
import com.google.cloud.hadoop.util.RetryHttpInitializer;
import java.io.IOException;
import java.net.URI;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test suites of integration tests for {@link GoogleCloudStorageFileSystemImpl} class configured to
 * use Apiary client to connect to gcs server.
 */
@RunWith(JUnit4.class)
public class GoogleCloudStorageFileSystemHTTPClientNewIntegrationTest
    extends GoogleCloudStorageFileSystemNewIntegrationTestBase {

  @BeforeClass
  public static void beforeClass() throws Throwable {
    Credentials credentials =
        checkNotNull(GoogleCloudStorageTestHelper.getCredentials(), "credentials must not be null");

    gcsOptions =
        getStandardOptionBuilder().setBatchThreads(0).setCopyWithRewriteEnabled(false).build();
    httpRequestsInitializer =
        new RetryHttpInitializer(credentials, gcsOptions.toRetryHttpInitializerOptions());

    GoogleCloudStorageFileSystem gcsfs =
        new GoogleCloudStorageFileSystemImpl(
            credentials,
            GoogleCloudStorageFileSystemOptions.builder()
                .setBucketDeleteEnabled(true)
                .setCloudStorageOptions(gcsOptions)
                .setClientType(ClientType.HTTP_API_CLIENT)
                .build());

    gcsfsIHelper = new GoogleCloudStorageFileSystemIntegrationHelper(gcsfs);
    gcsfsIHelper.beforeAllTests();
  }

  @AfterClass
  public static void afterClass() throws Throwable {
    gcsfsIHelper.afterAllTests();
    GoogleCloudStorageFileSystem gcsfs = gcsfsIHelper.gcsfs;
    assertThat(gcsfs.exists(new URI("gs://" + gcsfsIHelper.sharedBucketName1))).isFalse();
    assertThat(gcsfs.exists(new URI("gs://" + gcsfsIHelper.sharedBucketName2))).isFalse();
  }

  protected GoogleCloudStorageFileSystem newGcsFs(GoogleCloudStorageFileSystemOptions gcsfsOptions)
      throws IOException {
    return new GoogleCloudStorageFileSystemImpl(
        options ->
            GoogleCloudStorageImpl.builder()
                .setOptions(options)
                .setCredentials(httpRequestsInitializer.getCredentials())
                .setHttpRequestInitializer(gcsRequestsTracker)
                .build(),
        gcsfsOptions);
  }
}
