/*
 * Copyright 2019 Google Inc. All Rights Reserved.
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

package com.google.cloud.hadoop.fs.gcs;

import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_LAZY_INITIALIZATION_ENABLE;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.truth.Truth.assertWithMessage;

import com.google.api.client.auth.oauth2.Credential;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.TestBucketHelper;
import com.google.cloud.hadoop.gcsio.testing.TestConfiguration;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;

public class GoogleHadoopFileSystemIntegrationHelper {

  protected GoogleHadoopFileSystem ghfs;
  protected FileSystemDescriptor ghfsFileSystemDescriptor;

  protected GoogleCloudStorageFileSystem gcsfs;

  // Prefix used for naming test buckets.
  private static final String TEST_BUCKET_NAME_PREFIX = "gcs-test";

  private final TestBucketHelper bucketHelper = new TestBucketHelper(TEST_BUCKET_NAME_PREFIX);

  // Application name for OAuth.
  private static final String APP_NAME = "GCS-test";

  /** Perform clean-up once after all tests are turn. */
  public void after(GoogleCloudStorageFileSystem gcsfs) throws IOException {
    gcsfs.close();
    initializeGcfs().getGcs().close();
  }

  GoogleCloudStorageFileSystem initializeGcfs() throws IOException {
    String projectId =
        checkNotNull(TestConfiguration.getInstance().getProjectId(), "projectId can not be null");
    String appName = APP_NAME;
    Credential credential =
        checkNotNull(GoogleCloudStorageTestHelper.getCredential(), "credential must not be null");

    GoogleCloudStorageOptions gcsOptions =
        GoogleCloudStorageOptions.newBuilder().setAppName(appName).setProjectId(projectId).build();

    return new GoogleCloudStorageFileSystem(
        credential,
        GoogleCloudStorageFileSystemOptions.newBuilder()
            .setEnableBucketDelete(true)
            .setCloudStorageOptionsBuilder(gcsOptions.toBuilder())
            .build());
  }

  GoogleHadoopFileSystem initializeGhfs(GoogleHadoopFileSystem testInstance)
      throws IOException, URISyntaxException {
    ghfs = testInstance;
    Configuration conf = loadConfig();
    conf.set(
        GoogleHadoopFileSystemConfiguration.PATH_CODEC.getKey(),
        GoogleHadoopFileSystemBase.PATH_CODEC_USE_URI_ENCODING);

    URI initUri = new URI("gs://" + getUniqueBucketName("test"));
    ghfs.initialize(initUri, conf);

    if (GCS_LAZY_INITIALIZATION_ENABLE.get(ghfs.getConf(), ghfs.getConf()::getBoolean)) {
      testInstance.getGcsFs();
    }

    return ghfs;
  }

  /**
   * Helper to load all the GHFS-specific config values from environment variables, such as those
   * needed for setting up the credentials of a real GoogleCloudStorage.
   */
  private Configuration loadConfig() {
    TestConfiguration testConfiguration = TestConfiguration.getInstance();

    String projectId = testConfiguration.getProjectId();
    String privateKeyFile = testConfiguration.getPrivateKeyFile();
    String serviceAccount = testConfiguration.getServiceAccount();

    return loadConfig(projectId, serviceAccount, privateKeyFile);
  }

  /** Helper to load GHFS-specific config values other than those from the environment. */
  private Configuration loadConfig(String projectId, String serviceAccount, String privateKeyFile) {
    assertWithMessage("Expected value for env var %s", TestConfiguration.GCS_TEST_PROJECT_ID)
        .that(projectId)
        .isNotNull();
    assertWithMessage("Expected value for env var %s", TestConfiguration.GCS_TEST_SERVICE_ACCOUNT)
        .that(serviceAccount)
        .isNotNull();
    assertWithMessage("Expected value for env var %s", TestConfiguration.GCS_TEST_PRIVATE_KEYFILE)
        .that(privateKeyFile)
        .isNotNull();
    Configuration config = new Configuration();
    config.set(GoogleHadoopFileSystemConfiguration.GCS_PROJECT_ID.getKey(), projectId);
    config.set(
        GoogleHadoopFileSystemConfiguration.AUTH_SERVICE_ACCOUNT_EMAIL.getKey(), serviceAccount);
    config.set(
        GoogleHadoopFileSystemConfiguration.AUTH_SERVICE_ACCOUNT_KEY_FILE.getKey(), privateKeyFile);
    config.setBoolean(
        GoogleHadoopFileSystemConfiguration.GCS_REPAIR_IMPLICIT_DIRECTORIES_ENABLE.getKey(), true);
    config.setBoolean(
        GoogleHadoopFileSystemConfiguration.GCS_INFER_IMPLICIT_DIRECTORIES_ENABLE.getKey(), false);
    // Allow buckets to be deleted in test cleanup:
    config.setBoolean(GoogleHadoopFileSystemConfiguration.GCE_BUCKET_DELETE_ENABLE.getKey(), true);
    return config;
  }

  /**
   * Gets randomly generated name of a bucket.
   *
   * <p>The name is prefixed with an identifiable string. A bucket created by this method can be
   * identified by calling isTestBucketName() for that bucket.
   */
  private String getUniqueBucketName() {
    return getUniqueBucketName("");
  }

  /**
   * Gets randomly generated name of a bucket with the given suffix.
   *
   * <p>The name is prefixed with an identifiable string. A bucket created by this method can be
   * identified by calling isTestBucketName() for that bucket.
   */
  private String getUniqueBucketName(String suffix) {
    return bucketHelper.getUniqueBucketName(suffix);
  }
}
