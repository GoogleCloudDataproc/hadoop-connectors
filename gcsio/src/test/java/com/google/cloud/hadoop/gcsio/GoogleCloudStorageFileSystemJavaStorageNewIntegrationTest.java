/*
 * Copyright 2023 Google LLC
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
 * use java-storage client to connect to gcs server.
 */
// TODO : Wire out these tests from GoogleCloudStorageFileSystemNewIntegrationTestBase. Tests in
//  the base class are tightly coupled with the HTTP client.
@RunWith(JUnit4.class)
public class GoogleCloudStorageFileSystemJavaStorageNewIntegrationTest
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
                .setClientType(ClientType.STORAGE_CLIENT)
                .build());

    gcsfsIHelper = new GoogleCloudStorageFileSystemIntegrationHelper(gcsfs);
    gcsfsIHelper.beforeAllTests();
    isTracingSupported = false;
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
            GoogleCloudStorageClientImpl.builder()
                .setOptions(options)
                .setCredentials(httpRequestsInitializer.getCredentials())
                .setHttpRequestInitializer(gcsRequestsTracker)
                .build(),
        gcsfsOptions);
  }
}
