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

import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.listRequestWithTrailingDelimiter;
import static com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.getStandardOptionBuilder;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.truth.Truth.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertThrows;

import com.google.api.client.auth.oauth2.Credential;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions.ClientType;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper;
import com.google.cloud.hadoop.util.CredentialAdapter;
import com.google.cloud.hadoop.util.RetryHttpInitializer;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.List;
import java.util.zip.GZIPOutputStream;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Integration tests for GoogleCloudStorageFileSystem. GoogleCloudStorageFileSystemIntegrationHelper
 * is configured to use java-storage client to connect to GCS Server.
 */
@RunWith(JUnit4.class)
public class GoogleCloudStorageJavaStorageNewIntegrationTest
    extends GoogleCloudStorageNewIntegrationTestBase {

  @BeforeClass
  public static void beforeClass() throws Throwable {
    Credential credential =
        checkNotNull(GoogleCloudStorageTestHelper.getCredential(), "credential must not be null");

    gcsOptions =
        getStandardOptionBuilder().setBatchThreads(0).setCopyWithRewriteEnabled(false).build();
    httpRequestsInitializer =
        new RetryHttpInitializer(credential, gcsOptions.toRetryHttpInitializerOptions());

    GoogleCloudStorageFileSystem gcsfs =
        new GoogleCloudStorageFileSystem(
            credential,
            GoogleCloudStorageFileSystemOptions.builder()
                .setBucketDeleteEnabled(true)
                .setCloudStorageOptions(gcsOptions)
                .setClientType(ClientType.STORAGE_CLIENT)
                .build());
    gcsfsIHelper = new GoogleCloudStorageFileSystemIntegrationHelper(gcsfs);
    gcsfsIHelper.beforeAllTests();
  }

  @AfterClass
  public static void afterClass() {
    gcsfsIHelper.afterAllTests();
  }

  protected GoogleCloudStorage createGoogleCloudStorage(GoogleCloudStorageOptions options)
      throws IOException {
    return GoogleCloudStorageClientImpl.builder()
        .setOptions(options)
        .setCredential(httpRequestsInitializer.getCredential())
        .setCredentials(new CredentialAdapter(httpRequestsInitializer.getCredential()))
        .setHttpRequestInitializer(gcsRequestsTracker)
        .build();
  }

  @Override
  @Test
  public void listObjectInfo_allMetadataFieldsCorrect() throws Exception {
    GoogleCloudStorage gcs = createGoogleCloudStorage(gcsOptions);

    String testDirName = name.getMethodName() + "/";
    StorageResourceId objectId =
        new StorageResourceId(gcsfsIHelper.sharedBucketName1, testDirName + "object");

    // Create gzipped file so Content-Encoding will be not null
    CreateObjectOptions createOptions =
        GZIP_CREATE_OPTIONS
            .toBuilder()
            .setMetadata(ImmutableMap.of("test-key", "val".getBytes(UTF_8)))
            .build();

    try (OutputStream os =
        new GZIPOutputStream(
            Channels.newOutputStream(gcsfsIHelper.gcs.create(objectId, createOptions)))) {
      os.write((objectId + "-content").getBytes(UTF_8));
    }

    List<GoogleCloudStorageItemInfo> listedObjects =
        gcs.listObjectInfo(objectId.getBucketName(), testDirName);

    assertThat(getObjectNames(listedObjects)).containsExactly(objectId.getObjectName());
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            listRequestWithTrailingDelimiter(
                objectId.getBucketName(), testDirName, /* pageToken= */ null));
  }

  @Test
  public void create_gzipEncodedFile() throws Exception {
    String testBucket = gcsfsIHelper.sharedBucketName1;
    StorageResourceId testFile = new StorageResourceId(testBucket, getTestResource());

    GoogleCloudStorage gcs = createGoogleCloudStorage(gcsOptions);

    try (OutputStream os =
        new GZIPOutputStream(Channels.newOutputStream(gcs.create(testFile, GZIP_CREATE_OPTIONS)))) {
      os.write("content".getBytes(UTF_8));
    }

    assertThat(gcs.getItemInfo(testFile).getContentEncoding()).isEqualTo("gzip");
  }

  @Test
  public void open_gzipEncoded_fails() throws Exception {
    String testBucket = gcsfsIHelper.sharedBucketName1;
    StorageResourceId testFile = new StorageResourceId(testBucket, getTestResource());

    try (OutputStream os =
        new GZIPOutputStream(
            Channels.newOutputStream(gcsfsIHelper.gcs.create(testFile, GZIP_CREATE_OPTIONS)))) {
      os.write("content".getBytes(UTF_8));
    }

    GoogleCloudStorage gcs = createGoogleCloudStorage(gcsOptions);

    GoogleCloudStorageReadOptions readOptions = GoogleCloudStorageReadOptions.builder().build();
    IOException e = assertThrows(IOException.class, () -> gcs.open(testFile, readOptions));
    assertThat(e)
        .hasMessageThat()
        .startsWith("Cannot read GZIP-encoded file (gzip) (not supported via gRPC API):");
  }

  @Ignore("Gzip content read is is not supported via Java-storage yet.")
  @Test
  public void open_gzipEncoded_fails_ifContentEncodingSupportDisabled() {}

  @Ignore("Gzip content read is is not supported via Java-storage yet.")
  @Test
  public void open_itemInfo_gzipEncoded_fails_ifContentEncodingSupportDisabled() {}

  @Ignore("Gzip content read is is not supported via Java-storage yet.")
  @Test
  public void open_gzipEncoded_succeeds_ifContentEncodingSupportEnabled() {}

  @Ignore("Gzip content read is is not supported via Java-storage yet.")
  @Test
  public void open_itemInfo_gzipEncoded_succeeds_ifContentEncodingSupportEnabled() {}
}
