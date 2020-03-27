/*
 * Copyright 2020 Google Inc. All Rights Reserved.
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

package com.google.cloud.hadoop.gcsio.integration;

import static com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.assertObjectContent;
import static com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.writeObject;
import static com.google.common.truth.Truth.assertThat;

import com.google.api.client.auth.oauth2.Credential;
import com.google.cloud.hadoop.gcsio.CreateBucketOptions;
import com.google.cloud.hadoop.gcsio.CreateObjectOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.TestBucketHelper;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests that require a particular configuration of GoogleCloudStorageImpl. */
@RunWith(JUnit4.class)
public class GoogleCloudStorageCsekEncryptionIntegrationTest {

  private static final TestBucketHelper BUCKET_HELPER = new TestBucketHelper("gcs-impl-encryption");

  @AfterClass
  public static void afterAll() throws IOException {
    BUCKET_HELPER.cleanup(
        makeStorage(GoogleCloudStorageTestHelper.getStandardOptionBuilder().build()));
  }

  private static GoogleCloudStorageImpl makeStorage(GoogleCloudStorageOptions options)
      throws IOException {
    Credential credential = GoogleCloudStorageTestHelper.getCredential();
    return new GoogleCloudStorageImpl(options, credential);
  }

  @Test
  public void testUploadAndGetObjectWithEncryptionKey() throws IOException {
    GoogleCloudStorageImpl gcs =
        makeStorage(
            GoogleCloudStorageTestHelper.getStandardOptionBuilder()
                .setEncryptionAlgorithm("AES256")
                .setEncryptionKey("CSX19s0epGWZP3h271Idu8xma2WhMuKT8ZisYfcjLM8=")
                .setEncryptionKeyHash("LpH4y6Bki5zIhYrjGo1J4BuSt12G/1B+n3FwORpdoyQ=")
                .build());

    String bucketName = BUCKET_HELPER.getUniqueBucketName("encryption-bucket");
    StorageResourceId resourceId = new StorageResourceId(bucketName, "obj");

    gcs.create(bucketName);
    gcs.createEmptyObject(
        resourceId,
        new CreateObjectOptions(true, "text/plain", CreateObjectOptions.EMPTY_METADATA));

    assertThat(gcs.getItemInfo(resourceId).getContentType()).isEqualTo("text/plain");
  }

  @Test
  public void testRewriteEncryptedObject() throws IOException {
    GoogleCloudStorageImpl gcs =
        makeStorage(
            GoogleCloudStorageTestHelper.getStandardOptionBuilder()
                .setCopyWithRewriteEnabled(true)
                .setMaxBytesRewrittenPerCall(512 * 1024 * 1024)
                .setEncryptionAlgorithm("AES256")
                .setEncryptionKey("CSX19s0epGWZP3h271Idu8xma2WhMuKT8ZisYfcjLM8=")
                .setEncryptionKeyHash("LpH4y6Bki5zIhYrjGo1J4BuSt12G/1B+n3FwORpdoyQ=")
                .build());

    String srcBucketName = BUCKET_HELPER.getUniqueBucketName("rewrite-encryption-src");
    gcs.create(srcBucketName);

    String dstBucketName = BUCKET_HELPER.getUniqueBucketName("rewrite-encryption-dst");
    // Create destination bucket with different location and storage class,
    // because this is supported by rewrite but not copy requests
    gcs.create(dstBucketName, CreateBucketOptions.builder().setStorageClass("coldline").build());

    StorageResourceId resourceId =
        new StorageResourceId(srcBucketName, "testRewriteEncryptedObject_SourceObject");
    int partitionsCount = 32;
    byte[] partition =
        writeObject(gcs, resourceId, /* partitionSize= */ 64 * 1024 * 1024, partitionsCount);

    StorageResourceId copiedResourceId =
        new StorageResourceId(dstBucketName, "testRewriteEncryptedObject_DestinationObject");
    gcs.copy(
        srcBucketName, ImmutableList.of(resourceId.getObjectName()),
        dstBucketName, ImmutableList.of(copiedResourceId.getObjectName()));

    assertObjectContent(gcs, copiedResourceId, partition, partitionsCount);
  }
}
