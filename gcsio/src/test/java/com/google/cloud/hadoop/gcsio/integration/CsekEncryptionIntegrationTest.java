/*
 * Copyright 2020 Google LLC
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

package com.google.cloud.hadoop.gcsio.integration;

import static com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.assertObjectContent;
import static com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.writeObject;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.hadoop.gcsio.CreateObjectOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageGrpcClientImpl;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.TestBucketHelper;
import com.google.cloud.hadoop.util.RedactedString;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.List;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/** CSEK encryption integration tests. */
@RunWith(Parameterized.class)
public class CsekEncryptionIntegrationTest {

  private final TestBucketHelper bucketHelper =
      new TestBucketHelper("dataproc-gcs-csek-encryption");

  @Parameters
  public static Iterable<Boolean> getTesStorageClientImplParameter() {
    return List.of(false, true);
  }

  private final boolean testStorageClientImpl;

  public CsekEncryptionIntegrationTest(boolean tesStorageClientImpl) {
    this.testStorageClientImpl = tesStorageClientImpl;
  }

  @After
  public void after() throws IOException {
    bucketHelper.cleanup(
        makeStorage(GoogleCloudStorageTestHelper.getStandardOptionBuilder().build()));
  }

  @Test
  public void uploadAndGetObject() throws IOException {
    GoogleCloudStorage gcs = makeStorage(getCsekStorageOptions().build());

    String bucketName = bucketHelper.getUniqueBucketName("upload-and-get");
    StorageResourceId resourceId = new StorageResourceId(bucketName, "obj");

    gcs.createBucket(bucketName);
    gcs.createEmptyObject(
        resourceId, CreateObjectOptions.builder().setContentType("text/plain").build());

    assertThat(gcs.getItemInfo(resourceId).getContentType()).isEqualTo("text/plain");
  }

  @Test
  public void uploadAndVerifyContent() throws IOException {
    GoogleCloudStorage gcs = makeStorage(getCsekStorageOptions().build());

    String bucketName = bucketHelper.getUniqueBucketName("upload-and-verify");
    StorageResourceId resourceId = new StorageResourceId(bucketName, "obj");
    gcs.createBucket(bucketName);

    byte[] partition = writeObject(gcs, resourceId, /* partitionSize= */ 1 * 1024 * 1024);

    assertObjectContent(gcs, resourceId, partition);
  }

  @Test
  public void uploadAndReadThrow() throws IOException {
    GoogleCloudStorage gcs = makeStorage(getCsekStorageOptions().build());

    String bucketName = bucketHelper.getUniqueBucketName("upload-and-verify");
    StorageResourceId resourceId = new StorageResourceId(bucketName, "obj");
    gcs.createBucket(bucketName);

    writeObject(gcs, resourceId, /* partitionSize= */ 1 * 1024 * 1024);

    GoogleCloudStorage gcsWithoutEncryption =
        makeStorage(GoogleCloudStorageTestHelper.getStandardOptionBuilder().build());

    SeekableByteChannel readChannel = gcsWithoutEncryption.open(resourceId);
    assertThrows(IOException.class, () -> readChannel.read(ByteBuffer.allocate(1)));
  }

  @Test
  public void rewriteObject() throws IOException {
    GoogleCloudStorage gcs =
        makeStorage(
            getCsekStorageOptions()
                .setCopyWithRewriteEnabled(true)
                .setMaxRewriteChunkSize(512 * 1024 * 1024)
                .build());

    String srcBucketName = bucketHelper.getUniqueBucketName("rewrite-src");
    gcs.createBucket(srcBucketName);

    String dstBucketName = bucketHelper.getUniqueBucketName("rewrite-dst");
    gcs.createBucket(dstBucketName);

    StorageResourceId srcResourceId = new StorageResourceId(srcBucketName, "encryptedObject");
    int partitionsCount = 32;
    byte[] partition =
        writeObject(gcs, srcResourceId, /* partitionSize= */ 64 * 1024 * 1024, partitionsCount);

    StorageResourceId dstResourceId = new StorageResourceId(dstBucketName, "encryptedObject");
    gcs.copy(
        srcBucketName, ImmutableList.of(srcResourceId.getObjectName()),
        dstBucketName, ImmutableList.of(dstResourceId.getObjectName()));

    assertObjectContent(gcs, dstResourceId, partition, partitionsCount);
  }

  private GoogleCloudStorage makeStorage(GoogleCloudStorageOptions options) throws IOException {
    if (testStorageClientImpl) {
      return GoogleCloudStorageGrpcClientImpl.builder()
          .setOptions(options)
          .setCredentials(GoogleCloudStorageTestHelper.getCredentials())
          .build();
    }
    return GoogleCloudStorageImpl.builder()
        .setOptions(options)
        .setCredentials(GoogleCloudStorageTestHelper.getCredentials())
        .build();
  }

  private GoogleCloudStorageOptions.Builder getCsekStorageOptions() {
    return GoogleCloudStorageTestHelper.getStandardOptionBuilder()
        .setEncryptionAlgorithm("AES256")
        .setEncryptionKey(RedactedString.create("CSX19s0epGWZP3h271Idu8xma2WhMuKT8ZisYfcjLM8="))
        .setEncryptionKeyHash(
            RedactedString.create("LpH4y6Bki5zIhYrjGo1J4BuSt12G/1B+n3FwORpdoyQ="));
  }
}
