/*
 * Copyright 2014 Google Inc. All Rights Reserved.
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

import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.getBucketRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.getRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.resumableUploadChunkRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.resumableUploadRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.rewriteRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.uploadRequestString;
import static com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.assertObjectContent;
import static com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.getStandardOptionBuilder;
import static com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.writeObject;
import static com.google.common.truth.Truth.assertThat;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertThrows;

import com.google.cloud.hadoop.gcsio.CreateBucketOptions;
import com.google.cloud.hadoop.gcsio.CreateObjectOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageItemInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.TestBucketHelper;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.TrackingGoogleCloudStorage;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests that require a particular configuration of GoogleCloudStorageImpl. */
@RunWith(JUnit4.class)
public class GoogleCloudStorageImplTest {

  private static final TestBucketHelper BUCKET_HELPER = new TestBucketHelper("gcs-impl");
  private static final String TEST_BUCKET = BUCKET_HELPER.getUniqueBucketName("test-bucket");

  private static final GoogleCloudStorageOptions GCS_OPTIONS = getStandardOptionBuilder().build();

  private static GoogleCloudStorage helperGcs;

  @Rule public TestName name = new TestName();

  @BeforeClass
  public static void beforeAll() throws IOException {
    helperGcs = GoogleCloudStorageTestHelper.createGoogleCloudStorage();
    helperGcs.createBucket(TEST_BUCKET);
  }

  @AfterClass
  public static void afterAll() throws IOException {
    try {
      BUCKET_HELPER.cleanup(helperGcs);
    } finally {
      helperGcs.close();
    }
  }

  @Test
  public void open_lazyInit_whenFastFailOnNotFound_isFalse() throws IOException {
    int expectedSize = 5 * 1024 * 1024;
    StorageResourceId resourceId = new StorageResourceId(TEST_BUCKET, name.getMethodName());
    writeObject(helperGcs, resourceId, /* partitionSize= */ expectedSize, /* partitionsCount= */ 1);

    TrackingGoogleCloudStorage trackingGcs = new TrackingGoogleCloudStorage(GCS_OPTIONS);

    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setFastFailOnNotFound(false).build();

    try (SeekableByteChannel readChannel = trackingGcs.gcs.open(resourceId, readOptions)) {
      assertThat(readChannel.size()).isEqualTo(expectedSize);
    }

    assertThat(trackingGcs.requestsTracker.getAllRequestStrings())
        .containsExactly(
            getRequestString(
                resourceId.getBucketName(),
                resourceId.getObjectName(),
                /* fields= */ "contentEncoding,generation,size"));
  }

  @Test
  public void writeLargeObject_withSmallUploadChunk() throws IOException {
    StorageResourceId resourceId = new StorageResourceId(TEST_BUCKET, name.getMethodName());

    int uploadChunkSize = 1024 * 1024;
    TrackingGoogleCloudStorage trackingGcs =
        new TrackingGoogleCloudStorage(getOptionsWithUploadChunk(uploadChunkSize));

    int partitionsCount = 32;
    byte[] partition =
        writeObject(
            trackingGcs.gcs, resourceId, /* partitionSize= */ 5 * 1024 * 1024, partitionsCount);

    assertObjectContent(helperGcs, resourceId, partition, partitionsCount);

    assertThat(trackingGcs.requestsTracker.getAllRequestStrings())
        .containsExactlyElementsIn(
            getExpectedRequestsForCreateObject(
                resourceId, uploadChunkSize, partitionsCount, partition))
        .inOrder();
  }

  @Test
  public void writeObject_withNonAlignedUploadChunk() throws IOException {
    StorageResourceId resourceId = new StorageResourceId(TEST_BUCKET, name.getMethodName());

    int uploadChunkSize = 3 * 1024 * 1024;
    TrackingGoogleCloudStorage trackingGcs =
        new TrackingGoogleCloudStorage(getOptionsWithUploadChunk(uploadChunkSize));

    int partitionsCount = 17;
    byte[] partition =
        writeObject(trackingGcs.gcs, resourceId, /* partitionSize= */ 1024 * 1024, partitionsCount);

    assertObjectContent(helperGcs, resourceId, partition, partitionsCount);

    assertThat(trackingGcs.requestsTracker.getAllRequestStrings())
        .containsExactlyElementsIn(
            getExpectedRequestsForCreateObject(
                resourceId, uploadChunkSize, partitionsCount, partition))
        .inOrder();
  }

  @Test
  public void conflictingWrites_noOverwrite_lastFails() throws IOException {
    StorageResourceId resourceId = new StorageResourceId(TEST_BUCKET, name.getMethodName());
    TrackingGoogleCloudStorage trackingGcs = new TrackingGoogleCloudStorage(GCS_OPTIONS);

    byte[] bytesToWrite = new byte[1024];
    GoogleCloudStorageTestHelper.fillBytes(bytesToWrite);

    WritableByteChannel channel1 =
        trackingGcs.gcs.create(resourceId, CreateObjectOptions.DEFAULT_NO_OVERWRITE);
    channel1.write(ByteBuffer.wrap(bytesToWrite));

    // Creating this channel should succeed. Only when we close will an error bubble up.
    WritableByteChannel channel2 =
        trackingGcs.gcs.create(resourceId, CreateObjectOptions.DEFAULT_NO_OVERWRITE);

    channel1.close();

    // Closing byte channel2 should fail:
    Throwable thrown = assertThrows(Throwable.class, channel2::close);
    assertThat(thrown).hasCauseThat().hasMessageThat().contains("412 Precondition Failed");

    assertObjectContent(helperGcs, resourceId, bytesToWrite, /* partitionsCount= */ 1);

    assertThat(trackingGcs.requestsTracker.getAllRequestStrings())
        .containsExactly(
            getRequestString(resourceId.getBucketName(), resourceId.getObjectName()),
            getRequestString(resourceId.getBucketName(), resourceId.getObjectName()),
            resumableUploadRequestString(
                resourceId.getBucketName(),
                resourceId.getObjectName(),
                /* generationId= */ 1,
                /* replaceGenerationId= */ true),
            resumableUploadRequestString(
                resourceId.getBucketName(),
                resourceId.getObjectName(),
                /* generationId= */ 2,
                /* replaceGenerationId= */ true),
            resumableUploadChunkRequestString(
                resourceId.getBucketName(),
                resourceId.getObjectName(),
                /* generationId= */ 3,
                /* replaceGenerationId= */ 1),
            resumableUploadChunkRequestString(
                resourceId.getBucketName(),
                resourceId.getObjectName(),
                /* generationId= */ 4,
                /* replaceGenerationId= */ 2));
  }

  @Test
  public void create_doesNotRepairImplicitDirectories() throws IOException {
    String testDirectory = name.getMethodName();
    StorageResourceId resourceId = new StorageResourceId(TEST_BUCKET, testDirectory + "/obj");
    TrackingGoogleCloudStorage trackingGcs = new TrackingGoogleCloudStorage(GCS_OPTIONS);

    trackingGcs.gcs.createEmptyObject(resourceId);

    // Verify that explicit directory object does not exist
    GoogleCloudStorageItemInfo itemInfo =
        helperGcs.getItemInfo(new StorageResourceId(TEST_BUCKET, testDirectory + "/"));
    assertThat(itemInfo.exists()).isFalse();

    // Verify that directory object not listed
    List<GoogleCloudStorageItemInfo> listedItems =
        helperGcs.listObjectInfo(TEST_BUCKET, testDirectory + "/");
    assertThat(listedItems.stream().map(GoogleCloudStorageItemInfo::getResourceId).toArray())
        .asList()
        .containsExactly(resourceId);

    assertThat(trackingGcs.requestsTracker.getAllRequestStrings())
        .containsExactly(
            uploadRequestString(
                resourceId.getBucketName(), resourceId.getObjectName(), /* generationId= */ null));
  }

  @Test
  public void create_correctlySetsContentType() throws IOException {
    StorageResourceId resourceId1 =
        new StorageResourceId(TEST_BUCKET, name.getMethodName() + "_obj1");
    StorageResourceId resourceId2 =
        new StorageResourceId(TEST_BUCKET, name.getMethodName() + "_obj2");
    StorageResourceId resourceId3 =
        new StorageResourceId(TEST_BUCKET, name.getMethodName() + "obj3");

    TrackingGoogleCloudStorage trackingGcs = new TrackingGoogleCloudStorage(GCS_OPTIONS);

    trackingGcs.gcs.createEmptyObject(
        resourceId1, CreateObjectOptions.builder().setContentType("text/plain").build());
    trackingGcs
        .gcs
        .create(resourceId2, CreateObjectOptions.builder().setContentType("image/png").build())
        .close();
    // default content-type: "application/octet-stream"
    trackingGcs.gcs.create(resourceId3).close();

    assertThat(
            helperGcs.getItemInfos(ImmutableList.of(resourceId1, resourceId2, resourceId3)).stream()
                .map(GoogleCloudStorageItemInfo::getContentType)
                .toArray())
        .asList()
        .containsExactly("text/plain", "image/png", "application/octet-stream")
        .inOrder();

    assertThat(trackingGcs.requestsTracker.getAllRequestStrings())
        .containsExactly(
            uploadRequestString(
                resourceId1.getBucketName(), resourceId1.getObjectName(), /* generationId= */ 1),
            getRequestString(resourceId2.getBucketName(), resourceId2.getObjectName()),
            resumableUploadRequestString(
                resourceId2.getBucketName(),
                resourceId2.getObjectName(),
                /* generationId= */ 2,
                /* replaceGenerationId= */ true),
            resumableUploadChunkRequestString(
                resourceId2.getBucketName(),
                resourceId2.getObjectName(),
                /* generationId= */ 3,
                /* uploadId= */ 1),
            getRequestString(resourceId3.getBucketName(), resourceId3.getObjectName()),
            resumableUploadRequestString(
                resourceId3.getBucketName(),
                resourceId3.getObjectName(),
                /* generationId= */ 4,
                /* replaceGenerationId= */ true),
            resumableUploadChunkRequestString(
                resourceId3.getBucketName(),
                resourceId3.getObjectName(),
                /* generationId= */ 5,
                /* uploadId= */ 2));
  }

  @Test
  public void copy_withRewrite_multipleRequests() throws IOException {
    int maxBytesRewrittenPerCall = 256 * 1024 * 1024;
    TrackingGoogleCloudStorage trackingGcs =
        new TrackingGoogleCloudStorage(
            GoogleCloudStorageTestHelper.getStandardOptionBuilder()
                .setCopyWithRewriteEnabled(true)
                .setMaxBytesRewrittenPerCall(maxBytesRewrittenPerCall)
                .build());

    String srcBucketName = TEST_BUCKET;
    StorageResourceId resourceId =
        new StorageResourceId(srcBucketName, name.getMethodName() + "_src");

    String dstBucketName = BUCKET_HELPER.getUniqueBucketName("copy-with-rewrite-dst");
    // Create destination bucket with different storage class,
    // because this is supported only by rewrite but not copy requests
    helperGcs.createBucket(
        dstBucketName, CreateBucketOptions.builder().setStorageClass("nearline").build());
    StorageResourceId copiedResourceId =
        new StorageResourceId(dstBucketName, name.getMethodName() + "_dst");

    int partitionsCount = 10;
    byte[] partition =
        writeObject(helperGcs, resourceId, /* partitionSize= */ 64 * 1024 * 1024, partitionsCount);

    trackingGcs.gcs.copy(
        srcBucketName, ImmutableList.of(resourceId.getObjectName()),
        dstBucketName, ImmutableList.of(copiedResourceId.getObjectName()));

    assertObjectContent(helperGcs, copiedResourceId, partition, partitionsCount);

    assertThat(trackingGcs.requestsTracker.getAllRequestStrings())
        .containsExactly(
            getBucketRequestString(resourceId.getBucketName()),
            getBucketRequestString(copiedResourceId.getBucketName()),
            rewriteRequestString(
                resourceId.getBucketName(),
                resourceId.getObjectName(),
                copiedResourceId.getBucketName(),
                copiedResourceId.getObjectName(),
                maxBytesRewrittenPerCall,
                /* rewriteTokenId= */ null),
            rewriteRequestString(
                resourceId.getBucketName(),
                resourceId.getObjectName(),
                copiedResourceId.getBucketName(),
                copiedResourceId.getObjectName(),
                maxBytesRewrittenPerCall,
                /* rewriteTokenId= */ 1),
            rewriteRequestString(
                resourceId.getBucketName(),
                resourceId.getObjectName(),
                copiedResourceId.getBucketName(),
                copiedResourceId.getObjectName(),
                maxBytesRewrittenPerCall,
                /* rewriteTokenId= */ 2));
  }

  @Test
  public void create_gcsItemInfo_metadataEquals() throws IOException {
    StorageResourceId resourceId = new StorageResourceId(TEST_BUCKET, name.getMethodName());
    TrackingGoogleCloudStorage trackingGcs = new TrackingGoogleCloudStorage(GCS_OPTIONS);

    Map<String, byte[]> expectedMetadata =
        ImmutableMap.of(
            "key1", "value1".getBytes(StandardCharsets.UTF_8),
            "key2", "value2".getBytes(StandardCharsets.UTF_8));
    Map<String, byte[]> wrongMetadata =
        ImmutableMap.of(
            "key", "value1".getBytes(StandardCharsets.UTF_8),
            "key2", "value2".getBytes(StandardCharsets.UTF_8));

    trackingGcs.gcs.createEmptyObject(
        resourceId, CreateObjectOptions.builder().setMetadata(expectedMetadata).build());

    GoogleCloudStorageItemInfo itemInfo = helperGcs.getItemInfo(resourceId);

    assertThat(itemInfo.metadataEquals(expectedMetadata)).isTrue();
    assertThat(itemInfo.metadataEquals(itemInfo.getMetadata())).isTrue();
    assertThat(itemInfo.metadataEquals(wrongMetadata)).isFalse();

    assertThat(trackingGcs.requestsTracker.getAllRequestStrings())
        .containsExactly(
            uploadRequestString(
                resourceId.getBucketName(), resourceId.getObjectName(), /* generationId= */ 1));
  }

  private static GoogleCloudStorageOptions getOptionsWithUploadChunk(int uploadChunk) {
    return GoogleCloudStorageTestHelper.getStandardOptionBuilder()
        .setWriteChannelOptions(
            AsyncWriteChannelOptions.builder().setUploadChunkSize(uploadChunk).build())
        .build();
  }

  private static List<String> getExpectedRequestsForCreateObject(
      StorageResourceId resourceId, int uploadChunkSize, int partitionsCount, byte[] partition) {
    return ImmutableList.<String>builder()
        .add(getRequestString(resourceId.getBucketName(), resourceId.getObjectName()))
        .add(
            resumableUploadRequestString(
                resourceId.getBucketName(),
                resourceId.getObjectName(),
                /* generationId= */ 1,
                /* replaceGenerationId= */ true))
        .addAll(
            IntStream.rangeClosed(
                    1,
                    (int) Math.ceil((double) partition.length * partitionsCount / uploadChunkSize))
                .mapToObj(
                    i ->
                        resumableUploadChunkRequestString(
                            resourceId.getBucketName(),
                            resourceId.getObjectName(),
                            /* generationId= */ i + 1,
                            /* uploadId= */ i))
                .collect(toList()))
        .build();
  }
}
