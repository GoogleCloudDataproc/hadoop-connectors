/*
 * Copyright 2014 Google Inc.
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

import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.OBJECT_FIELDS;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.getBucketRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.rewriteRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.uploadRequestString;
import static com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.assertObjectContent;
import static com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.getStandardOptionBuilder;
import static com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.writeObject;
import static com.google.common.truth.Truth.assertThat;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertThrows;

import com.google.auth.Credentials;
import com.google.cloud.hadoop.gcsio.AssertingLogHandler;
import com.google.cloud.hadoop.gcsio.CreateBucketOptions;
import com.google.cloud.hadoop.gcsio.CreateObjectOptions;
import com.google.cloud.hadoop.gcsio.EventLoggingHttpRequestInitializer;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageClientImpl;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageItemInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.cloud.hadoop.gcsio.TrackingGrpcRequestInterceptor;
import com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.TestBucketHelper;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.TrackingStorageWrapper;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/** Tests that require a particular configuration of GoogleCloudStorageImpl. */
@RunWith(Parameterized.class)
public class GoogleCloudStorageImplTest {

  private static final TestBucketHelper BUCKET_HELPER = new TestBucketHelper("dataproc-gcs-impl");
  private static final String TEST_BUCKET = BUCKET_HELPER.getUniqueBucketPrefix();

  private static final GoogleCloudStorageOptions GCS_OPTIONS = getStandardOptionBuilder().build();

  private static GoogleCloudStorage helperGcs;

  private final boolean testStorageClientImpl;

  public GoogleCloudStorageImplTest(boolean tesStorageClientImpl) {
    this.testStorageClientImpl = tesStorageClientImpl;
  }

  @Parameters
  public static Iterable<Boolean> getTesStorageClientImplParameter() {
    return List.of(false, true);
  }

  @Rule
  public TestName name =
      new TestName() {
        // With parametrization method name will get [index] appended in their name.
        @Override
        public String getMethodName() {
          return super.getMethodName().replaceAll("[\\[,\\]]", "");
        }
      };

  @Before
  public void before() throws IOException {
    helperGcs =
        testStorageClientImpl
            ? GoogleCloudStorageTestHelper.createGcsClientImpl()
            : GoogleCloudStorageTestHelper.createGoogleCloudStorage();
    helperGcs.createBucket(TEST_BUCKET);
  }

  @After
  public void after() throws IOException {
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

    TrackingStorageWrapper<GoogleCloudStorage> trackingGcs =
        newTrackingGoogleCloudStorage(GCS_OPTIONS);

    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setFastFailOnNotFoundEnabled(false).build();

    try (SeekableByteChannel readChannel = trackingGcs.delegate.open(resourceId, readOptions)) {
      assertThat(readChannel.size()).isEqualTo(expectedSize);
    }
    assertThat(trackingGcs.requestsTracker.getAllRequestInvocationIds().size())
        .isEqualTo(trackingGcs.requestsTracker.getAllRequests().size());

    String filelds = "contentEncoding,generation,size";
    if (testStorageClientImpl) {
      // fail fast is not supported via java-storage as of now, overriding with default values.
      filelds = OBJECT_FIELDS;
    }

    assertThat(trackingGcs.getAllRequestStrings())
        .containsExactly(getObjectRequestString(resourceId, filelds, testStorageClientImpl));
    trackingGcs.delegate.close();
  }

  @Test
  public void open_withItemInfo() throws IOException {
    int expectedSize = 5 * 1024 * 1024;
    StorageResourceId resourceId = new StorageResourceId(TEST_BUCKET, name.getMethodName());
    writeObject(helperGcs, resourceId, /* partitionSize= */ expectedSize, /* partitionsCount= */ 1);

    TrackingStorageWrapper<GoogleCloudStorage> trackingGcs =
        newTrackingGoogleCloudStorage(GCS_OPTIONS);

    GoogleCloudStorageItemInfo itemInfo = helperGcs.getItemInfo(resourceId);

    try (SeekableByteChannel readChannel = trackingGcs.delegate.open(itemInfo)) {
      assertThat(readChannel.size()).isEqualTo(expectedSize);
    }
    assertThat(trackingGcs.requestsTracker.getAllRequestInvocationIds().size())
        .isEqualTo(trackingGcs.requestsTracker.getAllRequests().size());

    assertThat(trackingGcs.getAllRequestStrings()).isEmpty();
    trackingGcs.delegate.close();
  }

  @Test
  public void writeLargeObject_withSmallUploadChunk() throws IOException {
    StorageResourceId resourceId = new StorageResourceId(TEST_BUCKET, name.getMethodName());

    int uploadChunkSize = 2 * 1024 * 1024;
    TrackingStorageWrapper<GoogleCloudStorage> trackingGcs =
        newTrackingGoogleCloudStorage(getOptionsWithUploadChunk(uploadChunkSize));

    int partitionsCount = 1;
    byte[] partition =
        writeObject(
            trackingGcs.delegate,
            resourceId,
            /* partitionSize= */ 5 * 1024 * 1024,
            partitionsCount);

    assertObjectContent(helperGcs, resourceId, partition, partitionsCount);
    assertThat(trackingGcs.requestsTracker.getAllRequestInvocationIds().size())
        .isEqualTo(trackingGcs.requestsTracker.getAllRequests().size());

    assertThat(trackingGcs.getAllRequestStrings())
        .containsExactlyElementsIn(
            getExpectedRequestsForCreateObject(
                resourceId, uploadChunkSize, partitionsCount, partition))
        .inOrder();
    trackingGcs.delegate.close();
  }

  @Test
  public void getStatistics_writeReadDeleteLargeObject() throws IOException {
    StorageResourceId resourceId = new StorageResourceId(TEST_BUCKET, name.getMethodName());

    int uploadChunkSize = 1024 * 1024;
    GoogleCloudStorage gcs = getStorageFromOptions(getOptionsWithUploadChunk(uploadChunkSize));

    byte[] partition = writeObject(gcs, resourceId, /* objectSize= */ 5 * uploadChunkSize);

    if (!testStorageClientImpl) {
      assertThat(gcs.getStatistics())
          .containsExactlyEntriesIn(
              ImmutableMap.<String, Long>builder()
                  .put("http_get_404", 1L)
                  .put("http_post_200", 1L)
                  .put("http_put_200", 1L)
                  .put("http_put_308", 4L)
                  .build());

      assertObjectContent(gcs, resourceId, partition);

      assertThat(gcs.getStatistics())
          .containsExactlyEntriesIn(
              ImmutableMap.<String, Long>builder()
                  .put("http_get_200", 1L)
                  .put("http_get_206", 1L)
                  .put("http_get_404", 1L)
                  .put("http_post_200", 1L)
                  .put("http_put_200", 1L)
                  .put("http_put_308", 4L)
                  .build());

      gcs.deleteObjects(ImmutableList.of(resourceId));

      assertThat(gcs.getStatistics())
          .containsExactlyEntriesIn(
              ImmutableMap.<String, Long>builder()
                  .put("http_delete_204", 1L)
                  .put("http_get_200", 2L)
                  .put("http_get_206", 1L)
                  .put("http_get_404", 1L)
                  .put("http_post_200", 1L)
                  .put("http_put_200", 1L)
                  .put("http_put_308", 4L)
                  .build());
    }
  }

  @Test
  public void writeObject_withNonAlignedUploadChunk() throws IOException {
    StorageResourceId resourceId = new StorageResourceId(TEST_BUCKET, name.getMethodName());

    int uploadChunkSize = 2 * 1024 * 1024;
    TrackingStorageWrapper<GoogleCloudStorage> trackingGcs =
        newTrackingGoogleCloudStorage(getOptionsWithUploadChunk(uploadChunkSize));

    int partitionsCount = 17;
    byte[] partition =
        writeObject(
            trackingGcs.delegate, resourceId, /* partitionSize= */ 1024 * 1024, partitionsCount);

    assertObjectContent(helperGcs, resourceId, partition, partitionsCount);

    assertThat(trackingGcs.requestsTracker.getAllRequestInvocationIds().size())
        .isEqualTo(trackingGcs.requestsTracker.getAllRequests().size());

    assertThat(trackingGcs.getAllRequestStrings())
        .containsExactlyElementsIn(
            getExpectedRequestsForCreateObject(
                resourceId, uploadChunkSize, partitionsCount, partition))
        .inOrder();
    trackingGcs.delegate.close();
  }

  @Test
  public void conflictingWrites_noOverwrite_lastFails() throws IOException {
    StorageResourceId resourceId = new StorageResourceId(TEST_BUCKET, name.getMethodName());
    int uploadChunkSize = 2 * 1024 * 1024;
    TrackingStorageWrapper<GoogleCloudStorage> trackingGcs =
        newTrackingGoogleCloudStorage(getOptionsWithUploadChunk(uploadChunkSize));
    // Have separate request tracker for channels as clubbing them into one will cause flakiness
    // while asserting the order or requests.

    TrackingStorageWrapper<GoogleCloudStorage> trackingGcs2 =
        newTrackingGoogleCloudStorage(getOptionsWithUploadChunk(uploadChunkSize));

    // With Veneer client it will not even open channel unless amount of data written is >=
    // configured chunkSize.
    // To produce the race condition for write bytesToWrite >= chunkSize
    byte[] bytesToWrite = new byte[3 * uploadChunkSize];
    GoogleCloudStorageTestHelper.fillBytes(bytesToWrite);

    WritableByteChannel channel1 =
        trackingGcs.delegate.create(resourceId, CreateObjectOptions.DEFAULT_NO_OVERWRITE);
    channel1.write(ByteBuffer.wrap(bytesToWrite));

    // Creating this channel should succeed. Only when we close will an error bubble up.
    WritableByteChannel channel2 =
        trackingGcs2.delegate.create(resourceId, CreateObjectOptions.DEFAULT_NO_OVERWRITE);

    channel1.close();

    // Closing byte channel2 should fail:
    Throwable thrown = assertThrows(Throwable.class, channel2::close);
    assertThat(thrown)
        .hasCauseThat()
        .hasMessageThat()
        .containsMatch(Pattern.compile("(412 Precondition Failed)|(FAILED_PRECONDITION)"));

    assertObjectContent(helperGcs, resourceId, bytesToWrite, /* expectedBytesCount= */ 1);

    assertThat(trackingGcs.requestsTracker.getAllRequestInvocationIds().size())
        .isEqualTo(trackingGcs.requestsTracker.getAllRequests().size());

    assertThat(trackingGcs.getAllRequestStrings())
        .containsExactlyElementsIn(
            getExpectedRequestsForCreateObject(
                resourceId, uploadChunkSize, /* partitionCount= */ 1, bytesToWrite))
        .inOrder();

    assertThat(trackingGcs2.requestsTracker.getAllRequestInvocationIds().size())
        .isEqualTo(trackingGcs2.requestsTracker.getAllRequests().size());

    assertThat(trackingGcs.getAllRequestStrings())
        .containsExactlyElementsIn(
            getExpectedRequestsForCreateObject(resourceId, uploadChunkSize, 1, bytesToWrite))
        .inOrder();
    trackingGcs.delegate.close();
    trackingGcs2.delegate.close();
  }

  @Test
  public void create_doesNotRepairImplicitDirectories() throws IOException {
    String testDirectory = name.getMethodName();
    StorageResourceId resourceId = new StorageResourceId(TEST_BUCKET, testDirectory + "/obj");
    TrackingStorageWrapper<GoogleCloudStorage> trackingGcs =
        newTrackingGoogleCloudStorage(GCS_OPTIONS);

    trackingGcs.delegate.createEmptyObject(resourceId);

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

    assertThat(trackingGcs.requestsTracker.getAllRequestInvocationIds().size())
        .isEqualTo(trackingGcs.requestsTracker.getAllRequests().size());

    assertThat(trackingGcs.getAllRequestStrings())
        .containsExactly(
            emptyUploadRequestString(
                resourceId.getBucketName(), resourceId.getObjectName(), testStorageClientImpl));
    trackingGcs.delegate.close();
  }

  @Test
  public void create_correctlySetsContentType() throws IOException {
    StorageResourceId resourceId1 =
        new StorageResourceId(TEST_BUCKET, name.getMethodName() + "_obj1");
    StorageResourceId resourceId2 =
        new StorageResourceId(TEST_BUCKET, name.getMethodName() + "_obj2");
    StorageResourceId resourceId3 =
        new StorageResourceId(TEST_BUCKET, name.getMethodName() + "obj3");

    TrackingStorageWrapper<GoogleCloudStorage> trackingGcs =
        newTrackingGoogleCloudStorage(GCS_OPTIONS);

    trackingGcs
        .delegate
        .create(resourceId1, CreateObjectOptions.builder().setContentType("text/plain").build())
        .close();

    trackingGcs
        .delegate
        .create(resourceId2, CreateObjectOptions.builder().setContentType("image/png").build())
        .close();
    // default content-type: "application/octet-stream"
    trackingGcs.delegate.create(resourceId3).close();

    assertThat(
            helperGcs.getItemInfos(ImmutableList.of(resourceId1, resourceId2, resourceId3)).stream()
                .map(GoogleCloudStorageItemInfo::getContentType)
                .toArray())
        .asList()
        .containsExactly("text/plain", "image/png", "application/octet-stream")
        .inOrder();

    assertThat(trackingGcs.requestsTracker.getAllRequestInvocationIds().size())
        .isEqualTo(trackingGcs.requestsTracker.getAllRequests().size());

    assertThat(trackingGcs.getAllRequestStrings())
        .containsExactly(
            ImmutableList.builder()
                .add(getObjectRequestString(resourceId1, OBJECT_FIELDS, testStorageClientImpl))
                .add(
                    resumableUploadRequestString(
                        resourceId1.getBucketName(),
                        resourceId1.getObjectName(),
                        /* generationId= */ 1,
                        /* replaceGenerationId= */ true))
                .addAll(
                    resumableUploadChunkRequestString(
                        resourceId1.getBucketName(),
                        resourceId1.getObjectName(),
                        /* generationId= */ 2,
                        /* uploadId= */ 1,
                        /* writeOffset= */ 0,
                        /* length= */ 0,
                        /* finishWrite */ true))
                .add(getObjectRequestString(resourceId2, OBJECT_FIELDS, testStorageClientImpl))
                .add(
                    resumableUploadRequestString(
                        resourceId2.getBucketName(),
                        resourceId2.getObjectName(),
                        /* generationId= */ 3,
                        /* replaceGenerationId= */ true))
                .addAll(
                    resumableUploadChunkRequestString(
                        resourceId2.getBucketName(),
                        resourceId2.getObjectName(),
                        /* generationId= */ 4,
                        /* uploadId= */ 2,
                        /* writeOffset= */ 0,
                        /* length= */ 0,
                        /* finishWrite */ true))
                .add(getObjectRequestString(resourceId3, OBJECT_FIELDS, testStorageClientImpl))
                .add(
                    resumableUploadRequestString(
                        resourceId3.getBucketName(),
                        resourceId3.getObjectName(),
                        /* generationId= */ 5,
                        /* replaceGenerationId= */ true))
                .addAll(
                    resumableUploadChunkRequestString(
                        resourceId3.getBucketName(),
                        resourceId3.getObjectName(),
                        /* generationId= */ 6,
                        /* uploadId= */ 3,
                        /* writeOffset= */ 0,
                        /* length= */ 0,
                        /* finishWrite */ true))
                .build()
                .toArray());
    trackingGcs.delegate.close();
  }

  @Ignore("Test is failing")
  @Test
  public void copy_withRewrite_multipleRequests() throws IOException {
    int maxRewriteChunkSize = 256 * 1024 * 1024;
    TrackingStorageWrapper<GoogleCloudStorage> trackingGcs =
        newTrackingGoogleCloudStorage(
            getStandardOptionBuilder()
                .setCopyWithRewriteEnabled(true)
                .setMaxRewriteChunkSize(maxRewriteChunkSize)
                .build());

    String srcBucketName = TEST_BUCKET;
    StorageResourceId resourceId =
        new StorageResourceId(srcBucketName, name.getMethodName() + "_src");

    String dstBucketName =
        BUCKET_HELPER.getUniqueBucketName(
            UUID.randomUUID().toString().replaceAll("-", "").substring(0, 10));
    // Create destination bucket with different storage class,
    // because this is supported only by rewrite but not copy requests
    helperGcs.createBucket(
        dstBucketName, CreateBucketOptions.builder().setStorageClass("nearline").build());
    StorageResourceId copiedResourceId =
        new StorageResourceId(dstBucketName, name.getMethodName() + "_dst");

    int partitionsCount = 10;
    byte[] partition =
        writeObject(helperGcs, resourceId, /* partitionSize= */ 64 * 1024 * 1024, partitionsCount);

    trackingGcs.delegate.copy(
        srcBucketName, ImmutableList.of(resourceId.getObjectName()),
        dstBucketName, ImmutableList.of(copiedResourceId.getObjectName()));

    assertObjectContent(helperGcs, copiedResourceId, partition, partitionsCount);
    assertThat(trackingGcs.requestsTracker.getAllRequestInvocationIds().size())
        .isEqualTo(trackingGcs.requestsTracker.getAllRequests().size());

    assertThat(trackingGcs.getAllRequestStrings())
        .containsExactly(
            getBucketRequestString(resourceId.getBucketName()),
            getBucketRequestString(copiedResourceId.getBucketName()),
            rewriteRequestString(
                resourceId.getBucketName(),
                resourceId.getObjectName(),
                copiedResourceId.getBucketName(),
                copiedResourceId.getObjectName(),
                maxRewriteChunkSize,
                /* rewriteTokenId= */ null),
            rewriteRequestString(
                resourceId.getBucketName(),
                resourceId.getObjectName(),
                copiedResourceId.getBucketName(),
                copiedResourceId.getObjectName(),
                maxRewriteChunkSize,
                /* rewriteTokenId= */ 1),
            rewriteRequestString(
                resourceId.getBucketName(),
                resourceId.getObjectName(),
                copiedResourceId.getBucketName(),
                copiedResourceId.getObjectName(),
                maxRewriteChunkSize,
                /* rewriteTokenId= */ 2));
    trackingGcs.delegate.close();
  }

  @Test
  public void create_gcsItemInfo_metadataEquals() throws IOException {
    StorageResourceId resourceId = new StorageResourceId(TEST_BUCKET, name.getMethodName());
    TrackingStorageWrapper<GoogleCloudStorage> trackingGcs =
        newTrackingGoogleCloudStorage(GCS_OPTIONS);

    Map<String, byte[]> expectedMetadata =
        ImmutableMap.of(
            "key1", "value1".getBytes(StandardCharsets.UTF_8),
            "key2", "value2".getBytes(StandardCharsets.UTF_8));
    Map<String, byte[]> wrongMetadata =
        ImmutableMap.of(
            "key", "value1".getBytes(StandardCharsets.UTF_8),
            "key2", "value2".getBytes(StandardCharsets.UTF_8));

    trackingGcs
        .delegate
        .create(resourceId, CreateObjectOptions.builder().setMetadata(expectedMetadata).build())
        .close();

    GoogleCloudStorageItemInfo itemInfo = helperGcs.getItemInfo(resourceId);

    assertThat(itemInfo.metadataEquals(expectedMetadata)).isTrue();
    assertThat(itemInfo.metadataEquals(itemInfo.getMetadata())).isTrue();
    assertThat(itemInfo.metadataEquals(wrongMetadata)).isFalse();

    assertThat(trackingGcs.requestsTracker.getAllRequestInvocationIds().size())
        .isEqualTo(trackingGcs.requestsTracker.getAllRequests().size());

    assertThat(trackingGcs.getAllRequestStrings())
        .containsExactly(
            ImmutableList.builder()
                .add(getObjectRequestString(resourceId, OBJECT_FIELDS, testStorageClientImpl))
                .add(
                    resumableUploadRequestString(
                        resourceId.getBucketName(),
                        resourceId.getObjectName(),
                        /* generationId= */ 1,
                        /* replaceGenerationId= */ true))
                .addAll(
                    resumableUploadChunkRequestString(
                        resourceId.getBucketName(),
                        resourceId.getObjectName(),
                        /* generationId= */ 2,
                        /* uploadId= */ 1,
                        /* writeOffset= */ 0,
                        /* length= */ 0,
                        /* finishWrite= */ true))
                .build()
                .toArray());
    trackingGcs.delegate.close();
  }

  @Test
  public void tracelog_enabled() throws IOException {
    if (!testStorageClientImpl) {
      doTestTraceLog(true, 3, 5);
    }
  }

  @Test
  public void tracelog_disabled() throws IOException {
    doTestTraceLog(false, 0, 0);
  }

  private void doTestTraceLog(
      boolean traceLogEnabled, int expectedAfterWrite, int expectedAfterRead) throws IOException {
    AssertingLogHandler jsonLogHander = new AssertingLogHandler();
    Logger jsonTracingLogger =
        jsonLogHander.getLoggerForClass(EventLoggingHttpRequestInitializer.class.getName());

    try {

      TrackingStorageWrapper<GoogleCloudStorage> trackingGcs =
          newTrackingGoogleCloudStorage(
              getStandardOptionBuilder().setTraceLogEnabled(traceLogEnabled).build());

      int expectedSize = 5 * 1024 * 1024;
      StorageResourceId resourceId = new StorageResourceId(TEST_BUCKET, name.getMethodName());
      byte[] writtenData =
          writeObject(
              trackingGcs.delegate,
              resourceId,
              /* partitionSize= */ expectedSize,
              /* partitionsCount= */ 1);

      jsonLogHander.assertLogCount(expectedAfterWrite);

      assertObjectContent(trackingGcs.delegate, resourceId, writtenData);

      jsonLogHander.assertLogCount(expectedAfterRead);
      jsonLogHander.verifyJsonLogFields(TEST_BUCKET, name.getMethodName());
      trackingGcs.delegate.close();
    } finally {
      jsonTracingLogger.removeHandler(jsonLogHander);
    }
  }

  private TrackingStorageWrapper<GoogleCloudStorage> newTrackingGoogleCloudStorage(
      GoogleCloudStorageOptions options) throws IOException {
    Credentials credentials = GoogleCloudStorageTestHelper.getCredentials();
    return new TrackingStorageWrapper<>(
        options,
        (httpRequestInitializer, grpcRequestInterceptors) ->
            testStorageClientImpl
                ? GoogleCloudStorageClientImpl.builder()
                    .setOptions(options)
                    .setCredentials(credentials)
                    .setHttpRequestInitializer(httpRequestInitializer)
                    .setGRPCInterceptors(grpcRequestInterceptors)
                    .build()
                : GoogleCloudStorageImpl.builder()
                    .setOptions(options)
                    .setCredentials(credentials)
                    .setHttpRequestInitializer(httpRequestInitializer)
                    .build(),
        credentials);
  }

  private GoogleCloudStorage getStorageFromOptions(GoogleCloudStorageOptions options)
      throws IOException {
    Credentials credentials = GoogleCloudStorageTestHelper.getCredentials();
    return testStorageClientImpl
        ? GoogleCloudStorageClientImpl.builder()
            .setOptions(options)
            .setCredentials(credentials)
            .build()
        : GoogleCloudStorageImpl.builder().setOptions(options).setCredentials(credentials).build();
  }

  private static GoogleCloudStorageOptions getOptionsWithUploadChunk(int uploadChunk) {
    return getStandardOptionBuilder()
        .setWriteChannelOptions(
            AsyncWriteChannelOptions.builder().setUploadChunkSize(uploadChunk).build())
        .build();
  }

  public List<String> getExpectedRequestsForCreateObject(
      StorageResourceId resourceId, int uploadChunkSize, int partitionsCount, byte[] partition) {
    double contentLength = (double) partition.length * partitionsCount;
    int effectiveChunkSize;
    if (testStorageClientImpl) {
      // Unlike Apiary, java-storage library always send chunk of size 2MiB or configuredChunkSize.
      // Whichever is lower.
      effectiveChunkSize = Math.min(uploadChunkSize, 2 * 1024 * 1024);
    } else {
      effectiveChunkSize = uploadChunkSize;
    }

    int uploadChunkCount =
        (int)
            Math.max(
                1, Math.ceil((double) partition.length * partitionsCount / effectiveChunkSize));

    return ImmutableList.<String>builder()
        .add(getObjectRequestString(resourceId, OBJECT_FIELDS, testStorageClientImpl))
        .add(
            resumableUploadRequestString(
                resourceId.getBucketName(),
                resourceId.getObjectName(),
                /* generationId= */ 1,
                /* replaceGenerationId= */ true))
        .addAll(
            IntStream.rangeClosed(1, uploadChunkCount)
                .mapToObj(
                    i -> {
                      int writeOffset = (int) Math.min((i - 1) * effectiveChunkSize, contentLength);
                      return resumableUploadChunkRequestString(
                          resourceId.getBucketName(),
                          resourceId.getObjectName(),
                          /* generationId= */ i + 1,
                          /* uploadId= */ i,
                          /* writeOffset= */ writeOffset,
                          /* chunkLength= */ (int)
                              Math.min((contentLength - writeOffset), effectiveChunkSize),
                          /* finishWrite= */ i == uploadChunkCount);
                    })
                .collect(toList())
                .stream()
                .flatMap(List::stream)
                .collect(toList()))
        .build();
  }

  private String resumableUploadRequestString(
      String bucketName, String objectName, Integer generationId, boolean replaceGenrationId) {
    if (this.testStorageClientImpl) {
      return TrackingGrpcRequestInterceptor.resumableUploadRequestString(
          bucketName, objectName, generationId);
    }
    return TrackingHttpRequestInitializer.resumableUploadRequestString(
        bucketName, objectName, generationId, replaceGenrationId);
  }

  private ImmutableList<String> resumableUploadChunkRequestString(
      String bucketName,
      String objectName,
      Integer generationId,
      Integer uploadId,
      int writeOffset,
      long length,
      boolean finishWrite) {
    if (this.testStorageClientImpl) {
      List<String> requestsList = new ArrayList<>();
      if (!finishWrite) {
        requestsList.add(
            TrackingGrpcRequestInterceptor.resumableUploadChunkRequestString(
                generationId, uploadId, length, writeOffset, false));
      } else {

        if (length == 0) {
          requestsList.add(
              TrackingGrpcRequestInterceptor.resumableUploadChunkRequestString(
                  generationId,
                  uploadId,
                  /* contentLength= */ 0,
                  writeOffset,
                  /*finishWrite=*/ true));
        } else {
          // if there is data which needs to be uploaded, fist upload data and then submit
          // finalWrite.
          requestsList.add(
              TrackingGrpcRequestInterceptor.resumableUploadChunkRequestString(
                  generationId, uploadId, length, writeOffset, false));

          requestsList.add(
              TrackingGrpcRequestInterceptor.resumableUploadChunkRequestString(
                  generationId + 1,
                  uploadId + 1,
                  /*contentLength=*/ 0,
                  writeOffset + length,
                  /*finishWrite=*/ true));
        }
      }
      return ImmutableList.copyOf(requestsList);
    }
    return ImmutableList.of(
        TrackingHttpRequestInitializer.resumableUploadChunkRequestString(
            bucketName, objectName, generationId, uploadId));
  }

  private String emptyUploadRequestString(
      String bucketName, String objectName, boolean testStorageClientImpl) {
    if (testStorageClientImpl) {
      return TrackingGrpcRequestInterceptor.resumableUploadChunkRequestString(1, 1, 0, 0, true);
    }
    return uploadRequestString(bucketName, objectName, /* generationId= */ null);
  }

  private String getObjectRequestString(
      StorageResourceId resourceId, String fields, boolean testStorageClientImpl) {
    if (testStorageClientImpl) {
      return TrackingGrpcRequestInterceptor.getObjectRequestString();
    }
    return TrackingHttpRequestInitializer.getRequestString(
        resourceId.getBucketName(), resourceId.getObjectName(), fields);
  }
}
