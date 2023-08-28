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

package com.google.cloud.hadoop.gcsio.integration;

import static com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.assertObjectContent;
import static com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.getCredentials;
import static com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.getStandardOptionBuilder;
import static com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.writeObject;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.hadoop.gcsio.AssertingLogHandler;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageClientGrpcTracingInterceptor;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageClientImpl;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageTracingFields;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.TestBucketHelper;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.gson.Gson;
import com.google.storage.v2.BucketName;
import io.grpc.Status;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GoogleCloudStorageClientInterceptorIntegrationTest {
  private static final GoogleCloudStorageOptions GCS_TRACE_OPTIONS =
      getStandardOptionBuilder().setTraceLogEnabled(true).build();

  private static final Logger LOGGER =
      Logger.getLogger(GoogleCloudStorageClientGrpcTracingInterceptor.class.getName());

  private static final TestBucketHelper BUCKET_HELPER =
      new TestBucketHelper("dataproc-grpc-interceptor");
  private static final String TEST_BUCKET = BUCKET_HELPER.getUniqueBucketPrefix();

  private static GoogleCloudStorage helperGcs = GoogleCloudStorageTestHelper.createGcsClientImpl();

  private AssertingLogHandler assertingHandler;

  private final Gson gson = new Gson();

  @Rule
  public TestName name =
      new TestName() {
        // With parametrization method name will get [index] appended in their name.
        @Override
        public String getMethodName() {
          return super.getMethodName();
        }
      };

  @BeforeClass
  public static void setup() throws IOException {
    helperGcs.createBucket(TEST_BUCKET);
  }

  @AfterClass
  public static void after() throws IOException {
    try {
      BUCKET_HELPER.cleanup(helperGcs);
    } finally {
      helperGcs.close();
    }
  }

  @Before
  public void setUpHandlers() throws IOException {
    assertingHandler = new AssertingLogHandler();
    LOGGER.setUseParentHandlers(false);
    LOGGER.addHandler(assertingHandler);
    LOGGER.setLevel(Level.INFO);
  }

  @After
  public void verifyAndRemoveAssertingHandler() {
    LOGGER.removeHandler(assertingHandler);
  }

  @Test
  public void testWriteLogs() throws IOException {

    StorageResourceId resourceId = new StorageResourceId(TEST_BUCKET, name.getMethodName());
    int uploadChunkSize = 2 * 1024 * 1024;
    GoogleCloudStorageOptions storageOption =
        GCS_TRACE_OPTIONS.toBuilder()
            .setWriteChannelOptions(
                AsyncWriteChannelOptions.builder().setUploadChunkSize(uploadChunkSize).build())
            .build();

    GoogleCloudStorage gcsImpl = getGCSClientImpl(storageOption);
    gcsImpl.create(resourceId).close();

    assertingHandler.assertLogCount(2 * 3);

    verifyChannelCreation(
        assertingHandler.getSubListOfRecords(/* startIndex= */ 0, /* endIndex= */ 2), resourceId);

    Map<String, Object> writeObjectRequestRecord = assertingHandler.getLogRecordAtIndex(3);

    assertThat(writeObjectRequestRecord.get(GoogleCloudStorageTracingFields.WRITE_OFFSET.name))
        .isEqualTo(0);
    assertThat(writeObjectRequestRecord.get(GoogleCloudStorageTracingFields.CONTENT_LENGTH.name))
        .isEqualTo(0);
    assertThat(writeObjectRequestRecord.get(GoogleCloudStorageTracingFields.UPLOAD_ID.name))
        .isNotNull();
    String uploadId =
        (String) writeObjectRequestRecord.get(GoogleCloudStorageTracingFields.UPLOAD_ID.name);
    assertThat(writeObjectRequestRecord.get(GoogleCloudStorageTracingFields.FINALIZE_WRITE.name))
        .isEqualTo(true);

    Map<String, Object> writeObjectResponseRecord = assertingHandler.getLogRecordAtIndex(4);
    assertThat(writeObjectResponseRecord.get(GoogleCloudStorageTracingFields.UPLOAD_ID.name))
        .isEqualTo(uploadId);
    assertThat(writeObjectResponseRecord.get(GoogleCloudStorageTracingFields.PERSISTED_SIZE.name))
        .isEqualTo(0);

    Map<String, Object> writeObjectCloseStatusRecord = assertingHandler.getLogRecordAtIndex(5);
    verifyCloseStatus(writeObjectCloseStatusRecord, "WriteObject", Status.OK);
  }

  @Test
  public void testReadLogs() throws IOException {

    StorageResourceId resourceId = new StorageResourceId(TEST_BUCKET, name.getMethodName());
    int uploadChunkSize = 2 * 1024 * 1024;
    GoogleCloudStorageOptions storageOption =
        GCS_TRACE_OPTIONS.toBuilder()
            .setWriteChannelOptions(
                AsyncWriteChannelOptions.builder().setUploadChunkSize(uploadChunkSize).build())
            .build();

    GoogleCloudStorage gcsImpl = getGCSClientImpl(storageOption);
    int partitionsCount = 1;
    byte[] partition =
        writeObject(gcsImpl, resourceId, /* partitionSize= */ 2 * 1024 * 1024, partitionsCount);
    // there wil lbe three streams
    // 1. StartResumableUpload stream with 3 messages, 1 for each Req, Resp, and status
    // 2. WriteObject Stream with 3 messages, 1 for each Req, Resp, and status
    // 2. WriteObject Stream to finalize object with 3 messages, 1 for each Req, Resp, and status
    assertingHandler.assertLogCount(3 * 3);
    assertingHandler.flush();

    assertObjectContent(gcsImpl, resourceId, partition, partitionsCount);

    // One for Read Request
    // One for Read Response ( it can vary, request can be split into two chunks as well).
    // One for status
    assertingHandler.assertLogCount(3);
    StorageResourceId derivedResourceId = derivedResourceId(resourceId);

    Map<String, Object> readObjectRequestRecord = assertingHandler.getLogRecordAtIndex(0);
    assertThat(
            readObjectRequestRecord.get(GoogleCloudStorageTracingFields.RESOURCE.name).toString())
        .contains(derivedResourceId.toString());
    assertThat(readObjectRequestRecord.get(GoogleCloudStorageTracingFields.READ_OFFSET.name))
        .isEqualTo(0);
    assertThat(readObjectRequestRecord.get(GoogleCloudStorageTracingFields.READ_LIMIT.name))
        .isEqualTo(partition.length);

    Map<String, Object> readObjectResponseRecord = assertingHandler.getLogRecordAtIndex(1);
    assertThat(
            readObjectResponseRecord.get(GoogleCloudStorageTracingFields.RESOURCE.name).toString())
        .contains(derivedResourceId.toString());
    assertThat(readObjectResponseRecord.get(GoogleCloudStorageTracingFields.READ_OFFSET.name))
        .isEqualTo(0);
    assertThat(readObjectResponseRecord.get(GoogleCloudStorageTracingFields.READ_LIMIT.name))
        .isEqualTo(partition.length);
    assertThat(readObjectResponseRecord.get(GoogleCloudStorageTracingFields.BYTES_READ.name))
        .isEqualTo(partition.length);

    Map<String, Object> writeObjectCloseStatusRecord = assertingHandler.getLogRecordAtIndex(2);
    verifyCloseStatus(writeObjectCloseStatusRecord, "ReadObject", Status.OK);
  }

  public static GoogleCloudStorage getGCSClientImpl(GoogleCloudStorageOptions options) {
    try {
      return GoogleCloudStorageClientImpl.builder()
          .setOptions(options)
          .setCredentials(getCredentials())
          .build();
    } catch (IOException e) {
      throw new RuntimeException("Failed to create GoogleCloudStorage instance", e);
    }
  }

  private void verifyCommonFields(Map<String, Object> logRecord, String rpcMethod) {
    assertThat(logRecord.get(GoogleCloudStorageTracingFields.IDEMPOTENCY_TOKEN.name)).isNotNull();
    assertThat(logRecord.get(GoogleCloudStorageTracingFields.RPC_METHOD.name)).isEqualTo(rpcMethod);
  }

  private void verifyCloseStatus(Map<String, Object> logRecord, String rpcMethod, Status status) {
    verifyCommonFields(logRecord, rpcMethod);
    assertThat(logRecord.get(GoogleCloudStorageTracingFields.STATUS.name).toString())
        .contains(status.getCode().toString());
  }

  private StorageResourceId derivedResourceId(StorageResourceId resourceId) {
    String bucketName =
        BucketName.newBuilder()
            .setBucket(resourceId.getBucketName())
            .setProject("_")
            .build()
            .toString();
    return new StorageResourceId(
        bucketName, resourceId.getObjectName(), resourceId.getGenerationId());
  }

  private void verifyChannelCreation(
      List<Map<String, Object>> logRecord, StorageResourceId resourceId) {
    assertThat(logRecord.size()).isEqualTo(3);
    String rpcMethod = "StartResumableWrite";

    StorageResourceId derivedResourceId = derivedResourceId(resourceId);

    // logging assertions for request
    verifyCommonFields(logRecord.get(0), rpcMethod);

    assertThat((logRecord.get(0).get(GoogleCloudStorageTracingFields.RESOURCE.name)).toString())
        .contains(derivedResourceId.toString());
    assertThat(logRecord.get(0).get(GoogleCloudStorageTracingFields.REQUEST_COUNTER.name))
        .isEqualTo(0);

    // logging assertions for response
    verifyCommonFields(logRecord.get(1), rpcMethod);

    assertThat(logRecord.get(1).get(GoogleCloudStorageTracingFields.RESOURCE.name).toString())
        .contains(derivedResourceId.toString());
    assertThat(logRecord.get(1).get(GoogleCloudStorageTracingFields.RESPONSE_COUNTER.name))
        .isEqualTo(0);
    assertThat(logRecord.get(1).get(GoogleCloudStorageTracingFields.UPLOAD_ID.name)).isNotNull();

    // logging assertions for statue
    verifyCloseStatus(logRecord.get(2), rpcMethod, Status.OK);
  }
}
