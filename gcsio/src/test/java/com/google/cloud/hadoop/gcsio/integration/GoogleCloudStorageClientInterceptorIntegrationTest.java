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
import static com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.getCredential;
import static com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.getCredentials;
import static com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.getStandardOptionBuilder;
import static com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.writeObject;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertTrue;

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
import com.google.protobuf.Message;
import com.google.protobuf.TextFormat;
import com.google.protobuf.TextFormat.ParseException;
import com.google.storage.v2.BucketName;
import com.google.storage.v2.ReadObjectRequest;
import com.google.storage.v2.ReadObjectResponse;
import com.google.storage.v2.StartResumableWriteRequest;
import com.google.storage.v2.StartResumableWriteResponse;
import com.google.storage.v2.WriteObjectRequest;
import com.google.storage.v2.WriteObjectResponse;
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
        GCS_TRACE_OPTIONS
            .toBuilder()
            .setWriteChannelOptions(
                AsyncWriteChannelOptions.builder().setUploadChunkSize(uploadChunkSize).build())
            .build();

    GoogleCloudStorage gcsImpl = getGCSClientImpl(storageOption);
    int fileSize = uploadChunkSize - 1;
    writeObject(gcsImpl, resourceId, fileSize, 1);

    assertingHandler.assertLogCount(2 * 3);

    verifyChannelCreation(
        assertingHandler.getSubListOfRecords(/* startIndex= */ 0, /* endIndex= */ 2), resourceId);

    Map<String, Object> writeObjectRequestRecord = assertingHandler.getLogRecordAtIndex(3);
    String writeObjectInvocationId =
        writeObjectRequestRecord
            .get(GoogleCloudStorageTracingFields.IDEMPOTENCY_TOKEN.name)
            .toString();
    assertThat(writeObjectInvocationId).isNotNull();

    WriteObjectRequest request =
        (WriteObjectRequest)
            fromProtoToMsg(
                writeObjectRequestRecord
                    .get(GoogleCloudStorageTracingFields.REQUEST_MESSAGE_AS_STRING.name)
                    .toString(),
                WriteObjectRequest.newBuilder());
    assertThat(request.getUploadId()).isNotNull();
    assertTrue(request.getFinishWrite());
    assertThat(request.getChecksummedData().getContent().toStringUtf8())
        .isEqualTo(String.format("<size (%d)>", fileSize));

    Map<String, Object> writeObjectResponseRecord = assertingHandler.getLogRecordAtIndex(4);
    WriteObjectResponse response =
        (WriteObjectResponse)
            fromProtoToMsg(
                writeObjectResponseRecord
                    .get(GoogleCloudStorageTracingFields.RESPONSE_MESSAGE_AS_STRING.name)
                    .toString(),
                WriteObjectResponse.newBuilder());
    assertThat(response.getResource().getName()).isEqualTo(resourceId.getObjectName());
    assertThat(response.getResource().getSize()).isEqualTo(fileSize);

    Map<String, Object> writeObjectCloseStatusRecord = assertingHandler.getLogRecordAtIndex(5);
    verifyCloseStatus(
        writeObjectCloseStatusRecord, "WriteObject", writeObjectInvocationId, Status.OK);
    gcsImpl.close();
  }

  @Test
  public void testReadLogs() throws IOException {

    StorageResourceId resourceId = new StorageResourceId(TEST_BUCKET, name.getMethodName());
    int uploadChunkSize = 2 * 1024 * 1024;
    GoogleCloudStorageOptions storageOption =
        GCS_TRACE_OPTIONS
            .toBuilder()
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
    String streamInvocationId =
        readObjectRequestRecord
            .get(GoogleCloudStorageTracingFields.IDEMPOTENCY_TOKEN.name)
            .toString();
    assertThat(
            readObjectRequestRecord.get(GoogleCloudStorageTracingFields.RPC_METHOD.name).toString())
        .isEqualTo("ReadObject");
    ReadObjectRequest request =
        (ReadObjectRequest)
            fromProtoToMsg(
                readObjectRequestRecord
                    .get(GoogleCloudStorageTracingFields.REQUEST_MESSAGE_AS_STRING.name)
                    .toString(),
                ReadObjectRequest.newBuilder());
    assertThat(request.getBucket()).isEqualTo(derivedResourceId.getBucketName());
    assertThat(request.getObject()).isEqualTo(derivedResourceId.getObjectName());
    assertThat(request.getReadOffset()).isEqualTo(0);
    assertThat(request.getReadLimit()).isEqualTo(partition.length);

    Map<String, Object> readObjectResponseRecord = assertingHandler.getLogRecordAtIndex(1);

    ReadObjectResponse response =
        (ReadObjectResponse)
            fromProtoToMsg(
                readObjectResponseRecord
                    .get(GoogleCloudStorageTracingFields.RESPONSE_MESSAGE_AS_STRING.name)
                    .toString(),
                ReadObjectResponse.newBuilder());
    assertThat(response.getChecksummedData().getContent().toStringUtf8())
        .isEqualTo(String.format("<size (%d)>", partition.length));
    // asser both request-response have same invocationId
    assertThat(readObjectResponseRecord.get(GoogleCloudStorageTracingFields.IDEMPOTENCY_TOKEN.name))
        .isEqualTo(streamInvocationId);

    Map<String, Object> readObjectCloseStatusRecord = assertingHandler.getLogRecordAtIndex(2);
    verifyCloseStatus(readObjectCloseStatusRecord, "ReadObject", streamInvocationId, Status.OK);
    gcsImpl.close();
  }

  public static GoogleCloudStorage getGCSClientImpl(GoogleCloudStorageOptions options) {
    try {
      return GoogleCloudStorageClientImpl.builder()
          .setOptions(options)
          .setCredentials(getCredentials())
          .setCredential(getCredential())
          .build();
    } catch (IOException e) {
      throw new RuntimeException("Failed to create GoogleCloudStorage instance", e);
    }
  }

  private void verifyCommonFields(Map<String, Object> logRecord, String rpcMethod) {
    assertThat(logRecord.get(GoogleCloudStorageTracingFields.IDEMPOTENCY_TOKEN.name)).isNotNull();

    assertThat(logRecord.get(GoogleCloudStorageTracingFields.CURRENT_TIME.name)).isNotNull();
    assertThat(logRecord.get(GoogleCloudStorageTracingFields.RPC_METHOD.name)).isEqualTo(rpcMethod);
  }

  private void verifyCloseStatus(
      Map<String, Object> logRecord, String rpcMethod, String streamInvocationId, Status status) {
    verifyCommonFields(logRecord, rpcMethod);
    assertThat(logRecord.get(GoogleCloudStorageTracingFields.STATUS.name).toString())
        .contains(status.getCode().toString());
    assertThat(logRecord.get(GoogleCloudStorageTracingFields.DURATION_MS.name)).isNotNull();
    assertThat(logRecord.get(GoogleCloudStorageTracingFields.IDEMPOTENCY_TOKEN.name))
        .isEqualTo(streamInvocationId);
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
      List<Map<String, Object>> logRecord, StorageResourceId resourceId) throws ParseException {
    assertThat(logRecord.size()).isEqualTo(3);
    String rpcMethod = "StartResumableWrite";
    StorageResourceId derivedResourceId = derivedResourceId(resourceId);

    Map<String, Object> logRecordEntry = logRecord.get(0);
    // logging assertions for request
    verifyCommonFields(logRecord.get(0), rpcMethod);

    String streamInvocationId =
        logRecordEntry.get(GoogleCloudStorageTracingFields.IDEMPOTENCY_TOKEN.name).toString();
    assertThat(streamInvocationId).isNotNull();
    StartResumableWriteRequest request =
        (StartResumableWriteRequest)
            fromProtoToMsg(
                logRecordEntry
                    .get(GoogleCloudStorageTracingFields.REQUEST_MESSAGE_AS_STRING.name)
                    .toString(),
                StartResumableWriteRequest.newBuilder());
    assertThat(request.getWriteObjectSpec().getResource().getName())
        .isEqualTo(derivedResourceId.getObjectName());
    assertThat(request.getWriteObjectSpec().getResource().getBucket())
        .isEqualTo(derivedResourceId.getBucketName());
    assertThat(logRecordEntry.get(GoogleCloudStorageTracingFields.REQUEST_COUNTER.name))
        .isEqualTo(1);

    logRecordEntry = logRecord.get(1);
    // logging assertions for response
    verifyCommonFields(logRecordEntry, rpcMethod);

    StartResumableWriteResponse response =
        (StartResumableWriteResponse)
            fromProtoToMsg(
                logRecord
                    .get(1)
                    .get(GoogleCloudStorageTracingFields.RESPONSE_MESSAGE_AS_STRING.name)
                    .toString(),
                StartResumableWriteResponse.newBuilder());
    assertThat(response.getUploadId()).isNotNull();
    assertThat(logRecordEntry.get(GoogleCloudStorageTracingFields.RESPONSE_COUNTER.name))
        .isEqualTo(1);

    assertThat(logRecordEntry.get(GoogleCloudStorageTracingFields.IDEMPOTENCY_TOKEN.name))
        .isEqualTo(streamInvocationId);

    // logging assertions for statue
    verifyCloseStatus(logRecord.get(2), rpcMethod, streamInvocationId, Status.OK);
  }

  private Object fromProtoToMsg(String string, Message.Builder builder) throws ParseException {
    // setAllowUnknownFields(true) so that any newly added proto fields do not break parsing.
    TextFormat.Parser.newBuilder().setAllowUnknownFields(true).build().merge(string, builder);

    return builder.build();
  }
}
