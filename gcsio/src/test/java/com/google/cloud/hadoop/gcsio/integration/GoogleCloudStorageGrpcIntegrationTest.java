/*
* Copyright 2022 Google LLC
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

import static com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.assertByteArrayEquals;
import static com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.assertObjectContent;
import static com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.writeObject;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.Assert.assertThrows;

import com.google.cloud.hadoop.gcsio.AssertingLogHandler;
import com.google.cloud.hadoop.gcsio.EventLoggingHttpRequestInitializer;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageGrpcTracingInterceptor;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageItemInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions.MetricsSink;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.Fadvise;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageTestUtils;
import com.google.cloud.hadoop.gcsio.GrpcRequestTracingInfo;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.TestBucketHelper;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.common.flogger.GoogleLogger;
import com.google.gson.Gson;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class GoogleCloudStorageGrpcIntegrationTest {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  // Prefix this name with the prefix used in other gcs io integrate tests once it's whitelisted by
  // GCS to access gRPC API.
  private static final String BUCKET_NAME_PREFIX = "gcs-grpc-team";

  private static final TestBucketHelper BUCKET_HELPER = new TestBucketHelper(BUCKET_NAME_PREFIX);

  private static final String BUCKET_NAME = BUCKET_HELPER.getUniqueBucketName("shared");

  private final boolean tdEnabled;

  protected GoogleCloudStorage rawStorage;

  @After
  public void cleanup() {
    if (rawStorage != null) {
      rawStorage.close();
      rawStorage = null;
    }
  }

  @Parameters
  // Falling back to gRPC-LB for now as TD has issue with CloudBuild
  // TODO: issues/956 Enabled TD once resolved.
  public static Iterable<Boolean> tdEnabled() {
    return List.of(false);
  }

  public GoogleCloudStorageGrpcIntegrationTest(boolean tdEnabled) {
    this.tdEnabled = tdEnabled;
  }

  private GoogleCloudStorageOptions.Builder configureOptionsWithTD() {
    logger.atInfo().log("Creating client with tdEnabled %s", this.tdEnabled);
    return GoogleCloudStorageTestUtils.configureDefaultOptions()
        .setTrafficDirectorEnabled(this.tdEnabled);
  }

  private GoogleCloudStorage createGoogleCloudStorage() throws IOException {
    return GoogleCloudStorageImpl.builder()
        .setOptions(configureOptionsWithTD().build())
        .setCredentials(GoogleCloudStorageTestHelper.getCredentials())
        .build();
  }

  private GoogleCloudStorage createGoogleCloudStorage(
      AsyncWriteChannelOptions asyncWriteChannelOptions) throws IOException {
    return GoogleCloudStorageImpl.builder()
        .setOptions(
            configureOptionsWithTD().setWriteChannelOptions(asyncWriteChannelOptions).build())
        .setCredentials(GoogleCloudStorageTestHelper.getCredentials())
        .build();
  }

  @BeforeClass
  public static void createBuckets() throws IOException {
    GoogleCloudStorage rawStorage =
        GoogleCloudStorageImpl.builder()
            .setOptions(GoogleCloudStorageTestUtils.configureDefaultOptions().build())
            .setCredentials(GoogleCloudStorageTestHelper.getCredentials())
            .build();
    rawStorage.createBucket(BUCKET_NAME);
  }

  @AfterClass
  public static void cleanupBuckets() throws IOException {
    GoogleCloudStorage rawStorage =
        GoogleCloudStorageImpl.builder()
            .setOptions(GoogleCloudStorageTestUtils.configureDefaultOptions().build())
            .setCredentials(GoogleCloudStorageTestHelper.getCredentials())
            .build();
    BUCKET_HELPER.cleanup(rawStorage);
  }

  @Test
  public void testCreateObject() throws IOException {
    rawStorage = createGoogleCloudStorage();
    StorageResourceId objectToCreate =
        new StorageResourceId(BUCKET_NAME, "testCreateObject_Object");
    byte[] objectBytes = writeObject(rawStorage, objectToCreate, /* objectSize= */ 512);

    assertObjectContent(rawStorage, objectToCreate, objectBytes);
  }

  @Test
  public void testCreateExistingObject() throws IOException {
    rawStorage = createGoogleCloudStorage();
    StorageResourceId objectToCreate =
        new StorageResourceId(BUCKET_NAME, "testCreateExistingObject_Object");
    writeObject(rawStorage, objectToCreate, /* objectSize= */ 128);

    GoogleCloudStorageItemInfo createdItemInfo = rawStorage.getItemInfo(objectToCreate);
    assertThat(createdItemInfo.exists()).isTrue();
    assertThat(createdItemInfo.getSize()).isEqualTo(128);

    byte[] overwriteBytesToWrite = writeObject(rawStorage, objectToCreate, /* objectSize= */ 256);

    GoogleCloudStorageItemInfo overwrittenItemInfo = rawStorage.getItemInfo(objectToCreate);
    assertThat(overwrittenItemInfo.exists()).isTrue();
    assertThat(overwrittenItemInfo.getSize()).isEqualTo(256);
    assertObjectContent(rawStorage, objectToCreate, overwriteBytesToWrite);
  }

  @Test
  public void testCreateEmptyObject() throws IOException {
    rawStorage = createGoogleCloudStorage();
    StorageResourceId objectToCreate =
        new StorageResourceId(BUCKET_NAME, "testCreateEmptyObject_Object");

    rawStorage.createEmptyObject(objectToCreate);

    GoogleCloudStorageItemInfo itemInfo = rawStorage.getItemInfo(objectToCreate);

    assertThat(itemInfo.exists()).isTrue();
    assertThat(itemInfo.getSize()).isEqualTo(0);
  }

  @Test
  public void testCreateInvalidObject() throws IOException {
    rawStorage = createGoogleCloudStorage();
    StorageResourceId objectToCreate =
        new StorageResourceId(BUCKET_NAME, "testCreateInvalidObject_InvalidObject\n");

    assertThrows(
        IOException.class, () -> writeObject(rawStorage, objectToCreate, /* objectSize= */ 10));
  }

  @Test
  public void testOpen() throws IOException {
    rawStorage = createGoogleCloudStorage();
    StorageResourceId objectToCreate = new StorageResourceId(BUCKET_NAME, "testOpen_Object");
    byte[] objectBytes = writeObject(rawStorage, objectToCreate, /* objectSize= */ 100);

    assertObjectContent(rawStorage, objectToCreate, objectBytes);
  }

  @Test
  public void testOpenWithMetricsEnabled() throws IOException {
    rawStorage =
        GoogleCloudStorageImpl.builder()
            .setOptions(
                GoogleCloudStorageTestHelper.getStandardOptionBuilder()
                    .setMetricsSink(MetricsSink.CLOUD_MONITORING)
                    .build())
            .setCredentials(GoogleCloudStorageTestHelper.getCredentials())
            .build();
    StorageResourceId objectToCreate = new StorageResourceId(BUCKET_NAME, "testOpen_Object");
    byte[] objectBytes = writeObject(rawStorage, objectToCreate, /* objectSize= */ 100);
    assertObjectContent(rawStorage, objectToCreate, objectBytes);
  }

  @Test
  public void testOpenWithTracingLogEnabled() throws IOException {
    AssertingLogHandler assertingHandler = new AssertingLogHandler();
    Logger grpcTracingLogger =
        assertingHandler.getLoggerForClass(
            GoogleCloudStorageGrpcTracingInterceptor.class.getName());

    AssertingLogHandler jsonLogHandler = new AssertingLogHandler();
    Logger jsonTracingLogger =
        jsonLogHandler.getLoggerForClass(EventLoggingHttpRequestInitializer.class.getName());

    try {
      rawStorage =
          GoogleCloudStorageImpl.builder()
              .setOptions(configureOptionsWithTD().setTraceLogEnabled(true).build())
              .setCredentials(GoogleCloudStorageTestHelper.getCredentials())
              .build();

      StorageResourceId objectToCreate1 = new StorageResourceId(BUCKET_NAME, "testOpen_Object1");
      StorageResourceId objectToCreate2 = new StorageResourceId(BUCKET_NAME, "testOpen_Object2");

      int objectSize = 100000;
      byte[] objectBytes1 = writeObject(rawStorage, objectToCreate1, /* objectSize= */ objectSize);
      assertThat(getFilteredEvents(assertingHandler)).hasSize(6 * 2);

      byte[] objectBytes2 =
          writeObject(rawStorage, objectToCreate2, /* objectSize= */ objectSize * 2);
      assertThat(getFilteredEvents(assertingHandler)).hasSize(6 * 4);

      assertObjectContent(rawStorage, objectToCreate1, objectBytes1);
      assertThat(getFilteredEvents(assertingHandler)).hasSize(6 * 5);

      assertObjectContent(rawStorage, objectToCreate2, objectBytes2);
      assertThat(getFilteredEvents(assertingHandler)).hasSize(6 * 6);

      List<Map<String, Object>> traceEvents = getFilteredEvents(assertingHandler);
      int outboundMessageSentIndex = 2;
      int inboundMessageReadIndex = 4;

      int getUploadId1StartIndex = 0;
      int writeContent1StartIndex = 6;
      int writeContent2StartIndex = 6 * 3;
      int readContent1StartIndex = 6 * 4;
      int readContent2StartIndex = 6 * 5;

      int outboundMessageSentIndexUploadId1 = getUploadId1StartIndex + outboundMessageSentIndex;
      int inboundMessageReadIndexUploadId1 = getUploadId1StartIndex + inboundMessageReadIndex;
      int outboundMessageSentWriteContent1 = writeContent1StartIndex + outboundMessageSentIndex;
      int outboundMessageSentWriteContent2 = writeContent2StartIndex + outboundMessageSentIndex;
      int inboundMessageReadIndexWriteContent1 = writeContent1StartIndex + inboundMessageReadIndex;
      int outboundMessageSentReadContent1 = readContent1StartIndex + outboundMessageSentIndex;
      int inboundMessageReadIndexReadContent1 = readContent1StartIndex + inboundMessageReadIndex;
      int outboundMessageSentReadContent2 = readContent2StartIndex + outboundMessageSentIndex;
      int inboundMessageReadIndexReadContent2 = readContent2StartIndex + inboundMessageReadIndex;

      Map<String, Object> outboundUploadId1Details =
          traceEvents.get(outboundMessageSentIndexUploadId1);
      Map<String, Object> outboundWriteContent1Details =
          traceEvents.get(outboundMessageSentWriteContent1);
      Map<String, Object> outboundWriteContent2Details =
          traceEvents.get(outboundMessageSentWriteContent2);
      Map<String, Object> inboundUploadId1Details =
          traceEvents.get(inboundMessageReadIndexUploadId1);
      Map<String, Object> inboundWriteContent1Details =
          traceEvents.get(inboundMessageReadIndexWriteContent1);
      Map<String, Object> outboundReadContent1Details =
          traceEvents.get(outboundMessageSentReadContent1);
      Map<String, Object> inboundReadContent1Details =
          traceEvents.get(inboundMessageReadIndexReadContent1);
      Map<String, Object> outboundReadContent2Details =
          traceEvents.get(outboundMessageSentReadContent2);
      Map<String, Object> inboundReadContent2Details =
          traceEvents.get(inboundMessageReadIndexReadContent2);

      verifyTrace(outboundUploadId1Details, "write", "testOpen_Object1", "outboundMessageSent()");
      verifyTrace(
          outboundWriteContent1Details, "write", "testOpen_Object1", "outboundMessageSent()");
      verifyTrace(inboundUploadId1Details, "write", "testOpen_Object1", "inboundMessageRead()");
      verifyTrace(inboundWriteContent1Details, "write", "testOpen_Object1", "inboundMessageRead()");
      verifyTrace(outboundReadContent1Details, "read", "testOpen_Object1", "outboundMessageSent()");
      verifyTrace(inboundReadContent1Details, "read", "testOpen_Object1", "inboundMessageRead()");
      verifyTrace(outboundReadContent2Details, "read", "testOpen_Object2", "outboundMessageSent()");
      verifyTrace(inboundReadContent2Details, "read", "testOpen_Object2", "inboundMessageRead()");

      verifyWireSizeDifferenceWithinRange(
          outboundWriteContent1Details, outboundWriteContent2Details, objectSize, 50);

      verifyWireSizeDifferenceWithinRange(
          inboundReadContent1Details, inboundReadContent2Details, objectSize, 50);

      assertingHandler.verifyCommonTraceFields();
      jsonLogHandler.verifyJsonLogFields(BUCKET_NAME, "testOpen_Object");
      jsonLogHandler.assertLogCount(4);

    } finally {
      grpcTracingLogger.removeHandler(jsonLogHandler);
      jsonTracingLogger.removeHandler(jsonLogHandler);
    }
  }

  /**
   * The wire size is usually greater than the content size by a few bytes. Hence, checking that the
   * actual wire size is within the range. Also checking the difference b/w wireSize b/w two calls
   * is within a bound of few bytes of the expected value.
   */
  private void verifyWireSizeDifferenceWithinRange(
      Map<String, Object> event1, Map<String, Object> event2, int sizeDifference, int range) {
    double wireSize1 = (double) event1.get("optionalWireSize");
    double wireSize2 = (double) event2.get("optionalWireSize");
    double diff = wireSize2 - wireSize1;
    assertThat(diff).isGreaterThan(sizeDifference - range);
    assertThat(diff).isLessThan(sizeDifference + range);
  }

  // inboundTrailers() event is missing in some scenarios. Hence, filtering it out to make the
  // test non-flaky.
  private List<Map<String, Object>> getFilteredEvents(AssertingLogHandler assertingHandler) {
    return assertingHandler.getAllLogRecords().stream()
        .filter((a) -> !a.get("details").equals("inboundTrailers()"))
        .collect(Collectors.toList());
  }

  private void verifyTrace(
      Map<String, Object> traceDetails, String requestType, String objectName, String methodName) {
    Gson gson = new Gson();
    assertThat(traceDetails.get("details")).isEqualTo(methodName);
    assertThat(traceDetails).containsKey("elapsedmillis");

    GrpcRequestTracingInfo requestTracingInfo =
        gson.fromJson(traceDetails.get("requestinfo").toString(), GrpcRequestTracingInfo.class);
    assertThat(requestTracingInfo.getApi()).isEqualTo("grpc");
    assertThat(requestTracingInfo.getRequestType()).isEqualTo(requestType);
    assertThat(requestTracingInfo.getObjectName()).isEqualTo(objectName);
  }

  @Test
  public void testOpenNonExistentItem() throws IOException {
    rawStorage = createGoogleCloudStorage();
    StorageResourceId resourceId =
        new StorageResourceId(BUCKET_NAME, "testOpenNonExistentItem_Object");
    IOException exception = assertThrows(IOException.class, () -> rawStorage.open(resourceId));
    assertThat(exception).hasMessageThat().contains("File not found");
  }

  @Test
  public void testOpenEmptyObject() throws IOException {
    rawStorage = createGoogleCloudStorage();
    StorageResourceId resourceId = new StorageResourceId(BUCKET_NAME, "testOpenEmptyObject_Object");
    rawStorage.createEmptyObject(resourceId);

    assertObjectContent(rawStorage, resourceId, new byte[0]);
  }

  @Test
  public void testOpenLargeObject() throws IOException {
    rawStorage = createGoogleCloudStorage();
    StorageResourceId resourceId = new StorageResourceId(BUCKET_NAME, "testOpenLargeObject_Object");

    int partitionsCount = 50;
    byte[] partition =
        writeObject(rawStorage, resourceId, /* partitionSize= */ 10 * 1024 * 1024, partitionsCount);

    assertObjectContent(rawStorage, resourceId, partition, partitionsCount);
  }

  @Test
  public void testOpenObjectWithChecksum() throws IOException {
    AsyncWriteChannelOptions asyncWriteChannelOptions =
        AsyncWriteChannelOptions.builder().setGrpcChecksumsEnabled(true).build();
    rawStorage = createGoogleCloudStorage(asyncWriteChannelOptions);
    StorageResourceId objectToCreate =
        new StorageResourceId(BUCKET_NAME, "testOpenObjectWithChecksum_Object");
    byte[] objectBytes = writeObject(rawStorage, objectToCreate, /* objectSize= */ 100);

    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setGrpcChecksumsEnabled(true).build();
    assertObjectContent(rawStorage, objectToCreate, readOptions, objectBytes);
  }

  @Test
  public void testOpenObjectWithSeek() throws IOException {
    AsyncWriteChannelOptions asyncWriteChannelOptions =
        AsyncWriteChannelOptions.builder().setGrpcChecksumsEnabled(true).build();
    rawStorage = createGoogleCloudStorage(asyncWriteChannelOptions);
    StorageResourceId objectToCreate =
        new StorageResourceId(BUCKET_NAME, "testOpenObjectWithSeek_Object");
    byte[] objectBytes = writeObject(rawStorage, objectToCreate, /* objectSize= */ 100);
    int offset = 10;
    byte[] trimmedObjectBytes = Arrays.copyOfRange(objectBytes, offset, objectBytes.length);
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setGrpcChecksumsEnabled(true).build();
    assertObjectContent(
        rawStorage,
        objectToCreate,
        readOptions,
        trimmedObjectBytes,
        /* expectedBytesCount= */ 1,
        offset);
  }

  @Test
  public void testOpenObjectWithSeekOverBounds() throws IOException {
    AsyncWriteChannelOptions asyncWriteChannelOptions =
        AsyncWriteChannelOptions.builder().setGrpcChecksumsEnabled(true).build();
    rawStorage = createGoogleCloudStorage(asyncWriteChannelOptions);
    StorageResourceId objectToCreate =
        new StorageResourceId(BUCKET_NAME, "testOpenObjectWithSeekOverBounds_Object");
    byte[] objectBytes = writeObject(rawStorage, objectToCreate, /* objectSize= */ 4 * 1024 * 1024);
    int offset = 3 * 1024 * 1024;
    byte[] trimmedObjectBytes = Arrays.copyOfRange(objectBytes, offset, objectBytes.length);
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setGrpcChecksumsEnabled(true).build();
    assertObjectContent(
        rawStorage,
        objectToCreate,
        readOptions,
        trimmedObjectBytes,
        /* expectedBytesCount= */ 1,
        offset);
  }

  @Test
  public void testOpenObjectWithSeekLimits() throws IOException {
    AsyncWriteChannelOptions asyncWriteChannelOptions =
        AsyncWriteChannelOptions.builder().setGrpcChecksumsEnabled(true).build();
    rawStorage = createGoogleCloudStorage(asyncWriteChannelOptions);
    StorageResourceId objectToCreate =
        new StorageResourceId(BUCKET_NAME, "testOpenObjectWithSeekOverBounds_Object");
    byte[] objectBytes = writeObject(rawStorage, objectToCreate, /* objectSize= */ 1024);
    int offset = 100;
    byte[] trimmedObjectBytes = Arrays.copyOfRange(objectBytes, offset, objectBytes.length);
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder()
            .setInplaceSeekLimit(50)
            .setGrpcChecksumsEnabled(true)
            .build();
    assertObjectContent(
        rawStorage,
        objectToCreate,
        readOptions,
        trimmedObjectBytes,
        /* expectedBytesCount= */ 1,
        offset);
  }

  @Test
  public void testReadFooterDataWithGrpcChecksums() throws IOException {
    AsyncWriteChannelOptions asyncWriteChannelOptions =
        AsyncWriteChannelOptions.builder().setGrpcChecksumsEnabled(true).build();
    rawStorage = createGoogleCloudStorage(asyncWriteChannelOptions);
    StorageResourceId objectToCreate =
        new StorageResourceId(BUCKET_NAME, "testOpenObjectWithSeekToFooter_Object");
    int objectSize = 1024;
    byte[] objectBytes = writeObject(rawStorage, objectToCreate, /* objectSize= */ objectSize);
    int minRangeRequestSize = 200;
    int offset = objectSize - minRangeRequestSize / 2;
    byte[] trimmedObjectBytes = Arrays.copyOfRange(objectBytes, offset, objectBytes.length);
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder()
            .setInplaceSeekLimit(50)
            .setMinRangeRequestSize(minRangeRequestSize)
            .setFadvise(Fadvise.RANDOM)
            .setGrpcChecksumsEnabled(true)
            .build();
    assertObjectContent(
        rawStorage,
        objectToCreate,
        readOptions,
        trimmedObjectBytes,
        /* expectedBytesCount= */ 1,
        offset);
  }

  @Test
  public void testReadCachedFooterData() throws IOException {
    AsyncWriteChannelOptions asyncWriteChannelOptions = AsyncWriteChannelOptions.builder().build();
    rawStorage = createGoogleCloudStorage(asyncWriteChannelOptions);
    StorageResourceId objectToCreate =
        new StorageResourceId(BUCKET_NAME, "testReadCachedFooterData_Object");
    int objectSize = 10 * 1024 * 1024;
    byte[] objectBytes = writeObject(rawStorage, objectToCreate, /* objectSize= */ objectSize);

    int minRangeRequestSize = 200;
    int footerOffset = objectSize - minRangeRequestSize / 2;
    byte[] trimmedObjectBytes = Arrays.copyOfRange(objectBytes, footerOffset, objectBytes.length);
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setMinRangeRequestSize(minRangeRequestSize).build();
    assertObjectContent(
        rawStorage,
        objectToCreate,
        readOptions,
        trimmedObjectBytes,
        /* expectedBytesCount= */ 1,
        footerOffset);
  }

  @Test
  public void testReadSeekToFooterData() throws IOException {
    AsyncWriteChannelOptions asyncWriteChannelOptions = AsyncWriteChannelOptions.builder().build();
    rawStorage = createGoogleCloudStorage(asyncWriteChannelOptions);
    StorageResourceId objectToCreate =
        new StorageResourceId(BUCKET_NAME, "testReadSeekToFooterData_Object");
    int objectSize = 1024;
    byte[] objectBytes = writeObject(rawStorage, objectToCreate, /* objectSize= */ objectSize);

    int minRangeRequestSize = 200;
    int offset = objectSize - minRangeRequestSize / 4;
    byte[] trimmedObjectBytes = Arrays.copyOfRange(objectBytes, offset, objectBytes.length);
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setMinRangeRequestSize(minRangeRequestSize).build();
    assertObjectContent(
        rawStorage,
        objectToCreate,
        readOptions,
        trimmedObjectBytes,
        /* expectedBytesCount= */ 1,
        offset);
  }

  @Test
  public void testReadObjectCachedAsFooter() throws IOException {
    AsyncWriteChannelOptions asyncWriteChannelOptions = AsyncWriteChannelOptions.builder().build();
    rawStorage = createGoogleCloudStorage(asyncWriteChannelOptions);
    StorageResourceId objectToCreate =
        new StorageResourceId(BUCKET_NAME, "testReadSeekToFooterData_Object");
    int objectSize = 1024;
    byte[] objectBytes = writeObject(rawStorage, objectToCreate, /* objectSize= */ objectSize);

    int minRangeRequestSize = 4 * 1024;
    int offset = 0;
    byte[] trimmedObjectBytes = Arrays.copyOfRange(objectBytes, offset, objectBytes.length);
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setMinRangeRequestSize(minRangeRequestSize).build();
    assertObjectContent(
        rawStorage,
        objectToCreate,
        readOptions,
        trimmedObjectBytes,
        /* expectedBytesCount= */ 1,
        offset);
  }

  @Test
  public void testPartialReadFooterDataWithSingleChunk() throws IOException {
    AsyncWriteChannelOptions asyncWriteChannelOptions = AsyncWriteChannelOptions.builder().build();
    rawStorage = createGoogleCloudStorage(asyncWriteChannelOptions);
    StorageResourceId objectToCreate =
        new StorageResourceId(BUCKET_NAME, "testPartialReadFooterDataWithSingleChunk_Object");
    int objectSize = 2 * 1024 * 1024;
    byte[] objectBytes = writeObject(rawStorage, objectToCreate, /* objectSize= */ objectSize);

    int minRangeRequestSize = 1024; // server responds in 2MB chunks by default
    int readOffset = objectSize / 2;
    byte[] trimmedObjectBytes = Arrays.copyOfRange(objectBytes, readOffset, objectBytes.length);
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setMinRangeRequestSize(minRangeRequestSize).build();
    assertObjectContent(
        rawStorage,
        objectToCreate,
        readOptions,
        trimmedObjectBytes,
        /* expectedBytesCount= */ 1,
        readOffset);
  }

  @Test
  public void testPartialReadFooterDataWithMultipleChunks() throws IOException {
    AsyncWriteChannelOptions asyncWriteChannelOptions = AsyncWriteChannelOptions.builder().build();
    rawStorage = createGoogleCloudStorage(asyncWriteChannelOptions);
    StorageResourceId objectToCreate =
        new StorageResourceId(BUCKET_NAME, "testPartialReadFooterDataWithMultipleChunks_Object");
    int objectSize = 10 * 1024 * 1024;
    byte[] objectBytes = writeObject(rawStorage, objectToCreate, /* objectSize= */ objectSize);

    int minRangeRequestSize = 4 * 1024; // server responds in 2MB chunks by default
    int readOffset = objectSize / 2;
    byte[] trimmedObjectBytes = Arrays.copyOfRange(objectBytes, readOffset, objectBytes.length);
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setMinRangeRequestSize(minRangeRequestSize).build();
    assertObjectContent(
        rawStorage,
        objectToCreate,
        readOptions,
        trimmedObjectBytes,
        /* expectedBytesCount= */ 1,
        readOffset);
  }

  @Test
  public void testPartialReadFooterDataWithinSegment() throws IOException {
    AsyncWriteChannelOptions asyncWriteChannelOptions = AsyncWriteChannelOptions.builder().build();
    rawStorage = createGoogleCloudStorage(asyncWriteChannelOptions);
    StorageResourceId objectToCreate =
        new StorageResourceId(BUCKET_NAME, "testPartialReadFooterDataWithinSegment_Object");
    int objectSize = 10 * 1024;
    byte[] objectBytes = writeObject(rawStorage, objectToCreate, /* objectSize= */ objectSize);

    int minRangeRequestSize = 4 * 1024;
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setMinRangeRequestSize(minRangeRequestSize).build();
    int readOffset = 7 * 1024;
    try (SeekableByteChannel readChannel = rawStorage.open(objectToCreate, readOptions)) {
      byte[] segmentBytes = new byte[100];
      ByteBuffer segmentBuffer = ByteBuffer.wrap(segmentBytes);
      readChannel.position(readOffset);
      readChannel.read(segmentBuffer);
      byte[] expectedSegment =
          Arrays.copyOfRange(
              objectBytes, /* from= */ readOffset, /* to= */ readOffset + segmentBytes.length);
      assertWithMessage("Unexpected segment data read.")
          .that(segmentBytes)
          .isEqualTo(expectedSegment);
    }
  }

  @Test
  public void testPartialRead() throws IOException {
    rawStorage = createGoogleCloudStorage();
    int segmentSize = 553;
    int segmentCount = 5;

    StorageResourceId resourceId =
        new StorageResourceId(BUCKET_NAME, "testReadPartialObjects_Object");
    byte[] data = writeObject(rawStorage, resourceId, /* objectSize= */ segmentCount * segmentSize);

    byte[][] readSegments = new byte[segmentCount][segmentSize];
    try (SeekableByteChannel readChannel = rawStorage.open(resourceId)) {
      for (int i = 0; i < segmentCount; i++) {
        ByteBuffer segmentBuffer = ByteBuffer.wrap(readSegments[i]);
        int bytesRead = readChannel.read(segmentBuffer);
        assertThat(bytesRead).isEqualTo(segmentSize);
        byte[] expectedSegment =
            Arrays.copyOfRange(
                data, /* from= */ i * segmentSize, /* to= */ i * segmentSize + segmentSize);
        assertWithMessage("Unexpected segment data read.")
            .that(readSegments[i])
            .isEqualTo(expectedSegment);
      }
    }
  }

  @Test
  public void testReadSeekToOffsetGreaterThanMinRangeRequestSize() throws IOException {
    AsyncWriteChannelOptions asyncWriteChannelOptions = AsyncWriteChannelOptions.builder().build();
    rawStorage = createGoogleCloudStorage(asyncWriteChannelOptions);
    StorageResourceId objectToCreate =
        new StorageResourceId(
            BUCKET_NAME, "testReadSeekToOffsetGreaterThanMinRangeRequestSize_Object");
    int objectSize = 20 * 1024;
    int inPlaceSeekLimit = 8 * 1024;
    int minRangeRequestSize = 4 * 1024;
    byte[] objectBytes = writeObject(rawStorage, objectToCreate, /* objectSize= */ objectSize);
    int totalBytes = 1;
    byte[] readArray = new byte[totalBytes];
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder()
            .setInplaceSeekLimit(inPlaceSeekLimit)
            .setMinRangeRequestSize(minRangeRequestSize)
            .setGrpcChecksumsEnabled(false)
            .setFadvise(Fadvise.RANDOM)
            .build();

    int newPosition = 7 * 1024;
    ByteBuffer readBuffer = ByteBuffer.wrap(readArray);
    int bytesRead;
    try (SeekableByteChannel readableByteChannel = rawStorage.open(objectToCreate, readOptions)) {
      readableByteChannel.position(newPosition);
      bytesRead = readableByteChannel.read(readBuffer);
    }

    byte[] trimmedObjectBytes =
        Arrays.copyOfRange(objectBytes, newPosition, newPosition + totalBytes);
    byte[] readBufferByteArray = Arrays.copyOf(readBuffer.array(), readBuffer.limit());

    assertThat(bytesRead).isEqualTo(totalBytes);
    assertByteArrayEquals(trimmedObjectBytes, readBufferByteArray);
  }

  @Test
  public void testReadBeyondRangeWithFadviseRandom() throws IOException {
    AsyncWriteChannelOptions asyncWriteChannelOptions = AsyncWriteChannelOptions.builder().build();
    rawStorage = createGoogleCloudStorage(asyncWriteChannelOptions);
    StorageResourceId objectToCreate =
        new StorageResourceId(BUCKET_NAME, "testReadBeyondRangeWithFadviseRandom_Object");
    int objectSize = 20 * 1024;
    int inPlaceSeekLimit = 8 * 1024;
    int minRangeRequestSize = 4 * 1024;
    byte[] objectBytes = writeObject(rawStorage, objectToCreate, /* objectSize= */ objectSize);
    int totalBytes = 2 * 1024;
    byte[] readArray = new byte[totalBytes];
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder()
            .setInplaceSeekLimit(inPlaceSeekLimit)
            .setMinRangeRequestSize(minRangeRequestSize)
            .setGrpcChecksumsEnabled(false)
            .setFadvise(Fadvise.RANDOM)
            .build();
    int newPosition = 7 * 1024;
    ByteBuffer readBuffer = ByteBuffer.wrap(readArray);
    int bytesRead;
    try (SeekableByteChannel readableByteChannel = rawStorage.open(objectToCreate, readOptions)) {
      readableByteChannel.position(newPosition);
      bytesRead = readableByteChannel.read(readBuffer);
    }

    byte[] trimmedObjectBytes =
        Arrays.copyOfRange(objectBytes, newPosition, newPosition + totalBytes);
    byte[] readBufferByteArray = Arrays.copyOf(readBuffer.array(), readBuffer.limit());

    assertThat(bytesRead).isEqualTo(totalBytes);
    assertByteArrayEquals(trimmedObjectBytes, readBufferByteArray);
  }

  @Test
  public void testReadBeyondRangeWithFadviseAuto() throws IOException {
    AsyncWriteChannelOptions asyncWriteChannelOptions = AsyncWriteChannelOptions.builder().build();
    rawStorage = createGoogleCloudStorage(asyncWriteChannelOptions);
    StorageResourceId objectToCreate =
        new StorageResourceId(BUCKET_NAME, "testReadBeyondRangeWithFadviseAuto_Object");
    int objectSize = 20 * 1024;
    int inPlaceSeekLimit = 8 * 1024;
    int minRangeRequestSize = 4 * 1024;
    byte[] objectBytes = writeObject(rawStorage, objectToCreate, /* objectSize= */ objectSize);
    int totalBytes = 2 * 1024;
    byte[] readArray = new byte[totalBytes];
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder()
            .setInplaceSeekLimit(inPlaceSeekLimit)
            .setMinRangeRequestSize(minRangeRequestSize)
            .setGrpcChecksumsEnabled(false)
            .setFadvise(Fadvise.AUTO)
            .build();
    int newPosition = 7 * 1024;
    ByteBuffer readBuffer = ByteBuffer.wrap(readArray);
    int bytesRead;
    try (SeekableByteChannel readableByteChannel = rawStorage.open(objectToCreate, readOptions)) {
      readableByteChannel.position(newPosition);
      bytesRead = readableByteChannel.read(readBuffer);
    }

    byte[] trimmedObjectBytes =
        Arrays.copyOfRange(objectBytes, newPosition, newPosition + totalBytes);
    byte[] readBufferByteArray = Arrays.copyOf(readBuffer.array(), readBuffer.limit());

    assertThat(bytesRead).isEqualTo(totalBytes);
    assertByteArrayEquals(trimmedObjectBytes, readBufferByteArray);
  }

  @Test
  public void testReadBeyondRangeWithFadviseSequential() throws IOException {
    AsyncWriteChannelOptions asyncWriteChannelOptions = AsyncWriteChannelOptions.builder().build();
    rawStorage = createGoogleCloudStorage(asyncWriteChannelOptions);
    StorageResourceId objectToCreate =
        new StorageResourceId(BUCKET_NAME, "testReadBeyondRangeWithFadviseSequential_Object");
    int objectSize = 20 * 1024;
    int inPlaceSeekLimit = 8 * 1024;
    int minRangeRequestSize = 4 * 1024;
    byte[] objectBytes = writeObject(rawStorage, objectToCreate, /* objectSize= */ objectSize);
    int totalBytes = 2 * 1024;
    byte[] readArray = new byte[totalBytes];
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder()
            .setInplaceSeekLimit(inPlaceSeekLimit)
            .setMinRangeRequestSize(minRangeRequestSize)
            .setGrpcChecksumsEnabled(false)
            .setFadvise(Fadvise.SEQUENTIAL)
            .build();
    int newPosition = 7 * 1024;
    ByteBuffer readBuffer = ByteBuffer.wrap(readArray);
    int bytesRead;
    try (SeekableByteChannel readableByteChannel = rawStorage.open(objectToCreate, readOptions)) {
      readableByteChannel.position(newPosition);
      bytesRead = readableByteChannel.read(readBuffer);
    }
    byte[] trimmedObjectBytes =
        Arrays.copyOfRange(objectBytes, newPosition, newPosition + totalBytes);
    byte[] readBufferByteArray = Arrays.copyOf(readBuffer.array(), readBuffer.limit());

    assertThat(bytesRead).isEqualTo(totalBytes);
    assertByteArrayEquals(trimmedObjectBytes, readBufferByteArray);
  }

  @Test
  public void testChannelClosedException() throws IOException {
    rawStorage = createGoogleCloudStorage();
    int totalBytes = 1200;

    StorageResourceId resourceId =
        new StorageResourceId(BUCKET_NAME, "testChannelClosedException_Object");
    writeObject(rawStorage, resourceId, /* objectSize= */ totalBytes);

    byte[] readArray = new byte[totalBytes];
    SeekableByteChannel readableByteChannel = rawStorage.open(resourceId);
    ByteBuffer readBuffer = ByteBuffer.wrap(readArray);
    readBuffer.limit(5);
    readableByteChannel.read(readBuffer);
    assertThat(readableByteChannel.position()).isEqualTo(readBuffer.position());

    readableByteChannel.close();
    readBuffer.clear();

    assertThrows(ClosedChannelException.class, () -> readableByteChannel.read(readBuffer));
  }
}
