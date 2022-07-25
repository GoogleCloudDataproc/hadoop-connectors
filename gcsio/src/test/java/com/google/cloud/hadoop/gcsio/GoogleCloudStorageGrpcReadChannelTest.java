/*
 * Copyright 2022 Google Inc. All Rights Reserved.
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

package com.google.cloud.hadoop.gcsio;

import static com.google.common.truth.Truth.assertThat;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.google.api.client.util.BackOff;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.Fadvise;
import com.google.common.hash.Hashing;
import com.google.protobuf.ByteString;
import com.google.storage.v2.ChecksummedData;
import com.google.storage.v2.Object;
import com.google.storage.v2.ObjectChecksums;
import com.google.storage.v2.ReadObjectRequest;
import com.google.storage.v2.ReadObjectResponse;
import com.google.storage.v2.StorageGrpc;
import com.google.storage.v2.StorageGrpc.StorageBlockingStub;
import com.google.storage.v2.StorageGrpc.StorageImplBase;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;

@RunWith(JUnit4.class)
public final class GoogleCloudStorageGrpcReadChannelTest {

  private static final String V1_BUCKET_NAME = "bucket-name";
  private static final String BUCKET_NAME = GrpcChannelUtils.toV2BucketName(V1_BUCKET_NAME);
  private static final String OBJECT_NAME = "object-name";
  private static final long OBJECT_GENERATION = 7;
  private static final int OBJECT_SIZE =
      toIntExact(GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE + 10);
  private static final int DEFAULT_OBJECT_CRC32C = 185327488;
  private static final Object DEFAULT_OBJECT =
      Object.newBuilder()
          .setBucket(V1_BUCKET_NAME) // returning bucket name in v1 format as metadata is
          // fetched via json api
          .setName(OBJECT_NAME)
          .setSize(OBJECT_SIZE)
          .setChecksums(ObjectChecksums.newBuilder().setCrc32C(DEFAULT_OBJECT_CRC32C).build())
          .setGeneration(OBJECT_GENERATION)
          .build();
  private static final ReadObjectRequest GET_OBJECT_MEDIA_REQUEST =
      ReadObjectRequest.newBuilder()
          .setBucket(BUCKET_NAME)
          .setObject(OBJECT_NAME)
          .setGeneration(OBJECT_GENERATION)
          .build();
  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
  private StorageBlockingStub stub;
  private FakeService fakeService;
  private static final Watchdog watchdog = Watchdog.create(Duration.ofMillis(100));
  private long objectSize;
  private TestServerHeaderInterceptor headerInterceptor;

  @Before
  public void setUp() throws Exception {
    fakeService = spy(new FakeService());
    objectSize = OBJECT_SIZE;
    String serverName = InProcessServerBuilder.generateName();
    headerInterceptor = new TestServerHeaderInterceptor();
    grpcCleanup.register(
        InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(fakeService)
            .intercept(headerInterceptor)
            .build()
            .start());
    stub =
        StorageGrpc.newBlockingStub(
            grpcCleanup.register(
                InProcessChannelBuilder.forName(serverName).directExecutor().build()));
  }

  @Test
  public void readSingleChunkSucceeds() throws Exception {
    objectSize = FakeService.CHUNK_SIZE;
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(objectSize).build());
    verify(fakeService, times(1)).setObject(any());
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setMinRangeRequestSize(4).build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);

    ByteBuffer buffer = ByteBuffer.allocate(100);
    readChannel.read(buffer);

    verify(fakeService, times(1))
        .readObject(
            eq(
                ReadObjectRequest.newBuilder()
                    .setBucket(BUCKET_NAME)
                    .setObject(OBJECT_NAME)
                    .setGeneration(OBJECT_GENERATION)
                    .build()),
            any());
    assertThat(buffer.array()).isEqualTo(fakeService.data.substring(0, 100).toByteArray());
    verifyNoMoreInteractions(fakeService);

    headerInterceptor.verifyAllRequestsHasGoogRequestParamsHeader(V1_BUCKET_NAME, 1);
  }

  @Test
  public void readMultipleChunksSucceeds() throws Exception {
    // Enough to require multiple chunks.
    objectSize = FakeService.CHUNK_SIZE * 2;
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(objectSize).build());
    verify(fakeService, times(1)).setObject(any());
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setMinRangeRequestSize(4).build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);

    ByteBuffer buffer = ByteBuffer.allocate(toIntExact(objectSize));
    readChannel.read(buffer);

    verify(fakeService, times(1))
        .readObject(
            eq(
                ReadObjectRequest.newBuilder()
                    .setBucket(BUCKET_NAME)
                    .setObject(OBJECT_NAME)
                    .setGeneration(OBJECT_GENERATION)
                    .build()),
            any());
    assertThat(buffer.array())
        .isEqualTo(fakeService.data.substring(0, toIntExact(objectSize)).toByteArray());
    verifyNoMoreInteractions(fakeService);

    headerInterceptor.verifyAllRequestsHasGoogRequestParamsHeader(V1_BUCKET_NAME, 1);
  }

  @Test
  public void readAfterRepositioningAfterSkippingSucceeds() throws Exception {
    objectSize = GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE * 10;
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(objectSize).build());
    verify(fakeService, times(1)).setObject(any());
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setInplaceSeekLimit(10).build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);

    ByteBuffer bufferAtBeginning = ByteBuffer.allocate(20);
    readChannel.read(bufferAtBeginning);
    readChannel.position(25);
    ByteBuffer bufferFromSkippedSection1 = ByteBuffer.allocate(5);
    readChannel.read(bufferFromSkippedSection1);
    readChannel.position(35);
    ByteBuffer bufferFromSkippedSection2 = ByteBuffer.allocate(10);
    readChannel.read(bufferFromSkippedSection2);
    ByteBuffer bufferFromReposition = ByteBuffer.allocate(10);
    readChannel.position(1);
    readChannel.read(bufferFromReposition);

    assertThat(bufferAtBeginning.array())
        .isEqualTo(fakeService.data.substring(0, 20).toByteArray());
    verify(fakeService, times(1))
        .readObject(
            eq(
                ReadObjectRequest.newBuilder()
                    .setBucket(BUCKET_NAME)
                    .setObject(OBJECT_NAME)
                    .setGeneration(OBJECT_GENERATION)
                    .build()),
            any());
    verify(fakeService, times(1))
        .readObject(
            eq(
                ReadObjectRequest.newBuilder()
                    .setBucket(BUCKET_NAME)
                    .setObject(OBJECT_NAME)
                    .setGeneration(OBJECT_GENERATION)
                    .setReadOffset(1)
                    .build()),
            any());
    assertThat(bufferFromSkippedSection1.array())
        .isEqualTo(fakeService.data.substring(25, 30).toByteArray());
    assertThat(bufferFromSkippedSection2.array())
        .isEqualTo(fakeService.data.substring(35, 45).toByteArray());
    assertThat(bufferFromReposition.array())
        .isEqualTo(fakeService.data.substring(1, 11).toByteArray());
    verifyNoMoreInteractions(fakeService);

    headerInterceptor.verifyAllRequestsHasGoogRequestParamsHeader(V1_BUCKET_NAME, 2);
  }

  @Test
  public void multipleSequentialReads() throws Exception {
    objectSize = GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE * 10;
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(objectSize).build());
    verify(fakeService, times(1)).setObject(any());
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setInplaceSeekLimit(10).build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);

    ByteBuffer first_buffer = ByteBuffer.allocate(10);
    ByteBuffer second_buffer = ByteBuffer.allocate(20);
    readChannel.read(first_buffer);
    readChannel.read(second_buffer);

    assertThat(first_buffer.array()).isEqualTo(fakeService.data.substring(0, 10).toByteArray());
    assertThat(second_buffer.array()).isEqualTo(fakeService.data.substring(10, 30).toByteArray());
    verify(fakeService, times(1))
        .readObject(
            eq(
                ReadObjectRequest.newBuilder()
                    .setBucket(BUCKET_NAME)
                    .setObject(OBJECT_NAME)
                    .setGeneration(OBJECT_GENERATION)
                    .build()),
            any());
    verifyNoMoreInteractions(fakeService);

    headerInterceptor.verifyAllRequestsHasGoogRequestParamsHeader(V1_BUCKET_NAME, 1);
  }

  @Test
  public void randomReadRequestsExactBytes() throws Exception {
    objectSize = GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE * 10;
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(objectSize).build());
    verify(fakeService, times(1)).setObject(any());
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder()
            .setFadvise(Fadvise.RANDOM)
            .setGrpcChecksumsEnabled(true)
            .setInplaceSeekLimit(5)
            .build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);

    ByteBuffer buffer = ByteBuffer.allocate(50);
    readChannel.position(10);
    readChannel.read(buffer);

    ReadObjectRequest expectedRequest =
        ReadObjectRequest.newBuilder()
            .setBucket(BUCKET_NAME)
            .setObject(OBJECT_NAME)
            .setGeneration(OBJECT_GENERATION)
            .setReadLimit(GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE)
            .setReadOffset(10)
            .build();
    verify(fakeService, times(1)).readObject(eq(expectedRequest), any());
    assertThat(buffer.array()).isEqualTo(fakeService.data.substring(10, 60).toByteArray());
    verifyNoMoreInteractions(fakeService);

    headerInterceptor.verifyAllRequestsHasGoogRequestParamsHeader(V1_BUCKET_NAME, 1);
  }

  @Test
  public void repeatedRandomReadsWorkAsExpected() throws Exception {
    objectSize = GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE * 10;
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(objectSize).build());
    verify(fakeService, times(1)).setObject(any());
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder()
            .setFadvise(Fadvise.RANDOM)
            .setGrpcChecksumsEnabled(true)
            .setInplaceSeekLimit(5)
            .build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);

    ByteBuffer buffer = ByteBuffer.allocate(50);
    readChannel.position(10);
    readChannel.read(buffer);
    assertThat(buffer.array()).isEqualTo(fakeService.data.substring(10, 60).toByteArray());

    buffer = ByteBuffer.allocate(25);
    readChannel.position(20);
    readChannel.read(buffer);
    assertThat(buffer.array()).isEqualTo(fakeService.data.substring(20, 45).toByteArray());

    ReadObjectRequest firstExpectedRequest =
        ReadObjectRequest.newBuilder()
            .setBucket(BUCKET_NAME)
            .setObject(OBJECT_NAME)
            .setGeneration(OBJECT_GENERATION)
            .setReadLimit(GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE)
            .setReadOffset(10)
            .build();
    ReadObjectRequest secondExpectedRequest =
        ReadObjectRequest.newBuilder()
            .setBucket(BUCKET_NAME)
            .setObject(OBJECT_NAME)
            .setGeneration(OBJECT_GENERATION)
            .setReadLimit(GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE)
            .setReadOffset(20)
            .build();

    verify(fakeService, times(1)).readObject(eq(firstExpectedRequest), any());
    verify(fakeService, times(1)).readObject(eq(secondExpectedRequest), any());
    verifyNoMoreInteractions(fakeService);

    headerInterceptor.verifyAllRequestsHasGoogRequestParamsHeader(V1_BUCKET_NAME, 2);
  }

  @Test
  public void randomReadRequestsExpectedBytes() throws Exception {
    objectSize = GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE * 10;
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(objectSize).build());
    verify(fakeService, times(1)).setObject(any());
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder()
            .setFadvise(Fadvise.RANDOM)
            .setGrpcChecksumsEnabled(true)
            .setInplaceSeekLimit(5)
            .build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);

    // Request bytes less than minimum request size.
    ByteBuffer buffer = ByteBuffer.allocate(50);
    readChannel.position(10);
    readChannel.read(buffer);
    assertThat(buffer.array()).isEqualTo(fakeService.data.substring(10, 60).toByteArray());

    // Request bytes larger than minimum request size.
    int bufferSize = toIntExact(GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE + 1);
    buffer = ByteBuffer.allocate(bufferSize);
    readChannel.position(0);
    readChannel.read(buffer);
    assertThat(buffer.array())
        .isEqualTo(
            fakeService
                .data
                .substring(
                    0, toIntExact(GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE + 1))
                .toByteArray());

    ReadObjectRequest firstExpectedRequest =
        ReadObjectRequest.newBuilder()
            .setBucket(BUCKET_NAME)
            .setObject(OBJECT_NAME)
            .setGeneration(OBJECT_GENERATION)
            .setReadLimit(GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE)
            .setReadOffset(10)
            .build();
    ReadObjectRequest secondExpectedRequest =
        ReadObjectRequest.newBuilder()
            .setBucket(BUCKET_NAME)
            .setObject(OBJECT_NAME)
            .setGeneration(OBJECT_GENERATION)
            .setReadLimit(GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE + 1)
            .setReadOffset(0)
            .build();

    verify(fakeService, times(1)).readObject(eq(firstExpectedRequest), any());
    verify(fakeService, times(1)).readObject(eq(secondExpectedRequest), any());
    verifyNoMoreInteractions(fakeService);

    headerInterceptor.verifyAllRequestsHasGoogRequestParamsHeader(V1_BUCKET_NAME, 2);
  }

  @Test
  public void readToBufferWithArrayOffset() throws Exception {
    objectSize = 100;
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(objectSize).build());
    verify(fakeService, times(1)).setObject(any());
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setMinRangeRequestSize(4).build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);

    byte[] array = new byte[200];
    // `slice` generates a ByteBuffer with a non-zero `arrayOffset`.
    ByteBuffer buffer = ByteBuffer.wrap(array, 50, 150).slice();
    readChannel.read(buffer);

    byte[] expected = ByteString.copyFrom(array, 50, toIntExact(objectSize)).toByteArray();
    verify(fakeService, times(1))
        .readObject(
            eq(
                ReadObjectRequest.newBuilder()
                    .setBucket(BUCKET_NAME)
                    .setObject(OBJECT_NAME)
                    .setGeneration(OBJECT_GENERATION)
                    .build()),
            any());
    assertThat(fakeService.data.substring(0, toIntExact(objectSize)).toByteArray())
        .isEqualTo(expected);
    verifyNoMoreInteractions(fakeService);

    headerInterceptor.verifyAllRequestsHasGoogRequestParamsHeader(V1_BUCKET_NAME, 1);
  }

  @Test
  public void readSucceedsAfterSeek() throws Exception {
    objectSize = 100;
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(objectSize).build());
    verify(fakeService, times(1)).setObject(any());
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder()
            .setMinRangeRequestSize(4)
            .setInplaceSeekLimit(10)
            .build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);

    ByteBuffer buffer = ByteBuffer.allocate(10);
    readChannel.position(50);
    readChannel.read(buffer);

    verify(fakeService, times(1))
        .readObject(eq(GET_OBJECT_MEDIA_REQUEST.toBuilder().setReadOffset(50).build()), any());
    assertThat(buffer.array()).isEqualTo(fakeService.data.substring(50, 60).toByteArray());
    verifyNoMoreInteractions(fakeService);

    headerInterceptor.verifyAllRequestsHasGoogRequestParamsHeader(V1_BUCKET_NAME, 1);
  }

  @Test
  public void singleReadSucceedsWithValidObjectChecksum() throws Exception {
    fakeService.setObject(
        DEFAULT_OBJECT.toBuilder()
            .setChecksums(ObjectChecksums.newBuilder().setCrc32C(DEFAULT_OBJECT_CRC32C))
            .build());
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setGrpcChecksumsEnabled(true).build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);

    ByteBuffer buffer = ByteBuffer.allocate(OBJECT_SIZE);
    readChannel.read(buffer);

    assertThat(buffer.array()).isEqualTo(fakeService.data.toByteArray());

    headerInterceptor.verifyAllRequestsHasGoogRequestParamsHeader(V1_BUCKET_NAME, 1);
  }

  @Test
  public void partialReadSucceedsWithInvalidObjectChecksum() throws Exception {
    fakeService.setObject(
        DEFAULT_OBJECT.toBuilder()
            .setChecksums(ObjectChecksums.newBuilder().setCrc32C(DEFAULT_OBJECT_CRC32C))
            .build());
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setGrpcChecksumsEnabled(true).build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);

    ByteBuffer buffer = ByteBuffer.allocate(OBJECT_SIZE - 10);
    readChannel.read(buffer);

    assertThat(buffer.array())
        .isEqualTo(fakeService.data.substring(0, OBJECT_SIZE - 10).toByteArray());

    headerInterceptor.verifyAllRequestsHasGoogRequestParamsHeader(V1_BUCKET_NAME, 1);
  }

  @Test
  public void multipleSequentialReadsSucceedWithValidObjectChecksum() throws Exception {
    fakeService.setObject(
        DEFAULT_OBJECT.toBuilder()
            .setChecksums(ObjectChecksums.newBuilder().setCrc32C(DEFAULT_OBJECT_CRC32C))
            .build());
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setGrpcChecksumsEnabled(true).build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);

    ByteBuffer firstBuffer = ByteBuffer.allocate(100);
    ByteBuffer secondBuffer = ByteBuffer.allocate(OBJECT_SIZE - 100);
    readChannel.read(firstBuffer);
    readChannel.read(secondBuffer);

    assertThat(firstBuffer.array()).isEqualTo(fakeService.data.substring(0, 100).toByteArray());
    assertThat(secondBuffer.array()).isEqualTo(fakeService.data.substring(100).toByteArray());

    headerInterceptor.verifyAllRequestsHasGoogRequestParamsHeader(V1_BUCKET_NAME, 1);
  }

  @Test
  public void readFailsWithInvalidMessageChecksum() throws Exception {
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setGrpcChecksumsEnabled(true).build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);

    fakeService.setReturnIncorrectMessageChecksum();

    ByteBuffer buffer = ByteBuffer.allocate(10);
    IOException thrown = assertThrows(IOException.class, () -> readChannel.read(buffer));
    assertThat(thrown).hasMessageThat().contains("checksum");

    headerInterceptor.verifyAllRequestsHasGoogRequestParamsHeader(V1_BUCKET_NAME, 1);
  }

  @Test
  public void readToBufferWithArrayOffsetSucceeds() throws Exception {
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setGrpcChecksumsEnabled(true).build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);

    byte[] array = new byte[OBJECT_SIZE + 100];
    // `ByteBuffer.slice` generates a ByteBuffer with a non-zero `arrayOffset`.
    ByteBuffer buffer = ByteBuffer.wrap(array, 50, OBJECT_SIZE).slice();
    readChannel.read(buffer);

    byte[] expected = ByteString.copyFrom(array, 50, OBJECT_SIZE).toByteArray();
    assertThat(fakeService.data.toByteArray()).isEqualTo(expected);

    headerInterceptor.verifyAllRequestsHasGoogRequestParamsHeader(V1_BUCKET_NAME, 1);
  }

  @Test
  public void readToBufferWithArrayOffsetFailsWithInvalidMessageChecksum() throws Exception {
    fakeService.setReturnIncorrectMessageChecksum();
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setGrpcChecksumsEnabled(true).build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);
    ByteBuffer buffer = ByteBuffer.allocate(10);

    IOException thrown = assertThrows(IOException.class, () -> readChannel.read(buffer));
    assertThat(thrown).hasMessageThat().contains("checksum");

    headerInterceptor.verifyAllRequestsHasGoogRequestParamsHeader(V1_BUCKET_NAME, 1);
  }

  @Test
  public void multipleReadsIgnoreObjectChecksumForLatestGenerationReads() throws Exception {
    fakeService.setObject(
        DEFAULT_OBJECT.toBuilder()
            .setChecksums(ObjectChecksums.newBuilder().setCrc32C(DEFAULT_OBJECT_CRC32C))
            .build());
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setGrpcChecksumsEnabled(true).build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);

    ByteBuffer firstBuffer = ByteBuffer.allocate(100);
    ByteBuffer secondBuffer = ByteBuffer.allocate(OBJECT_SIZE - 100);
    readChannel.read(firstBuffer);
    readChannel.read(secondBuffer);

    assertThat(firstBuffer.array()).isEqualTo(fakeService.data.substring(0, 100).toByteArray());
    assertThat(secondBuffer.array()).isEqualTo(fakeService.data.substring(100).toByteArray());
  }

  @Test
  public void readHandlesGetMediaError() throws Exception {
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel();
    fakeService.setReadObjectException(
        Status.fromCode(Status.Code.INTERNAL)
            .withDescription("Custom error message.")
            .asException());

    ByteBuffer buffer = ByteBuffer.allocate(10);
    IOException thrown = assertThrows(IOException.class, () -> readChannel.read(buffer));
    assertThat(thrown).hasCauseThat().hasMessageThat().contains("Custom error message.");
  }

  @Test
  public void testOpenThrowsIOExceptionOnGetMediaError() throws IOException {
    fakeService.setReadObjectException(
        Status.fromCode(Status.Code.INTERNAL)
            .withDescription("Custom error message.")
            .asException());
    verify(fakeService).setReadObjectException(any());
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel();
    ByteBuffer buffer = ByteBuffer.allocate(10);
    IOException thrown = assertThrows(IOException.class, () -> readChannel.read(buffer));
    assertThat(thrown).hasCauseThat().hasMessageThat().contains("Custom error message.");
    verify(fakeService).readObject(any(), any());
    verifyNoMoreInteractions(fakeService);

    headerInterceptor.verifyAllRequestsHasGoogRequestParamsHeader(V1_BUCKET_NAME, 1);
  }

  @Test
  public void testOpenThrowsIOExceptionOnGetMediaFileNotFound() throws IOException {
    fakeService.setReadObjectException(
        Status.fromCode(Code.NOT_FOUND).withDescription("Custom error message.").asException());
    verify(fakeService).setReadObjectException(any());
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel();
    ByteBuffer buffer = ByteBuffer.allocate(10);
    IOException thrown = assertThrows(IOException.class, () -> readChannel.read(buffer));
    assertThat(thrown)
        .hasCauseThat()
        .hasCauseThat()
        .hasMessageThat()
        .contains("Custom error message.");
    verify(fakeService).readObject(any(), any());
    verifyNoMoreInteractions(fakeService);
  }

  @Test
  public void testOpenThrowsIOExceptionOnGzipContent() {
    GoogleCloudStorageItemInfo itemInfo =
        GoogleCloudStorageItemInfo.createObject(
            new StorageResourceId(V1_BUCKET_NAME, OBJECT_NAME),
            /* creationTime= */ 10L,
            /* modificationTime= */ 15L,
            /* size= */ objectSize,
            /* contentType= */ "text/plain",
            /* contentEncoding= */ "gzip",
            /* metadata= */ null,
            /* contentGeneration= */ OBJECT_GENERATION,
            /* metaGeneration= */ 2L,
            /* verificationAttributes= */ null);

    IOException e =
        assertThrows(
            IOException.class,
            () -> newReadChannel(itemInfo, GoogleCloudStorageReadOptions.DEFAULT));

    assertThat(e)
        .hasMessageThat()
        .isEqualTo(
            "Cannot read GZIP-encoded file (gzip) (not supported via gRPC API): "
                + itemInfo.getResourceId());
  }

  @Test
  public void retryGetMediaError() throws Exception {
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel();
    fakeService.setReadObjectException(
        Status.fromCode(Status.Code.INTERNAL)
            .withDescription("Custom error message.")
            .asException());

    ByteBuffer buffer = ByteBuffer.allocate(10);
    IOException thrown = assertThrows(IOException.class, () -> readChannel.read(buffer));
    assertThat(thrown).hasCauseThat().hasMessageThat().contains("Custom error message");
  }

  @Test
  public void readFailsOnClosedChannel() throws Exception {
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel();

    readChannel.close();
    ByteBuffer buffer = ByteBuffer.allocate(10);
    assertThrows(ClosedChannelException.class, () -> readChannel.read(buffer));

    headerInterceptor.verifyAllRequestsHasGoogRequestParamsHeader(V1_BUCKET_NAME, 0);
  }

  @Test
  public void readWithStrictGenerationReadConsistencySucceeds() throws Exception {
    objectSize = 100;
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(objectSize).setGeneration(1).build());
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setMinRangeRequestSize(4).build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);

    ByteBuffer buffer = ByteBuffer.allocate(10);
    readChannel.read(buffer);
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(objectSize).setGeneration(2).build());
    readChannel.position(0);
    buffer.clear();
    readChannel.read(buffer);

    ArgumentCaptor<ReadObjectRequest> requestCaptor =
        ArgumentCaptor.forClass(ReadObjectRequest.class);
    verify(fakeService, times(2)).readObject(requestCaptor.capture(), any());

    headerInterceptor.verifyAllRequestsHasGoogRequestParamsHeader(V1_BUCKET_NAME, 2);
  }

  @Test
  public void readWithLatestGenerationReadConsistencySucceeds() throws Exception {
    objectSize = 100;
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(objectSize).setGeneration(1).build());
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setMinRangeRequestSize(4).build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);

    ByteBuffer buffer = ByteBuffer.allocate(10);
    readChannel.read(buffer);
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(objectSize).setGeneration(2).build());
    readChannel.position(0);
    buffer.clear();
    readChannel.read(buffer);

    ArgumentCaptor<ReadObjectRequest> requestCaptor =
        ArgumentCaptor.forClass(ReadObjectRequest.class);
    verify(fakeService, times(2)).readObject(requestCaptor.capture(), any());

    headerInterceptor.verifyAllRequestsHasGoogRequestParamsHeader(V1_BUCKET_NAME, 2);
  }

  @Test
  public void seekUnderInplaceSeekLimitReadsCorrectBufferedData() throws Exception {
    objectSize = 100;
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(objectSize).build());
    verify(fakeService, times(1)).setObject(any());
    int minRangeRequestSize = 10;
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder()
            .setMinRangeRequestSize(minRangeRequestSize)
            .setInplaceSeekLimit(10)
            .build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);

    ByteBuffer buffer = ByteBuffer.allocate(20);
    readChannel.read(buffer);
    readChannel.position(25);
    buffer.clear();
    readChannel.read(buffer);

    verify(fakeService, times(1))
        .readObject(
            eq(
                ReadObjectRequest.newBuilder()
                    .setBucket(BUCKET_NAME)
                    .setObject(OBJECT_NAME)
                    .setGeneration(OBJECT_GENERATION)
                    .build()),
            any());
    assertThat(buffer.array()).isEqualTo(fakeService.data.substring(25, 45).toByteArray());
    verifyNoMoreInteractions(fakeService);

    headerInterceptor.verifyAllRequestsHasGoogRequestParamsHeader(V1_BUCKET_NAME, 1);
  }

  @Test
  public void consecutiveSeekBackToSamePosition() throws Exception {
    objectSize = 100;
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(objectSize).build());
    verify(fakeService, times(1)).setObject(any());

    int minRangeRequestSize = 10;
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder()
            .setMinRangeRequestSize(minRangeRequestSize)
            .setInplaceSeekLimit(10)
            .build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);
    assertThat(readChannel.position()).isEqualTo(0);

    readChannel.position(5);
    assertThat(readChannel.position()).isEqualTo(5);

    readChannel.position(0);
    assertThat(readChannel.position()).isEqualTo(0);

    headerInterceptor.verifyAllRequestsHasGoogRequestParamsHeader(V1_BUCKET_NAME, 0);
  }

  @Test
  public void seekUnderInplaceSeekLimitReadsCorrectDataWithSomeBuffered() throws Exception {
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(FakeService.CHUNK_SIZE * 4).build());
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setInplaceSeekLimit(10).build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);

    ByteBuffer buffer = ByteBuffer.allocate(20);
    readChannel.read(buffer);
    readChannel.position(50);
    buffer.clear();
    buffer = ByteBuffer.allocate(FakeService.CHUNK_SIZE * 3 + 7);
    readChannel.read(buffer);

    assertThat(buffer.array())
        .isEqualTo(
            fakeService.data.substring(50, 50 + FakeService.CHUNK_SIZE * 3 + 7).toByteArray());

    headerInterceptor.verifyAllRequestsHasGoogRequestParamsHeader(V1_BUCKET_NAME, 2);
  }

  @Test
  public void seekBeyondInplaceSeekLimitReadsNoBufferedData() throws Exception {
    objectSize = 100;
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(objectSize).build());
    verify(fakeService, times(1)).setObject(any());
    int minRangeRequestSize = 10;
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder()
            .setInplaceSeekLimit(10)
            .setFadvise(Fadvise.AUTO)
            .setMinRangeRequestSize(minRangeRequestSize)
            .build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);

    ByteBuffer buffer = ByteBuffer.allocate(20);
    readChannel.read(buffer);
    readChannel.position(35);
    buffer.clear();
    readChannel.read(buffer);

    verify(fakeService, times(1))
        .readObject(
            eq(
                ReadObjectRequest.newBuilder()
                    .setBucket(BUCKET_NAME)
                    .setObject(OBJECT_NAME)
                    .setGeneration(OBJECT_GENERATION)
                    .build()),
            any());
    verify(fakeService, times(1))
        .readObject(
            eq(
                ReadObjectRequest.newBuilder()
                    .setBucket(BUCKET_NAME)
                    .setObject(OBJECT_NAME)
                    .setGeneration(OBJECT_GENERATION)
                    .setReadOffset(35)
                    .setReadLimit(20)
                    .build()),
            any());
    assertThat(buffer.array()).isEqualTo(fakeService.data.substring(35, 55).toByteArray());
    verifyNoMoreInteractions(fakeService);

    headerInterceptor.verifyAllRequestsHasGoogRequestParamsHeader(V1_BUCKET_NAME, 2);
  }

  @Test
  public void firstReadBeyondInPlaceSeekLimit() throws Exception {
    int objectSize = 100;
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(objectSize).build());
    storageObject.setSize(BigInteger.valueOf(objectSize));
    verify(fakeService, times(1)).setObject(any());
    int inplaceSeekLimit = 10;
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder()
            .setInplaceSeekLimit(inplaceSeekLimit)
            .setFadvise(Fadvise.AUTO)
            .setMinRangeRequestSize(inplaceSeekLimit)
            .build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);

    ByteBuffer buffer = ByteBuffer.allocate(20);
    readChannel.position(inplaceSeekLimit * 2);
    readChannel.read(buffer);

    verify(get).setFields(METADATA_FIELDS);
    verify(get).execute();
    verify(fakeService, times(1))
        .readObject(
            eq(
                ReadObjectRequest.newBuilder()
                    .setBucket(BUCKET_NAME)
                    .setObject(OBJECT_NAME)
                    .setGeneration(OBJECT_GENERATION)
                    .setReadOffset(inplaceSeekLimit * 2)
                    .build()),
            any());

    verifyNoMoreInteractions(fakeService);

    headerInterceptor.verifyAllRequestsHasGoogRequestParamsHeader(V1_BUCKET_NAME, 1);
  }

  @Test
  public void testFooterSizeBiggerThanContent() throws Exception {
    objectSize = 100;
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(objectSize).build());
    verify(fakeService, times(1)).setObject(any());
    int minRangeRequestSize = 2 * 1024;
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setMinRangeRequestSize(minRangeRequestSize).build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);
    ByteBuffer buffer = ByteBuffer.allocate(20);
    readChannel.position(80);
    readChannel.read(buffer);

    /* footerSize is bigger than object size, only the content is read */
    verify(fakeService, times(1))
        .readObject(
            eq(
                ReadObjectRequest.newBuilder()
                    .setBucket(BUCKET_NAME)
                    .setObject(OBJECT_NAME)
                    .setGeneration(OBJECT_GENERATION)
                    .build()),
            any());
    assertThat(buffer.array()).isEqualTo(fakeService.data.substring(80).toByteArray());
    verifyNoMoreInteractions(fakeService);

    headerInterceptor.verifyAllRequestsHasGoogRequestParamsHeader(V1_BUCKET_NAME, 1);
  }

  @Test
  public void testReadCachedFooter() throws Exception {
    objectSize = 8 * 1024;
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(objectSize).build());
    // verify data setup on mock to ensure this interaction does not conflict with `verify`calls
    verify(fakeService, times(1)).setObject(any());
    int minRangeRequestSize = 2 * 1024;
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder()
            .setMinRangeRequestSize(minRangeRequestSize)
            .setInplaceSeekLimit(2 * 1024)
            .build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);
    ByteBuffer buffer = ByteBuffer.allocate(1024);
    readChannel.read(buffer);

    int footerOffset = 7 * 1024;
    buffer.clear();
    readChannel.position(footerOffset);
    readChannel.read(buffer);

    verify(fakeService, times(1))
        .readObject(
            eq(
                ReadObjectRequest.newBuilder()
                    .setBucket(BUCKET_NAME)
                    .setObject(OBJECT_NAME)
                    .setGeneration(OBJECT_GENERATION)
                    .setReadOffset(footerOffset)
                    .build()),
            any());
    verify(fakeService, times(1))
        .readObject(
            eq(
                ReadObjectRequest.newBuilder()
                    .setBucket(BUCKET_NAME)
                    .setObject(OBJECT_NAME)
                    .setGeneration(OBJECT_GENERATION)
                    .build()),
            any());

    assertThat(buffer.array()).isEqualTo(fakeService.data.substring(footerOffset).toByteArray());

    // reading the footer twice to ensure there are no additional calls to GCS
    buffer.clear();
    readChannel.position(footerOffset);
    readChannel.read(buffer);

    assertThat(buffer.array()).isEqualTo(fakeService.data.substring(footerOffset).toByteArray());
    verifyNoMoreInteractions(fakeService);

    headerInterceptor.verifyAllRequestsHasGoogRequestParamsHeader(V1_BUCKET_NAME, 2);
  }

  @Test
  public void testReadCachedFooterPartially() throws Exception {
    objectSize = 16 * 1024;
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(objectSize).build());
    verify(fakeService, times(1)).setObject(any());
    int minRangeRequestSize = 4 * 1024;
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder()
            .setMinRangeRequestSize(minRangeRequestSize)
            .setInplaceSeekLimit(512)
            .build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);
    ByteBuffer buffer = ByteBuffer.allocate(2 * 1024);
    readChannel.read(buffer);

    verify(fakeService, times(1))
        .readObject(
            eq(
                ReadObjectRequest.newBuilder()
                    .setBucket(BUCKET_NAME)
                    .setObject(OBJECT_NAME)
                    .setGeneration(OBJECT_GENERATION)
                    .build()),
            any());

    buffer.clear();
    int readOffset = 13 * 1024;
    readChannel.position(readOffset);
    readChannel.read(buffer);

    verify(fakeService, times(1))
        .readObject(
            eq(
                ReadObjectRequest.newBuilder()
                    .setBucket(BUCKET_NAME)
                    .setObject(OBJECT_NAME)
                    .setGeneration(OBJECT_GENERATION)
                    .setReadOffset(readOffset)
                    .build()),
            any());
    assertThat(buffer.array())
        .isEqualTo(fakeService.data.substring(readOffset, readOffset + (2 * 1024)).toByteArray());

    // reading the footer twice to ensure there are no additional calls to GCS
    buffer.clear();
    int footerOffset = 14 * 1024;
    readChannel.position(footerOffset);
    readChannel.read(buffer);
    verify(fakeService, times(1))
        .readObject(
            eq(
                ReadObjectRequest.newBuilder()
                    .setBucket(BUCKET_NAME)
                    .setObject(OBJECT_NAME)
                    .setGeneration(OBJECT_GENERATION)
                    .setReadOffset(footerOffset)
                    .build()),
            any());

    assertThat(buffer.array()).isEqualTo(fakeService.data.substring(footerOffset).toByteArray());
    verifyNoMoreInteractions(fakeService);

    headerInterceptor.verifyAllRequestsHasGoogRequestParamsHeader(V1_BUCKET_NAME, 3);
  }

  @Test
  public void testSeekBeforeFooterAndSequentialRead() throws Exception {
    objectSize = 4 * 1024;
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(objectSize).build());
    verify(fakeService, times(1)).setObject(any());
    int minRangeRequestSize = 4 * 1024;
    int readOffset = 1024;
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder()
            .setMinRangeRequestSize(minRangeRequestSize)
            .setInplaceSeekLimit(2 * 1024)
            .build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);
    ByteBuffer buffer = ByteBuffer.allocate(2 * 1024);

    readChannel.position(readOffset);
    readChannel.read(buffer);
    // Only one ReadObjectRequest is sent and there is no separate request to prefetch footer
    verify(fakeService, times(1))
        .readObject(
            eq(
                ReadObjectRequest.newBuilder()
                    .setBucket(BUCKET_NAME)
                    .setObject(OBJECT_NAME)
                    .setGeneration(OBJECT_GENERATION)
                    .setReadOffset(readOffset)
                    .build()),
            any());

    verifyNoMoreInteractions(fakeService);

    headerInterceptor.verifyAllRequestsHasGoogRequestParamsHeader(V1_BUCKET_NAME, 1);
  }

  @Test
  public void testFooterNotCachedInSequentialRead() throws Exception {
    objectSize = 4 * 1024;
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(objectSize).build());
    verify(fakeService, times(1)).setObject(any());
    int minRangeRequestSize = 4 * 1024;
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder()
            .setMinRangeRequestSize(minRangeRequestSize)
            .setInplaceSeekLimit(2 * 1024)
            .build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);
    ByteBuffer buffer = ByteBuffer.allocate(2 * 1024);

    readChannel.read(buffer);
    buffer.clear();

    readChannel.read(buffer);
    buffer.clear();

    // Only one ReadObjectRequest is sent and there is no separate request to prefetch footer
    verify(fakeService, times(1))
        .readObject(
            eq(
                ReadObjectRequest.newBuilder()
                    .setBucket(BUCKET_NAME)
                    .setObject(OBJECT_NAME)
                    .setGeneration(OBJECT_GENERATION)
                    .build()),
            any());

    verifyNoMoreInteractions(fakeService);

    headerInterceptor.verifyAllRequestsHasGoogRequestParamsHeader(V1_BUCKET_NAME, 1);
  }

  @Test
  public void testReadCachedFooterPartiallyWithInplaceSeek() throws Exception {
    objectSize = 16 * 1024;
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(objectSize).build());
    verify(fakeService, times(1)).setObject(any());
    int minRangeRequestSize = 4 * 1024;
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder()
            .setMinRangeRequestSize(minRangeRequestSize)
            .setInplaceSeekLimit(2 * 1024)
            .build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);
    ByteBuffer buffer = ByteBuffer.allocate(2 * 1024);
    readChannel.read(buffer);

    // This should just issue a read from offset 0
    verify(fakeService, times(1))
        .readObject(
            eq(
                ReadObjectRequest.newBuilder()
                    .setBucket(BUCKET_NAME)
                    .setObject(OBJECT_NAME)
                    .setGeneration(OBJECT_GENERATION)
                    .build()),
            any());

    buffer.clear();
    int readOffset = 13 * 1024;
    readChannel.position(readOffset);
    readChannel.read(buffer);

    // This should just issue a read from given offset. Note that we are not
    // yet prefetching the footer
    verify(fakeService, times(1))
        .readObject(
            eq(
                ReadObjectRequest.newBuilder()
                    .setBucket(BUCKET_NAME)
                    .setObject(OBJECT_NAME)
                    .setGeneration(OBJECT_GENERATION)
                    .setReadOffset(readOffset)
                    .build()),
            any());
    assertThat(buffer.array())
        .isEqualTo(fakeService.data.substring(readOffset, readOffset + (2 * 1024)).toByteArray());

    int footerOffset = 14 * 1024;
    buffer.clear();
    readChannel.position(footerOffset);
    readChannel.read(buffer);
    // This is when the footer is read
    verify(fakeService, times(1))
        .readObject(
            eq(
                ReadObjectRequest.newBuilder()
                    .setBucket(BUCKET_NAME)
                    .setObject(OBJECT_NAME)
                    .setGeneration(OBJECT_GENERATION)
                    .setReadOffset(footerOffset)
                    .build()),
            any());

    assertThat(buffer.array()).isEqualTo(fakeService.data.substring(footerOffset).toByteArray());
    verifyNoMoreInteractions(fakeService);

    headerInterceptor.verifyAllRequestsHasGoogRequestParamsHeader(V1_BUCKET_NAME, 3);
  }

  @Test
  public void testReadWithInplaceSeekAndFadviseRandom() throws Exception {
    objectSize = 16 * 1024;
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(objectSize).build());
    verify(fakeService, times(1)).setObject(any());
    int minRangeRequestSize = 4 * 1024;
    int inplaceSeekLimit = 6 * 1024;
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder()
            .setMinRangeRequestSize(minRangeRequestSize)
            .setFadvise(Fadvise.RANDOM)
            .setInplaceSeekLimit(inplaceSeekLimit)
            .build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);
    ByteBuffer buffer = ByteBuffer.allocate(2 * 1024);
    readChannel.read(buffer);

    verify(fakeService, times(1))
        .readObject(
            eq(
                ReadObjectRequest.newBuilder()
                    .setBucket(BUCKET_NAME)
                    .setObject(OBJECT_NAME)
                    .setGeneration(OBJECT_GENERATION)
                    .setReadLimit(inplaceSeekLimit)
                    .build()),
            any());
    buffer.clear();
    int readOffset = 7 * 1024;
    readChannel.position(readOffset);
    int capacity = 4 * 1024;
    buffer = ByteBuffer.allocate(capacity);
    readChannel.read(buffer);

    // verify new request after seek
    verify(fakeService, times(1))
        .readObject(
            eq(
                ReadObjectRequest.newBuilder()
                    .setBucket(BUCKET_NAME)
                    .setObject(OBJECT_NAME)
                    .setGeneration(OBJECT_GENERATION)
                    .setReadOffset(readOffset)
                    .setReadLimit(inplaceSeekLimit)
                    .build()),
            any());
    assertThat(buffer.array())
        .isEqualTo(fakeService.data.substring(readOffset, readOffset + (capacity)).toByteArray());

    verifyNoMoreInteractions(fakeService);

    headerInterceptor.verifyAllRequestsHasGoogRequestParamsHeader(V1_BUCKET_NAME, 2);
  }

  @Test
  public void testReadWithInplaceSeekAndFadviseAuto() throws Exception {
    objectSize = 16 * 1024;
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(objectSize).build());
    verify(fakeService, times(1)).setObject(any());
    int minRangeRequestSize = 4 * 1024;
    int inplaceSeekLimit = 6 * 1024;
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder()
            .setMinRangeRequestSize(minRangeRequestSize)
            .setFadvise(Fadvise.AUTO)
            .setInplaceSeekLimit(inplaceSeekLimit)
            .build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);
    ByteBuffer buffer = ByteBuffer.allocate(2 * 1024);
    readChannel.read(buffer);

    verify(fakeService, times(1))
        .readObject(
            eq(
                ReadObjectRequest.newBuilder()
                    .setBucket(BUCKET_NAME)
                    .setObject(OBJECT_NAME)
                    .setGeneration(OBJECT_GENERATION)
                    .build()),
            any());

    buffer.clear();
    int readOffset = 7 * 1024;
    readChannel.position(readOffset);
    int capacity = 4 * 1024;
    buffer = ByteBuffer.allocate(capacity);
    readChannel.read(buffer);

    assertThat(buffer.array())
        .isEqualTo(fakeService.data.substring(readOffset, readOffset + (capacity)).toByteArray());

    verifyNoMoreInteractions(fakeService);

    headerInterceptor.verifyAllRequestsHasGoogRequestParamsHeader(V1_BUCKET_NAME, 1);
  }

  @Test
  public void testReadWithInplaceSeekAndFadviseSequential() throws Exception {
    objectSize = 16 * 1024;
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(objectSize).build());
    verify(fakeService, times(1)).setObject(any());
    int minRangeRequestSize = 4 * 1024;
    int inplaceSeekLimit = 6 * 1024;
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder()
            .setMinRangeRequestSize(minRangeRequestSize)
            .setFadvise(Fadvise.SEQUENTIAL)
            .setInplaceSeekLimit(inplaceSeekLimit)
            .build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);
    ByteBuffer buffer = ByteBuffer.allocate(2 * 1024);
    readChannel.read(buffer);

    verify(fakeService, times(1))
        .readObject(
            eq(
                ReadObjectRequest.newBuilder()
                    .setBucket(BUCKET_NAME)
                    .setObject(OBJECT_NAME)
                    .setGeneration(OBJECT_GENERATION)
                    .build()),
            any());

    buffer.clear();
    int readOffset = 7 * 1024;
    readChannel.position(readOffset);
    int capacity = 4 * 1024;
    buffer = ByteBuffer.allocate(capacity);
    readChannel.read(buffer);

    assertThat(buffer.array())
        .isEqualTo(fakeService.data.substring(readOffset, readOffset + (capacity)).toByteArray());

    verifyNoMoreInteractions(fakeService);

    headerInterceptor.verifyAllRequestsHasGoogRequestParamsHeader(V1_BUCKET_NAME, 1);
  }

  @Test
  public void testReadWithMultipleSeeks() throws Exception {
    objectSize = 16 * 1024;
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(objectSize).build());
    verify(fakeService, times(1)).setObject(any());
    int minRangeRequestSize = 4 * 1024;
    int inplaceSeekLimit = 6 * 1024;
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder()
            .setMinRangeRequestSize(minRangeRequestSize)
            .setFadvise(Fadvise.AUTO)
            .setInplaceSeekLimit(inplaceSeekLimit)
            .build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);
    ByteBuffer buffer = ByteBuffer.allocate(2 * 1024);
    readChannel.read(buffer);

    verify(fakeService, times(1))
        .readObject(
            eq(
                ReadObjectRequest.newBuilder()
                    .setBucket(BUCKET_NAME)
                    .setObject(OBJECT_NAME)
                    .setGeneration(OBJECT_GENERATION)
                    .build()),
            any());

    buffer.clear();
    readChannel.position(0);
    int readOffset = 7 * 1024;
    readChannel.position(readOffset);
    int capacity = 4 * 1024;
    buffer = ByteBuffer.allocate(capacity);
    readChannel.read(buffer);

    assertThat(buffer.array())
        .isEqualTo(fakeService.data.substring(readOffset, readOffset + (capacity)).toByteArray());

    verifyNoMoreInteractions(fakeService);

    headerInterceptor.verifyAllRequestsHasGoogRequestParamsHeader(V1_BUCKET_NAME, 1);
  }

  @Test
  public void seekFailsOnNegative() throws Exception {
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel();

    assertThrows(EOFException.class, () -> readChannel.position(-1));
  }

  @Test
  public void seekFailsOnClosedChannel() throws Exception {
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel();

    readChannel.close();
    assertThrows(ClosedChannelException.class, () -> readChannel.position(2));
  }

  @Test
  public void positionUpdatesOnRead() throws Exception {
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(100).build());
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel();

    ByteBuffer buffer = ByteBuffer.allocate(50);
    readChannel.read(buffer);

    assertThat(readChannel.position()).isEqualTo(50);

    headerInterceptor.verifyAllRequestsHasGoogRequestParamsHeader(V1_BUCKET_NAME, 1);
  }

  @Test
  public void positionUpdatesOnSeek() throws Exception {
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel();

    readChannel.position(50);

    assertThat(readChannel.position()).isEqualTo(50);
  }

  @Test
  public void positionFailsOnClosedChannel() throws Exception {
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel();

    readChannel.close();
    assertThrows(ClosedChannelException.class, readChannel::position);
  }

  @Test
  public void fastFailOnNotFoundFailsOnCreateWhenEnabled() {
    StorageResourceId resourceId = new StorageResourceId(V1_BUCKET_NAME, OBJECT_NAME);
    GoogleCloudStorageItemInfo itemInfo = GoogleCloudStorageItemInfo.createNotFound(resourceId);
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setFastFailOnNotFoundEnabled(true).build();

    IOException thrown = assertThrows(IOException.class, () -> newReadChannel(itemInfo, options));

    assertThat(thrown).hasMessageThat().isEqualTo("File not found: " + resourceId);
  }

  @Test
  public void fastFailOnNotFoundFailsOnCreateWhenDisabled() {
    StorageResourceId resourceId = new StorageResourceId(V1_BUCKET_NAME, OBJECT_NAME);
    GoogleCloudStorageItemInfo itemInfo = GoogleCloudStorageItemInfo.createNotFound(resourceId);
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setFastFailOnNotFoundEnabled(false).build();

    IOException thrown = assertThrows(IOException.class, () -> newReadChannel(itemInfo, options));

    assertThat(thrown).hasMessageThat().isEqualTo("File not found: " + resourceId);
  }

  @Test
  public void sizeReturnsObjectSize() throws Exception {
    objectSize = 1234;
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(objectSize).build());
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel();

    assertThat(readChannel.size()).isEqualTo(1234);
  }

  @Test
  public void sizeFailsOnClosedChannel() throws Exception {
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel();

    readChannel.close();
    assertThrows(ClosedChannelException.class, readChannel::size);
  }

  @Test
  public void sizeIsCached() throws Exception {
    objectSize = 1234;
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(objectSize).build());
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel();

    assertThat(readChannel.size()).isEqualTo(1234);
    assertThat(readChannel.size()).isEqualTo(1234);
  }

  @Test
  public void isOpenReturnsTrueOnCreate() throws Exception {
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel();

    assertThat(readChannel.isOpen()).isTrue();
  }

  @Test
  public void isOpenReturnsFalseAfterClose() throws Exception {
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel();

    readChannel.close();

    assertThat(readChannel.isOpen()).isFalse();
  }

  @Test
  public void traceLogEnabledGrpcTest() throws Exception {
    AssertingLogHandler assertingHandler = new AssertingLogHandler();
    Logger grpcTracingLogger =
        Logger.getLogger(GoogleCloudStorageGrpcTracingInterceptor.class.getName());
    grpcTracingLogger.setUseParentHandlers(false);
    grpcTracingLogger.addHandler(assertingHandler);
    grpcTracingLogger.setLevel(Level.INFO);

    try {
      readObjectAndVerify(GoogleCloudStorageOptions.builder().setTraceLogEnabled(true).build());
      assertingHandler.assertLogCount(7);
      assertingHandler.verifyCommonTraceFields();

      assertThat(assertingHandler.getMethodAtIndex(0)).isEqualTo("streamCreated");
      assertThat(assertingHandler.getMethodAtIndex(1)).isEqualTo("outboundMessage");
      // InProcessTransport is not reporting the correct size
      // (https://github.com/grpc/grpc-java/blob/master/core/src/main/java/io/grpc/inprocess/InProcessTransport.java#L519).
      // Hence only validating that the relevant methods are called.
      assertThat(assertingHandler.getMethodAtIndex(2)).isEqualTo("outboundMessageSent");
      assertThat(assertingHandler.getMethodAtIndex(3)).isEqualTo("inboundMessage");
      assertThat(assertingHandler.getMethodAtIndex(4)).isEqualTo("inboundMessageRead");
      assertThat(assertingHandler.getMethodAtIndex(5)).isEqualTo("inboundTrailers");
      assertThat(assertingHandler.getMethodAtIndex(6)).isEqualTo("streamClosed");
    } finally {
      grpcTracingLogger.removeHandler(assertingHandler);
    }
  }

  @Test
  public void traceLogDisabledGrpcTest() throws Exception {
    AssertingLogHandler assertingHandler = new AssertingLogHandler();
    Logger grpcTracingLogger =
        Logger.getLogger(GoogleCloudStorageGrpcTracingInterceptor.class.getName());
    grpcTracingLogger.setUseParentHandlers(false);
    grpcTracingLogger.addHandler(assertingHandler);
    grpcTracingLogger.setLevel(Level.INFO);

    try {
      readObjectAndVerify(GoogleCloudStorageOptions.builder().setTraceLogEnabled(false).build());
      assertingHandler.assertLogCount(0);
    } finally {
      grpcTracingLogger.removeHandler(assertingHandler);
    }
  }

  private void readObjectAndVerify(GoogleCloudStorageOptions storageOptions) throws IOException {
    objectSize = FakeService.CHUNK_SIZE;
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(objectSize).build());
    verify(fakeService, times(1)).setObject(any());
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setMinRangeRequestSize(4).build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options, storageOptions);

    ByteBuffer buffer = ByteBuffer.allocate(100);
    readChannel.read(buffer);

    verify(fakeService, times(1))
        .readObject(
            eq(
                ReadObjectRequest.newBuilder()
                    .setBucket(BUCKET_NAME)
                    .setObject(OBJECT_NAME)
                    .setGeneration(OBJECT_GENERATION)
                    .build()),
            any());
    assertThat(buffer.array()).isEqualTo(fakeService.data.substring(0, 100).toByteArray());
    verifyNoMoreInteractions(fakeService);

    headerInterceptor.verifyAllRequestsHasGoogRequestParamsHeader(V1_BUCKET_NAME, 1);
  }

  private GoogleCloudStorageGrpcReadChannel newReadChannel() throws IOException {
    return newReadChannel(GoogleCloudStorageReadOptions.DEFAULT);
  }

  private GoogleCloudStorageGrpcReadChannel newReadChannel(
      GoogleCloudStorageReadOptions readOptions) throws IOException {
    return newReadChannel(readOptions, GoogleCloudStorageOptions.DEFAULT);
  }

  private GoogleCloudStorageGrpcReadChannel newReadChannel(
      GoogleCloudStorageReadOptions readOptions, GoogleCloudStorageOptions storageOptions)
      throws IOException {
    return newReadChannel(
        GoogleCloudStorageItemInfo.createObject(
            new StorageResourceId(V1_BUCKET_NAME, OBJECT_NAME),
            /* creationTime= */ 10L,
            /* modificationTime= */ 15L,
            /* size= */ objectSize,
            /* contentType= */ "text/plain",
            /* contentEncoding= */ "lzma",
            /* metadata= */ null,
            /* contentGeneration= */ OBJECT_GENERATION,
            /* metaGeneration= */ 2L,
            /* verificationAttributes= */ null),
        readOptions,
        storageOptions);
  }

  private GoogleCloudStorageGrpcReadChannel newReadChannel(
      GoogleCloudStorageItemInfo itemInfo, GoogleCloudStorageReadOptions readOptions)
      throws IOException {
    return newReadChannel(itemInfo, readOptions, GoogleCloudStorageOptions.DEFAULT);
  }

  private GoogleCloudStorageGrpcReadChannel newReadChannel(
      GoogleCloudStorageItemInfo itemInfo,
      GoogleCloudStorageReadOptions readOptions,
      GoogleCloudStorageOptions storageOptions)
      throws IOException {
    return new GoogleCloudStorageGrpcReadChannel(
        new FakeStubProvider(),
        itemInfo,
        watchdog,
        new NoOpMetricsRecorder(),
        storageOptions,
        readOptions,
        () -> BackOff.STOP_BACKOFF);
  }

  private static class FakeGrpcDecorator implements StorageStubProvider.GrpcDecorator {

    @Override
    public ManagedChannelBuilder<?> createChannelBuilder(String target) {
      return null;
    }

    @Override
    public AbstractStub<?> applyCallOption(AbstractStub<?> stub) {
      return null;
    }
  }

  private class FakeStubProvider extends StorageStubProvider {
    FakeStubProvider() {
      super(GoogleCloudStorageOptions.DEFAULT, null, new FakeGrpcDecorator());
    }

    @Override
    protected StorageBlockingStub newBlockingStubInternal() {
      return stub;
    }
  }

  private static class FakeService extends StorageImplBase {

    private static final int CHUNK_SIZE = 2048;
    ByteString data;
    private Object object;
    private Throwable readObjectException;
    private boolean alterMessageChecksum = false;

    public FakeService() {
      setObject(DEFAULT_OBJECT);
    }

    private static ByteString createTestData(int numBytes) {
      byte[] result = new byte[numBytes];
      for (int i = 0; i < numBytes; ++i) {
        result[i] = (byte) i;
      }

      return ByteString.copyFrom(result);
    }

    @Override
    public void readObject(
        ReadObjectRequest request, StreamObserver<ReadObjectResponse> responseObserver) {
      if (readObjectException != null) {
        responseObserver.onError(readObjectException);
      } else {
        long readStart = request.getReadOffset();
        long readEnd =
            request.getReadLimit() > 0
                ? min(object.getSize(), readStart + request.getReadLimit())
                : object.getSize();
        for (long position = readStart; position < readEnd; position += CHUNK_SIZE) {
          long endIndex = min(min(object.getSize(), position + CHUNK_SIZE), readEnd);
          ByteString messageData = data.substring(toIntExact(position), toIntExact(endIndex));
          int crc32c = Hashing.crc32c().hashBytes(messageData.toByteArray()).asInt();
          if (alterMessageChecksum) {
            crc32c += 1;
          }
          ReadObjectResponse response =
              ReadObjectResponse.newBuilder()
                  .setChecksummedData(
                      ChecksummedData.newBuilder().setContent(messageData).setCrc32C((crc32c)))
                  .build();
          responseObserver.onNext(response);
        }
        responseObserver.onCompleted();
      }
    }

    public void setObject(Object object) {
      this.object = object;
      data = createTestData((int) object.getSize());
    }

    void setReadObjectException(Throwable t) {
      readObjectException = t;
    }

    void setReturnIncorrectMessageChecksum() {
      alterMessageChecksum = true;
    }
  }
}
