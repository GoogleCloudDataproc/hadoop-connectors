package com.google.cloud.hadoop.gcsio;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.google.api.client.util.BackOff;
import com.google.auth.Credentials;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.Fadvise;
import com.google.common.hash.Hashing;
import com.google.google.storage.v1.ChecksummedData;
import com.google.google.storage.v1.GetObjectMediaRequest;
import com.google.google.storage.v1.GetObjectMediaResponse;
import com.google.google.storage.v1.GetObjectRequest;
import com.google.google.storage.v1.Object;
import com.google.google.storage.v1.StorageGrpc;
import com.google.google.storage.v1.StorageGrpc.StorageBlockingStub;
import com.google.google.storage.v1.StorageGrpc.StorageImplBase;
import com.google.protobuf.ByteString;
import com.google.protobuf.UInt32Value;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.Arrays;
import java.util.List;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(JUnit4.class)
public final class GoogleCloudStorageGrpcReadChannelTest {

  private static final String BUCKET_NAME = "bucket-name";
  private static final String OBJECT_NAME = "object-name";
  private static final long OBJECT_GENERATION = 7;
  private static final int OBJECT_SIZE =
      GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE + 10;
  private static final int DEFAULT_OBJECT_CRC32C = 185327488;
  private static Object DEFAULT_OBJECT =
      Object.newBuilder()
          .setBucket(BUCKET_NAME)
          .setName(OBJECT_NAME)
          .setSize(OBJECT_SIZE)
          .setCrc32C(UInt32Value.newBuilder().setValue(DEFAULT_OBJECT_CRC32C))
          .setGeneration(OBJECT_GENERATION)
          .build();
  private static GetObjectRequest GET_OBJECT_REQUEST =
      GetObjectRequest.newBuilder().setBucket(BUCKET_NAME).setObject(OBJECT_NAME).build();
  private static GetObjectMediaRequest GET_OBJECT_MEDIA_REQUEST =
      GetObjectMediaRequest.newBuilder()
          .setBucket(BUCKET_NAME)
          .setObject(OBJECT_NAME)
          .setGeneration(OBJECT_GENERATION)
          .build();
  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
  private StorageBlockingStub stub;
  private FakeService fakeService;
  @Mock private Credentials mockCredentials;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    fakeService = spy(new FakeService());
    String serverName = InProcessServerBuilder.generateName();
    grpcCleanup.register(
        InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(fakeService)
            .build()
            .start());
    stub =
        StorageGrpc.newBlockingStub(
            grpcCleanup.register(
                InProcessChannelBuilder.forName(serverName).directExecutor().build()));
  }

  @Test
  public void readSingleChunkSucceeds() throws Exception {
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(FakeService.CHUNK_SIZE).build());
    verify(fakeService, times(1)).setObject(any());
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setMinRangeRequestSize(4).build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);

    ByteBuffer buffer = ByteBuffer.allocate(100);
    readChannel.read(buffer);

    verify(fakeService, times(1)).getObject(eq(GET_OBJECT_REQUEST), any());
    verify(fakeService, times(1)).getObjectMedia(eq(GetObjectMediaRequest.newBuilder()
        .setBucket(BUCKET_NAME)
        .setObject(OBJECT_NAME)
        .setReadOffset((FakeService.CHUNK_SIZE) - 2)
        .setReadLimit(4)
        .build()), any());
    verify(fakeService, times(1)).getObjectMedia(eq(GetObjectMediaRequest.newBuilder()
        .setBucket(BUCKET_NAME)
        .setObject(OBJECT_NAME)
        .setGeneration(OBJECT_GENERATION)
        .setReadLimit((FakeService.CHUNK_SIZE) - 2)
        .build()), any());
    assertArrayEquals(fakeService.data.substring(0, 100).toByteArray(), buffer.array());
    verifyNoMoreInteractions(fakeService);
  }

  @Test
  public void readMultipleChunksSucceeds() throws Exception {
    // Enough to require multiple chunks.
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(FakeService.CHUNK_SIZE * 2).build());
    verify(fakeService, times(1)).setObject(any());
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setMinRangeRequestSize(4).build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);

    ByteBuffer buffer = ByteBuffer.allocate(FakeService.CHUNK_SIZE * 2);
    readChannel.read(buffer);

    verify(fakeService, times(1)).getObject(eq(GET_OBJECT_REQUEST), any());
    verify(fakeService, times(1)).getObjectMedia(eq(GetObjectMediaRequest.newBuilder()
        .setBucket(BUCKET_NAME)
        .setObject(OBJECT_NAME)
        .setReadOffset((FakeService.CHUNK_SIZE * 2) - 2)
        .setReadLimit(4)
        .build()), any());
    verify(fakeService, times(1)).getObjectMedia(eq(GetObjectMediaRequest.newBuilder()
        .setBucket(BUCKET_NAME)
        .setObject(OBJECT_NAME)
        .setGeneration(OBJECT_GENERATION)
        .setReadLimit((FakeService.CHUNK_SIZE * 2) - 2)
        .build()), any());
    assertArrayEquals(
        fakeService.data.substring(0, FakeService.CHUNK_SIZE * 2).toByteArray(), buffer.array());
    verifyNoMoreInteractions(fakeService);
  }

  @Test
  public void readAfterRepositioningAfterSkippingSucceeds() throws Exception {
    int objectSize = GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE * 10;
    fakeService.setObject(DEFAULT_OBJECT.toBuilder()
        .setSize(objectSize).build());
    verify(fakeService, times(1)).setObject(any());
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setInplaceSeekLimit(10)
            .build();
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

    verify(fakeService, times(1)).getObject(eq(GET_OBJECT_REQUEST), any());
    assertArrayEquals(fakeService.data.substring(0, 20).toByteArray(), bufferAtBeginning.array());
    verify(fakeService, times(1)).getObjectMedia(eq(GetObjectMediaRequest.newBuilder()
        .setBucket(BUCKET_NAME)
        .setObject(OBJECT_NAME)
        .setReadOffset(
            objectSize - (GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE / 2))
        .setReadLimit(GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE)
        .build()), any());
    verify(fakeService, times(1)).getObjectMedia(eq(GetObjectMediaRequest.newBuilder()
        .setBucket(BUCKET_NAME)
        .setObject(OBJECT_NAME)
        .setGeneration(OBJECT_GENERATION)
        .setReadLimit(
            objectSize - (GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE / 2))
        .build()), any());
    verify(fakeService, times(1)).getObjectMedia(eq(GetObjectMediaRequest.newBuilder()
        .setBucket(BUCKET_NAME)
        .setObject(OBJECT_NAME)
        .setGeneration(OBJECT_GENERATION)
        .setReadOffset(1)
        .setReadLimit(
            objectSize - (GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE / 2) - 1)
        .build()), any());
    assertArrayEquals(
        fakeService.data.substring(25, 30).toByteArray(), bufferFromSkippedSection1.array());
    assertArrayEquals(
        fakeService.data.substring(40, 50).toByteArray(), bufferFromSkippedSection2.array());
    assertArrayEquals(
        fakeService.data.substring(1, 11).toByteArray(), bufferFromReposition.array());
    verifyNoMoreInteractions(fakeService);
  }

  @Test
  public void multipleSequentialReads() throws Exception {
    int objectSize = GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE * 10;
    fakeService.setObject(DEFAULT_OBJECT.toBuilder()
        .setSize(objectSize).build());
    verify(fakeService, times(1)).setObject(any());
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setInplaceSeekLimit(10)
            .build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);

    ByteBuffer first_buffer = ByteBuffer.allocate(10);
    ByteBuffer second_buffer = ByteBuffer.allocate(20);
    readChannel.read(first_buffer);
    readChannel.read(second_buffer);

    verify(fakeService, times(1)).getObject(eq(GET_OBJECT_REQUEST), any());
    assertArrayEquals(fakeService.data.substring(0, 10).toByteArray(), first_buffer.array());
    assertArrayEquals(fakeService.data.substring(10, 30).toByteArray(), second_buffer.array());
    verify(fakeService, times(1)).getObjectMedia(eq(GetObjectMediaRequest.newBuilder()
        .setBucket(BUCKET_NAME)
        .setObject(OBJECT_NAME)
        .setReadOffset(
            objectSize - (GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE / 2))
        .setReadLimit(GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE)
        .build()), any());
    verify(fakeService, times(1)).getObjectMedia(eq(GetObjectMediaRequest.newBuilder()
        .setBucket(BUCKET_NAME)
        .setObject(OBJECT_NAME)
        .setGeneration(OBJECT_GENERATION)
        .setReadLimit(
            objectSize - (GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE / 2))
        .build()), any());
    verifyNoMoreInteractions(fakeService);
  }

  @Test
  public void randomReadRequestsExactBytes() throws Exception {
    int objectSize = GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE * 10;
    fakeService.setObject(DEFAULT_OBJECT.toBuilder()
        .setSize(objectSize).build());
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

    verify(fakeService, times(1)).getObject(eq(GET_OBJECT_REQUEST), any());

    GetObjectMediaRequest expectedRequest =
        GetObjectMediaRequest.newBuilder()
            .setBucket(BUCKET_NAME)
            .setObject(OBJECT_NAME)
            .setGeneration(OBJECT_GENERATION)
            .setReadLimit(GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE)
            .setReadOffset(10)
            .build();
    verify(fakeService, times(1)).getObjectMedia(eq(expectedRequest), any());
    assertArrayEquals(fakeService.data.substring(10, 60).toByteArray(), buffer.array());
    verify(fakeService, times(1)).getObjectMedia(eq(GetObjectMediaRequest.newBuilder()
        .setBucket(BUCKET_NAME)
        .setObject(OBJECT_NAME)
        .setReadOffset(
            objectSize - (GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE / 2))
        .setReadLimit(GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE)
        .build()), any());
    verifyNoMoreInteractions(fakeService);
  }

  @Test
  public void repeatedRandomReadsWorkAsExpected() throws Exception {
    int objectSize = GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE * 10;
    fakeService.setObject(DEFAULT_OBJECT.toBuilder()
        .setSize(objectSize).build());
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
    assertArrayEquals(fakeService.data.substring(10, 60).toByteArray(), buffer.array());

    buffer = ByteBuffer.allocate(25);
    readChannel.position(20);
    readChannel.read(buffer);
    assertArrayEquals(fakeService.data.substring(20, 45).toByteArray(), buffer.array());

    GetObjectMediaRequest firstExpectedRequest =
        GetObjectMediaRequest.newBuilder()
            .setBucket(BUCKET_NAME)
            .setObject(OBJECT_NAME)
            .setGeneration(OBJECT_GENERATION)
            .setReadLimit(GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE)
            .setReadOffset(10)
            .build();
    GetObjectMediaRequest secondExpectedRequest =
        GetObjectMediaRequest.newBuilder()
            .setBucket(BUCKET_NAME)
            .setObject(OBJECT_NAME)
            .setGeneration(OBJECT_GENERATION)
            .setReadLimit(GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE)
            .setReadOffset(20)
            .build();

    verify(fakeService, times(1)).getObject(eq(GET_OBJECT_REQUEST), any());
    verify(fakeService, times(1)).getObjectMedia(eq(GetObjectMediaRequest.newBuilder()
        .setBucket(BUCKET_NAME)
        .setObject(OBJECT_NAME)
        .setReadOffset(
            objectSize - (GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE / 2))
        .setReadLimit(GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE)
        .build()), any());
    verify(fakeService, times(1)).getObjectMedia(eq(firstExpectedRequest), any());
    verify(fakeService, times(1)).getObjectMedia(eq(secondExpectedRequest), any());
    verifyNoMoreInteractions(fakeService);
  }

  @Test
  public void randomReadRequestsExpectedBytes() throws Exception {
    int objectSize = GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE * 10;
    fakeService.setObject(DEFAULT_OBJECT.toBuilder()
        .setSize(objectSize).build());
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
    assertArrayEquals(fakeService.data.substring(10, 60).toByteArray(), buffer.array());

    // Request bytes larger than minimum request size.
    buffer = ByteBuffer.allocate(GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE + 1);
    readChannel.position(0);
    readChannel.read(buffer);
    assertArrayEquals(
        fakeService
            .data
            .substring(0, GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE + 1)
            .toByteArray(),
        buffer.array());

    GetObjectMediaRequest firstExpectedRequest =
        GetObjectMediaRequest.newBuilder()
            .setBucket(BUCKET_NAME)
            .setObject(OBJECT_NAME)
            .setGeneration(OBJECT_GENERATION)
            .setReadLimit(GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE)
            .setReadOffset(10)
            .build();
    GetObjectMediaRequest secondExpectedRequest =
        GetObjectMediaRequest.newBuilder()
            .setBucket(BUCKET_NAME)
            .setObject(OBJECT_NAME)
            .setGeneration(OBJECT_GENERATION)
            .setReadLimit(GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE + 1)
            .setReadOffset(0)
            .build();

    verify(fakeService, times(1)).getObject(eq(GET_OBJECT_REQUEST), any());
    verify(fakeService, times(1)).getObjectMedia(eq(GetObjectMediaRequest.newBuilder()
        .setBucket(BUCKET_NAME)
        .setObject(OBJECT_NAME)
        .setReadOffset(
            objectSize - (GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE / 2))
        .setReadLimit(GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE)
        .build()), any());
    verify(fakeService, times(1)).getObjectMedia(eq(firstExpectedRequest), any());
    verify(fakeService, times(1)).getObjectMedia(eq(secondExpectedRequest), any());
    verifyNoMoreInteractions(fakeService);
  }

  @Test
  public void readToBufferWithArrayOffset() throws Exception {
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(100).build());
    verify(fakeService, times(1)).setObject(any());
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setMinRangeRequestSize(4).build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);

    byte[] array = new byte[200];
    // `slice` generates a ByteBuffer with a non-zero `arrayOffset`.
    ByteBuffer buffer = ByteBuffer.wrap(array, 50, 150).slice();
    readChannel.read(buffer);

    verify(fakeService, times(1)).getObject(eq(GET_OBJECT_REQUEST), any());
    byte[] expected = ByteString.copyFrom(array, 50, 100).toByteArray();
    verify(fakeService, times(1)).getObjectMedia(eq(GetObjectMediaRequest.newBuilder()
        .setBucket(BUCKET_NAME)
        .setObject(OBJECT_NAME)
        .setReadOffset(98)
        .setReadLimit(4)
        .build()), any());
    verify(fakeService, times(1)).getObjectMedia(eq(GetObjectMediaRequest.newBuilder()
        .setBucket(BUCKET_NAME)
        .setObject(OBJECT_NAME)
        .setGeneration(OBJECT_GENERATION)
        .setReadLimit(98)
        .build()), any());
    assertArrayEquals(fakeService.data.substring(0, 100).toByteArray(), expected);
    verifyNoMoreInteractions(fakeService);
  }

  @Test
  public void readSucceedsAfterSeek() throws Exception {
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(100).build());
    verify(fakeService, times(1)).setObject(any());
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setMinRangeRequestSize(4).setInplaceSeekLimit(10)
            .build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);

    ByteBuffer buffer = ByteBuffer.allocate(10);
    readChannel.position(50);
    readChannel.read(buffer);

    verify(fakeService, times(1)).getObject(eq(GET_OBJECT_REQUEST), any());
    verify(fakeService, times(1))
        .getObjectMedia(
            eq(GET_OBJECT_MEDIA_REQUEST.toBuilder().setReadOffset(50).setReadLimit(48).build()),
            any());
    verify(fakeService, times(1)).getObjectMedia(eq(GetObjectMediaRequest.newBuilder()
        .setBucket(BUCKET_NAME)
        .setObject(OBJECT_NAME)
        .setReadOffset(98)
        .setReadLimit(4)
        .build()), any());
    assertArrayEquals(fakeService.data.substring(50, 60).toByteArray(), buffer.array());
    verifyNoMoreInteractions(fakeService);
  }

  @Test
  public void singleReadSucceedsWithValidObjectChecksum() throws Exception {
    fakeService.setObject(
        DEFAULT_OBJECT.toBuilder()
            .setCrc32C(UInt32Value.newBuilder().setValue(DEFAULT_OBJECT_CRC32C))
            .build());
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setGrpcChecksumsEnabled(true).build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);

    ByteBuffer buffer = ByteBuffer.allocate(OBJECT_SIZE);
    readChannel.read(buffer);

    assertArrayEquals(fakeService.data.toByteArray(), buffer.array());
  }

  @Test
  public void partialReadSucceedsWithInvalidObjectChecksum() throws Exception {
    fakeService.setObject(
        DEFAULT_OBJECT.toBuilder().setCrc32C(UInt32Value.newBuilder().setValue(0)).build());
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setGrpcChecksumsEnabled(true).build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);

    ByteBuffer buffer = ByteBuffer.allocate(OBJECT_SIZE - 10);
    readChannel.read(buffer);

    assertArrayEquals(
        fakeService.data.substring(0, OBJECT_SIZE - 10).toByteArray(), buffer.array());
  }

  @Test
  public void multipleSequentialsReadsSucceedWithValidObjectChecksum() throws Exception {
    fakeService.setObject(
        DEFAULT_OBJECT.toBuilder()
            .setCrc32C(UInt32Value.newBuilder().setValue(DEFAULT_OBJECT_CRC32C))
            .build());
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setGrpcChecksumsEnabled(true).build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);

    ByteBuffer firstBuffer = ByteBuffer.allocate(100);
    ByteBuffer secondBuffer = ByteBuffer.allocate(OBJECT_SIZE - 100);
    readChannel.read(firstBuffer);
    readChannel.read(secondBuffer);

    assertArrayEquals(fakeService.data.substring(0, 100).toByteArray(), firstBuffer.array());
    assertArrayEquals(fakeService.data.substring(100).toByteArray(), secondBuffer.array());
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
    assertArrayEquals(fakeService.data.toByteArray(), expected);
  }

  @Test
  public void readToBufferWithArrayOffsetFailsWithInvalidMessageChecksum() throws Exception {
    fakeService.setReturnIncorrectMessageChecksum();
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setGrpcChecksumsEnabled(true).build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);

    byte[] array = new byte[OBJECT_SIZE + 100];
    // `ByteBuffer.slice` generates a ByteBuffer with a non-zero `arrayOffset`.
    ByteBuffer buffer = ByteBuffer.wrap(array, 50, OBJECT_SIZE).slice();

    IOException thrown = assertThrows(IOException.class, () -> readChannel.read(buffer));
    assertTrue(
        thrown.getMessage() + " should have contained 'checksum'",
        thrown.getMessage().contains("checksum"));
  }

  @Test
  public void multipleReadsIgnoreObjectChecksumForLatestGenerationReads() throws Exception {
    fakeService.setObject(
        DEFAULT_OBJECT.toBuilder().setCrc32C(UInt32Value.newBuilder().setValue(0)).build());
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setGrpcChecksumsEnabled(true).build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);

    ByteBuffer firstBuffer = ByteBuffer.allocate(100);
    ByteBuffer secondBuffer = ByteBuffer.allocate(OBJECT_SIZE - 100);
    readChannel.read(firstBuffer);
    readChannel.read(secondBuffer);

    assertArrayEquals(fakeService.data.substring(0, 100).toByteArray(), firstBuffer.array());
    assertArrayEquals(fakeService.data.substring(100).toByteArray(), secondBuffer.array());
  }

  @Test
  public void testOpenThrowsIOExceptionOnGetError() {
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setFastFailOnNotFound(false).build();
    fakeService.setGetException(
        Status.fromCode(Status.Code.INTERNAL)
            .withDescription("Custom error message.")
            .asException());
    verify(fakeService).setGetException(any());
    IOException thrown = assertThrows(IOException.class, () -> newReadChannel(options));
    assertThat(thrown)
        .hasCauseThat()
        .hasCauseThat()
        .hasMessageThat()
        .contains("Custom error message.");
    verify(fakeService).getObject(eq(GET_OBJECT_REQUEST), any());
    verifyNoMoreInteractions(fakeService);
  }

  @Test
  public void readHandlesGetMediaError() throws Exception {
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel();
    fakeService.setGetMediaException(
        Status.fromCode(Status.Code.INTERNAL)
            .withDescription("Custom error message.")
            .asException());

    ByteBuffer buffer = ByteBuffer.allocate(10);
    IOException thrown = assertThrows(IOException.class, () -> readChannel.read(buffer));
    assertThat(thrown)
        .hasCauseThat()
        .hasCauseThat()
        .hasMessageThat()
        .contains("Custom error message.");
  }

  @Test
  public void testOpenThrowsIOExceptionOnGetMediaError() {
    fakeService.setGetMediaException(
        Status.fromCode(Status.Code.INTERNAL)
            .withDescription("Custom error message.")
            .asException());
    verify(fakeService).setGetMediaException(any());
    IOException thrown = assertThrows(IOException.class, this::newReadChannel);
    assertThat(thrown)
        .hasCauseThat()
        .hasCauseThat()
        .hasMessageThat()
        .contains("Custom error message.");
    verify(fakeService).getObject(eq(GET_OBJECT_REQUEST), any());
    verify(fakeService).getObjectMedia(any(), any());
    verifyNoMoreInteractions(fakeService);
  }

  @Test
  public void retryGetMediaError() throws Exception {
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel();
    fakeService.setGetMediaException(
        Status.fromCode(Status.Code.INTERNAL)
            .withDescription("Custom error message.")
            .asException());

    ByteBuffer buffer = ByteBuffer.allocate(10);
    IOException thrown = assertThrows(IOException.class, () -> readChannel.read(buffer));
    assertThat(thrown)
        .hasCauseThat()
        .hasCauseThat()
        .hasMessageThat()
        .contains("Custom error message");
  }

  @Test
  public void readFailsOnClosedChannel() throws Exception {
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel();

    readChannel.close();
    ByteBuffer buffer = ByteBuffer.allocate(10);
    assertThrows(ClosedChannelException.class, () -> readChannel.read(buffer));
  }

  @Test
  public void readWithStrictGenerationReadConsistencySucceeds() throws Exception {
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(100).setGeneration(1).build());
    GoogleCloudStorageReadOptions options = GoogleCloudStorageReadOptions.builder().setMinRangeRequestSize(4).build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);

    ByteBuffer buffer = ByteBuffer.allocate(10);
    readChannel.read(buffer);
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(100).setGeneration(2).build());
    readChannel.position(0);
    buffer.clear();
    readChannel.read(buffer);

    List<GetObjectMediaRequest> expectedRequests =
        Arrays.asList(
            GET_OBJECT_MEDIA_REQUEST,
            GET_OBJECT_MEDIA_REQUEST.toBuilder()
                .setReadOffset(10)
                .setReadLimit(20)
                .setGeneration(1)
                .build());
    ArgumentCaptor<GetObjectMediaRequest> requestCaptor =
        ArgumentCaptor.forClass(GetObjectMediaRequest.class);
    verify(fakeService, times(1)).getObject(eq(GET_OBJECT_REQUEST), any());
    verify(fakeService, times(3)).getObjectMedia(requestCaptor.capture(), any());
  }

  @Test
  public void readWithLatestGenerationReadConsistencySucceeds() throws Exception {
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(100).setGeneration(1).build());
    GoogleCloudStorageReadOptions options = GoogleCloudStorageReadOptions.builder().setMinRangeRequestSize(4).build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);

    ByteBuffer buffer = ByteBuffer.allocate(10);
    readChannel.read(buffer);
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(100).setGeneration(2).build());
    readChannel.position(0);
    buffer.clear();
    readChannel.read(buffer);

    List<GetObjectMediaRequest> expectedRequests =
        Arrays.asList(
            GET_OBJECT_MEDIA_REQUEST,
            GET_OBJECT_MEDIA_REQUEST.toBuilder().setReadOffset(10).setReadLimit(20).build());
    ArgumentCaptor<GetObjectMediaRequest> requestCaptor =
        ArgumentCaptor.forClass(GetObjectMediaRequest.class);
    verify(fakeService, times(1)).getObject(eq(GET_OBJECT_REQUEST), any());
    verify(fakeService, times(3)).getObjectMedia(requestCaptor.capture(), any());
  }

  @Test
  public void seekUnderInplaceSeekLimitReadsCorrectBufferedData() throws Exception {
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(100).build());
    // verify data setup on mock to ensure this interaction does not conflict with `verify`calls
    verify(fakeService, times(1)).setObject(any());
    int minRangeRequestSize = 10;
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setMinRangeRequestSize(minRangeRequestSize)
            .setInplaceSeekLimit(10).build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);

    ByteBuffer buffer = ByteBuffer.allocate(20);
    readChannel.read(buffer);
    readChannel.position(25);
    buffer.clear();
    readChannel.read(buffer);

    verify(fakeService, times(1)).getObject(eq(GET_OBJECT_REQUEST), any());
    verify(fakeService, times(1)).getObjectMedia(eq(GetObjectMediaRequest.newBuilder()
        .setBucket(BUCKET_NAME)
        .setObject(OBJECT_NAME)
        .setReadOffset(95)
        .setReadLimit(minRangeRequestSize)
        .build()), any());
    verify(fakeService, times(1)).getObjectMedia(eq(GetObjectMediaRequest.newBuilder()
        .setBucket(BUCKET_NAME)
        .setObject(OBJECT_NAME)
        .setGeneration(OBJECT_GENERATION)
        .setReadLimit(95)
        .build()), any());
    assertArrayEquals(fakeService.data.substring(25, 45).toByteArray(), buffer.array());
    verifyNoMoreInteractions(fakeService);
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

    assertArrayEquals(
        fakeService.data.substring(50, 50 + FakeService.CHUNK_SIZE * 3 + 7).toByteArray(),
        buffer.array());
  }

  @Test
  public void seekBeyondInplaceSeekLimitReadsNoBufferedData() throws Exception {
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(100).build());
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

    verify(fakeService, times(1)).getObject(eq(GET_OBJECT_REQUEST), any());
    verify(fakeService, times(1)).getObjectMedia(eq(GetObjectMediaRequest.newBuilder()
        .setBucket(BUCKET_NAME)
        .setObject(OBJECT_NAME)
        .setReadOffset(95)
        .setReadLimit(minRangeRequestSize)
        .build()), any());
    verify(fakeService, times(1)).getObjectMedia(eq(GetObjectMediaRequest.newBuilder()
        .setBucket(BUCKET_NAME)
        .setObject(OBJECT_NAME)
        .setGeneration(OBJECT_GENERATION)
        .setReadLimit(95)
        .build()), any());
    verify(fakeService, times(1)).getObjectMedia(eq(GetObjectMediaRequest.newBuilder()
        .setBucket(BUCKET_NAME)
        .setObject(OBJECT_NAME)
        .setGeneration(OBJECT_GENERATION)
        .setReadOffset(35)
        .setReadLimit(20)
        .build()), any());
    assertArrayEquals(fakeService.data.substring(35, 55).toByteArray(), buffer.array());
    verifyNoMoreInteractions(fakeService);
  }

  @Test
  public void testReadPrefetchedFooter() throws Exception {
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(100).build());
    verify(fakeService, times(1)).setObject(any());
    int minRangeRequestSize = 2 * 1024;
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder()
            .setMinRangeRequestSize(minRangeRequestSize)
            .build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);
    ByteBuffer buffer = ByteBuffer.allocate(20);
    readChannel.position(80);
    readChannel.read(buffer);

    verify(fakeService, times(1)).getObject(eq(GET_OBJECT_REQUEST), any());
    // footerSize is bigger than object size, footer content essential is entire object content
    verify(fakeService, times(1)).getObjectMedia(eq(GetObjectMediaRequest.newBuilder()
        .setBucket(BUCKET_NAME)
        .setObject(OBJECT_NAME)
        .setReadOffset(0)
        .setReadLimit(minRangeRequestSize)
        .build()), any());
    assertArrayEquals(fakeService.data.substring(80).toByteArray(), buffer.array());
    verifyNoMoreInteractions(fakeService);
  }

  @Test
  public void testReadCachedFooter() throws Exception {
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(8 * 1024).build());
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
    verify(fakeService, times(1)).getObject(eq(GET_OBJECT_REQUEST), any());

    int footerOffset = 7 * 1024;
    verify(fakeService, times(1)).getObjectMedia(eq(GetObjectMediaRequest.newBuilder()
        .setBucket(BUCKET_NAME)
        .setObject(OBJECT_NAME)
        .setReadOffset(footerOffset)
        .setReadLimit(minRangeRequestSize)
        .build()), any());
    verify(fakeService, times(1)).getObjectMedia(eq(GetObjectMediaRequest.newBuilder()
        .setBucket(BUCKET_NAME)
        .setObject(OBJECT_NAME)
        .setGeneration(OBJECT_GENERATION)
        .setReadLimit(footerOffset)
        .build()), any());

    buffer.clear();
    readChannel.position(footerOffset);
    readChannel.read(buffer);

    assertArrayEquals(fakeService.data.substring(footerOffset).toByteArray(), buffer.array());

    // reading the footer twice to ensure there are no additional calls to GCS
    buffer.clear();
    readChannel.position(footerOffset);
    readChannel.read(buffer);

    assertArrayEquals(fakeService.data.substring(footerOffset).toByteArray(), buffer.array());
    verifyNoMoreInteractions(fakeService);
  }

  @Test
  public void testReadCachedFooterPartially() throws Exception {
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(16 * 1024).build());
    // verify data setup on mock to ensure this interaction does not conflict with `verify`calls
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
    verify(fakeService, times(1)).getObject(eq(GET_OBJECT_REQUEST), any());

    int footerOffset = 14 * 1024;
    verify(fakeService, times(1)).getObjectMedia(eq(GetObjectMediaRequest.newBuilder()
        .setBucket(BUCKET_NAME)
        .setObject(OBJECT_NAME)
        .setReadOffset(footerOffset)
        .setReadLimit(minRangeRequestSize)
        .build()), any());
    verify(fakeService, times(1)).getObjectMedia(eq(GetObjectMediaRequest.newBuilder()
        .setBucket(BUCKET_NAME)
        .setObject(OBJECT_NAME)
        .setGeneration(OBJECT_GENERATION)
        .setReadLimit(footerOffset)
        .build()), any());

    buffer.clear();
    int readOffset = 13 * 1024;
    readChannel.position(readOffset);
    readChannel.read(buffer);

    verify(fakeService, times(1)).getObjectMedia(eq(GetObjectMediaRequest.newBuilder()
        .setBucket(BUCKET_NAME)
        .setObject(OBJECT_NAME)
        .setGeneration(OBJECT_GENERATION)
        .setReadOffset(readOffset)
        .setReadLimit(1024)
        .build()), any());
    assertArrayEquals(fakeService.data.substring(readOffset, readOffset + (2 * 1024)).toByteArray(), buffer.array());

    // reading the footer twice to ensure there are no additional calls to GCS
    buffer.clear();
    readChannel.position(footerOffset);
    readChannel.read(buffer);

    assertArrayEquals(fakeService.data.substring(footerOffset).toByteArray(), buffer.array());
    verifyNoMoreInteractions(fakeService);
  }

  @Test
  public void testReadCachedFooterPartiallyWithInplaceSeek() throws Exception {
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(16 * 1024).build());
    // verify data setup on mock to ensure this interaction does not conflict with `verify`calls
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
    verify(fakeService, times(1)).getObject(eq(GET_OBJECT_REQUEST), any());

    int footerOffset = 14 * 1024;
    verify(fakeService, times(1)).getObjectMedia(eq(GetObjectMediaRequest.newBuilder()
        .setBucket(BUCKET_NAME)
        .setObject(OBJECT_NAME)
        .setReadOffset(footerOffset)
        .setReadLimit(minRangeRequestSize)
        .build()), any());
    verify(fakeService, times(1)).getObjectMedia(eq(GetObjectMediaRequest.newBuilder()
        .setBucket(BUCKET_NAME)
        .setObject(OBJECT_NAME)
        .setGeneration(OBJECT_GENERATION)
        .setReadLimit(footerOffset)
        .build()), any());

    buffer.clear();
    int readOffset = 13 * 1024;
    readChannel.position(readOffset);
    readChannel.read(buffer);

    verify(fakeService, times(1)).getObjectMedia(eq(GetObjectMediaRequest.newBuilder()
        .setBucket(BUCKET_NAME)
        .setObject(OBJECT_NAME)
        .setGeneration(OBJECT_GENERATION)
        .setReadOffset(readOffset)
        .setReadLimit(1024)
        .build()), any());
    assertArrayEquals(fakeService.data.substring(readOffset, readOffset + (2 * 1024)).toByteArray(), buffer.array());

    // reading the footer twice to ensure there are no additional calls to GCS
    buffer.clear();
    readChannel.position(footerOffset);
    readChannel.read(buffer);

    assertArrayEquals(fakeService.data.substring(footerOffset).toByteArray(), buffer.array());
    verifyNoMoreInteractions(fakeService);
  }

  @Test
  public void seekFailsOnNegative() throws Exception {
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel();

    assertThrows(IllegalArgumentException.class, () -> readChannel.position(-1));
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

    assertEquals(50, readChannel.position());
  }

  @Test
  public void positionUpdatesOnSeek() throws Exception {
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel();

    readChannel.position(50);

    assertEquals(50, readChannel.position());
  }

  @Test
  public void positionFailsOnClosedChannel() throws Exception {
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel();

    readChannel.close();
    assertThrows(ClosedChannelException.class, readChannel::position);
  }

  @Test
  public void fastFailOnNotFoundFailsOnCreateWhenEnabled() {
    fakeService.setGetException(Status.NOT_FOUND.asException());
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setFastFailOnNotFound(true).build();

    Throwable throwable = assertThrows(IOException.class, () -> newReadChannel(options));
    assertThat(throwable).hasCauseThat().hasMessageThat().contains("Item not found");
  }

  @Test
  public void fastFailOnNotFoundFailsByReadWhenDisabled() {
    fakeService.setGetException(Status.NOT_FOUND.asException());
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setFastFailOnNotFound(false).build();

    // If the user hasn't mandated fail fast, it is permissible for either open() or read() to
    // raise this exception.
    IOException thrown = assertThrows(IOException.class, () -> newReadChannel(options));
    assertThat(thrown).hasCauseThat().hasMessageThat().contains("Item not found");
  }

  @Test
  public void sizeReturnsObjectSize() throws Exception {
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(1234).build());
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel();

    assertEquals(1234L, readChannel.size());
    verify(fakeService, times(1)).getObject(eq(GET_OBJECT_REQUEST), any());
  }

  @Test
  public void sizeFailsOnClosedChannel() throws Exception {
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel();

    readChannel.close();
    assertThrows(ClosedChannelException.class, readChannel::size);
  }

  @Test
  public void sizeIsCached() throws Exception {
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(1234).build());
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel();

    assertEquals(1234L, readChannel.size());
    assertEquals(1234L, readChannel.size());
    verify(fakeService, times(1)).getObject(eq(GET_OBJECT_REQUEST), any());
  }

  @Test
  public void isOpenReturnsTrueOnCreate() throws Exception {
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel();

    assertTrue(readChannel.isOpen());
  }

  @Test
  public void isOpenReturnsFalseAfterClose() throws Exception {
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel();

    readChannel.close();
    assertFalse(readChannel.isOpen());
  }

  private GoogleCloudStorageGrpcReadChannel newReadChannel() throws IOException {
    return newReadChannel(GoogleCloudStorageReadOptions.DEFAULT);
  }

  private GoogleCloudStorageGrpcReadChannel newReadChannel(GoogleCloudStorageReadOptions options)
      throws IOException {
    return GoogleCloudStorageGrpcReadChannel.open(
        new FakeStubProvider(mockCredentials),
        new StorageResourceId(BUCKET_NAME, OBJECT_NAME),
        options,
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
    FakeStubProvider(Credentials credentials) {
      super(GoogleCloudStorageOptions.DEFAULT, null, new FakeGrpcDecorator());
    }

    @Override
    public StorageBlockingStub newBlockingStub() {
      return stub;
    }
  }

  private static class FakeService extends StorageImplBase {

    private static final int CHUNK_SIZE = 2048;
    ByteString data;
    private Object object;
    private Throwable getException;
    private Throwable getMediaException;
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
    public void getObject(GetObjectRequest request, StreamObserver<Object> responseObserver) {
      if (getException != null) {
        responseObserver.onError(getException);
      } else {
        responseObserver.onNext(object);
        responseObserver.onCompleted();
      }
    }

    @Override
    public void getObjectMedia(
        GetObjectMediaRequest request, StreamObserver<GetObjectMediaResponse> responseObserver) {
      if (getMediaException != null) {
        responseObserver.onError(getMediaException);
      } else {
        int readStart = (int) request.getReadOffset();
        int readEnd =
            request.getReadLimit() > 0
                ? (int) Math.min(object.getSize(), readStart + request.getReadLimit())
                : (int) object.getSize();
        for (int position = readStart; position < readEnd; position += CHUNK_SIZE) {
          int endIndex = Math.min((int) object.getSize(), position + CHUNK_SIZE);
          endIndex = Math.min(endIndex, readEnd);
          ByteString messageData =
              data.substring(position, endIndex);
          int crc32c = Hashing.crc32c().hashBytes(messageData.toByteArray()).asInt();
          if (alterMessageChecksum) {
            crc32c += 1;
          }
          GetObjectMediaResponse response =
              GetObjectMediaResponse.newBuilder()
                  .setChecksummedData(
                      ChecksummedData.newBuilder()
                          .setContent(messageData)
                          .setCrc32C(UInt32Value.newBuilder().setValue(crc32c)))
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

    void setGetException(Throwable t) {
      getException = t;
    }

    void setGetMediaException(Throwable t) {
      getMediaException = t;
    }

    void setReturnIncorrectMessageChecksum() {
      alterMessageChecksum = true;
    }
  }
}
