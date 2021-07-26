package com.google.cloud.hadoop.gcsio;

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageGrpcReadChannel.METADATA_FIELDS;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageTest.newStorageObject;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageTestUtils.JSON_FACTORY;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.jsonDataResponse;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.jsonErrorResponse;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.mockTransport;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.client.http.HttpRequest;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.util.BackOff;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.Storage.Objects;
import com.google.api.services.storage.Storage.Objects.Get;
import com.google.api.services.storage.model.StorageObject;
import com.google.auth.Credentials;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.Fadvise;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.ErrorResponses;
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
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
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

  private static final String V1_BUCKET_NAME = "bucket-name";
  private static final String BUCKET_NAME = StringPaths.toV2BucketName(V1_BUCKET_NAME);
  private static final String OBJECT_NAME = "object-name";
  private static final long OBJECT_GENERATION = 7;
  private static final int OBJECT_SIZE =
      GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE + 10;
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
  @Mock private Credentials mockCredentials;
  private Storage storage;
  private ApiErrorExtractor errorExtractor;
  private Get get;
  private StorageObject storageObject;

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
    storage = mock(Storage.class);
    get = mock(Get.class);
    Objects objects = mock(Objects.class);
    when(storage.objects()).thenReturn(objects);
    when(objects.get(V1_BUCKET_NAME, OBJECT_NAME)).thenReturn(get);
    storageObject = new StorageObject();
    storageObject.setBucket(V1_BUCKET_NAME);
    storageObject.setGeneration(OBJECT_GENERATION);
    storageObject.setSize(BigInteger.valueOf(OBJECT_SIZE));
    when(get.setFields(any())).thenCallRealMethod();
    when(get.execute()).thenReturn(storageObject);
    errorExtractor = ApiErrorExtractor.INSTANCE;
  }

  @Test
  public void readSingleChunkSucceeds() throws Exception {
    int objectSize = FakeService.CHUNK_SIZE;
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(objectSize).build());
    storageObject.setSize(BigInteger.valueOf(objectSize));
    verify(fakeService, times(1)).setObject(any());
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setMinRangeRequestSize(4).build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);

    ByteBuffer buffer = ByteBuffer.allocate(100);
    readChannel.read(buffer);

    verify(get).setFields(METADATA_FIELDS);
    verify(get).execute();
    verify(fakeService, times(1))
        .readObject(
            eq(
                ReadObjectRequest.newBuilder()
                    .setBucket(BUCKET_NAME)
                    .setObject(OBJECT_NAME)
                    .setReadOffset(objectSize - 2)
                    .build()),
            any());
    verify(fakeService, times(1))
        .readObject(
            eq(
                ReadObjectRequest.newBuilder()
                    .setBucket(BUCKET_NAME)
                    .setObject(OBJECT_NAME)
                    .setGeneration(OBJECT_GENERATION)
                    .setReadLimit(objectSize - 2)
                    .build()),
            any());
    assertArrayEquals(fakeService.data.substring(0, 100).toByteArray(), buffer.array());
    verifyNoMoreInteractions(fakeService);
  }

  @Test
  public void readMultipleChunksSucceeds() throws Exception {
    // Enough to require multiple chunks.
    int objectSize = FakeService.CHUNK_SIZE * 2;
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(objectSize).build());
    storageObject.setSize(BigInteger.valueOf(objectSize));
    verify(fakeService, times(1)).setObject(any());
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setMinRangeRequestSize(4).build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);

    ByteBuffer buffer = ByteBuffer.allocate(objectSize);
    readChannel.read(buffer);

    verify(get).setFields(METADATA_FIELDS);
    verify(get).execute();
    verify(fakeService, times(1))
        .readObject(
            eq(
                ReadObjectRequest.newBuilder()
                    .setBucket(BUCKET_NAME)
                    .setObject(OBJECT_NAME)
                    .setReadOffset(objectSize - 2)
                    .build()),
            any());
    verify(fakeService, times(1))
        .readObject(
            eq(
                ReadObjectRequest.newBuilder()
                    .setBucket(BUCKET_NAME)
                    .setObject(OBJECT_NAME)
                    .setGeneration(OBJECT_GENERATION)
                    .setReadLimit(objectSize - 2)
                    .build()),
            any());
    assertArrayEquals(fakeService.data.substring(0, objectSize).toByteArray(), buffer.array());
    verifyNoMoreInteractions(fakeService);
  }

  @Test
  public void readAfterRepositioningAfterSkippingSucceeds() throws Exception {
    int objectSize = GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE * 10;
    storageObject.setSize(BigInteger.valueOf(objectSize));
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

    verify(get).setFields(METADATA_FIELDS);
    verify(get).execute();
    assertArrayEquals(fakeService.data.substring(0, 20).toByteArray(), bufferAtBeginning.array());
    int footerOffset =
        objectSize - (GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE / 2);
    verify(fakeService, times(1))
        .readObject(
            eq(
                ReadObjectRequest.newBuilder()
                    .setBucket(BUCKET_NAME)
                    .setObject(OBJECT_NAME)
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
                    .setReadLimit(footerOffset)
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
                    .setReadLimit(
                        objectSize
                            - (GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE / 2)
                            - 1)
                    .build()),
            any());
    assertArrayEquals(
        fakeService.data.substring(25, 30).toByteArray(), bufferFromSkippedSection1.array());
    assertArrayEquals(
        fakeService.data.substring(35, 45).toByteArray(), bufferFromSkippedSection2.array());
    assertArrayEquals(
        fakeService.data.substring(1, 11).toByteArray(), bufferFromReposition.array());
    verifyNoMoreInteractions(fakeService);
  }

  @Test
  public void multipleSequentialReads() throws Exception {
    int objectSize = GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE * 10;
    storageObject.setSize(BigInteger.valueOf(objectSize));
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(objectSize).build());
    verify(fakeService, times(1)).setObject(any());
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setInplaceSeekLimit(10).build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);

    ByteBuffer first_buffer = ByteBuffer.allocate(10);
    ByteBuffer second_buffer = ByteBuffer.allocate(20);
    readChannel.read(first_buffer);
    readChannel.read(second_buffer);

    verify(get).setFields(METADATA_FIELDS);
    verify(get).execute();
    assertArrayEquals(fakeService.data.substring(0, 10).toByteArray(), first_buffer.array());
    assertArrayEquals(fakeService.data.substring(10, 30).toByteArray(), second_buffer.array());
    int footerOffset =
        objectSize - (GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE / 2);
    verify(fakeService, times(1))
        .readObject(
            eq(
                ReadObjectRequest.newBuilder()
                    .setBucket(BUCKET_NAME)
                    .setObject(OBJECT_NAME)
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
                    .setReadLimit(footerOffset)
                    .build()),
            any());
    verifyNoMoreInteractions(fakeService);
  }

  @Test
  public void randomReadRequestsExactBytes() throws Exception {
    int objectSize = GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE * 10;
    storageObject.setSize(BigInteger.valueOf(objectSize));
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

    verify(get).setFields(METADATA_FIELDS);
    verify(get).execute();
    ReadObjectRequest expectedRequest =
        ReadObjectRequest.newBuilder()
            .setBucket(BUCKET_NAME)
            .setObject(OBJECT_NAME)
            .setGeneration(OBJECT_GENERATION)
            .setReadLimit(GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE)
            .setReadOffset(10)
            .build();
    verify(fakeService, times(1)).readObject(eq(expectedRequest), any());
    assertArrayEquals(fakeService.data.substring(10, 60).toByteArray(), buffer.array());
    int footerOffset =
        objectSize - (GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE / 2);
    verify(fakeService, times(1))
        .readObject(
            eq(
                ReadObjectRequest.newBuilder()
                    .setBucket(BUCKET_NAME)
                    .setObject(OBJECT_NAME)
                    .setReadOffset(footerOffset)
                    .build()),
            any());
    verifyNoMoreInteractions(fakeService);
  }

  @Test
  public void repeatedRandomReadsWorkAsExpected() throws Exception {
    int objectSize = GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE * 10;
    storageObject.setSize(BigInteger.valueOf(objectSize));
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
    assertArrayEquals(fakeService.data.substring(10, 60).toByteArray(), buffer.array());

    buffer = ByteBuffer.allocate(25);
    readChannel.position(20);
    readChannel.read(buffer);
    assertArrayEquals(fakeService.data.substring(20, 45).toByteArray(), buffer.array());

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

    verify(get).setFields(METADATA_FIELDS);
    verify(get).execute();
    int footerOffset =
        objectSize - (GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE / 2);
    verify(fakeService, times(1))
        .readObject(
            eq(
                ReadObjectRequest.newBuilder()
                    .setBucket(BUCKET_NAME)
                    .setObject(OBJECT_NAME)
                    .setReadOffset(footerOffset)
                    .build()),
            any());
    verify(fakeService, times(1)).readObject(eq(firstExpectedRequest), any());
    verify(fakeService, times(1)).readObject(eq(secondExpectedRequest), any());
    verifyNoMoreInteractions(fakeService);
  }

  @Test
  public void randomReadRequestsExpectedBytes() throws Exception {
    int objectSize = GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE * 10;
    storageObject.setSize(BigInteger.valueOf(objectSize));
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

    verify(get).setFields(METADATA_FIELDS);
    verify(get).execute();

    int footerOffset =
        objectSize - (GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE / 2);
    verify(fakeService, times(1))
        .readObject(
            eq(
                ReadObjectRequest.newBuilder()
                    .setBucket(BUCKET_NAME)
                    .setObject(OBJECT_NAME)
                    .setReadOffset(footerOffset)
                    .build()),
            any());
    verify(fakeService, times(1)).readObject(eq(firstExpectedRequest), any());
    verify(fakeService, times(1)).readObject(eq(secondExpectedRequest), any());
    verifyNoMoreInteractions(fakeService);
  }

  @Test
  public void readToBufferWithArrayOffset() throws Exception {
    int objectSize = 100;
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(objectSize).build());
    storageObject.setSize(BigInteger.valueOf(objectSize));
    verify(fakeService, times(1)).setObject(any());
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setMinRangeRequestSize(4).build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);

    byte[] array = new byte[200];
    // `slice` generates a ByteBuffer with a non-zero `arrayOffset`.
    ByteBuffer buffer = ByteBuffer.wrap(array, 50, 150).slice();
    readChannel.read(buffer);

    verify(get).setFields(METADATA_FIELDS);
    verify(get).execute();
    byte[] expected = ByteString.copyFrom(array, 50, objectSize).toByteArray();
    int footerOffset = 98;
    verify(fakeService, times(1))
        .readObject(
            eq(
                ReadObjectRequest.newBuilder()
                    .setBucket(BUCKET_NAME)
                    .setObject(OBJECT_NAME)
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
                    .setReadLimit(footerOffset)
                    .build()),
            any());
    assertArrayEquals(fakeService.data.substring(0, objectSize).toByteArray(), expected);
    verifyNoMoreInteractions(fakeService);
  }

  @Test
  public void readSucceedsAfterSeek() throws Exception {
    int objectSize = 100;
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(objectSize).build());
    storageObject.setSize(BigInteger.valueOf(objectSize));
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

    verify(get).setFields(METADATA_FIELDS);
    verify(get).execute();
    verify(fakeService, times(1))
        .readObject(
            eq(GET_OBJECT_MEDIA_REQUEST.toBuilder().setReadOffset(50).setReadLimit(48).build()),
            any());
    verify(fakeService, times(1))
        .readObject(
            eq(
                ReadObjectRequest.newBuilder()
                    .setBucket(BUCKET_NAME)
                    .setObject(OBJECT_NAME)
                    .setReadOffset(98)
                    .build()),
            any());
    assertArrayEquals(fakeService.data.substring(50, 60).toByteArray(), buffer.array());
    verifyNoMoreInteractions(fakeService);
  }

  @Test
  public void singleReadSucceedsWithValidObjectChecksum() throws Exception {
    fakeService.setObject(
        DEFAULT_OBJECT
            .toBuilder()
            .setChecksums(ObjectChecksums.newBuilder().setCrc32C(DEFAULT_OBJECT_CRC32C))
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
        DEFAULT_OBJECT
            .toBuilder()
            .setChecksums(ObjectChecksums.newBuilder().setCrc32C(DEFAULT_OBJECT_CRC32C))
            .build());
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setGrpcChecksumsEnabled(true).build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);

    ByteBuffer buffer = ByteBuffer.allocate(OBJECT_SIZE - 10);
    readChannel.read(buffer);

    assertArrayEquals(
        fakeService.data.substring(0, OBJECT_SIZE - 10).toByteArray(), buffer.array());
  }

  @Test
  public void multipleSequentialReadsSucceedWithValidObjectChecksum() throws Exception {
    fakeService.setObject(
        DEFAULT_OBJECT
            .toBuilder()
            .setChecksums(ObjectChecksums.newBuilder().setCrc32C(DEFAULT_OBJECT_CRC32C))
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
        DEFAULT_OBJECT
            .toBuilder()
            .setChecksums(ObjectChecksums.newBuilder().setCrc32C(DEFAULT_OBJECT_CRC32C))
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
  public void testOpenReadsMetadata() throws IOException {
    int objectSize = 8 * 1024;
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(objectSize).build());
    storageObject.setSize(BigInteger.valueOf(objectSize));
    GoogleCloudStorageReadOptions options = GoogleCloudStorageReadOptions.builder().build();
    StorageResourceId storageResourceId =
        new StorageResourceId(V1_BUCKET_NAME, OBJECT_NAME, OBJECT_GENERATION);
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(storageResourceId, options);

    assertTrue(readChannel.isOpen());
    assertEquals(objectSize, readChannel.size());
    verify(get).setFields(METADATA_FIELDS);
    verify(get).execute();
  }

  @Test
  public void testOpenThrowsIOExceptionOnGetError() throws IOException {
    MockHttpTransport transport = mockTransport(jsonErrorResponse(ErrorResponses.SERVER_ERROR));

    List<HttpRequest> requests = new ArrayList<>();

    Storage storage = new Storage(transport, JSON_FACTORY, requests::add);

    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setFastFailOnNotFound(false).build();

    IOException thrown = assertThrows(IOException.class, () -> newReadChannel(storage, options));
    assertThat(thrown).hasCauseThat().hasCauseThat().hasMessageThat().contains("backendError");
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
    assertThat(thrown)
        .hasCauseThat()
        .hasCauseThat()
        .hasMessageThat()
        .contains("Custom error message.");
  }

  @Test
  public void testOpenThrowsIOExceptionOnGetMediaError() throws IOException {
    fakeService.setReadObjectException(
        Status.fromCode(Status.Code.INTERNAL)
            .withDescription("Custom error message.")
            .asException());
    verify(fakeService).setReadObjectException(any());
    IOException thrown = assertThrows(IOException.class, this::newReadChannel);
    assertThat(thrown)
        .hasCauseThat()
        .hasCauseThat()
        .hasMessageThat()
        .contains("Custom error message.");
    verify(get).setFields(METADATA_FIELDS);
    verify(get).execute();
    verify(fakeService).readObject(any(), any());
    verifyNoMoreInteractions(fakeService);
  }

  @Test
  public void testOpenThrowsIOExceptionOnGetMediaFileNotFound() throws IOException {
    fakeService.setReadObjectException(
        Status.fromCode(Code.NOT_FOUND).withDescription("Custom error message.").asException());
    verify(fakeService).setReadObjectException(any());
    IOException thrown = assertThrows(IOException.class, this::newReadChannel);
    assertThat(thrown)
        .hasCauseThat()
        .hasCauseThat()
        .hasCauseThat()
        .hasMessageThat()
        .contains("Custom error message.");
    verify(get).setFields(METADATA_FIELDS);
    verify(get).execute();
    verify(fakeService).readObject(any(), any());
    verifyNoMoreInteractions(fakeService);
  }

  @Test
  public void testOpenThrowsIOExceptionOnGzipContent() throws Exception {
    MockHttpTransport transport =
        mockTransport(
            jsonDataResponse(
                newStorageObject(BUCKET_NAME, OBJECT_NAME).setContentEncoding("gzip")));

    Storage storage = new Storage(transport, JSON_FACTORY, r -> {});

    GoogleCloudStorageReadOptions readOptions = GoogleCloudStorageReadOptions.builder().build();

    IOException e = assertThrows(IOException.class, () -> newReadChannel(storage, readOptions));
    assertThat(e)
        .hasCauseThat()
        .hasMessageThat()
        .isEqualTo("Cannot read GZIP encoded files - content encoding support is disabled.");
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
    int objectSize = 100;
    storageObject.setSize(BigInteger.valueOf(objectSize));
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
    verify(get).setFields(METADATA_FIELDS);
    verify(get).execute();
    verify(fakeService, times(3)).readObject(requestCaptor.capture(), any());
  }

  @Test
  public void readWithLatestGenerationReadConsistencySucceeds() throws Exception {
    int objectSize = 100;
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(objectSize).setGeneration(1).build());
    storageObject.setSize(BigInteger.valueOf(objectSize));
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
    verify(get).setFields(METADATA_FIELDS);
    verify(get).execute();
    verify(fakeService, times(3)).readObject(requestCaptor.capture(), any());
  }

  @Test
  public void seekUnderInplaceSeekLimitReadsCorrectBufferedData() throws Exception {
    int objectSize = 100;
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(objectSize).build());
    storageObject.setSize(BigInteger.valueOf(objectSize));
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

    verify(get).setFields(METADATA_FIELDS);
    verify(get).execute();
    verify(fakeService, times(1))
        .readObject(
            eq(
                ReadObjectRequest.newBuilder()
                    .setBucket(BUCKET_NAME)
                    .setObject(OBJECT_NAME)
                    .setReadOffset(95)
                    .build()),
            any());
    verify(fakeService, times(1))
        .readObject(
            eq(
                ReadObjectRequest.newBuilder()
                    .setBucket(BUCKET_NAME)
                    .setObject(OBJECT_NAME)
                    .setGeneration(OBJECT_GENERATION)
                    .setReadLimit(95)
                    .build()),
            any());
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
    int objectSize = 100;
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(objectSize).build());
    storageObject.setSize(BigInteger.valueOf(objectSize));
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

    verify(get).setFields(METADATA_FIELDS);
    verify(get).execute();
    verify(fakeService, times(1))
        .readObject(
            eq(
                ReadObjectRequest.newBuilder()
                    .setBucket(BUCKET_NAME)
                    .setObject(OBJECT_NAME)
                    .setReadOffset(95)
                    .build()),
            any());
    verify(fakeService, times(1))
        .readObject(
            eq(
                ReadObjectRequest.newBuilder()
                    .setBucket(BUCKET_NAME)
                    .setObject(OBJECT_NAME)
                    .setGeneration(OBJECT_GENERATION)
                    .setReadLimit(95)
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
    assertArrayEquals(fakeService.data.substring(35, 55).toByteArray(), buffer.array());
    verifyNoMoreInteractions(fakeService);
  }

  @Test
  public void testReadPrefetchedFooter() throws Exception {
    int objectSize = 100;
    storageObject.setSize(BigInteger.valueOf(objectSize));
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(objectSize).build());
    verify(fakeService, times(1)).setObject(any());
    int minRangeRequestSize = 2 * 1024;
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setMinRangeRequestSize(minRangeRequestSize).build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);
    ByteBuffer buffer = ByteBuffer.allocate(20);
    readChannel.position(80);
    readChannel.read(buffer);

    verify(get).setFields(METADATA_FIELDS);
    verify(get).execute();
    // footerSize is bigger than object size, footer content essential is entire object content
    verify(fakeService, times(1))
        .readObject(
            eq(
                ReadObjectRequest.newBuilder()
                    .setBucket(BUCKET_NAME)
                    .setObject(OBJECT_NAME)
                    .setReadOffset(0)
                    .build()),
            any());
    assertArrayEquals(fakeService.data.substring(80).toByteArray(), buffer.array());
    verifyNoMoreInteractions(fakeService);
  }

  @Test
  public void testReadCachedFooter() throws Exception {
    int objectSize = 8 * 1024;
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(objectSize).build());
    storageObject.setSize(BigInteger.valueOf(objectSize));
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

    verify(get).setFields(METADATA_FIELDS);
    verify(get).execute();

    int footerOffset = 7 * 1024;
    verify(fakeService, times(1))
        .readObject(
            eq(
                ReadObjectRequest.newBuilder()
                    .setBucket(BUCKET_NAME)
                    .setObject(OBJECT_NAME)
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
                    .setReadLimit(footerOffset)
                    .build()),
            any());

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
    int objectSize = 16 * 1024;
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(objectSize).build());
    storageObject.setSize(BigInteger.valueOf(objectSize));
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

    verify(get).setFields(METADATA_FIELDS);
    verify(get).execute();

    int footerOffset = 14 * 1024;
    verify(fakeService, times(1))
        .readObject(
            eq(
                ReadObjectRequest.newBuilder()
                    .setBucket(BUCKET_NAME)
                    .setObject(OBJECT_NAME)
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
                    .setReadLimit(footerOffset)
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
                    .setReadLimit(1024)
                    .build()),
            any());
    assertArrayEquals(
        fakeService.data.substring(readOffset, readOffset + (2 * 1024)).toByteArray(),
        buffer.array());

    // reading the footer twice to ensure there are no additional calls to GCS
    buffer.clear();
    readChannel.position(footerOffset);
    readChannel.read(buffer);

    assertArrayEquals(fakeService.data.substring(footerOffset).toByteArray(), buffer.array());
    verifyNoMoreInteractions(fakeService);
  }

  @Test
  public void testReadCachedFooterPartiallyWithInplaceSeek() throws Exception {
    int objectSize = 16 * 1024;
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(objectSize).build());
    storageObject.setSize(BigInteger.valueOf(objectSize));
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

    verify(get).setFields(METADATA_FIELDS);
    verify(get).execute();

    int footerOffset = 14 * 1024;
    verify(fakeService, times(1))
        .readObject(
            eq(
                ReadObjectRequest.newBuilder()
                    .setBucket(BUCKET_NAME)
                    .setObject(OBJECT_NAME)
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
                    .setReadLimit(footerOffset)
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
                    .setReadLimit(1024)
                    .build()),
            any());
    assertArrayEquals(
        fakeService.data.substring(readOffset, readOffset + (2 * 1024)).toByteArray(),
        buffer.array());

    // reading the footer twice to ensure there are no additional calls to GCS
    buffer.clear();
    readChannel.position(footerOffset);
    readChannel.read(buffer);

    assertArrayEquals(fakeService.data.substring(footerOffset).toByteArray(), buffer.array());
    verifyNoMoreInteractions(fakeService);
  }

  @Test
  public void testReadWithInplaceSeekAndFadviseRandom() throws Exception {
    int objectSize = 16 * 1024;
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(objectSize).build());
    storageObject.setSize(BigInteger.valueOf(objectSize));
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

    verify(get).setFields(METADATA_FIELDS);
    verify(get).execute();

    int footerOffset = 14 * 1024;
    verify(fakeService, times(1))
        .readObject(
            eq(
                ReadObjectRequest.newBuilder()
                    .setBucket(BUCKET_NAME)
                    .setObject(OBJECT_NAME)
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
    assertArrayEquals(
        fakeService.data.substring(readOffset, readOffset + (capacity)).toByteArray(),
        buffer.array());

    verifyNoMoreInteractions(fakeService);
  }

  @Test
  public void testReadWithInplaceSeekAndFadviseAuto() throws Exception {
    int objectSize = 16 * 1024;
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(objectSize).build());
    storageObject.setSize(BigInteger.valueOf(objectSize));
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

    verify(get).setFields(METADATA_FIELDS);
    verify(get).execute();

    int footerOffset = 14 * 1024;
    verify(fakeService, times(1))
        .readObject(
            eq(
                ReadObjectRequest.newBuilder()
                    .setBucket(BUCKET_NAME)
                    .setObject(OBJECT_NAME)
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
                    .setReadLimit(footerOffset)
                    .build()),
            any());

    buffer.clear();
    int readOffset = 7 * 1024;
    readChannel.position(readOffset);
    int capacity = 4 * 1024;
    buffer = ByteBuffer.allocate(capacity);
    readChannel.read(buffer);

    assertArrayEquals(
        fakeService.data.substring(readOffset, readOffset + (capacity)).toByteArray(),
        buffer.array());

    verifyNoMoreInteractions(fakeService);
  }

  @Test
  public void testReadWithInplaceSeekAndFadviseSequential() throws Exception {
    int objectSize = 16 * 1024;
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(objectSize).build());
    storageObject.setSize(BigInteger.valueOf(objectSize));
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

    verify(get).setFields(METADATA_FIELDS);
    verify(get).execute();

    int footerOffset = 14 * 1024;
    verify(fakeService, times(1))
        .readObject(
            eq(
                ReadObjectRequest.newBuilder()
                    .setBucket(BUCKET_NAME)
                    .setObject(OBJECT_NAME)
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
                    .setReadLimit(footerOffset)
                    .build()),
            any());

    buffer.clear();
    int readOffset = 7 * 1024;
    readChannel.position(readOffset);
    int capacity = 4 * 1024;
    buffer = ByteBuffer.allocate(capacity);
    readChannel.read(buffer);

    assertArrayEquals(
        fakeService.data.substring(readOffset, readOffset + (capacity)).toByteArray(),
        buffer.array());

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
  public void fastFailOnNotFoundFailsOnCreateWhenEnabled() throws IOException {
    MockHttpTransport transport = mockTransport(jsonErrorResponse(ErrorResponses.NOT_FOUND));

    List<HttpRequest> requests = new ArrayList<>();

    Storage storage = new Storage(transport, JSON_FACTORY, requests::add);

    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setFastFailOnNotFound(true).build();

    Throwable throwable = assertThrows(IOException.class, () -> newReadChannel(storage, options));
    assertThat(throwable).hasCauseThat().hasMessageThat().contains("Item not found");
  }

  @Test
  public void fastFailOnNotFoundFailsByReadWhenDisabled() throws IOException {
    MockHttpTransport transport = mockTransport(jsonErrorResponse(ErrorResponses.NOT_FOUND));

    List<HttpRequest> requests = new ArrayList<>();

    Storage storage = new Storage(transport, JSON_FACTORY, requests::add);

    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setFastFailOnNotFound(false).build();

    // If the user hasn't mandated fail fast, it is permissible for either open() or read() to
    // raise this exception.
    IOException thrown = assertThrows(IOException.class, () -> newReadChannel(storage, options));
    assertThat(thrown).hasCauseThat().hasMessageThat().contains("Item not found");
  }

  @Test
  public void sizeReturnsObjectSize() throws Exception {
    int objectSize = 1234;
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(objectSize).build());
    storageObject.setSize(BigInteger.valueOf(objectSize));
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel();

    assertEquals(1234L, readChannel.size());
    verify(get).setFields(METADATA_FIELDS);
    verify(get).execute();
  }

  @Test
  public void sizeFailsOnClosedChannel() throws Exception {
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel();

    readChannel.close();
    assertThrows(ClosedChannelException.class, readChannel::size);
  }

  @Test
  public void sizeIsCached() throws Exception {
    int objectSize = 1234;
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(objectSize).build());
    storageObject.setSize(BigInteger.valueOf(objectSize));
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel();

    assertEquals(1234L, readChannel.size());
    assertEquals(1234L, readChannel.size());
    verify(get).setFields(METADATA_FIELDS);
    verify(get).execute();
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

  private GoogleCloudStorageGrpcReadChannel newReadChannel(
      Storage storage, GoogleCloudStorageReadOptions options) throws IOException {
    return GoogleCloudStorageGrpcReadChannel.open(
        new FakeStubProvider(mockCredentials),
        storage,
        errorExtractor,
        new StorageResourceId(BUCKET_NAME, OBJECT_NAME),
        options,
        () -> BackOff.STOP_BACKOFF);
  }

  private GoogleCloudStorageGrpcReadChannel newReadChannel(GoogleCloudStorageReadOptions options)
      throws IOException {
    return GoogleCloudStorageGrpcReadChannel.open(
        new FakeStubProvider(mockCredentials),
        storage,
        errorExtractor,
        new StorageResourceId(V1_BUCKET_NAME, OBJECT_NAME),
        options,
        () -> BackOff.STOP_BACKOFF);
  }

  private GoogleCloudStorageGrpcReadChannel newReadChannel(
      StorageResourceId storageResourceId, GoogleCloudStorageReadOptions options)
      throws IOException {
    return GoogleCloudStorageGrpcReadChannel.open(
        new FakeStubProvider(mockCredentials),
        storage,
        errorExtractor,
        storageResourceId,
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
        int readStart = (int) request.getReadOffset();
        int readEnd =
            request.getReadLimit() > 0
                ? (int) Math.min(object.getSize(), readStart + request.getReadLimit())
                : (int) object.getSize();
        for (int position = readStart; position < readEnd; position += CHUNK_SIZE) {
          int endIndex = Math.min((int) object.getSize(), position + CHUNK_SIZE);
          endIndex = Math.min(endIndex, readEnd);
          ByteString messageData = data.substring(position, endIndex);
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
