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

package com.google.cloud.hadoop.gcsio;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.spy;

import com.google.auth.Credentials;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.protobuf.ByteString;
import com.google.storage.v2.ChecksummedData;
import com.google.storage.v2.Object;
import com.google.storage.v2.StorageGrpc;
import com.google.storage.v2.StorageGrpc.StorageImplBase;
import com.google.storage.v2.StorageGrpc.StorageStub;
import com.google.storage.v2.WriteObjectRequest;
import com.google.storage.v2.WriteObjectResponse;
import com.google.storage.v2.WriteObjectResponse.WriteStatusCase;
import io.grpc.ManagedChannelBuilder;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.time.Duration;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(JUnit4.class)
public class GoogleCloudStorageContentWriteStreamTest {

  private static final String V1_BUCKET_NAME = "bucket-name";
  private static final String BUCKET_NAME = GrpcChannelUtils.toV2BucketName(V1_BUCKET_NAME);
  private static final String OBJECT_NAME = "object-name";
  private static final StorageResourceId RESOURCE_ID =
      new StorageResourceId(V1_BUCKET_NAME, OBJECT_NAME);
  private StorageStub stub;
  private static final String UPLOAD_ID = "upload-id";
  private static final AsyncWriteChannelOptions DEFAULT_OPTIONS =
      AsyncWriteChannelOptions.builder().build();
  @Mock private Credentials mockCredentials;
  private Watchdog watchdog = Watchdog.create(Duration.ofMillis(100));

  private FakeService fakeService;

  private static final Object DEFAULT_OBJECT =
      Object.newBuilder()
          .setBucket(BUCKET_NAME)
          .setName(OBJECT_NAME)
          .setGeneration(1)
          .setMetageneration(2)
          .build();

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    fakeService = spy(new GoogleCloudStorageContentWriteStreamTest.FakeService());
    String serverName = InProcessServerBuilder.generateName();
    InProcessServerBuilder.forName(serverName)
        .directExecutor()
        .addService(fakeService)
        .build()
        .start();
    stub =
        StorageGrpc.newStub(InProcessChannelBuilder.forName(serverName).directExecutor().build());
  }

  @Test
  public void openStreamTest() throws IOException {
    GoogleCloudStorageContentWriteStream contentWriteStream = getContentWriteStream();
    contentWriteStream.openStream();
    assertThat(watchdog.getOpenStreams().size()).isEqualTo(1);
    contentWriteStream.closeStream();
    assertThat(watchdog.getOpenStreams().size()).isEqualTo(0);
  }

  @Test
  public void streamNotOpenThrows() {
    // Trying to write without opening stream
    GoogleCloudStorageContentWriteStream contentWriteStream = getContentWriteStream();
    ByteString data = ByteString.copyFromUtf8("test data");
    WriteObjectRequest insertRequest = getInsertRequest(data, 0);
    assertThrows(IOException.class, () -> contentWriteStream.writeChunk(insertRequest));
    assertThat(watchdog.getOpenStreams().size()).isEqualTo(0);
  }

  @Test
  public void streamAlreadyOpenThrows() throws IOException {
    GoogleCloudStorageContentWriteStream contentWriteStream = getContentWriteStream();
    contentWriteStream.openStream();
    assertThat(watchdog.getOpenStreams().size()).isEqualTo(1);
    assertThrows(IOException.class, () -> contentWriteStream.openStream());
  }

  @Test
  public void inflightRequestTest() throws IOException {
    GoogleCloudStorageContentWriteStream contentWriteStream = getContentWriteStream();
    contentWriteStream.openStream();
    int chunksToWrite = 4;
    int writeOffset = 0;
    for (int i = 0; i < chunksToWrite; i++) {
      ByteString data = ByteString.copyFromUtf8("test data");
      WriteObjectRequest insertRequest = getInsertRequest(data, writeOffset);
      contentWriteStream.writeChunk(insertRequest);
      writeOffset += data.size();
    }
    assertThat(watchdog.getOpenStreams().size()).isEqualTo(1);
    assertThat(contentWriteStream.getInflightRequestCount()).isEqualTo(chunksToWrite);
    contentWriteStream.closeStream();
    assertThat(watchdog.getOpenStreams().size()).isEqualTo(0);
  }

  @Test
  public void writeChunkResponseThrows() throws IOException {
    fakeService.setInsertRequestException(new IOException("ERROR!"));

    GoogleCloudStorageContentWriteStream contentWriteStream = getContentWriteStream();
    contentWriteStream.openStream();
    ByteString data = ByteString.copyFromUtf8("test data");
    WriteObjectRequest insertRequest = getInsertRequest(data, 0);
    assertThrows(IOException.class, () -> contentWriteStream.writeChunk(insertRequest));
    assertThat(contentWriteStream.getInflightRequestCount()).isEqualTo(1);
    // Stream gets closed whenever there is an error in writing chunk.
    assertThat(contentWriteStream.isOpen()).isFalse();
    // Exception is thrown from responseObserver which resulted in closer of stream as well
    assertThat(watchdog.getOpenStreams().size()).isEqualTo(0);
  }

  @Test
  public void closeStreamWithFinalize() throws IOException {
    GoogleCloudStorageContentWriteStream contentWriteStream = getContentWriteStream();
    contentWriteStream.openStream();
    ByteString data = ByteString.copyFromUtf8("test data");
    WriteObjectRequest insertRequest = getInsertRequest(data, /* writeOffset */ 0, true);
    contentWriteStream.writeChunk(insertRequest);

    assertThat(contentWriteStream.getInflightRequestCount()).isEqualTo(1);

    WriteObjectResponse response = contentWriteStream.closeStream();
    assertThat(response.getWriteStatusCase()).isEqualTo(WriteStatusCase.RESOURCE);
    assertThat(contentWriteStream.isOpen()).isFalse();
    assertThat(watchdog.getOpenStreams().size()).isEqualTo(0);
  }

  @Test
  public void closeStreamWithoutFinalize() throws IOException {
    GoogleCloudStorageContentWriteStream contentWriteStream = getContentWriteStream();
    contentWriteStream.openStream();
    ByteString data = ByteString.copyFromUtf8("test data");
    WriteObjectRequest insertRequest = getInsertRequest(data, /* writeOffset */ 0, false);
    contentWriteStream.writeChunk(insertRequest);

    assertThat(contentWriteStream.getInflightRequestCount()).isEqualTo(1);

    WriteObjectResponse response = contentWriteStream.closeStream();
    assertThat(response.getWriteStatusCase()).isEqualTo(WriteStatusCase.PERSISTED_SIZE);
    assertThat(response.getPersistedSize()).isEqualTo(data.size());
    assertThat(contentWriteStream.isOpen()).isFalse();
    assertThat(watchdog.getOpenStreams().size()).isEqualTo(0);
  }

  private GoogleCloudStorageContentWriteStream getContentWriteStream() {

    return new GoogleCloudStorageContentWriteStream(
        RESOURCE_ID,
        GoogleCloudStorageOptions.builder().setWriteChannelOptions(DEFAULT_OPTIONS).build(),
        new FakeStubProvider(mockCredentials).newAsyncStub(BUCKET_NAME),
        UPLOAD_ID,
        /* writeOffset */ 0,
        watchdog);
  }

  private WriteObjectRequest getInsertRequest(ByteString data, long writeOffset) {
    return getInsertRequest(data, writeOffset, false);
  }

  private WriteObjectRequest getInsertRequest(
      ByteString data, long writeOffset, boolean finalizeObject) {
    return WriteObjectRequest.newBuilder()
        .setUploadId(UPLOAD_ID)
        .setChecksummedData(ChecksummedData.newBuilder().setContent(data))
        .setWriteOffset(writeOffset)
        .setFinishWrite(finalizeObject)
        .build();
  }

  private static class FakeGrpcDecorator implements StorageStubProvider.GrpcDecorator {

    @Override
    public ManagedChannelBuilder<?> createChannelBuilder(String target) {
      return null;
    }

    @Override
    public AbstractStub<?> applyCallOption(AbstractStub<?> stub) {
      return stub;
    }
  }

  private class FakeStubProvider extends StorageStubProvider {

    FakeStubProvider(Credentials credentials) {
      super(GoogleCloudStorageOptions.DEFAULT, null, new FakeGrpcDecorator());
    }

    @Override
    protected StorageStub newAsyncStubInternal() {
      return stub;
    }
  }

  private static class FakeService extends StorageImplBase {

    private Throwable insertRequestException = null;

    @Override
    public StreamObserver<WriteObjectRequest> writeObject(
        StreamObserver<WriteObjectResponse> responseObserver) {
      long committedWriteOffset = 0;
      InsertRequestObserver insertRequestObserver =
          spy(new InsertRequestObserver(committedWriteOffset, insertRequestException));
      if (insertRequestException != null) {
        Throwable throwable = insertRequestException;
        if (!throwable.getClass().isAssignableFrom(Throwable.class)
            || throwable.getCause() != null) {
          insertRequestObserver.insertRequestException = throwable;
        }
      }
      insertRequestObserver.responseObserver = responseObserver;
      return insertRequestObserver;
    }

    void setInsertRequestException(Throwable t) {
      insertRequestException = t;
    }
  }

  private static class InsertRequestObserver implements StreamObserver<WriteObjectRequest> {

    private StreamObserver<WriteObjectResponse> responseObserver;
    private Object object = DEFAULT_OBJECT;

    // tracks the writeOffset of next chunk
    private long expectedWriteOffset = 0;
    // tracks the committed writeOffset of a stream and will only be updated during onCompleted
    private long committedWriteOffset = 0;
    WriteObjectResponse writeObjectResponse =
        WriteObjectResponse.newBuilder().setResource(object).build();
    Throwable insertRequestException;

    boolean objectFinalized = false;

    public InsertRequestObserver(long startingWriteOffset, Throwable throwable) {
      if (throwable != null) {
        this.insertRequestException = throwable;
      }
      this.committedWriteOffset = startingWriteOffset;
      this.expectedWriteOffset = startingWriteOffset;
    }

    @Override
    public void onNext(WriteObjectRequest request) {
      // Handle error cases
      if (insertRequestException != null) {
        responseObserver.onError(insertRequestException);
        return;
      }

      if (request.getWriteOffset() != expectedWriteOffset) {
        onError(
            new IOException(
                String.format(
                    "Out of order writes encountered. Expecting offset %d, got %d",
                    expectedWriteOffset, request.getWriteOffset())));
        return;
      }

      expectedWriteOffset += request.getChecksummedData().getContent().size();

      if (request.getFinishWrite() == true) {
        objectFinalized = true;
        onCompleted();
      }
    }

    @Override
    public void onError(Throwable t) {
      responseObserver.onError(t);
    }

    @Override
    public void onCompleted() {
      committedWriteOffset = expectedWriteOffset;
      WriteObjectResponse response = createWriteObjectResponse();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    }

    private WriteObjectResponse createWriteObjectResponse() {
      if (objectFinalized) {
        return WriteObjectResponse.newBuilder()
            .setPersistedSize(committedWriteOffset)
            .setResource(
                writeObjectResponse.getResource().toBuilder().setSize(committedWriteOffset).build())
            .build();
      }

      return WriteObjectResponse.newBuilder().setPersistedSize(committedWriteOffset).build();
    }
  }
}
