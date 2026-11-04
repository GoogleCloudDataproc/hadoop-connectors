/*
 * Copyright 2026 Google LLC
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
import static java.lang.Math.min;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertThrows;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.Fadvise;
import com.google.cloud.hadoop.util.GrpcErrorTypeExtractor;
import com.google.cloud.storage.Storage;
import com.google.common.base.Strings;
import com.google.protobuf.ByteString;
import com.google.storage.v2.ChecksummedData;
import com.google.storage.v2.GetObjectRequest;
import com.google.storage.v2.ReadObjectRequest;
import com.google.storage.v2.ReadObjectResponse;
import com.google.storage.v2.StorageGrpc.StorageImplBase;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GoogleCloudStorageClientReadChannelGrpcTest {

  private static final StorageResourceId RESOURCE_ID =
      new StorageResourceId("test-bucket", "test-object");
  private static final int OBJECT_SIZE = 64 * 1024;
  private static final byte[] CONTENT = randomBytes(OBJECT_SIZE);

  private static final GoogleCloudStorageReadOptions LAZY_READ_OPTIONS =
      GoogleCloudStorageReadOptions.builder()
          .setFastFailOnNotFoundEnabled(false)
          .setGzipEncodingSupportEnabled(true)
          .build();

  private static final GoogleCloudStorageReadOptions LAZY_SEQUENTIAL_READ_OPTIONS =
      LAZY_READ_OPTIONS.toBuilder().setFadvise(Fadvise.SEQUENTIAL).build();

  @Rule public final Timeout timeout = Timeout.seconds(30);

  @Test
  public void read_pastObjectEnd_throwsEofException() throws Exception {
    FakeStorage fakeStorage = FakeStorage.withObject(CONTENT);
    withChannel(
        fakeStorage,
        LAZY_SEQUENTIAL_READ_OPTIONS,
        channel -> {
          channel.position(OBJECT_SIZE + 1);

          assertThrows(EOFException.class, () -> channel.read(ByteBuffer.allocate(10)));
        });
    assertThat(fakeStorage.getObjectCalls()).isEqualTo(1);
  }

  @Test
  public void read_pastObjectEnd_thenSeekBack_returnsContent() throws Exception {
    withChannel(
        FakeStorage.withObject(CONTENT),
        LAZY_SEQUENTIAL_READ_OPTIONS,
        channel -> {
          channel.position(OBJECT_SIZE + 1);
          assertThrows(EOFException.class, () -> channel.read(ByteBuffer.allocate(10)));

          channel.position(10_000);
          ByteBuffer buffer = ByteBuffer.allocate(5_000);

          assertThat(channel.read(buffer)).isEqualTo(5_000);
          assertThat(buffer.array()).isEqualTo(Arrays.copyOfRange(CONTENT, 10_000, 15_000));
        });
  }

  @Test
  public void read_readExactPastObjectEnd_throwsEofException() throws Exception {
    GoogleCloudStorageReadOptions readOptions =
        LAZY_READ_OPTIONS.toBuilder().setReadExactRequestedBytesEnabled(true).build();
    withChannel(
        FakeStorage.withObject(CONTENT),
        readOptions,
        channel -> {
          channel.position(OBJECT_SIZE + 100);

          assertThrows(EOFException.class, () -> channel.read(ByteBuffer.allocate(10)));
        });
  }

  @Test
  public void read_atObjectEnd_serverReturnsOutOfRange_returnsEof() throws Exception {
    withChannel(
        FakeStorage.withObject(CONTENT).outOfRangeAtObjectEnd(true),
        LAZY_SEQUENTIAL_READ_OPTIONS,
        channel -> {
          channel.position(OBJECT_SIZE);

          assertThat(channel.read(ByteBuffer.allocate(10))).isEqualTo(-1);
          assertThat(channel.read(ByteBuffer.allocate(10))).isEqualTo(-1);
        });
  }

  @Test
  public void read_atObjectEnd_serverReturnsNoData_returnsEof() throws Exception {
    withChannel(
        FakeStorage.withObject(CONTENT).outOfRangeAtObjectEnd(false),
        LAZY_SEQUENTIAL_READ_OPTIONS,
        channel -> {
          channel.position(OBJECT_SIZE);

          assertThat(channel.read(ByteBuffer.allocate(10))).isEqualTo(-1);
          assertThat(channel.read(ByteBuffer.allocate(10))).isEqualTo(-1);
        });
  }

  @Test
  public void read_emptyObject_serverReturnsOutOfRange_returnsEof() throws Exception {
    withChannel(
        FakeStorage.withObject(new byte[0]).outOfRangeAtObjectEnd(true),
        LAZY_READ_OPTIONS,
        channel -> {
          assertThat(channel.read(ByteBuffer.allocate(10))).isEqualTo(-1);
          assertThat(channel.size()).isEqualTo(0);
        });
  }

  @Test
  public void read_emptyObject_serverReturnsNoData_returnsEof() throws Exception {
    withChannel(
        FakeStorage.withObject(new byte[0]).outOfRangeAtObjectEnd(false),
        LAZY_READ_OPTIONS,
        channel -> {
          assertThat(channel.read(ByteBuffer.allocate(10))).isEqualTo(-1);
          assertThat(channel.size()).isEqualTo(0);
        });
  }

  @Test
  public void read_wholeObjectInChunks_returnsContent() throws Exception {
    GoogleCloudStorageReadOptions readOptions =
        LAZY_READ_OPTIONS.toBuilder().setMinRangeRequestSize(4 * 1024).build();
    FakeStorage fakeStorage = FakeStorage.withObject(CONTENT);
    withChannel(
        fakeStorage,
        readOptions,
        channel -> assertThat(readToEnd(channel, 7_000)).isEqualTo(CONTENT));
    assertThat(fakeStorage.getObjectCalls()).isEqualTo(0);
  }

  @Test
  public void read_afterSeek_returnsContent() throws Exception {
    FakeStorage fakeStorage = FakeStorage.withObject(CONTENT);
    withChannel(
        fakeStorage,
        LAZY_READ_OPTIONS,
        channel -> {
          channel.position(10_000);
          ByteBuffer buffer = ByteBuffer.allocate(5_000);

          assertThat(channel.read(buffer)).isEqualTo(5_000);
          assertThat(buffer.array()).isEqualTo(Arrays.copyOfRange(CONTENT, 10_000, 15_000));
        });
    assertThat(fakeStorage.getObjectCalls()).isEqualTo(0);
  }

  @Test
  public void read_randomFadvise_returnsContent() throws Exception {
    GoogleCloudStorageReadOptions readOptions =
        LAZY_READ_OPTIONS.toBuilder()
            .setFadvise(Fadvise.RANDOM)
            .setMinRangeRequestSize(4 * 1024)
            .build();
    FakeStorage fakeStorage = FakeStorage.withObject(CONTENT);
    withChannel(
        fakeStorage,
        readOptions,
        channel -> {
          channel.position(20_000);
          ByteBuffer buffer = ByteBuffer.allocate(1_000);

          assertThat(channel.read(buffer)).isEqualTo(1_000);
          assertThat(buffer.array()).isEqualTo(Arrays.copyOfRange(CONTENT, 20_000, 21_000));
        });
    assertThat(fakeStorage.getObjectCalls()).isEqualTo(0);
  }

  @Test
  public void read_gzipEncodedObject_returnsDecompressedContent() throws Exception {
    byte[] content = compressibleBytes(100_000);
    withChannel(
        FakeStorage.withObject(gzip(content)).contentEncoding("gzip"),
        LAZY_READ_OPTIONS,
        channel -> assertThat(readToEnd(channel, 8 * 1024)).isEqualTo(content));
  }

  @Test
  public void read_gzipEncodedObjectPastCompressedSize_returnsDecompressedContent()
      throws Exception {
    byte[] content = compressibleBytes(100_000);
    byte[] compressed = gzip(content);
    int position = 50_000;
    assertThat(compressed.length).isLessThan(position);
    withChannel(
        FakeStorage.withObject(compressed).contentEncoding("gzip"),
        LAZY_SEQUENTIAL_READ_OPTIONS,
        channel -> {
          channel.position(position);
          ByteBuffer buffer = ByteBuffer.allocate(1_000);

          assertThat(channel.read(buffer)).isEqualTo(1_000);
          assertThat(buffer.array())
              .isEqualTo(Arrays.copyOfRange(content, position, position + 1_000));
        });
  }

  @Test
  public void read_missingObject_throwsFileNotFoundException() throws Exception {
    withChannel(
        FakeStorage.withoutObject(),
        LAZY_READ_OPTIONS,
        channel -> {
          assertThrows(FileNotFoundException.class, () -> channel.read(ByteBuffer.allocate(10)));
          assertThrows(FileNotFoundException.class, () -> channel.read(ByteBuffer.allocate(10)));
        });
  }

  @Test
  public void read_metadataKnownUpFront_returnsContent() throws Exception {
    GoogleCloudStorageReadOptions readOptions =
        LAZY_READ_OPTIONS.toBuilder()
            .setFastFailOnNotFoundEnabled(true)
            .setMinRangeRequestSize(4 * 1024)
            .build();
    withChannel(
        FakeStorage.withObject(CONTENT),
        readOptions,
        channel -> assertThat(readToEnd(channel, 7_000)).isEqualTo(CONTENT));
  }

  @Test
  public void read_metadataKnownUpFront_gzipEncodedObject_returnsDecompressedContent()
      throws Exception {
    byte[] content = compressibleBytes(100_000);
    GoogleCloudStorageReadOptions readOptions =
        LAZY_READ_OPTIONS.toBuilder().setFastFailOnNotFoundEnabled(true).build();
    withChannel(
        FakeStorage.withObject(gzip(content)).contentEncoding("gzip"),
        readOptions,
        channel -> assertThat(readToEnd(channel, 8 * 1024)).isEqualTo(content));
  }

  private interface ChannelConsumer {
    void accept(GoogleCloudStorageClientReadChannel channel) throws Exception;
  }

  private static void withChannel(
      FakeStorage fakeStorage, GoogleCloudStorageReadOptions readOptions, ChannelConsumer test)
      throws Exception {
    try (FakeServer server = FakeServer.of(fakeStorage);
        Storage storage = server.getGrpcStorageOptions().getService()) {
      GoogleCloudStorageClientReadChannel channel =
          new GoogleCloudStorageClientReadChannel(
              storage,
              RESOURCE_ID,
              /* itemInfo= */ null,
              readOptions,
              GrpcErrorTypeExtractor.INSTANCE,
              GoogleCloudStorageOptions.DEFAULT);
      try {
        test.accept(channel);
      } finally {
        channel.close();
      }
    }
  }

  private static byte[] readToEnd(GoogleCloudStorageClientReadChannel channel, int bufferSize)
      throws IOException {
    ByteArrayOutputStream content = new ByteArrayOutputStream();
    ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
    int bytesRead;
    while ((bytesRead = channel.read(buffer)) != -1) {
      assertThat(bytesRead).isGreaterThan(0);
      content.write(buffer.array(), 0, bytesRead);
      buffer.clear();
    }
    return content.toByteArray();
  }

  private static byte[] randomBytes(int size) {
    byte[] bytes = new byte[size];
    new Random(42).nextBytes(bytes);
    return bytes;
  }

  private static byte[] compressibleBytes(int size) {
    return Strings.repeat("0123456789", size / 10).getBytes(UTF_8);
  }

  private static byte[] gzip(byte[] content) throws IOException {
    ByteArrayOutputStream compressed = new ByteArrayOutputStream();
    try (GZIPOutputStream gzipStream = new GZIPOutputStream(compressed)) {
      gzipStream.write(content);
    }
    return compressed.toByteArray();
  }

  private static final class FakeStorage extends StorageImplBase {

    private static final int RESPONSE_CHUNK_SIZE = 16 * 1024;
    private static final long GENERATION = 1;

    private final byte[] storedContent;
    private final AtomicInteger getObjectCalls = new AtomicInteger();
    private String contentEncoding;
    private boolean outOfRangeAtObjectEnd = true;

    private FakeStorage(byte[] storedContent) {
      this.storedContent = storedContent;
    }

    static FakeStorage withObject(byte[] storedContent) {
      return new FakeStorage(storedContent);
    }

    static FakeStorage withoutObject() {
      return new FakeStorage(null);
    }

    FakeStorage contentEncoding(String contentEncoding) {
      this.contentEncoding = contentEncoding;
      return this;
    }

    FakeStorage outOfRangeAtObjectEnd(boolean outOfRangeAtObjectEnd) {
      this.outOfRangeAtObjectEnd = outOfRangeAtObjectEnd;
      return this;
    }

    int getObjectCalls() {
      return getObjectCalls.get();
    }

    @Override
    public void readObject(
        ReadObjectRequest request, StreamObserver<ReadObjectResponse> responseObserver) {
      if (storedContent == null) {
        responseObserver.onError(Status.NOT_FOUND.withDescription("No such object").asException());
        return;
      }
      long offset = request.getReadOffset();
      if (offset > storedContent.length
          || (offset == storedContent.length && outOfRangeAtObjectEnd)) {
        responseObserver.onError(
            Status.OUT_OF_RANGE.withDescription("Offset is past the object end").asException());
        return;
      }
      long end =
          request.getReadLimit() > 0
              ? min(storedContent.length, offset + request.getReadLimit())
              : storedContent.length;
      long position = offset;
      boolean first = true;
      // The first response carries the object metadata, even if there is no data to return.
      do {
        int chunkSize = (int) min(RESPONSE_CHUNK_SIZE, end - position);
        ReadObjectResponse.Builder response =
            ReadObjectResponse.newBuilder()
                .setChecksummedData(
                    ChecksummedData.newBuilder()
                        .setContent(ByteString.copyFrom(storedContent, (int) position, chunkSize)));
        if (first) {
          response.setMetadata(metadata(request.getBucket(), request.getObject()));
          first = false;
        }
        responseObserver.onNext(response.build());
        position += chunkSize;
      } while (position < end);
      responseObserver.onCompleted();
    }

    @Override
    public void getObject(
        GetObjectRequest request, StreamObserver<com.google.storage.v2.Object> responseObserver) {
      getObjectCalls.incrementAndGet();
      if (storedContent == null) {
        responseObserver.onError(Status.NOT_FOUND.withDescription("No such object").asException());
        return;
      }
      responseObserver.onNext(metadata(request.getBucket(), request.getObject()));
      responseObserver.onCompleted();
    }

    private com.google.storage.v2.Object metadata(String bucket, String name) {
      com.google.storage.v2.Object.Builder metadata =
          com.google.storage.v2.Object.newBuilder()
              .setBucket(bucket)
              .setName(name)
              .setGeneration(GENERATION)
              .setSize(storedContent.length);
      if (contentEncoding != null) {
        metadata.setContentEncoding(contentEncoding);
      }
      return metadata.build();
    }
  }
}
