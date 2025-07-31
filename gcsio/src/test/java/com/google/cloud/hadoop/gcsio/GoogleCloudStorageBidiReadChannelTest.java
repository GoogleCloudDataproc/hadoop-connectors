<<<<<<< HEAD
package com.google.cloud.hadoop.gcsio;

import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.mockTransport;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.core.ApiFutures;
import com.google.cloud.storage.Storage;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.IntFunction;
import org.junit.Test;

public class GoogleCloudStorageBidiReadChannelTest {

  private static final String TEST_BUCKET_NAME = "foo-bucket";

  private static final String TEST_OBJECT_NAME = "foo-object";

  private static final int OBJECT_SIZE = 1024 * 1024;
  private static final StorageResourceId RESOURCE_ID =
      new StorageResourceId(TEST_BUCKET_NAME, TEST_OBJECT_NAME);
  private static MockHttpTransport transport = mockTransport();
=======
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

import static com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.assertByteArrayEquals;
import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.junit.Assert.assertThrows;
import java.util.Arrays;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;

import com.google.cloud.storage.BlobReadSession;
import com.google.cloud.storage.ReadAsSeekableChannel;
import com.google.cloud.storage.ReadProjectionConfigs;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;
import com.google.api.core.ApiFuture;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.Fadvise;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper;
import com.google.cloud.hadoop.util.GrpcErrorTypeExtractor;
import com.google.cloud.storage.Storage;
import com.google.protobuf.ByteString;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GoogleCloudStorageBidiReadChannelTest {

  private static final String V1_BUCKET_NAME = "bucket-name";
  private static final String OBJECT_NAME = "object-name";
  private static final int CHUNK_SIZE = 10;
  private static final int OBJECT_SIZE = 10 * 10;
  private static final int IN_PLACE_SEEK_LIMIT = 5;
  private static final StorageResourceId RESOURCE_ID =
      new StorageResourceId(V1_BUCKET_NAME, OBJECT_NAME);
  private static final ByteString CONTENT =
      GoogleCloudStorageTestHelper.createTestData(OBJECT_SIZE);

  private static final GoogleCloudStorageReadOptions DEFAULT_READ_OPTION =
      GoogleCloudStorageReadOptions.builder()
          .setFadvise(Fadvise.RANDOM)
          .setGrpcChecksumsEnabled(true)
          .setInplaceSeekLimit(IN_PLACE_SEEK_LIMIT)
              .setMinRangeRequestSize(CHUNK_SIZE).setBidiReadEnabled(true)
          .build();
>>>>>>> 170df0e808ffb70c7529acf18e14dfc1ef9d4bdb

  private static final GoogleCloudStorageItemInfo DEFAULT_ITEM_INFO =
      GoogleCloudStorageItemInfo.createObject(
          RESOURCE_ID,
          /* creationTime= */ 10L,
          /* modificationTime= */ 15L,
          /* size= */ OBJECT_SIZE,
          /* contentType= */ "text/plain",
          /* contentEncoding= */ "text",
          /* metadata= */ null,
          /* contentGeneration= */ 1,
          /* metaGeneration= */ 2L,
          /* verificationAttributes= */ null);

<<<<<<< HEAD
  @Test
  public void readVectored_successfulRead()
      throws IOException, ExecutionException, InterruptedException, TimeoutException,
          URISyntaxException {
    GoogleCloudStorageBidiReadChannel bidiReadChannel = getMockedBidiReadChannel();
    IntFunction<ByteBuffer> allocator = ByteBuffer::allocateDirect;
    // Returns 3 ranges, with the following {Offset, Length}: {20, 10}, {50, 7}, {65, 17}
    List<VectoredIORange> ranges = getListOfVectoredIORange();

    bidiReadChannel.readVectored(ranges, allocator);

    assertEquals(getReadVectoredData(ranges.get(0)), FakeBlobReadSession.SUBSTRING_20_10);
    assertEquals(getReadVectoredData(ranges.get(1)), FakeBlobReadSession.SUBSTRING_50_7);
    assertEquals(getReadVectoredData(ranges.get(2)), FakeBlobReadSession.SUBSTRING_65_17);
  }

  @Test
  public void write_unsupportedOperationException() throws IOException {
    GoogleCloudStorageBidiReadChannel bidiReadChannel = getMockedBidiReadChannel();
    assertThrows(
        UnsupportedOperationException.class,
        () -> bidiReadChannel.write(ByteBuffer.allocateDirect(0)));
  }

  // TODO(dhritichopra) Dummy test to complete coverage, remove with actual test once we have these
  // implemented.
  @Test
  public void readPath_dummyImplementation() throws IOException {
    GoogleCloudStorageBidiReadChannel bidiReadChannel = getMockedBidiReadChannel();
    assertEquals(bidiReadChannel.read(ByteBuffer.allocateDirect(0)), 0);
    assertEquals(bidiReadChannel.position(), 0);
    assertEquals(bidiReadChannel.position(12), null);
    assertEquals(bidiReadChannel.size(), 0);
    assertEquals(bidiReadChannel.truncate(12), null);
    assertEquals(bidiReadChannel.isOpen(), false);
  }

  private String getReadVectoredData(VectoredIORange range)
      throws ExecutionException, InterruptedException, TimeoutException {
    Charset charset = StandardCharsets.UTF_8;
    return charset.decode(range.getData().get(3, TimeUnit.SECONDS)).toString();
  }

  private GoogleCloudStorageBidiReadChannel getMockedBidiReadChannel() throws IOException {
    Storage storage = mock(Storage.class);
    when(storage.blobReadSession(any(), any()))
        .thenReturn(ApiFutures.immediateFuture(new FakeBlobReadSession()));
    return new GoogleCloudStorageBidiReadChannel(
        storage,
        DEFAULT_ITEM_INFO,
        GoogleCloudStorageReadOptions.builder().build(),
        Executors.newSingleThreadExecutor());
  }

  private List<VectoredIORange> getListOfVectoredIORange() {
    return List.of(
        VectoredIORange.builder()
            .setLength(10)
            .setOffset(20)
            .setData(new CompletableFuture<>())
            .build(),
        VectoredIORange.builder()
            .setLength(7)
            .setOffset(50)
            .setData(new CompletableFuture<>())
            .build(),
        VectoredIORange.builder()
            .setLength(17)
            .setOffset(65)
            .setData(new CompletableFuture<>())
            .build());
=======
  private SeekableByteChannel fakeSeekableByteChannel = mock(SeekableByteChannel.class);
  private Storage mockedStorage = mock(Storage.class);

  private BlobReadSession mockBlobReadSession;

  @SuppressWarnings("unchecked") // For mocking generic ApiFuture
  private ApiFuture<BlobReadSession> mockFutureBlobReadSession;

  private GoogleCloudStorageBidiReadChannel readChannel;
  private byte[] contentBytes;

  // The state for our mock: its current position
  private final AtomicLong position = new AtomicLong(0);

  @Before
  public void setUp() throws Exception{
    mockedStorage = mock(Storage.class);
    mockFutureBlobReadSession = mock(ApiFuture.class);
    mockBlobReadSession = mock(BlobReadSession.class);

    when(mockedStorage.blobReadSession(any(BlobId.class))).thenReturn(mockFutureBlobReadSession);
    when(mockFutureBlobReadSession.get(anyLong(), any(TimeUnit.class))).thenReturn(mockBlobReadSession);

    when(mockBlobReadSession.readAs(any(ReadAsSeekableChannel.class)))
            .thenReturn(this.fakeSeekableByteChannel);

    // 1. Convert your content string to bytes
    contentBytes = CONTENT.toByteArray();
    position.set(0); // Reset position for each test


    when(fakeSeekableByteChannel.size()).thenReturn((long) contentBytes.length);
    when(fakeSeekableByteChannel.position()).thenAnswer(invocation -> position.get());
    when(fakeSeekableByteChannel.position(anyLong())).thenAnswer(invocation -> {
      long newPosition = invocation.getArgument(0);
      position.set(newPosition);
      return fakeSeekableByteChannel;
    });

    // 5. Mock the core read() method using a lambda.
    //    The explicit cast to (Answer<Integer>) is not necessary.
    when(fakeSeekableByteChannel.read(any(ByteBuffer.class))).thenAnswer(
            invocation -> {
              ByteBuffer buffer = invocation.getArgument(0);
              long currentPos = position.get();

              if (currentPos >= contentBytes.length) {
                return -1; // EOF
              }

              int bytesToRead = (int) Math.min(buffer.remaining(), contentBytes.length - currentPos);
              buffer.put(contentBytes, (int) currentPos, bytesToRead);
              position.addAndGet(bytesToRead);

              return bytesToRead; // This will be autoboxed to an Integer
            });
    readChannel = getJavaStorageChannel(mockedStorage, DEFAULT_ITEM_INFO, DEFAULT_READ_OPTION);

  }

  @Test
  public void validPositionUpdate() throws IOException {
    int seekPosition = 10;
    readChannel.position(seekPosition);
    assertThat(readChannel.currentPosition).isEqualTo(seekPosition);
    assertThat(readChannel.contentReadChannel.position()).isEqualTo(seekPosition);
  }
  @Test
  public void inValidSeekPositions() {
    int seekPosition = -1;
    assertThrows(IllegalArgumentException.class, () -> readChannel.position(seekPosition));
    assertThrows(EOFException.class, () -> readChannel.position(OBJECT_SIZE));
  }

  @Test
  public void readThrowsClosedChannel() throws IOException {
    readChannel.close();
    assertThrows(ClosedChannelException.class, () -> readChannel.read(ByteBuffer.allocate(1)));
  }

  @Test
  public void closingClosedChannel() throws IOException {
    readChannel.read(ByteBuffer.allocate(1));
    readChannel.close();
    readChannel.close();
    readChannel.close();
    // Channel's close was closed only once.
    verify(fakeSeekableByteChannel, times(1)).close();
  }

  @Test
  public void readSingleChunkSuccess() throws IOException {
    int readBytes = 100;
    readChannel = getJavaStorageChannel(mockedStorage, DEFAULT_ITEM_INFO, DEFAULT_READ_OPTION);
    int startPosition = 0;
    readChannel.position(startPosition);
    assertThat(readChannel.position()).isEqualTo(startPosition);

    ByteBuffer buffer = ByteBuffer.allocate(readBytes);
    readChannel.read(buffer);
    System.out.println("Buffer position");
    System.out.println(buffer.position());

    verifyContent(buffer, startPosition, readBytes);
    verify(fakeSeekableByteChannel, times(1)).read(any());
    verifyNoMoreInteractions(fakeSeekableByteChannel);
  }

  // Chunk Size is not working right now
  @Test
  public void readMultipleChunkSuccessSequential() throws IOException {
    int chunksToRead = 2;
    int chunkSize = CHUNK_SIZE;
    int readBytes = chunksToRead * chunkSize;
    System.out.println("ChunksToRead:" + chunksToRead);
    System.out.println("ChunksSize:" + chunkSize);
    System.out.println("ReadBytes:" + readBytes);
    readChannel =
            getJavaStorageChannel(
                    mockedStorage,
                    DEFAULT_ITEM_INFO,
                    GoogleCloudStorageReadOptions.builder()
                            .setMinRangeRequestSize(chunkSize)
                            .build());
    int startPosition = 0;
    readChannel.position(startPosition);
    assertThat(readChannel.position()).isEqualTo(startPosition);

    ByteBuffer buffer = ByteBuffer.allocate(readBytes);
    readChannel.read(buffer);
    verifyContent(buffer, startPosition, readBytes);

    verify(fakeSeekableByteChannel, times(chunksToRead)).read(any());
    verifyNoMoreInteractions(fakeSeekableByteChannel);
  }

  // Chunk Size fixing should fix this test as well.
  @Test
  public void readFullObject() throws IOException {
    int chunkSize = CHUNK_SIZE;
    long startPosition = 0;
    readChannel.position(startPosition);
    assertThat(readChannel.position()).isEqualTo(startPosition);
    int totalBytesRead = 0;
    ByteBuffer buffer = ByteBuffer.allocate(chunkSize);
    while (totalBytesRead < OBJECT_SIZE) {
      System.out.println("TotalBytesRead:" + totalBytesRead);
      readChannel.position(totalBytesRead);
      assertThat(readChannel.position()).isEqualTo(totalBytesRead);
      buffer.clear();
      readChannel.read(buffer);
      Charset charset = StandardCharsets.UTF_8;
      String decodedString1 = Arrays.toString(buffer.array());
      System.out.println("Here is the buffer: " + decodedString1);
      verifyContent(buffer, totalBytesRead, chunkSize);
      totalBytesRead += buffer.limit();
    }

    verify(fakeSeekableByteChannel, times(OBJECT_SIZE / chunkSize)).close();
    verify(fakeSeekableByteChannel, times((OBJECT_SIZE / chunkSize * 2) - 1)).read(any());
    verifyNoMoreInteractions(fakeSeekableByteChannel);
  }


  private void verifyContent(ByteBuffer buffer, int startPosition, int length) {
    assertThat(buffer.position()).isEqualTo(length);
    assertByteArrayEquals(
            buffer.array(), CONTENT.substring(startPosition, (startPosition + length)).toByteArray());
  }

  private GoogleCloudStorageBidiReadChannel getJavaStorageChannel(Storage storage,
          GoogleCloudStorageItemInfo itemInfo, GoogleCloudStorageReadOptions readOptions)
          throws IOException {
    GoogleCloudStorageItemInfo objectInfo = itemInfo;
    if (itemInfo == null) {
      objectInfo = DEFAULT_ITEM_INFO;
    }
    return new GoogleCloudStorageBidiReadChannel(
            mockedStorage,
            objectInfo,
            readOptions,
            GrpcErrorTypeExtractor.INSTANCE,
            GoogleCloudStorageOptions.DEFAULT.toBuilder().build());
>>>>>>> 170df0e808ffb70c7529acf18e14dfc1ef9d4bdb
  }
}
