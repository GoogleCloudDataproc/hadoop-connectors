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
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.cloud.ReadChannel;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.Fadvise;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper;
import com.google.cloud.storage.Storage;
import com.google.protobuf.ByteString;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.Random;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GoogleCloudStorageClientReadChannelTest {

  private static final String V1_BUCKET_NAME = "bucket-name";
  private static final String OBJECT_NAME = "object-name";
  private static final int CHUNK_SIZE = FakeReadChannel.CHUNK_SIZE;
  private static final int OBJECT_SIZE = 1024 * 1024;
  private static final int IN_PLACE_SEEK_LIMIT = 5;
  private static final StorageResourceId RESOURCE_ID =
      new StorageResourceId(V1_BUCKET_NAME, OBJECT_NAME);
  private static final ByteString CONTENT =
      GoogleCloudStorageTestHelper.createTestData(OBJECT_SIZE);

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

  private ReadChannel fakeReadChannel;
  private Storage mockedStorage = mock(Storage.class);

  private GoogleCloudStorageClientReadChannel readChannel;

  @Before
  public void setUp() throws IOException {
    fakeReadChannel = spy(new FakeReadChannel(CONTENT));
    when(mockedStorage.reader(any(), any())).thenReturn(fakeReadChannel);
    readChannel =
        getJavaStorageChannel(
            DEFAULT_ITEM_INFO,
            GoogleCloudStorageReadOptions.builder()
                .setFadvise(Fadvise.RANDOM)
                .setGrpcChecksumsEnabled(true)
                .setInplaceSeekLimit(IN_PLACE_SEEK_LIMIT)
                .setMinRangeRequestSize(FakeReadChannel.CHUNK_SIZE)
                .build());
  }

  @Test
  public void inValidSeekPositions() throws IOException {
    int seekPosition = -1;
    assertThrows(EOFException.class, () -> readChannel.position(seekPosition));
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
    verify(fakeReadChannel, times(1)).close();
  }

  @Test
  public void readSingleChunkSuccess() throws IOException {
    int readBytes = 100;
    GoogleCloudStorageClientReadChannel readChannel =
        getJavaStorageChannel(
            DEFAULT_ITEM_INFO,
            GoogleCloudStorageReadOptions.builder()
                .setFadvise(Fadvise.RANDOM)
                .setGrpcChecksumsEnabled(true)
                .setInplaceSeekLimit(5)
                .setMinRangeRequestSize(readBytes)
                .build());
    int startPosition = 0;
    readChannel.position(startPosition);
    assertThat(readChannel.position()).isEqualTo(startPosition);

    ByteBuffer buffer = ByteBuffer.allocate(readBytes);
    readChannel.read(buffer);
    verifyContent(buffer, startPosition, readBytes);
    verify(fakeReadChannel, times(1)).seek(anyLong());
    verify(fakeReadChannel, times(1)).limit(anyLong());
    verify(fakeReadChannel, times(1)).read(any());

    verifyNoMoreInteractions(fakeReadChannel);
  }

  @Test
  public void readMultipleChunkSuccessSequential() throws IOException {
    int chunksToRead = 2;
    int chunkSize = FakeReadChannel.CHUNK_SIZE;
    int readBytes = chunksToRead * chunkSize;
    GoogleCloudStorageClientReadChannel readChannel =
        getJavaStorageChannel(
            DEFAULT_ITEM_INFO,
            GoogleCloudStorageReadOptions.builder()
                .setFadvise(Fadvise.SEQUENTIAL)
                .setGrpcChecksumsEnabled(true)
                .setInplaceSeekLimit(5)
                .setMinRangeRequestSize(chunkSize)
                .build());
    int startPosition = 0;
    readChannel.position(startPosition);
    assertThat(readChannel.position()).isEqualTo(startPosition);

    ByteBuffer buffer = ByteBuffer.allocate(readBytes);
    readChannel.read(buffer);
    verifyContent(buffer, startPosition, readBytes);

    verify(fakeReadChannel, times(1)).seek(anyLong());
    verify(fakeReadChannel, times(1)).limit(anyLong());
    verify(fakeReadChannel, times(chunksToRead)).read(any());

    verifyNoMoreInteractions(fakeReadChannel);
  }

  @Test
  public void readFullObject() throws IOException {
    int chunkSize = FakeReadChannel.CHUNK_SIZE;
    long startPosition = 0;
    readChannel.position(startPosition);
    assertThat(readChannel.position()).isEqualTo(startPosition);
    int totalBytesRead = 0;
    ByteBuffer buffer = ByteBuffer.allocate(chunkSize);
    while (totalBytesRead < OBJECT_SIZE) {
      readChannel.position(totalBytesRead);
      assertThat(readChannel.position()).isEqualTo(totalBytesRead);
      buffer.clear();
      readChannel.read(buffer);
      verifyContent(buffer, totalBytesRead, chunkSize);
      totalBytesRead += buffer.limit();
    }

    verify(fakeReadChannel, times(OBJECT_SIZE / chunkSize)).seek(anyLong());
    verify(fakeReadChannel, times(OBJECT_SIZE / chunkSize)).limit(anyLong());
    verify(fakeReadChannel, times(OBJECT_SIZE / chunkSize)).close();
    // read will be called two times for every chunk
    // Content channel will be created with size of a CHUNK
    // First read will provide requested chunk
    // second read will return -1, pointing towards channel limits being breached and new channel
    // will be created subsequently. Although this will not happen for very last chunk.
    verify(fakeReadChannel, times((OBJECT_SIZE / chunkSize * 2) - 1)).read(any());

    verifyNoMoreInteractions(fakeReadChannel);
  }

  @Test
  public void fadviseAuto_onForwardRead_switchesToRandom() throws IOException {
    int chunkSize = FakeReadChannel.CHUNK_SIZE;
    int seekPosition = FakeReadChannel.CHUNK_SIZE * 2;
    GoogleCloudStorageClientReadChannel readChannel =
        getJavaStorageChannel(
            DEFAULT_ITEM_INFO,
            GoogleCloudStorageReadOptions.builder()
                .setFadvise(Fadvise.AUTO)
                .setGrpcChecksumsEnabled(true)
                .setInplaceSeekLimit(5)
                .setMinRangeRequestSize(chunkSize)
                .build());

    ByteBuffer buffer = ByteBuffer.allocate(chunkSize);
    readChannel.read(buffer);
    assertThat(readChannel.randomAccessStatus()).isFalse();
    verifyContent(buffer, 0, chunkSize);

    buffer.clear();
    readChannel.position(seekPosition);
    readChannel.read(buffer);
    assertThat(readChannel.randomAccessStatus()).isTrue();
    verifyContent(buffer, seekPosition, chunkSize);

    verify(fakeReadChannel, times(2)).seek(anyLong());
    verify(fakeReadChannel, times(2)).limit(anyLong());
    verify(fakeReadChannel, times(1)).close();
    verify(fakeReadChannel, times(2)).read(any());
    verifyNoMoreInteractions(fakeReadChannel);
  }

  @Test
  public void fadviseAuto_onBackwardRead_switchesToRandom() throws IOException {
    int chunkSize = FakeReadChannel.CHUNK_SIZE;
    int seekPosition = FakeReadChannel.CHUNK_SIZE * 2;
    GoogleCloudStorageClientReadChannel readChannel =
        getJavaStorageChannel(
            DEFAULT_ITEM_INFO,
            GoogleCloudStorageReadOptions.builder()
                .setFadvise(Fadvise.AUTO)
                .setGrpcChecksumsEnabled(true)
                .setInplaceSeekLimit(5)
                .setMinRangeRequestSize(chunkSize)
                .build());

    ByteBuffer buffer = ByteBuffer.allocate(chunkSize);
    readChannel.position(seekPosition);
    readChannel.read(buffer);
    assertThat(readChannel.randomAccessStatus()).isFalse();
    verifyContent(buffer, seekPosition, chunkSize);

    buffer.clear();
    readChannel.position(0);
    readChannel.read(buffer);
    assertThat(readChannel.randomAccessStatus()).isTrue();
    verifyContent(buffer, 0, chunkSize);

    verify(fakeReadChannel, times(2)).seek(anyLong());
    verify(fakeReadChannel, times(2)).limit(anyLong());
    verify(fakeReadChannel, times(1)).close();
    verify(fakeReadChannel, times(2)).read(any());
    verifyNoMoreInteractions(fakeReadChannel);
  }

  @Test
  public void footerPrefetch_reused() throws IOException {
    int chunkSize = FakeReadChannel.CHUNK_SIZE;

    // footerSize is the minimumChunkSize
    int footerSize = chunkSize;
    // chunk which starts somewhere within the footer length
    int startPosition = OBJECT_SIZE - (new Random().nextInt(footerSize - 1) + 1);
    readChannel.position(startPosition);
    assertThat(readChannel.position()).isEqualTo(startPosition);
    int bytesToRead = 1;
    ByteBuffer buffer = ByteBuffer.allocate(bytesToRead);
    readChannel.read(buffer);
    verifyContent(buffer, startPosition, bytesToRead);
    verify(fakeReadChannel, times(1)).seek(anyLong());
    verify(fakeReadChannel, times(1)).limit(anyLong());
    verify(fakeReadChannel, times(1)).read(any());
    verify(fakeReadChannel, times(1)).close();
    // another request within the footer will not result into `read` and served via cache
    startPosition = OBJECT_SIZE - new Random().nextInt(footerSize);
    readChannel.position(startPosition);
    buffer.clear();
    readChannel.read(buffer);
    verifyContent(buffer, startPosition, bytesToRead);

    verifyNoMoreInteractions(fakeReadChannel);
  }

  @Test
  public void read_whenBufferIsEmpty() throws IOException {
    ByteBuffer emptyBuffer = ByteBuffer.wrap(new byte[0]);
    readChannel.read(emptyBuffer);
    assertThat(readChannel.read(emptyBuffer)).isEqualTo(0);
  }

  @Test
  public void read_whenPositionIsEqualToSize() throws IOException {
    readChannel =
        getJavaStorageChannel(
            GoogleCloudStorageItemInfo.createObject(
                RESOURCE_ID,
                /* creationTime= */ 10L,
                /* modificationTime= */ 15L,
                /* size= */ 0,
                /* contentType= */ "text/plain",
                /* contentEncoding= */ "text",
                /* metadata= */ null,
                /* contentGeneration= */ 1,
                /* metaGeneration= */ 2L,
                /* verificationAttributes= */ null),
            GoogleCloudStorageReadOptions.DEFAULT);

    ByteBuffer readBuffer = ByteBuffer.wrap(new byte[1]);
    assertThat(readChannel.position()).isEqualTo(readChannel.size());
    assertThat(readChannel.read(readBuffer)).isEqualTo(-1);
  }

  @Test
  public void gzipEncodedObject_throwWhileChannelCreation() {
    assertThrows(
        IOException.class,
        () ->
            getJavaStorageChannel(
                GoogleCloudStorageItemInfo.createObject(
                    RESOURCE_ID,
                    /* creationTime= */ 10L,
                    /* modificationTime= */ 15L,
                    /* size= */ 0,
                    /* contentType= */ "text/plain",
                    /* contentEncoding= */ "gzip",
                    /* metadata= */ null,
                    /* contentGeneration= */ 1,
                    /* metaGeneration= */ 2L,
                    /* verificationAttributes= */ null),
                GoogleCloudStorageReadOptions.DEFAULT));
  }

  @Test
  public void verifyInPlaceSeek() throws IOException {
    int startPosition = 0;
    int bytesToRead = 100;
    readChannel.position(startPosition);
    assertThat(readChannel.position()).isEqualTo(startPosition);
    ByteBuffer buffer = ByteBuffer.allocate(bytesToRead);
    readChannel.read(buffer);
    verifyContent(buffer, startPosition, bytesToRead);
    verify(fakeReadChannel, times(1)).seek(anyLong());
    verify(fakeReadChannel, times(1)).limit(anyLong());

    buffer.clear();
    int seekPosition =
        startPosition + bytesToRead + IN_PLACE_SEEK_LIMIT; // within the in placeSeek limit
    readChannel.position(seekPosition);
    readChannel.read(buffer);
    // Total 3 read calls
    // 2 for actual read operation, 1 for in place seek.
    verify(fakeReadChannel, times(3)).read(any());
    verify(fakeReadChannel, times(0)).close();

    verifyNoMoreInteractions(fakeReadChannel);
  }

  @Test
  public void requestRangeOverlapFooterCache() throws IOException {
    int chunkSize = FakeReadChannel.CHUNK_SIZE;

    // footerSize is the minimumChunkSize
    int footerSize = chunkSize;
    // position just before footer start
    int startPosition = OBJECT_SIZE - footerSize - 5;
    readChannel.position(startPosition);
    assertThat(readChannel.position()).isEqualTo(startPosition);
    // requested range is overlapping with footer
    int bytesToRead = CHUNK_SIZE;
    ByteBuffer buffer = ByteBuffer.allocate(bytesToRead);
    readChannel.read(buffer);
    verifyContent(buffer, startPosition, bytesToRead);
    verify(fakeReadChannel, times(2)).seek(anyLong());
    verify(fakeReadChannel, times(2)).limit(anyLong());
    verify(fakeReadChannel, times(3)).read(any());
    verify(fakeReadChannel, times(2)).close();

    // Footer is served from cache
    startPosition = OBJECT_SIZE - footerSize;
    readChannel.position(startPosition);
    readChannel.read(buffer);
    verifyNoMoreInteractions(fakeReadChannel);
  }

  @Test
  public void readThrowException() throws IOException {
    fakeReadChannel = spy(new FakeReadChannel(CONTENT, true, false));
    when(mockedStorage.reader(any(), any())).thenReturn(fakeReadChannel);
    readChannel =
        getJavaStorageChannel(
            DEFAULT_ITEM_INFO,
            GoogleCloudStorageReadOptions.builder()
                .setFadvise(Fadvise.RANDOM)
                .setGrpcChecksumsEnabled(true)
                .setInplaceSeekLimit(IN_PLACE_SEEK_LIMIT)
                .setMinRangeRequestSize(FakeReadChannel.CHUNK_SIZE)
                .build());

    int startPosition = 0;
    readChannel.position(startPosition);
    assertThrows(IOException.class, () -> readChannel.read(ByteBuffer.allocate(1)));
  }

  @Test
  public void closeThrowsException() throws IOException {
    fakeReadChannel = spy(new FakeReadChannel(CONTENT, false, true));
    when(mockedStorage.reader(any(), any())).thenReturn(fakeReadChannel);
    readChannel =
        getJavaStorageChannel(
            DEFAULT_ITEM_INFO,
            GoogleCloudStorageReadOptions.builder()
                .setFadvise(Fadvise.RANDOM)
                .setGrpcChecksumsEnabled(true)
                .setInplaceSeekLimit(IN_PLACE_SEEK_LIMIT)
                .setMinRangeRequestSize(FakeReadChannel.CHUNK_SIZE)
                .build());
    readChannel.read(ByteBuffer.allocate(CHUNK_SIZE));
    readChannel.close();
    verify(fakeReadChannel, times(1)).seek(anyLong());
    verify(fakeReadChannel, times(1)).limit(anyLong());
    verify(fakeReadChannel, times(1)).read(any());
    verify(fakeReadChannel, times(1)).close();
    verifyNoMoreInteractions(fakeReadChannel);
  }

  private void verifyContent(ByteBuffer buffer, int startPosition, int length) {
    assertThat(buffer.limit()).isEqualTo(length);
    assertByteArrayEquals(
        buffer.array(), CONTENT.substring(startPosition, (startPosition + length)).toByteArray());
  }

  private GoogleCloudStorageClientReadChannel getJavaStorageChannel(
      GoogleCloudStorageItemInfo itemInfo, GoogleCloudStorageReadOptions readOptions)
      throws IOException {
    GoogleCloudStorageItemInfo objectInfo = itemInfo;
    if (itemInfo == null) {
      objectInfo = DEFAULT_ITEM_INFO;
    }
    return new GoogleCloudStorageClientReadChannel(mockedStorage, objectInfo, readOptions);
  }
}
