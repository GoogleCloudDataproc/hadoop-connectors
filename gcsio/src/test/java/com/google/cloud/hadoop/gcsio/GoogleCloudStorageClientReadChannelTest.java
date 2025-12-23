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
import com.google.cloud.hadoop.gcsio.FakeReadChannel.REQUEST_TYPE;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.Fadvise;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper;
import com.google.cloud.hadoop.util.GrpcErrorTypeExtractor;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobSourceOption;
import com.google.common.collect.ImmutableList;
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
import org.mockito.ArgumentCaptor;

@RunWith(JUnit4.class)
public class GoogleCloudStorageClientReadChannelTest {

  private static final String V1_BUCKET_NAME = "bucket-name";
  private static final String OBJECT_NAME = "object-name";
  private static final int CHUNK_SIZE = FakeReadChannel.CHUNK_SIZE;
  private static final int OBJECT_SIZE = 5 * 1024 * 1024;
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
          .setMinRangeRequestSize(FakeReadChannel.CHUNK_SIZE)
          .build();

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
    readChannel = getJavaStorageChannel(DEFAULT_ITEM_INFO, DEFAULT_READ_OPTION);
  }

  @Test
  public void inValidSeekPositions() {
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
    fakeReadChannel = spy(new FakeReadChannel(CONTENT));
    when(mockedStorage.reader(any(), any())).thenReturn(fakeReadChannel);
    readChannel = getJavaStorageChannel(DEFAULT_ITEM_INFO, DEFAULT_READ_OPTION);
    int startPosition = 0;
    readChannel.position(startPosition);
    assertThat(readChannel.position()).isEqualTo(startPosition);

    ByteBuffer buffer = ByteBuffer.allocate(readBytes);
    readChannel.read(buffer);
    verifyContent(buffer, startPosition, readBytes);
    verify(fakeReadChannel, times(1)).seek(anyLong());
    verify(fakeReadChannel, times(1)).limit(anyLong());
    verify(fakeReadChannel, times(1)).setChunkSize(0);
    verify(fakeReadChannel, times(1)).read(any());

    verifyNoMoreInteractions(fakeReadChannel);
  }

  @Test
  public void readMultipleChunkSuccessSequential() throws IOException {
    int chunksToRead = 2;
    int chunkSize = FakeReadChannel.CHUNK_SIZE;
    int readBytes = chunksToRead * chunkSize;
    fakeReadChannel = spy(new FakeReadChannel(CONTENT));
    when(mockedStorage.reader(any(), any())).thenReturn(fakeReadChannel);
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
    verify(fakeReadChannel, times(1)).setChunkSize(0);
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
    verify(fakeReadChannel, times(OBJECT_SIZE / chunkSize)).setChunkSize(0);
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
  public void read_withPositiveGeneration_usesGenerationMatchPrecondition() throws IOException {
    long generation = 12345L;
    GoogleCloudStorageItemInfo itemInfo =
        GoogleCloudStorageItemInfo.createObject(
            new StorageResourceId(V1_BUCKET_NAME, OBJECT_NAME, generation),
            /* creationTime= */ 10L,
            /* modificationTime= */ 15L,
            /* size= */ OBJECT_SIZE,
            /* contentType= */ "text/plain",
            /* contentEncoding= */ "text",
            /* metadata= */ null,
            /* contentGeneration= */ generation,
            /* metaGeneration= */ 2L,
            /* verificationAttributes= */ null);

    readChannel = getJavaStorageChannel(itemInfo, DEFAULT_READ_OPTION);
    readChannel.read(ByteBuffer.allocate(1));

    ArgumentCaptor<BlobSourceOption> optionsCaptor =
        ArgumentCaptor.forClass(BlobSourceOption.class);
    verify(mockedStorage).reader(any(BlobId.class), optionsCaptor.capture());

    assertThat(optionsCaptor.getAllValues()).contains(BlobSourceOption.generationMatch(generation));
  }

  @Test
  public void read_withZeroGeneration_doesNotUseGenerationMatchPrecondition() throws IOException {
    long generation = 0L;
    GoogleCloudStorageItemInfo itemInfo =
        GoogleCloudStorageItemInfo.createObject(
            new StorageResourceId(V1_BUCKET_NAME, OBJECT_NAME, generation),
            /* creationTime= */ 10L,
            /* modificationTime= */ 15L,
            /* size= */ OBJECT_SIZE,
            /* contentType= */ "text/plain",
            /* contentEncoding= */ "text",
            /* metadata= */ null,
            /* contentGeneration= */ generation,
            /* metaGeneration= */ 2L,
            /* verificationAttributes= */ null);

    readChannel = getJavaStorageChannel(itemInfo, DEFAULT_READ_OPTION);
    readChannel.read(ByteBuffer.allocate(1));

    ArgumentCaptor<BlobSourceOption> optionsCaptor =
        ArgumentCaptor.forClass(BlobSourceOption.class);
    verify(mockedStorage).reader(any(BlobId.class), optionsCaptor.capture());

    assertThat(optionsCaptor.getAllValues())
        .doesNotContain(BlobSourceOption.generationMatch(generation));
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
    verify(fakeReadChannel, times(2)).setChunkSize(0);
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
    verify(fakeReadChannel, times(2)).setChunkSize(0);
    verify(fakeReadChannel, times(1)).close();
    verify(fakeReadChannel, times(2)).read(any());
    verifyNoMoreInteractions(fakeReadChannel);
  }

  @Test
  public void fadviseAutoRandom_onSequentialRead_switchToSequential() throws IOException {
    long blockSize = CHUNK_SIZE;
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder()
            .setFadvise(Fadvise.AUTO_RANDOM)
            .setGrpcChecksumsEnabled(true)
            .setInplaceSeekLimit(5)
            .setMinRangeRequestSize(10)
            .setBlockSize(blockSize)
            .build();
    GoogleCloudStorageClientReadChannel readChannel =
        getJavaStorageChannel(DEFAULT_ITEM_INFO, readOptions);

    int seekPosition = 0;
    int readLength = (int) readOptions.getMinRangeRequestSize();

    for (int i = 0; i < readOptions.getFadviseRequestTrackCount() + 1; i++) {
      ByteBuffer buffer = ByteBuffer.allocate(readLength);
      fakeReadChannel = spy(new FakeReadChannel(CONTENT));
      when(mockedStorage.reader(any(), any())).thenReturn(fakeReadChannel);

      readChannel.position(seekPosition);
      readChannel.read(buffer);
      verifyContent(buffer, seekPosition, readLength);
      if (i < readOptions.getFadviseRequestTrackCount()) {
        assertThat(readChannel.randomAccessStatus()).isTrue();
        verify(fakeReadChannel, times(1)).seek(seekPosition);
        verify(fakeReadChannel, times(1)).limit(seekPosition + readLength);
        verify(fakeReadChannel, times(1)).setChunkSize(0);
      } else {
        assertThat(readChannel.randomAccessStatus()).isFalse();
        verify(fakeReadChannel, times(1)).seek(seekPosition);
        verify(fakeReadChannel, times(1)).limit(seekPosition + blockSize);
        verify(fakeReadChannel, times(1)).setChunkSize(0);
      }

      seekPosition += readLength;
      buffer.clear();
    }
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
    verify(fakeReadChannel, times(1)).setChunkSize(0);
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
    verify(fakeReadChannel, times(1)).setChunkSize(0);
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
  public void readThrowException() throws IOException {
    fakeReadChannel =
        spy(new FakeReadChannel(CONTENT, ImmutableList.of(REQUEST_TYPE.READ_EXCEPTION)));
    when(mockedStorage.reader(any(), any())).thenReturn(fakeReadChannel);
    readChannel = getJavaStorageChannel(DEFAULT_ITEM_INFO, DEFAULT_READ_OPTION);

    int startPosition = 0;
    readChannel.position(startPosition);
    assertThrows(IOException.class, () -> readChannel.read(ByteBuffer.allocate(1)));
  }

  @Test
  public void closeThrowsException() throws IOException {
    fakeReadChannel =
        spy(
            new FakeReadChannel(CONTENT) {
              @Override
              public void close() {
                throw new RuntimeException("Runtime exception while closing content Channel");
              }
            });
    when(mockedStorage.reader(any(), any())).thenReturn(fakeReadChannel);
    readChannel = getJavaStorageChannel(DEFAULT_ITEM_INFO, DEFAULT_READ_OPTION);
    readChannel.read(ByteBuffer.allocate(CHUNK_SIZE));
    readChannel.close();
    verify(fakeReadChannel, times(1)).seek(anyLong());
    verify(fakeReadChannel, times(1)).limit(anyLong());
    verify(fakeReadChannel, times(1)).setChunkSize(0);
    verify(fakeReadChannel, times(1)).read(any());
    verify(fakeReadChannel, times(1)).close();
    verifyNoMoreInteractions(fakeReadChannel);
  }

  @Test
  public void partialReadThrows() throws IOException {
    int readBytes = 10;
    int partialByteRead = readBytes / 2;
    fakeReadChannel =
        spy(new FakeReadChannel(CONTENT, ImmutableList.of(REQUEST_TYPE.PARTIAL_READ)));
    when(mockedStorage.reader(any(), any())).thenReturn(fakeReadChannel);
    readChannel = getJavaStorageChannel(DEFAULT_ITEM_INFO, DEFAULT_READ_OPTION);

    // Read partial content
    ByteBuffer buffer = ByteBuffer.allocate(readBytes);
    assertThrows(IOException.class, () -> readChannel.read(buffer));
    verify(fakeReadChannel, times(1)).seek(anyLong());
    verify(fakeReadChannel, times(1)).limit(anyLong());
    verify(fakeReadChannel, times(1)).setChunkSize(0);
    verify(fakeReadChannel, times(1)).read(any());
    verify(fakeReadChannel, times(1)).close();
    assertThat(buffer.position()).isEqualTo(partialByteRead);
    assertThat(readChannel.position()).isEqualTo(partialByteRead);
    verifyNoMoreInteractions(fakeReadChannel);
  }

  @Test
  public void readThrowsEventuallyPass() throws IOException {
    int readBytes = 10;
    int partialByteRead = readBytes / 2;
    fakeReadChannel =
        spy(
            new FakeReadChannel(
                CONTENT, ImmutableList.of(REQUEST_TYPE.PARTIAL_READ, REQUEST_TYPE.READ_CHUNK)));
    when(mockedStorage.reader(any(), any())).thenReturn(fakeReadChannel);
    readChannel = getJavaStorageChannel(DEFAULT_ITEM_INFO, DEFAULT_READ_OPTION);

    // Read partial content
    ByteBuffer buffer = ByteBuffer.allocate(readBytes);
    assertThrows(IOException.class, () -> readChannel.read(buffer));
    assertThat(buffer.position()).isEqualTo(partialByteRead);
    assertThat(readChannel.position()).isEqualTo(partialByteRead);

    // subsequent read successful
    readChannel.read(buffer);
    verify(fakeReadChannel, times(2)).seek(anyLong());
    verify(fakeReadChannel, times(2)).limit(anyLong());
    verify(fakeReadChannel, times(2)).setChunkSize(0);
    verify(fakeReadChannel, times(2)).read(any());
    verify(fakeReadChannel, times(1)).close();
    assertThat(buffer.position()).isEqualTo(readBytes);
    verifyContent(buffer, 0, readBytes);
    verifyNoMoreInteractions(fakeReadChannel);
  }

  @Test
  public void contentChannelCreationException() throws IOException {
    fakeReadChannel =
        spy(
            new FakeReadChannel(CONTENT) {
              @Override
              public void seek(long seek) throws IOException {
                throw new IOException(
                    "Intentionally triggered while setting position for content channel");
              }
            });
    when(mockedStorage.reader(any(), any())).thenReturn(fakeReadChannel);
    readChannel = getJavaStorageChannel(DEFAULT_ITEM_INFO, DEFAULT_READ_OPTION);
    assertThrows(IOException.class, () -> readChannel.read(ByteBuffer.allocate(1)));
    verify(fakeReadChannel, times(1)).seek(anyLong());
    verifyNoMoreInteractions(fakeReadChannel);

    fakeReadChannel =
        spy(
            new FakeReadChannel(CONTENT) {
              @Override
              public ReadChannel limit(long seek) {
                throw new RuntimeException(
                    "Intentionally triggered while setting position for content channel");
              }
            });
    when(mockedStorage.reader(any(), any())).thenReturn(fakeReadChannel);
    readChannel = getJavaStorageChannel(DEFAULT_ITEM_INFO, DEFAULT_READ_OPTION);
    assertThrows(IOException.class, () -> readChannel.read(ByteBuffer.allocate(1)));
    verify(fakeReadChannel, times(1)).seek(anyLong());
    verify(fakeReadChannel, times(1)).limit(anyLong());
    verifyNoMoreInteractions(fakeReadChannel);
  }

  @Test
  public void requestRangeOverlapWithCachedFooter() throws IOException {
    int chunkSize = FakeReadChannel.CHUNK_SIZE;
    // footerSize is the minimumChunkSize
    int footerSize = chunkSize;
    // chunk which starts somewhere within the footer length
    int startPosition = OBJECT_SIZE - footerSize;
    fakeReadChannel = spy(new FakeReadChannel(CONTENT));
    when(mockedStorage.reader(any(), any())).thenReturn(fakeReadChannel);
    readChannel = getJavaStorageChannel(DEFAULT_ITEM_INFO, DEFAULT_READ_OPTION);
    readChannel.position(startPosition);
    assertThat(readChannel.position()).isEqualTo(startPosition);
    ByteBuffer buffer = ByteBuffer.allocate(chunkSize);
    readChannel.read(buffer);
    verifyContent(buffer, startPosition, chunkSize);

    startPosition = startPosition - 1;
    buffer.clear();
    readChannel.position(startPosition);
    readChannel.read(buffer);
    verifyContent(buffer, startPosition, chunkSize);
    ArgumentCaptor<Long> seekValue = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<Long> limitValue = ArgumentCaptor.forClass(Long.class);
    verify(fakeReadChannel, times(2)).seek(seekValue.capture());
    verify(fakeReadChannel, times(2)).limit(limitValue.capture());
    verify(fakeReadChannel, times(2)).setChunkSize(0);
    verify(fakeReadChannel, times(3)).read(any());
    verify(fakeReadChannel, times(2)).close();
    // First request fetched full footer
    assertThat(seekValue.getAllValues().get(0)).isEqualTo(OBJECT_SIZE - footerSize);
    assertThat(limitValue.getAllValues().get(0)).isEqualTo(OBJECT_SIZE);
    // Second request although requested for whole 1024 bytes
    // but fetches only 1 byte from contentChannel
    // rest is served from cached footer
    assertThat(seekValue.getAllValues().get(1)).isEqualTo(startPosition);
    assertThat(limitValue.getAllValues().get(1)).isEqualTo(startPosition + 1);
  }

  @Test
  public void lazyMetadataFetch_whenFastFailFalse_usesFallback() throws IOException {
    // Setup with fastFailOnNotFound = false and null itemInfo
    GoogleCloudStorageReadOptions readOptions =
        DEFAULT_READ_OPTION.toBuilder().setFastFailOnNotFoundEnabled(false).build();

    // Create channel directly to pass null itemInfo
    readChannel =
        new GoogleCloudStorageClientReadChannel(
            mockedStorage,
            RESOURCE_ID,
            null, // itemInfo
            readOptions,
            GrpcErrorTypeExtractor.INSTANCE,
            GoogleCloudStorageOptions.DEFAULT.toBuilder().build());

    // Verify storage.get was NOT called during construction
    verify(mockedStorage, times(0)).get(any(com.google.cloud.storage.BlobId.class));

    // Mock storage.get for the fallback/lazy fetch
    com.google.cloud.storage.Blob mockBlob = mock(com.google.cloud.storage.Blob.class);
    when(mockBlob.getSize()).thenReturn((long) OBJECT_SIZE);
    when(mockBlob.getContentEncoding()).thenReturn("text/plain");
    when(mockedStorage.get(any(com.google.cloud.storage.BlobId.class), any())).thenReturn(mockBlob);

    // Perform a read, which should trigger metadata fetch (fallback since FakeReadChannel has no
    // getObject)
    readChannel.read(ByteBuffer.allocate(1));

    // Verify storage.get WAS called now
    verify(mockedStorage, times(1)).get(any(com.google.cloud.storage.BlobId.class), any());
    assertThat(readChannel.size()).isEqualTo(OBJECT_SIZE);
  }

  @Test
  public void lazyMetadataFetch_viaReflection_success() throws Exception {
    // Setup with fastFailOnNotFound = false and null itemInfo
    GoogleCloudStorageReadOptions readOptions =
        DEFAULT_READ_OPTION.toBuilder().setFastFailOnNotFoundEnabled(false).build();

    // Create a mock BlobInfo to be returned by reflection
    com.google.cloud.storage.BlobInfo mockBlobInfo = mock(com.google.cloud.storage.BlobInfo.class);
    when(mockBlobInfo.getSize()).thenReturn((long) OBJECT_SIZE);
    when(mockBlobInfo.getContentEncoding()).thenReturn("text/plain");
    when(mockBlobInfo.getGeneration()).thenReturn(12345L);

    // Create our fake channel that supports reflection
    MockStorageReadChannel fakeChannel = new MockStorageReadChannel(CONTENT, mockBlobInfo);
    when(mockedStorage.reader(any(), any())).thenReturn(fakeChannel);

    readChannel =
        new GoogleCloudStorageClientReadChannel(
            mockedStorage,
            RESOURCE_ID,
            null, // itemInfo
            readOptions,
            GrpcErrorTypeExtractor.INSTANCE,
            GoogleCloudStorageOptions.DEFAULT.toBuilder().build());

    // Verify storage.get was NOT called during construction
    verify(mockedStorage, times(0)).get(any(com.google.cloud.storage.BlobId.class));

    // Perform a read, which should trigger metadata fetch via reflection
    ByteBuffer buffer = ByteBuffer.allocate(10);
    readChannel.read(buffer);

    // Verify storage.get was NOT called (because reflection succeeded)
    verify(mockedStorage, times(0)).get(any(com.google.cloud.storage.BlobId.class), any());

    // Verify size is correct
    assertThat(readChannel.size()).isEqualTo(OBJECT_SIZE);
  }

  @Test
  public void size_whenFastFailFalse_triggersMetadataFetch() throws IOException {
    // Setup with fastFailOnNotFound = false and null itemInfo
    GoogleCloudStorageReadOptions readOptions =
        DEFAULT_READ_OPTION.toBuilder().setFastFailOnNotFoundEnabled(false).build();

    readChannel =
        new GoogleCloudStorageClientReadChannel(
            mockedStorage,
            RESOURCE_ID,
            null, // itemInfo
            readOptions,
            GrpcErrorTypeExtractor.INSTANCE,
            GoogleCloudStorageOptions.DEFAULT.toBuilder().build());

    // Verify size is initially unknown (MAX_VALUE) internally, but size() triggers fetch
    // Mock storage.get for the lazy fetch
    com.google.cloud.storage.Blob mockBlob = mock(com.google.cloud.storage.Blob.class);
    when(mockBlob.getSize()).thenReturn((long) OBJECT_SIZE);
    when(mockBlob.getContentEncoding()).thenReturn("text/plain");
    when(mockedStorage.get(any(com.google.cloud.storage.BlobId.class), any())).thenReturn(mockBlob);

    // Call size()
    long size = readChannel.size();

    // Verify fetch happened and size is correct
    verify(mockedStorage, times(1)).get(any(com.google.cloud.storage.BlobId.class), any());
    assertThat(size).isEqualTo(OBJECT_SIZE);
  }

  @Test
  public void testChunkedRead_whenObjectSizeUnknown_doesNotTruncateSize() throws IOException {
    // fastFailOnNotFound = false implies objectSize is initially Long.MAX_VALUE
    // Use AUTO_RANDOM to trigger chunked reads
    GoogleCloudStorageReadOptions readOptions =
        DEFAULT_READ_OPTION.toBuilder()
            .setFastFailOnNotFoundEnabled(false)
            .setFadvise(Fadvise.AUTO_RANDOM)
            .setBlockSize(10) // Small block size
            .build();

    // Content larger than block size
    ByteString content = ByteString.copyFromUtf8("12345678901234567890"); // 20 bytes
    com.google.cloud.storage.BlobInfo mockBlobInfo = mock(com.google.cloud.storage.BlobInfo.class);
    when(mockBlobInfo.getSize()).thenReturn(20L);
    MockStorageReadChannel fakeChannel = new MockStorageReadChannel(content, mockBlobInfo);
    when(mockedStorage.reader(any(), any())).thenReturn(fakeChannel);

    readChannel =
        new GoogleCloudStorageClientReadChannel(
            mockedStorage,
            RESOURCE_ID,
            null, // itemInfo is null, so objectSize will be MAX_VALUE
            readOptions,
            GrpcErrorTypeExtractor.INSTANCE,
            GoogleCloudStorageOptions.DEFAULT.toBuilder().build());

    // Read first 10 bytes (one block)
    ByteBuffer buffer = ByteBuffer.allocate(10);
    int bytesRead = readChannel.read(buffer);

    assertThat(bytesRead).isEqualTo(10);
    // objectSize should NOT be updated to 10.
    // Since objectSize is still MAX_VALUE (or 20 if fetched), calling size() triggers
    // fetchMetadata() which returns 20.
    // If it was incorrectly updated to 10, size() would return 10.
    assertThat(readChannel.size()).isEqualTo(20L);

    // Read next 10 bytes
    buffer.clear();
    bytesRead = readChannel.read(buffer);
    assertThat(bytesRead).isEqualTo(10);
  }

  // A fake ReadChannel that mimics StorageReadChannel by having a getObject method
  // and a class name ending in "StorageReadChannel"
  private static class MockStorageReadChannel extends FakeReadChannel {
    private final com.google.cloud.storage.BlobInfo blobInfo;

    public MockStorageReadChannel(ByteString content, com.google.cloud.storage.BlobInfo blobInfo) {
      super(content);
      this.blobInfo = blobInfo;
    }

    public com.google.api.core.ApiFuture<com.google.cloud.storage.BlobInfo> getObject() {
      return com.google.api.core.ApiFutures.immediateFuture(blobInfo);
    }
  }

  private void verifyContent(ByteBuffer buffer, int startPosition, int length) {
    assertThat(buffer.position()).isEqualTo(length);
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
    return new GoogleCloudStorageClientReadChannel(
        mockedStorage,
        new StorageResourceId(
            objectInfo.getBucketName(),
            objectInfo.getObjectName(),
            objectInfo.getContentGeneration()),
        objectInfo,
        readOptions,
        GrpcErrorTypeExtractor.INSTANCE,
        GoogleCloudStorageOptions.DEFAULT.toBuilder().build());
  }
}
