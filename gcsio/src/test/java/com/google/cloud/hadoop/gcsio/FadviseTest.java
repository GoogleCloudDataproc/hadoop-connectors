/*
 * Copyright 2024 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.Fadvise;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper;
import com.google.cloud.hadoop.util.GrpcErrorTypeExtractor;
import com.google.cloud.storage.Storage;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class FadviseTest {

  private static final String BUCKET_NAME = "bucket-name";
  private static final String OBJECT_NAME = "object-name";
  private static final int OBJECT_SIZE = 5 * 1024 * 1024;
  private static final int IN_PLACE_SEEK_LIMIT = 5;
  private static final StorageResourceId RESOURCE_ID =
      new StorageResourceId(BUCKET_NAME, OBJECT_NAME);
  private static final ByteString CONTENT =
      GoogleCloudStorageTestHelper.createTestData(OBJECT_SIZE);
  private Storage mockedStorage = mock(Storage.class);
  private GoogleCloudStorageClientReadChannel readChannel;

  private List<FakeReadChannel> streamReadChannelList = new ArrayList<>();

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

  private static final GoogleCloudStorageReadOptions DEFAULT_READ_OPTION =
      GoogleCloudStorageReadOptions.builder()
          .setGrpcChecksumsEnabled(true)
          .setInplaceSeekLimit(IN_PLACE_SEEK_LIMIT)
          .setMinRangeRequestSize(FakeReadChannel.CHUNK_SIZE)
          .setBlockSize(FakeReadChannel.CHUNK_SIZE * 2)
          .build();

  @Test
  public void readWithAutoMode() throws IOException {
    // should be lesser than Block_size and minRangeRequestSize
    int readBytes = 100;
    FakeReadChannel fakeReadChannel = spy(new FakeReadChannel(CONTENT));
    when(mockedStorage.reader(any(), any())).thenReturn(fakeReadChannel);
    GoogleCloudStorageReadOptions readOptions =
        DEFAULT_READ_OPTION.toBuilder().setFadvise(Fadvise.AUTO).build();
    readChannel = getJavaStorageChannel(DEFAULT_ITEM_INFO, readOptions);
    // 1st Read
    int startPosition = 0;
    readChannel.position(startPosition);
    assertThat(readChannel.position()).isEqualTo(startPosition);

    ByteBuffer buffer = ByteBuffer.allocate(readBytes);
    readChannel.read(buffer);
    verifyContent(buffer, startPosition, readBytes);

    verifyFakeReadChannelInteractions(
        fakeReadChannel, startPosition, startPosition + Math.max(readBytes, OBJECT_SIZE));

    // 2nd Read
    // backward seek, should cause AUTO to switch to random
    startPosition = 1;
    buffer.clear();
    fakeReadChannel = spy(new FakeReadChannel(CONTENT));
    when(mockedStorage.reader(any(), any())).thenReturn(fakeReadChannel);

    readChannel.position(startPosition);
    readChannel.read(buffer);
    verifyContent(buffer, startPosition, readBytes);

    verifyFakeReadChannelInteractions(
        fakeReadChannel,
        startPosition,
        startPosition + Math.max(readBytes, readOptions.getMinRangeRequestSize()));
  }

  @Test
  public void readWithRandomMode() throws IOException {
    FakeReadChannel fakeReadChannel = spy(new FakeReadChannel(CONTENT));
    when(mockedStorage.reader(any(), any())).thenReturn(fakeReadChannel);
    GoogleCloudStorageReadOptions readOptions =
        DEFAULT_READ_OPTION.toBuilder().setFadvise(Fadvise.RANDOM).build();
    readChannel = getJavaStorageChannel(DEFAULT_ITEM_INFO, readOptions);

    int readBytes = readOptions.getMinRangeRequestSize() + 1;
    // 1st Read
    int startPosition = 0;
    int endPosition = startPosition + readBytes;
    readChannel.position(startPosition);
    assertThat(readChannel.position()).isEqualTo(startPosition);

    ByteBuffer buffer = ByteBuffer.allocate(readBytes);
    readChannel.read(buffer);
    verifyContent(buffer, startPosition, readBytes);

    // random mode, channel limiting to requestedBytes
    verifyFakeReadChannelInteractions(fakeReadChannel, startPosition, startPosition + readBytes);

    // 2nd Read
    startPosition = endPosition;
    endPosition = startPosition + readBytes;
    buffer.clear();
    fakeReadChannel = spy(new FakeReadChannel(CONTENT));
    when(mockedStorage.reader(any(), any())).thenReturn(fakeReadChannel);
    readChannel.position(startPosition);
    readChannel.read(buffer);
    verifyContent(buffer, startPosition, readBytes);

    // random mode, channel limiting to requestedBytes even if request is sequential
    verifyFakeReadChannelInteractions(fakeReadChannel, startPosition, startPosition + readBytes);
  }

  @Test
  public void readWithSequentialMode() throws IOException {
    FakeReadChannel fakeReadChannel = spy(new FakeReadChannel(CONTENT));
    when(mockedStorage.reader(any(), any())).thenReturn(fakeReadChannel);
    GoogleCloudStorageReadOptions readOptions =
        DEFAULT_READ_OPTION.toBuilder().setFadvise(Fadvise.SEQUENTIAL).build();
    readChannel = getJavaStorageChannel(DEFAULT_ITEM_INFO, readOptions);

    int readBytes = readOptions.getMinRangeRequestSize() + 1;
    // 1st Read
    int startPosition = 0;
    int endPosition = startPosition + readBytes;
    readChannel.position(startPosition);
    assertThat(readChannel.position()).isEqualTo(startPosition);

    ByteBuffer buffer = ByteBuffer.allocate(readBytes);
    readChannel.read(buffer);
    verifyContent(buffer, startPosition, readBytes);

    // sequential mode, channel limiting to EOF
    verifyFakeReadChannelInteractions(fakeReadChannel, startPosition, OBJECT_SIZE);

    // 2nd Read
    startPosition = 1;
    endPosition = startPosition + readBytes;
    buffer.clear();
    fakeReadChannel = spy(new FakeReadChannel(CONTENT));
    when(mockedStorage.reader(any(), any())).thenReturn(fakeReadChannel);
    readChannel.position(startPosition);
    readChannel.read(buffer);
    verifyContent(buffer, startPosition, readBytes);

    // sequential mode, channel limiting to EOF even if request are random
    verifyFakeReadChannelInteractions(fakeReadChannel, startPosition, OBJECT_SIZE);
  }

  @Test
  public void readWithAutoRandomMode() throws IOException {
    FakeReadChannel fakeReadChannel = spy(new FakeReadChannel(CONTENT));
    when(mockedStorage.reader(any(), any())).thenReturn(fakeReadChannel);
    GoogleCloudStorageReadOptions readOptions =
        DEFAULT_READ_OPTION.toBuilder().setFadvise(Fadvise.AUTO_RANDOM).build();
    readChannel = getJavaStorageChannel(DEFAULT_ITEM_INFO, readOptions);

    int readBytes = readOptions.getMinRangeRequestSize();
    // 1st Read
    int startPosition = 0;
    int endPosition = startPosition + readBytes;
    readChannel.position(startPosition);
    assertThat(readChannel.position()).isEqualTo(startPosition);

    ByteBuffer buffer = ByteBuffer.allocate(readBytes);
    readChannel.read(buffer);
    verifyContent(buffer, startPosition, readBytes);

    verifyFakeReadChannelInteractions(
        fakeReadChannel,
        startPosition,
        startPosition + Math.max(readBytes, readOptions.getMinRangeRequestSize()));

    // 2nd Read
    // continue reading in sequential manner
    startPosition = endPosition;
    endPosition = startPosition + readBytes;
    buffer.clear();
    fakeReadChannel = spy(new FakeReadChannel(CONTENT));
    when(mockedStorage.reader(any(), any())).thenReturn(fakeReadChannel);

    readChannel.position(startPosition);
    readChannel.read(buffer);
    verifyContent(buffer, startPosition, readBytes);

    verifyFakeReadChannelInteractions(
        fakeReadChannel,
        startPosition,
        startPosition + Math.max(readBytes, readOptions.getMinRangeRequestSize()));

    // 3rd Read
    // continue reading in sequential manner, AUTO_RANDOM flips to sequential mode
    startPosition = endPosition;
    endPosition = startPosition + readBytes;
    buffer.clear();
    fakeReadChannel = spy(new FakeReadChannel(CONTENT));
    when(mockedStorage.reader(any(), any())).thenReturn(fakeReadChannel);

    readChannel.position(startPosition);
    readChannel.read(buffer);
    verifyContent(buffer, startPosition, readBytes);

    // AUTO_RANDOM flips to sequential mode, resulting in channel limit to blockSize instead of
    // RangeRequestSize
    verifyFakeReadChannelInteractions(
        fakeReadChannel,
        startPosition,
        startPosition + Math.max(readBytes, readOptions.getBlockSize()));

    // 4th Read
    //  continue reading in sequential manner
    startPosition = endPosition;
    endPosition = startPosition + readBytes;
    buffer.clear();
    fakeReadChannel = spy(new FakeReadChannel(CONTENT));
    when(mockedStorage.reader(any(), any())).thenReturn(fakeReadChannel);

    readChannel.position(startPosition);
    readChannel.read(buffer);
    verifyContent(buffer, startPosition, readBytes);
    // read is served from channel opened in previous request
    verifyNoMoreInteractions(fakeReadChannel);

    // 5th Read
    // backward seek
    startPosition = 0;
    endPosition = startPosition + readBytes;
    buffer.clear();
    fakeReadChannel = spy(new FakeReadChannel(CONTENT));
    when(mockedStorage.reader(any(), any())).thenReturn(fakeReadChannel);

    readChannel.position(startPosition);
    readChannel.read(buffer);
    verifyContent(buffer, startPosition, readBytes);

    // AUTO_RANDOM already flipped to sequential, now it will treat any request as sequentail read
    verifyFakeReadChannelInteractions(
        fakeReadChannel,
        startPosition,
        startPosition + Math.max(readBytes, readOptions.getBlockSize()));
  }

  private void verifyFakeReadChannelInteractions(
      FakeReadChannel fakeReadChannel, long seek, long limit) throws IOException {
    verify(fakeReadChannel, times(1)).seek(seek);
    verify(fakeReadChannel, times(1)).limit(limit);
    verify(fakeReadChannel, times(1)).setChunkSize(0);
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
        objectInfo,
        readOptions,
        GrpcErrorTypeExtractor.INSTANCE,
        GoogleCloudStorageOptions.DEFAULT.toBuilder().build());
  }
}
