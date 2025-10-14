/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.gcsio;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.google.cloud.storage.BlobAppendableUpload;
import com.google.cloud.storage.BlobAppendableUpload.AppendableUploadWriteableByteChannel;
import com.google.cloud.storage.BlobAppendableUploadConfig;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class GoogleCloudStorageBidiWriteChannelTest {

  private static final String BUCKET_NAME = "test-bucket";
  private static final String OBJECT_NAME = "test-object.txt";
  @Mock private Storage mockStorage;
  @Mock private BlobAppendableUpload mockAppendUploadSession;
  @Mock private AppendableUploadWriteableByteChannel mockGcsAppendChannel;
  private StorageResourceId resourceId;
  private GoogleCloudStorageBidiWriteChannel writeChannel;

  @Before
  public void setUp() throws Exception {
    resourceId = new StorageResourceId(BUCKET_NAME, OBJECT_NAME);

    when(mockStorage.blobAppendableUpload(
            any(BlobInfo.class),
            any(BlobAppendableUploadConfig.class),
            any(Storage.BlobWriteOption.class)))
        .thenReturn(mockAppendUploadSession);
    when(mockAppendUploadSession.open()).thenReturn(mockGcsAppendChannel);
  }

  @Test
  public void testWrite_success() throws IOException {
    writeChannel = getJavaStorageChannel();
    ByteBuffer src = ByteBuffer.wrap(new byte[] {1, 2, 3});
    when(mockGcsAppendChannel.write(any(ByteBuffer.class)))
        .thenAnswer(
            invocation -> {
              ByteBuffer buffer = invocation.getArgument(0);
              int remaining = buffer.remaining();
              // Move the buffer's position to its limit to simulate a full write
              buffer.position(buffer.limit());
              return remaining;
            });

    int bytesWritten = writeChannel.write(src);

    assertEquals(3, bytesWritten);
    verify(mockGcsAppendChannel).write(src);
  }

  @Test
  public void testWrite_whenClosed_throwsClosedChannelException() throws IOException {
    writeChannel = getJavaStorageChannel();
    ByteBuffer src = ByteBuffer.wrap(new byte[] {1});

    writeChannel.close();

    assertFalse(writeChannel.isOpen());
    assertThrows(ClosedChannelException.class, () -> writeChannel.write(src));
  }

  @Test
  public void testWrite_nullBuffer_throwsNullPointerException() throws IOException {
    writeChannel = getJavaStorageChannel();
    assertThrows(NullPointerException.class, () -> writeChannel.write(null));
  }

  @Test
  public void testClose_whenAlreadyClosed_doesNothing() throws IOException {
    writeChannel = getJavaStorageChannel();

    // Call close twice
    writeChannel.close();
    writeChannel.close();

    // Verify finalization methods were only called once
    verify(mockGcsAppendChannel, times(1)).close();
  }

  @Test
  public void testClose_callsAppendChannelClose() throws IOException {
    writeChannel = getJavaStorageChannel();

    writeChannel.close();

    verify(mockGcsAppendChannel, times(1)).close();
    verify(mockGcsAppendChannel, times(0)).finalizeAndClose();
  }

  @Test
  public void testFinalizeAndClose_callsAppendChannelFinalizeAndClose() throws IOException {
    writeChannel = getJavaStorageChannel();

    writeChannel.finalizeAndClose();

    verify(mockGcsAppendChannel, times(1)).finalizeAndClose();
    verify(mockGcsAppendChannel, times(0)).close();
  }

  @Test
  public void testWrite_success_multipleChunks() throws IOException {
    writeChannel = getJavaStorageChannel();
    byte[] chunk1 = {1, 2, 3};
    byte[] chunk2 = {4, 5};
    byte[] expectedContent = {1, 2, 3, 4, 5};
    ByteBuffer buffer1 = ByteBuffer.wrap(chunk1);
    ByteBuffer buffer2 = ByteBuffer.wrap(chunk2);

    // This stream will act as our "channel" to verify what was written.
    ByteArrayOutputStream capturedContent = new ByteArrayOutputStream();

    when(mockGcsAppendChannel.write(any(ByteBuffer.class)))
        .thenAnswer(
            invocation -> {
              ByteBuffer buffer = invocation.getArgument(0);
              int bytesToWrite = buffer.remaining();
              // Create a temporary byte array to hold the data from the buffer
              byte[] data = new byte[bytesToWrite];
              // This get() operation reads from the buffer and advances its position
              buffer.get(data);
              // Write the captured data to our stream for later verification
              capturedContent.write(data);
              return bytesToWrite;
            });

    // Act
    int bytesWritten1 = writeChannel.write(buffer1);
    int bytesWritten2 = writeChannel.write(buffer2);

    // Assert
    assertEquals(3, bytesWritten1);
    assertEquals(2, bytesWritten2);
    // Verify that the combined content written to the mock channel is correct
    assertArrayEquals(
        "The combined content written to the channel is incorrect",
        expectedContent,
        capturedContent.toByteArray());

    verify(mockGcsAppendChannel, times(2)).write(any(ByteBuffer.class));
  }

  private GoogleCloudStorageBidiWriteChannel getJavaStorageChannel() throws IOException {
    return new GoogleCloudStorageBidiWriteChannel(
        mockStorage,
        GoogleCloudStorageOptions.DEFAULT.toBuilder().setFinalizeBeforeClose(false).build(),
        resourceId,
        CreateObjectOptions.DEFAULT_NO_OVERWRITE.toBuilder().build());
  }
}
