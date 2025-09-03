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
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
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
    when(mockGcsAppendChannel.write(src)).thenReturn(3);

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
  public void testWrite_success_multipleChucks() throws IOException {
    writeChannel = getJavaStorageChannel();
    byte[] chunk1 = new byte[] {1, 2, 3};
    byte[] chunk2 = new byte[] {4, 5};
    byte[] expectedContent = new byte[] {1, 2, 3, 4, 5};
    ByteBuffer buffer1 = ByteBuffer.wrap(chunk1);
    ByteBuffer buffer2 = ByteBuffer.wrap(chunk2);
    when(mockGcsAppendChannel.write(any(ByteBuffer.class)))
        .thenAnswer(
            invocation -> {
              ByteBuffer arg = invocation.getArgument(0);
              return arg.remaining();
            });

    int bytesWritten1 = writeChannel.write(buffer1);
    int bytesWritten2 = writeChannel.write(buffer2);

    assertEquals(3, bytesWritten1);
    assertEquals(2, bytesWritten2);
    ArgumentCaptor<ByteBuffer> byteBufferCaptor = ArgumentCaptor.forClass(ByteBuffer.class);
    verify(mockGcsAppendChannel, times(2)).write(byteBufferCaptor.capture());
    List<ByteBuffer> capturedBuffers = byteBufferCaptor.getAllValues();
    assertEquals("Should have captured two buffers", 2, capturedBuffers.size());
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    for (ByteBuffer buffer : capturedBuffers) {
      byte[] data = new byte[buffer.remaining()];
      buffer.get(data);
      outputStream.writeBytes(data); // writeBytes() doesn't throw IOException
    }
    byte[] actualContent = outputStream.toByteArray();
    assertArrayEquals(
        "The combined content written to the channel is incorrect", expectedContent, actualContent);
  }

  private GoogleCloudStorageBidiWriteChannel getJavaStorageChannel() throws IOException {
    return new GoogleCloudStorageBidiWriteChannel(
        mockStorage,
        GoogleCloudStorageOptions.DEFAULT.toBuilder().setFinalizeBeforeClose(false).build(),
        resourceId,
        CreateObjectOptions.DEFAULT_NO_OVERWRITE.toBuilder().build());
  }
}
