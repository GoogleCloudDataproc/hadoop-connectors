package com.google.cloud.hadoop.gcsio;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertArrayEquals;
import java.io.ByteArrayOutputStream;
import static org.junit.Assert.assertNull;
import org.mockito.ArgumentCaptor;
import java.util.List;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.storage.BlobId;

import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.channels.ClosedChannelException;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.api.core.ApiFuture;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.cloud.storage.BlobAppendableUpload;
import com.google.cloud.storage.BlobAppendableUploadConfig;
import com.google.cloud.storage.BlobAppendableUpload.AppendableUploadWriteableByteChannel; // Specific channel type
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class GoogleCloudStorageBidiWriteChannelTest {

    private static final String BUCKET_NAME = "test-bucket";
    private static final String OBJECT_NAME = "test-object.txt";
    private static final String CONTENT_TYPE = "text/plain";

    private static final String CONTENT_ENCODING = "content-encoding";

    private static final Map<String, String> metadata =
            Map.of("metadata-key-1", "dGVzdC1tZXRhZGF0YQ==");

    private static final String KMS_KEY = "kms-key";
    @Mock
    private Storage mockStorage;
    @Mock
    private BlobAppendableUpload mockAppendUploadSession;
    @Mock
    private AppendableUploadWriteableByteChannel mockGcsAppendChannel;
    @Mock
    private ApiFuture<BlobInfo> mockResultFuture;

    private StorageResourceId resourceId;
    private GoogleCloudStorageBidiWriteChannel writeChannel;

    @Before
    public void setUp() throws Exception {
        resourceId = new StorageResourceId(BUCKET_NAME, OBJECT_NAME);
        // Mock the standard session creation flow
        when(mockStorage.blobAppendableUpload(any(BlobInfo.class), any(BlobAppendableUploadConfig.class)))
                .thenReturn(mockAppendUploadSession);
        when(mockAppendUploadSession.open()).thenReturn(mockGcsAppendChannel);
        when(mockAppendUploadSession.getResult()).thenReturn(mockResultFuture);
        writeChannel = getJavaStorageChannel();
    }

    @Test
    public void testWrite_success() throws IOException {
        ByteBuffer src = ByteBuffer.wrap(new byte[]{1, 2, 3});

        when(mockGcsAppendChannel.write(src)).thenReturn(3);
        int bytesWritten = writeChannel.write(src);

        assertEquals(3, bytesWritten);
        verify(mockGcsAppendChannel).write(src);
    }

    @Test
    public void testWrite_whenClosed_throwsClosedChannelException() throws IOException, ExecutionException, InterruptedException, TimeoutException {
        // Close the channel without errors
        BlobInfo fakeResult = BlobInfo.newBuilder(BlobId.of(BUCKET_NAME, OBJECT_NAME)).build();
        when(mockResultFuture.get(anyLong(), any(TimeUnit.class))).thenReturn(fakeResult);
        writeChannel.close();

        assertFalse(writeChannel.isOpen());

        ByteBuffer src = ByteBuffer.wrap(new byte[]{1});
        assertThrows(ClosedChannelException.class, () -> writeChannel.write(src));
    }

    @Test
    public void testWrite_nullBuffer_throwsNullPointerException() throws IOException {
        assertThrows(NullPointerException.class, () -> writeChannel.write(null));
    }

    @Test
    public void testClose_success() throws Exception {
        BlobInfo finalBlobInfo = BlobInfo.newBuilder(BlobId.of(BUCKET_NAME, OBJECT_NAME))
                .build();
        when(mockResultFuture.get(anyLong(), any(TimeUnit.class))).thenReturn(finalBlobInfo);

        assertTrue(writeChannel.isOpen());
        assertNull(writeChannel.getFinalizedBlobInfo());

        writeChannel.close();

        assertFalse(writeChannel.isOpen());
        verify(mockGcsAppendChannel).finalizeAndClose();
        verify(mockAppendUploadSession).getResult();
        verify(mockResultFuture).get(anyLong(), any(TimeUnit.class));

        assertEquals(finalBlobInfo, writeChannel.getFinalizedBlobInfo());
    }


    @Test
    public void testClose_whenAlreadyClosed_doesNothing() throws IOException, ExecutionException, InterruptedException, TimeoutException {
        BlobInfo fakeResult = BlobInfo.newBuilder(BlobId.of(BUCKET_NAME, OBJECT_NAME)).build();
        when(mockResultFuture.get(anyLong(), any(TimeUnit.class))).thenReturn(fakeResult);
        writeChannel.close();

        // Call close again
        writeChannel.close();

        // Verify finalization methods were only called once
        verify(mockGcsAppendChannel, times(1)).finalizeAndClose();
        verify(mockAppendUploadSession, times(1)).getResult();
    }

    @Test
    public void testWrite_multipleAppendsBeforeFinalizing() throws IOException {
        // 1. Define the data chunks and the expected final content
        byte[] chunk1 = new byte[]{1, 2, 3};
        byte[] chunk2 = new byte[]{4, 5};
        byte[] expectedContent = new byte[]{1, 2, 3, 4, 5};

        ByteBuffer buffer1 = ByteBuffer.wrap(chunk1);
        ByteBuffer buffer2 = ByteBuffer.wrap(chunk2);

        when(mockGcsAppendChannel.write(any(ByteBuffer.class))).thenAnswer(
                invocation -> {
                    ByteBuffer arg = invocation.getArgument(0);
                    return arg.remaining(); // A realistic mock that returns bytes written
                });

        int bytesWritten1 = writeChannel.write(buffer1);
        int bytesWritten2 = writeChannel.write(buffer2);

        assertEquals(3, bytesWritten1);
        assertEquals(2, bytesWritten2);

        ArgumentCaptor<ByteBuffer> byteBufferCaptor = ArgumentCaptor.forClass(ByteBuffer.class);

        // 3. Verify that write was called twice, and capture the arguments each time
        verify(mockGcsAppendChannel, times(2)).write(byteBufferCaptor.capture());

        // 4. Retrieve the captured buffers
        List<ByteBuffer> capturedBuffers = byteBufferCaptor.getAllValues();
        assertEquals("Should have captured two buffers", 2, capturedBuffers.size());

        // 5. Combine the captured buffers into a single byte array for verification
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        capturedBuffers.forEach(
                buffer -> {
                    // Make sure to handle the buffer's state correctly
                    byte[] data = new byte[buffer.remaining()];
                    buffer.get(data);
                    try {
                        outputStream.write(data);
                    } catch (IOException e) {
                        // This should not happen with a ByteArrayOutputStream
                        throw new RuntimeException(e);
                    }
                });

        byte[] actualContent = outputStream.toByteArray();

        // 6. Assert that the combined content matches our expectation
        assertArrayEquals("The combined content written to the channel is incorrect",
                expectedContent, actualContent);

        // Final check that the channel wasn't closed prematurely
        assertTrue(writeChannel.isOpen());
        verify(mockGcsAppendChannel, never()).finalizeAndClose();
    }

    private GoogleCloudStorageBidiWriteChannel getJavaStorageChannel() throws IOException {
        return new GoogleCloudStorageBidiWriteChannel(
                mockStorage,
                GoogleCloudStorageOptions.DEFAULT.toBuilder()
                        .setWriteChannelOptions(
                                AsyncWriteChannelOptions.DEFAULT.toBuilder().setGrpcChecksumsEnabled(true).build())
                        .build(),
                resourceId,
                CreateObjectOptions.DEFAULT_NO_OVERWRITE.toBuilder()
                        .setContentType(CONTENT_TYPE)
                        .setContentEncoding(CONTENT_ENCODING)
                        .setMetadata(GoogleCloudStorageTestHelper.getDecodedMetadata(metadata))
                        .setKmsKeyName(KMS_KEY)
                        .build());
    }
}
