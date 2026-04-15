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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BlobReadSession;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobSourceOption;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.ZeroCopySupport.DisposableByteString;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.IntFunction;
import org.junit.Test;
import org.mockito.ArgumentMatchers;

public class GoogleCloudStorageBidiReadChannelTest {

  private static final String TEST_BUCKET_NAME = "foo-bucket";

  private static final String TEST_OBJECT_NAME = "foo-object";

  private static final int OBJECT_SIZE = FakeBlobReadSession.TEST_STRING.length();
  private static final StorageResourceId RESOURCE_ID =
      new StorageResourceId(TEST_BUCKET_NAME, TEST_OBJECT_NAME);

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

  @Test
  public void getBlobReadSession_whenFutureFails_throwsIOException() throws IOException {
    Storage storage = mock(Storage.class);
    when(storage.blobReadSession(any()))
        .thenReturn(ApiFutures.immediateFailedFuture(new ExecutionException(new Throwable())));

    GoogleCloudStorageBidiReadChannel channel =
        new GoogleCloudStorageBidiReadChannel(
            storage,
            RESOURCE_ID,
            DEFAULT_ITEM_INFO,
            GoogleCloudStorageReadOptions.builder().build(),
            Executors.newSingleThreadExecutor());

    IOException e =
        assertThrows(
            IOException.class,
            // Calling read() will trigger getBlobReadSession()
            () -> channel.read(ByteBuffer.allocate(1)));

    assertThat(e).hasMessageThat().isEqualTo("Failed to get BlobReadSession");
    assertThat(e).hasCauseThat().isInstanceOf(ExecutionException.class);
  }

  // TODO(dhritichopra): Remove test after support for Gzip is added
  @Test
  public void constructor_whenItemIsGzipEncoded_throwsUnsupportedOperationException()
      throws UnsupportedOperationException {
    Storage storage = mock(Storage.class);
    // Mock session creation to succeed (so the constructor gets to the metadata check)
    when(storage.blobReadSession(any()))
        .thenReturn(ApiFutures.immediateFuture(new FakeBlobReadSession()));

    GoogleCloudStorageItemInfo gzipItemInfo =
        GoogleCloudStorageItemInfo.createObject(
            RESOURCE_ID,
            /* creationTime= */ 10L,
            /* modificationTime= */ 15L,
            /* size= */ OBJECT_SIZE,
            /* contentType= */ "text/plain",
            /* contentEncoding= */ "gzip", // <-- Set gzip encoding
            /* metadata= */ null,
            /* contentGeneration= */ 1,
            /* metaGeneration= */ 2L,
            /* verificationAttributes= */ null);

    UnsupportedOperationException e =
        assertThrows(
            UnsupportedOperationException.class,
            () ->
                new GoogleCloudStorageBidiReadChannel(
                    storage,
                    RESOURCE_ID,
                    gzipItemInfo,
                    GoogleCloudStorageReadOptions.builder().build(),
                    Executors.newSingleThreadExecutor()));

    assertThat(e).hasMessageThat().isEqualTo("Gzip Encoded Files are not supported");
  }

  @Test
  public void lazyMetadataFetch_whenFastFailFalse_usesSessionMetadata() throws Exception {
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setFastFailOnNotFoundEnabled(false).build();
    Storage mockStorage = mock(Storage.class);
    BlobReadSession mockSession = mock(BlobReadSession.class);
    BlobInfo mockBlobInfo = mock(BlobInfo.class);
    when(mockBlobInfo.getSize()).thenReturn(2048L);
    when(mockBlobInfo.getContentEncoding()).thenReturn("text/plain");
    when(mockBlobInfo.getGeneration()).thenReturn(1L);
    when(mockSession.getBlobInfo()).thenReturn(mockBlobInfo);
    when(mockStorage.blobReadSession(any(BlobId.class)))
        .thenReturn(ApiFutures.immediateFuture(mockSession));
    GoogleCloudStorageBidiReadChannel channel =
        new GoogleCloudStorageBidiReadChannel(
            mockStorage, RESOURCE_ID, null, readOptions, Executors.newSingleThreadExecutor());

    verify(mockStorage, times(0)).get(any(BlobId.class));
    verify(mockStorage, times(0)).get(any(BlobId.class), any(Storage.BlobGetOption.class));

    long size = channel.size();

    assertThat(size).isEqualTo(2048L);
    verify(mockSession, times(1)).getBlobInfo();
    verify(mockStorage, times(0)).get(any(BlobId.class));
    verify(mockStorage, times(0)).get(any(BlobId.class), any(Storage.BlobGetOption.class));
  }

  @Test
  public void lazyMetadataFetch_whenSessionFutureThrowsException_usesFallbackGet()
      throws Exception {
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setFastFailOnNotFoundEnabled(false).build();
    Storage mockStorage = mock(Storage.class);
    ApiFuture<BlobReadSession> mockSessionFuture =
        ApiFutures.immediateFailedFuture(
            new java.util.concurrent.ExecutionException(
                "Simulated Bidi Handshake Failure", new Exception()));
    when(mockStorage.blobReadSession(any(BlobId.class))).thenReturn(mockSessionFuture);
    Blob mockBlob = mock(Blob.class);
    when(mockBlob.getSize()).thenReturn(8192L);
    when(mockBlob.getContentEncoding()).thenReturn("text/plain");
    when(mockBlob.getGeneration()).thenReturn(1L);
    when(mockStorage.get(any(BlobId.class), any(Storage.BlobGetOption.class))).thenReturn(mockBlob);
    GoogleCloudStorageBidiReadChannel channel =
        new GoogleCloudStorageBidiReadChannel(
            mockStorage, RESOURCE_ID, null, readOptions, Executors.newSingleThreadExecutor());

    verify(mockStorage, times(0)).get(any(BlobId.class), any(Storage.BlobGetOption.class));

    // Trigger lazy initialization (this will catch the ExecutionException and trigger fallback)
    long size = channel.size();

    // Verify fallback worked
    assertThat(size).isEqualTo(8192L);

    // Verify storage.get() WAS called exactly once as the fallback mechanism
    verify(mockStorage, times(1)).get(any(BlobId.class), any(Storage.BlobGetOption.class));
  }

  @Test
  public void lazyMetadataFetch_whenSessionReturnsNull_usesFallbackGet() throws Exception {
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setFastFailOnNotFoundEnabled(false).build();
    Storage mockStorage = mock(Storage.class);
    BlobReadSession mockSession = mock(BlobReadSession.class);
    when(mockSession.getBlobInfo()).thenReturn(null);
    when(mockStorage.blobReadSession(any(BlobId.class)))
        .thenReturn(ApiFutures.immediateFuture(mockSession));
    Blob mockBlob = mock(Blob.class);
    when(mockBlob.getSize()).thenReturn(4096L);
    when(mockBlob.getContentEncoding()).thenReturn("text/plain");
    when(mockBlob.getGeneration()).thenReturn(1L);
    when(mockStorage.get(any(BlobId.class), any(Storage.BlobGetOption.class))).thenReturn(mockBlob);
    GoogleCloudStorageBidiReadChannel channel =
        new GoogleCloudStorageBidiReadChannel(
            mockStorage, RESOURCE_ID, null, readOptions, Executors.newSingleThreadExecutor());

    verify(mockStorage, times(0)).get(any(BlobId.class), any(Storage.BlobGetOption.class));

    // Trigger the lazy initialization
    long size = channel.size();

    assertThat(size).isEqualTo(4096L);
    verify(mockStorage, times(1)).get(any(BlobId.class), any(Storage.BlobGetOption.class));
  }

  @Test
  public void fetchMetadata_whenBlobIsNull_throwsFileNotFoundException() throws Exception {
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setFastFailOnNotFoundEnabled(false).build();
    Storage mockStorage = mock(Storage.class);
    BlobReadSession mockSession = mock(BlobReadSession.class);
    when(mockSession.getBlobInfo()).thenReturn(null);
    when(mockStorage.blobReadSession(any(BlobId.class)))
        .thenReturn(ApiFutures.immediateFuture(mockSession));
    when(mockStorage.get(any(BlobId.class), any(Storage.BlobGetOption.class))).thenReturn(null);

    GoogleCloudStorageBidiReadChannel channel =
        new GoogleCloudStorageBidiReadChannel(
            mockStorage, RESOURCE_ID, null, readOptions, Executors.newSingleThreadExecutor());

    FileNotFoundException e = assertThrows(FileNotFoundException.class, channel::size);
    assertThat(e).hasMessageThat().contains("Item not found: " + RESOURCE_ID);
  }

  @Test
  public void fetchMetadata_whenStorageException_throwsIOException() throws Exception {
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setFastFailOnNotFoundEnabled(false).build();
    Storage mockStorage = mock(Storage.class);
    BlobReadSession mockSession = mock(BlobReadSession.class);
    when(mockSession.getBlobInfo()).thenReturn(null);
    when(mockStorage.blobReadSession(any(BlobId.class)))
        .thenReturn(ApiFutures.immediateFuture(mockSession));
    when(mockStorage.get(any(BlobId.class), any(Storage.BlobGetOption.class)))
        .thenThrow(new StorageException(500, "Simulated Server Error"));

    GoogleCloudStorageBidiReadChannel channel =
        new GoogleCloudStorageBidiReadChannel(
            mockStorage, RESOURCE_ID, null, readOptions, Executors.newSingleThreadExecutor());

    IOException e = assertThrows(IOException.class, channel::size);
    assertThat(e).hasMessageThat().contains("Failed to fetch metadata for " + RESOURCE_ID);
    assertThat(e).hasCauseThat().isInstanceOf(StorageException.class);
  }

  @Test
  public void initMetadata_withoutGenerationId_updatesResourceId() throws Exception {
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setFastFailOnNotFoundEnabled(false).build();
    Storage mockStorage = mock(Storage.class);
    StorageResourceId noGenId = new StorageResourceId("test-bucket", "test-object");
    GoogleCloudStorageBidiReadChannel channel =
        new GoogleCloudStorageBidiReadChannel(
            mockStorage, noGenId, null, readOptions, Executors.newSingleThreadExecutor());

    channel.initMetadata("text/plain", 1024L, 999L);

    assertThat(channel.size()).isEqualTo(1024L);
  }

  @Test
  public void initMetadata_withMismatchedGenerationId_throwsIllegalStateException()
      throws Exception {
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setFastFailOnNotFoundEnabled(false).build();
    Storage mockStorage = mock(Storage.class);
    StorageResourceId withGenId = new StorageResourceId("test-bucket", "test-object", 123L);

    GoogleCloudStorageBidiReadChannel channel =
        new GoogleCloudStorageBidiReadChannel(
            mockStorage, withGenId, null, readOptions, Executors.newSingleThreadExecutor());

    IllegalStateException e =
        assertThrows(
            IllegalStateException.class,
            () -> {
              channel.initMetadata("text/plain", 1024L, 999L);
            });

    assertThat(e).hasMessageThat().contains("should be equal to fetched generation");
  }

  @Test
  public void lazyMetadataFetch_triggeredByRead_usesSessionMetadata() throws Exception {
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setFastFailOnNotFoundEnabled(false).build();
    Storage mockStorage = mock(Storage.class);
    BlobReadSession mockSession = mock(BlobReadSession.class);
    BlobInfo mockBlobInfo = mock(BlobInfo.class);
    when(mockBlobInfo.getSize()).thenReturn(5L);
    when(mockBlobInfo.getContentEncoding()).thenReturn("text/plain");
    when(mockBlobInfo.getGeneration()).thenReturn(1L);
    when(mockSession.getBlobInfo()).thenReturn(mockBlobInfo);
    DisposableByteString mockDisposable = mock(DisposableByteString.class);
    ByteString mockByteString = ByteString.copyFromUtf8("hello");
    when(mockDisposable.byteString()).thenReturn(mockByteString);
    ApiFuture<DisposableByteString> mockReadFuture = ApiFutures.immediateFuture(mockDisposable);
    when(mockSession.readAs(any())).thenReturn(mockReadFuture);
    when(mockStorage.blobReadSession(any(BlobId.class)))
        .thenReturn(ApiFutures.immediateFuture(mockSession));
    GoogleCloudStorageBidiReadChannel channel =
        new GoogleCloudStorageBidiReadChannel(
            mockStorage, RESOURCE_ID, null, readOptions, Executors.newSingleThreadExecutor());

    ByteBuffer buffer = ByteBuffer.allocate(5);
    int bytesRead = channel.read(buffer);

    assertThat(bytesRead).isEqualTo(5);
    verify(mockSession, times(1)).getBlobInfo();
    verify(mockSession, times(1)).readAs(any());
    verify(mockStorage, times(0)).get(any(BlobId.class));
    verify(mockStorage, times(0)).get(any(BlobId.class), any(Storage.BlobGetOption.class));
  }

  @Test
  public void lazyMetadataFetch_triggeredByReadVectored_usesSessionMetadata() throws Exception {
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setFastFailOnNotFoundEnabled(false).build();
    Storage mockStorage = mock(Storage.class);
    BlobReadSession mockSession = mock(BlobReadSession.class);
    BlobInfo mockBlobInfo = mock(BlobInfo.class);
    when(mockBlobInfo.getSize()).thenReturn(2048L);
    when(mockBlobInfo.getContentEncoding()).thenReturn("text/plain");
    when(mockBlobInfo.getGeneration()).thenReturn(1L);
    when(mockSession.getBlobInfo()).thenReturn(mockBlobInfo);
    when(mockStorage.blobReadSession(any(BlobId.class)))
        .thenReturn(ApiFutures.immediateFuture(mockSession));
    GoogleCloudStorageBidiReadChannel channel =
        new GoogleCloudStorageBidiReadChannel(
            mockStorage, RESOURCE_ID, null, readOptions, Executors.newSingleThreadExecutor());

    channel.readVectored(ImmutableList.of(), ByteBuffer::allocate);

    verify(mockSession, times(1)).getBlobInfo();
    verify(mockStorage, times(0)).get(any(BlobId.class));
    verify(mockStorage, times(0)).get(any(BlobId.class), any(Storage.BlobGetOption.class));
  }

  @Test
  public void ensureMetadataInitialized_onInterruptedException_restoresInterruptFlag()
      throws Exception {
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setFastFailOnNotFoundEnabled(false).build();
    Storage mockStorage = mock(Storage.class);
    ApiFuture<BlobReadSession> mockFuture = mock(ApiFuture.class);
    when(mockFuture.get(anyLong(), any(java.util.concurrent.TimeUnit.class)))
        .thenThrow(new InterruptedException("Simulated Interrupt"));
    when(mockStorage.blobReadSession(any(BlobId.class))).thenReturn(mockFuture);
    GoogleCloudStorageBidiReadChannel channel =
        new GoogleCloudStorageBidiReadChannel(
            mockStorage, RESOURCE_ID, null, readOptions, Executors.newSingleThreadExecutor());

    IOException thrown = assertThrows(IOException.class, channel::size);
    assertThat(thrown).hasMessageThat().contains("Thread interrupt received.");
    assertThat(thrown).hasCauseThat().isInstanceOf(InterruptedException.class);
    assertThat(Thread.currentThread().isInterrupted()).isTrue();

    // Clear the interrupt flag so it doesn't affect other tests!
    Thread.interrupted();
  }

  @Test
  public void eagerMetadataFetch_whenFastFailTrue_callsGetImmediately() throws Exception {
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setFastFailOnNotFoundEnabled(true).build();
    Storage mockStorage = mock(Storage.class);
    Blob mockBlob = mock(Blob.class);
    when(mockBlob.getSize()).thenReturn(8192L);
    when(mockBlob.getContentEncoding()).thenReturn("text/plain");
    when(mockBlob.getGeneration()).thenReturn(1L);
    when(mockStorage.get(any(BlobId.class), any(Storage.BlobGetOption.class))).thenReturn(mockBlob);
    BlobReadSession mockSession = mock(BlobReadSession.class);
    when(mockStorage.blobReadSession(any(BlobId.class)))
        .thenReturn(ApiFutures.immediateFuture(mockSession));
    GoogleCloudStorageBidiReadChannel channel =
        new GoogleCloudStorageBidiReadChannel(
            mockStorage, RESOURCE_ID, null, readOptions, Executors.newSingleThreadExecutor());

    verify(mockStorage, times(1)).get(any(BlobId.class), any(Storage.BlobGetOption.class));

    long size = channel.size();

    assertThat(size).isEqualTo(8192L);
    verify(mockStorage, times(1)).get(any(BlobId.class), any(Storage.BlobGetOption.class));
  }

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
  public void performStandardRead_successful() throws IOException {
    GoogleCloudStorageBidiReadChannel bidiReadChannel = getMockedBidiReadChannel();
    ByteBuffer buffer = ByteBuffer.allocate(10);

    int bytesRead = bidiReadChannel.performStandardRead(buffer);
    assertEquals(10, bytesRead);
    assertEquals(10, bidiReadChannel.position());
    buffer.flip();
    assertEquals(
        FakeBlobReadSession.SUBSTRING_0_10, StandardCharsets.UTF_8.decode(buffer).toString());

    // Read second chunk
    buffer.clear();

    bytesRead = bidiReadChannel.performStandardRead(buffer);
    assertEquals(10, bytesRead);
    assertEquals(20, bidiReadChannel.position());
    buffer.flip();
    assertEquals(
        FakeBlobReadSession.SUBSTRING_10_10, StandardCharsets.UTF_8.decode(buffer).toString());
  }

  @Test
  public void performStandardRead_partialReadAtEnd() throws IOException {
    GoogleCloudStorageBidiReadChannel bidiReadChannel = getMockedBidiReadChannel();
    bidiReadChannel.position(OBJECT_SIZE - 5);
    ByteBuffer buffer = ByteBuffer.allocate(10);

    int bytesRead = bidiReadChannel.performStandardRead(buffer);

    assertEquals(5, bytesRead);
    assertEquals(OBJECT_SIZE, bidiReadChannel.position());
    buffer.flip();
    String expectedContent = FakeBlobReadSession.TEST_STRING.substring(OBJECT_SIZE - 5);
    assertEquals(expectedContent, StandardCharsets.UTF_8.decode(buffer).toString());
  }

  @Test
  public void performStandardRead_whenReadZeroBytes_throwsIOException() throws IOException {
    Storage storage = mock(Storage.class);
    BlobReadSession fakeSession =
        new FakeBlobReadSession(FakeBlobReadSession.Behavior.READ_ZERO_BYTES);
    when(storage.blobReadSession(any(), any(BlobSourceOption.class)))
        .thenReturn(ApiFutures.immediateFuture(fakeSession));

    GoogleCloudStorageBidiReadChannel bidiReadChannel =
        new GoogleCloudStorageBidiReadChannel(
            storage,
            RESOURCE_ID,
            DEFAULT_ITEM_INFO,
            GoogleCloudStorageReadOptions.builder().build(),
            Executors.newSingleThreadExecutor());

    IOException e =
        assertThrows(
            IOException.class, () -> bidiReadChannel.performStandardRead(ByteBuffer.allocate(10)));
    assertThat(e).hasMessageThat().contains("Read 0 bytes without blocking");
  }

  @Test
  public void performStandardRead_whenFutureFails_throwsIOException() throws IOException {
    Storage storage = mock(Storage.class);
    BlobReadSession fakeSession = new FakeBlobReadSession(FakeBlobReadSession.Behavior.FAIL_FUTURE);
    when(storage.blobReadSession(any(), any(BlobSourceOption.class)))
        .thenReturn(ApiFutures.immediateFuture(fakeSession));

    GoogleCloudStorageBidiReadChannel bidiReadChannel =
        new GoogleCloudStorageBidiReadChannel(
            storage,
            RESOURCE_ID,
            DEFAULT_ITEM_INFO,
            GoogleCloudStorageReadOptions.builder().build(),
            Executors.newSingleThreadExecutor());

    IOException e =
        assertThrows(
            IOException.class, () -> bidiReadChannel.performStandardRead(ByteBuffer.allocate(10)));
    assertThat(e).hasMessageThat().startsWith("Read failed on");
    assertThat(e).hasCauseThat().isInstanceOf(ExecutionException.class);
    assertThat(e).hasCauseThat().hasCauseThat().isInstanceOf(StorageException.class);
    assertThat(e).hasCauseThat().hasCauseThat().hasMessageThat().isEqualTo("Not Found");
  }

  @Test
  public void performStandardRead_whenFutureTimesOut_throwsIOException() throws IOException {
    Storage storage = mock(Storage.class);
    BlobReadSession fakeSession =
        new FakeBlobReadSession(FakeBlobReadSession.Behavior.TIMEOUT_FUTURE);
    when(storage.blobReadSession(any(), any(BlobSourceOption.class)))
        .thenReturn(ApiFutures.immediateFuture(fakeSession));

    GoogleCloudStorageBidiReadChannel bidiReadChannel =
        new GoogleCloudStorageBidiReadChannel(
            storage,
            RESOURCE_ID,
            DEFAULT_ITEM_INFO,
            GoogleCloudStorageReadOptions.builder()
                .setGrpcReadTimeout(java.time.Duration.ofNanos(1))
                .build(),
            Executors.newSingleThreadExecutor());

    IOException e =
        assertThrows(
            IOException.class, () -> bidiReadChannel.performStandardRead(ByteBuffer.allocate(10)));
    assertThat(e).hasMessageThat().startsWith("Read failed on");
    assertThat(e).hasCauseThat().isInstanceOf(TimeoutException.class);
  }

  @Test
  public void read_whenPositionIsAtSize_returnsNegativeOne() throws IOException {
    GoogleCloudStorageBidiReadChannel bidiReadChannel = getMockedBidiReadChannel();
    bidiReadChannel.position(OBJECT_SIZE - 1);

    // Read the last byte to advance position to OBJECT_SIZE
    bidiReadChannel.read(ByteBuffer.allocate(1));
    assertEquals(OBJECT_SIZE, bidiReadChannel.position());

    // Next read should return -1
    assertEquals(-1, bidiReadChannel.read(ByteBuffer.allocate(10)));
  }

  @Test
  public void read_whenDstIsEmpty_returnsZero() throws IOException {
    GoogleCloudStorageBidiReadChannel bidiReadChannel = getMockedBidiReadChannel();

    ByteBuffer buffer = ByteBuffer.allocate(0);

    assertEquals(0, bidiReadChannel.read(buffer));
    assertEquals(0, bidiReadChannel.position());
  }

  @Test
  public void read_onClosedChannel_throwsClosedChannelException() throws IOException {
    GoogleCloudStorageBidiReadChannel bidiReadChannel = getMockedBidiReadChannel();

    bidiReadChannel.close();

    assertThrows(ClosedChannelException.class, () -> bidiReadChannel.read(ByteBuffer.allocate(1)));
  }

  @Test
  public void position_getAndSet() throws IOException {
    GoogleCloudStorageBidiReadChannel bidiReadChannel = getMockedBidiReadChannel();
    assertEquals(0, bidiReadChannel.position());

    bidiReadChannel.position(12);
    assertEquals(12, bidiReadChannel.position());
  }

  @Test
  public void position_setToNegative_throwsEofException() throws IOException {
    GoogleCloudStorageBidiReadChannel bidiReadChannel = getMockedBidiReadChannel();
    assertThrows(EOFException.class, () -> bidiReadChannel.position(-1));
  }

  @Test
  public void position_setToBeyondSize_throwsEofException() throws IOException {
    GoogleCloudStorageBidiReadChannel bidiReadChannel = getMockedBidiReadChannel();
    assertThrows(EOFException.class, () -> bidiReadChannel.position(OBJECT_SIZE + 1));
  }

  @Test
  public void position_setToSize_throwsEofException() throws IOException {
    GoogleCloudStorageBidiReadChannel bidiReadChannel = getMockedBidiReadChannel();
    assertThrows(EOFException.class, () -> bidiReadChannel.position(OBJECT_SIZE));
  }

  @Test
  public void position_onClosedChannel_throwsClosedChannelException() throws IOException {
    GoogleCloudStorageBidiReadChannel bidiReadChannel = getMockedBidiReadChannel();

    bidiReadChannel.close();

    assertThrows(ClosedChannelException.class, () -> bidiReadChannel.position(10));
  }

  @Test
  public void size_successful() throws IOException {
    GoogleCloudStorageBidiReadChannel bidiReadChannel = getMockedBidiReadChannel();

    assertEquals(OBJECT_SIZE, bidiReadChannel.size());
  }

  @Test
  public void isOpen_isTrueOnNewChannel() throws IOException {
    GoogleCloudStorageBidiReadChannel bidiReadChannel = getMockedBidiReadChannel();

    assertTrue(bidiReadChannel.isOpen());
  }

  @Test
  public void isOpen_isFalseAfterClose() throws IOException {
    GoogleCloudStorageBidiReadChannel bidiReadChannel = getMockedBidiReadChannel();

    bidiReadChannel.close();

    assertFalse(bidiReadChannel.isOpen());
  }

  @Test
  public void write_unsupportedOperationException() throws IOException {
    GoogleCloudStorageBidiReadChannel bidiReadChannel = getMockedBidiReadChannel();

    assertThrows(
        UnsupportedOperationException.class,
        () -> bidiReadChannel.write(ByteBuffer.allocateDirect(0)));
  }

  @Test
  public void truncate_unsupportedOperationException() throws IOException {
    GoogleCloudStorageBidiReadChannel bidiReadChannel = getMockedBidiReadChannel();

    assertThrows(UnsupportedOperationException.class, () -> bidiReadChannel.truncate(0));
  }

  @Test
  public void initMetadata_whenGzipNotSupported_throwsUnsupportedOperationException()
      throws Exception {
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder()
            .setFastFailOnNotFoundEnabled(false)
            .setGzipEncodingSupportEnabled(false)
            .build();

    GoogleCloudStorageBidiReadChannel bidiReadChannel = getMockedBidiReadChannel(null, readOptions);

    UnsupportedOperationException thrown =
        assertThrows(
            UnsupportedOperationException.class,
            () -> bidiReadChannel.initMetadata("gzip", 10L, 1L));

    assertThat(thrown).hasMessageThat().contains("Gzip Encoded Files are not supported");
  }

  @Test
  public void read_succeedsAfterIoException() throws IOException {
    int bytesToRead = 10;
    ByteBuffer buffer = ByteBuffer.allocate(bytesToRead);

    Storage storage = mock(Storage.class);
    FakeBlobReadSession fakeSession =
        spy(
            new FakeBlobReadSession(
                ImmutableList.of(
                    FakeBlobReadSession.Behavior.IO_EXCEPTION,
                    FakeBlobReadSession.Behavior.DEFAULT),
                null));
    when(storage.blobReadSession(any(), any(BlobSourceOption.class)))
        .thenReturn(ApiFutures.immediateFuture(fakeSession));

    GoogleCloudStorageBidiReadChannel bidiReadChannel =
        new GoogleCloudStorageBidiReadChannel(
            storage,
            RESOURCE_ID,
            DEFAULT_ITEM_INFO,
            GoogleCloudStorageReadOptions.builder().build(),
            Executors.newSingleThreadExecutor());

    IOException e = assertThrows(IOException.class, () -> bidiReadChannel.read(buffer));
    assertThat(e).hasCauseThat().isInstanceOf(ExecutionException.class);
    assertThat(buffer.position()).isEqualTo(0);
    assertThat(bidiReadChannel.position()).isEqualTo(0);

    int bytesRead = bidiReadChannel.read(buffer);

    assertThat(bytesRead).isEqualTo(bytesToRead);
    assertThat(buffer.position()).isEqualTo(bytesToRead);
    assertThat(bidiReadChannel.position()).isEqualTo(bytesToRead);

    buffer.flip();
    String content = StandardCharsets.UTF_8.decode(buffer).toString();
    assertThat(content).isEqualTo(FakeBlobReadSession.SUBSTRING_0_10);

    verify(fakeSession, times(2)).readAs(any());
    verifyNoMoreInteractions(fakeSession);
  }

  @Test
  public void read_whenReadExactRequestedBytesEnabled_skipsFooterCache() throws Exception {
    int footerSize = 20;
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder()
            .setMinRangeRequestSize(footerSize)
            .setReadExactRequestedBytesEnabled(true)
            .build();

    FakeBlobReadSession fakeSession = spy(new FakeBlobReadSession());
    Storage storage = mock(Storage.class);
    when(storage.blobReadSession(any())).thenReturn(ApiFutures.immediateFuture(fakeSession));

    GoogleCloudStorageBidiReadChannel channel =
        new GoogleCloudStorageBidiReadChannel(
            storage,
            RESOURCE_ID,
            DEFAULT_ITEM_INFO,
            readOptions,
            Executors.newSingleThreadExecutor());

    // Verify footer is not initially cached
    assertNull("Footer should be null initially", getPrivateField(channel, "footerContent"));

    // Position the read within the footer range
    long readPosition = OBJECT_SIZE - 10; // Inside 20-byte footer range
    channel.position(readPosition);

    ByteBuffer buffer = ByteBuffer.allocate(5);
    int bytesRead = channel.read(buffer);

    assertEquals(5, bytesRead);

    // Verify footer is STILL null, because ReadExact disabled the optimization
    assertNull("Footer should remain null", getPrivateField(channel, "footerContent"));
  }

  @Test
  public void cacheFooter_whenFooterSizeIsZero_doesNothing() throws Exception {
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setMinRangeRequestSize(0).build();
    GoogleCloudStorageBidiReadChannel channel = getMockedBidiReadChannel(readOptions);

    channel.cacheFooter();

    byte[] footerContent = (byte[]) getPrivateField(channel, "footerContent");
    assertNull(footerContent);
  }

  @Test
  public void read_whenFooterPrefetchFails_throwsIOException() throws IOException {
    Storage storage = mock(Storage.class);
    BlobReadSession fakeSession = new FakeBlobReadSession(FakeBlobReadSession.Behavior.FAIL_FUTURE);
    when(storage.blobReadSession(any(), any(BlobSourceOption.class)))
        .thenReturn(ApiFutures.immediateFuture(fakeSession));

    GoogleCloudStorageBidiReadChannel channel =
        new GoogleCloudStorageBidiReadChannel(
            storage,
            RESOURCE_ID,
            DEFAULT_ITEM_INFO,
            GoogleCloudStorageReadOptions.builder().setMinRangeRequestSize(10).build(),
            Executors.newSingleThreadExecutor());

    channel.position(OBJECT_SIZE - 5);

    IOException e = assertThrows(IOException.class, () -> channel.read(ByteBuffer.allocate(5)));
    assertThat(e).hasMessageThat().startsWith("Footer prefetch failed on");
    assertThat(e).hasCauseThat().isInstanceOf(ExecutionException.class);
  }

  @Test
  public void cacheFooter_whenFutureFails_throwsIOException() throws Exception {
    Storage storage = mock(Storage.class);
    BlobReadSession fakeSession = new FakeBlobReadSession(FakeBlobReadSession.Behavior.FAIL_FUTURE);
    when(storage.blobReadSession(any(), any(BlobSourceOption.class)))
        .thenReturn(ApiFutures.immediateFuture(fakeSession));

    GoogleCloudStorageBidiReadChannel channel =
        new GoogleCloudStorageBidiReadChannel(
            storage,
            RESOURCE_ID,
            DEFAULT_ITEM_INFO,
            GoogleCloudStorageReadOptions.builder().setMinRangeRequestSize(10).build(),
            Executors.newSingleThreadExecutor());

    ExecutionException e = assertThrows(ExecutionException.class, () -> channel.cacheFooter());
    assertThat(e).hasCauseThat().isInstanceOf(StorageException.class);
  }

  @Test
  public void cacheFooter_whenIncompleteRead_throwsIOException() throws Exception {
    Storage storage = mock(Storage.class);
    BlobReadSession fakeSession =
        new FakeBlobReadSession(FakeBlobReadSession.Behavior.READ_PARTIAL_BYTES);
    when(storage.blobReadSession(any(), any(BlobSourceOption.class)))
        .thenReturn(ApiFutures.immediateFuture(fakeSession));

    GoogleCloudStorageBidiReadChannel channel =
        new GoogleCloudStorageBidiReadChannel(
            storage,
            RESOURCE_ID,
            DEFAULT_ITEM_INFO,
            GoogleCloudStorageReadOptions.builder().setMinRangeRequestSize(10).build(),
            Executors.newSingleThreadExecutor());

    IOException e = assertThrows(IOException.class, () -> channel.cacheFooter());
    assertThat(e).hasMessageThat().startsWith("Failed to read complete footer");
  }

  @Test
  public void isFooterRead_returnsCorrectBoolean() throws Exception {
    int minRangeRequestSize = 16;
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setMinRangeRequestSize(minRangeRequestSize).build();
    GoogleCloudStorageBidiReadChannel channel = getMockedBidiReadChannel(readOptions);

    long footerStartPosition = OBJECT_SIZE - minRangeRequestSize;

    // Test position before the footer
    channel.position(footerStartPosition - 1);
    assertFalse(channel.isFooterRead());

    // Test position at the start of the footer
    channel.position(footerStartPosition);
    assertTrue(channel.isFooterRead());

    // Test position inside the footer
    channel.position(OBJECT_SIZE - 1);
    assertTrue(channel.isFooterRead());
  }

  @Test
  public void cacheFooter_populatesFooterContentCorrectly() throws Exception {
    int minRangeRequestSize = 16;
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setMinRangeRequestSize(minRangeRequestSize).build();
    GoogleCloudStorageBidiReadChannel channel = getMockedBidiReadChannel(readOptions);

    channel.cacheFooter();

    byte[] footerContent = (byte[]) getPrivateField(channel, "footerContent");

    String expectedFooter =
        FakeBlobReadSession.TEST_STRING.substring(OBJECT_SIZE - minRangeRequestSize);
    byte[] expectedFooterBytes = expectedFooter.getBytes(StandardCharsets.UTF_8);

    assertNotNull(footerContent);
    assertArrayEquals(expectedFooterBytes, footerContent);
  }

  @Test
  public void readFromCache_readsCorrectDataAndUpdatesPosition() throws Exception {
    GoogleCloudStorageBidiReadChannel channel = getMockedBidiReadChannel();

    int footerSize = 16;
    long footerStartPosition = OBJECT_SIZE - footerSize;
    String footerString = FakeBlobReadSession.TEST_STRING.substring((int) footerStartPosition);
    byte[] footerBytes = footerString.getBytes(StandardCharsets.UTF_8);

    setPrivateField(channel, "footerContent", footerBytes);
    setPrivateField(channel, "objectSize", (long) OBJECT_SIZE);

    long readPosition = footerStartPosition + 4; // Read from 4 bytes into the footer
    channel.position(readPosition);

    ByteBuffer buffer = ByteBuffer.allocate(10);
    int bytesRead = channel.readFromCache(buffer);

    // We expect to read min(buffer.remaining=10, cache.remaining=12) = 10 bytes
    assertEquals(10, bytesRead);

    assertEquals(readPosition + bytesRead, channel.position());

    buffer.flip();
    String bufferContent = StandardCharsets.UTF_8.decode(buffer).toString();
    String expectedContent = footerString.substring(4, 4 + 10);
    assertEquals(expectedContent, bufferContent);
  }

  @Test
  public void read_triggersFooterCaching() throws Exception {
    int minRangeRequestSize = 20;
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setMinRangeRequestSize(minRangeRequestSize).build();
    GoogleCloudStorageBidiReadChannel channel = getMockedBidiReadChannel(readOptions);

    // Verify footer is not initially cached
    assertNull("Footer should be null initially", getPrivateField(channel, "footerContent"));

    // Position the read within the footer range
    long readPosition = OBJECT_SIZE - 10;
    channel.position(readPosition);

    ByteBuffer buffer = ByteBuffer.allocate(5);
    int bytesRead = channel.read(buffer);

    // Assert that 5 bytes were read and position was updated
    assertEquals(5, bytesRead);
    assertEquals(readPosition + 5, channel.position());

    // Verify footer has now been cached
    byte[] footerContent = (byte[]) getPrivateField(channel, "footerContent");
    assertNotNull(footerContent);
    assertEquals(minRangeRequestSize, footerContent.length);

    // Verify the data read into the buffer is correct
    buffer.flip();
    String actualContent = StandardCharsets.UTF_8.decode(buffer).toString();
    String expectedContent =
        FakeBlobReadSession.TEST_STRING.substring((int) readPosition, (int) readPosition + 5);
    assertEquals(expectedContent, actualContent);
  }

  @Test
  public void read_usesPrePopulatedFooterCache() throws Exception {
    int footerSize = 20;
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setMinRangeRequestSize(footerSize).build();
    GoogleCloudStorageBidiReadChannel channel = getMockedBidiReadChannel(readOptions);

    // Manually populate the footer cache to simulate it being pre-cached
    long footerStartPosition = OBJECT_SIZE - footerSize;
    String footerString = FakeBlobReadSession.TEST_STRING.substring((int) footerStartPosition);
    setPrivateField(channel, "footerContent", footerString.getBytes(StandardCharsets.UTF_8));

    // Update position within the pre-cached footer range
    long readPosition = OBJECT_SIZE - 10;
    channel.position(readPosition);

    ByteBuffer buffer = ByteBuffer.allocate(8);
    int bytesRead = channel.read(buffer);

    assertEquals(8, bytesRead);
    assertEquals(readPosition + 8, channel.position());

    // Verify the data read into the buffer is correct
    buffer.flip();
    String actualContent = StandardCharsets.UTF_8.decode(buffer).toString();
    String expectedContent =
        FakeBlobReadSession.TEST_STRING.substring((int) readPosition, (int) readPosition + 8);
    assertEquals(expectedContent, actualContent);
  }

  @Test
  public void isBufferValid_variousScenarios() throws Exception {
    int bufferSize = 20;
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setMinRangeRequestSize(bufferSize).build();
    GoogleCloudStorageBidiReadChannel channel = getMockedBidiReadChannel(readOptions);
    long bufferStart = 10;
    ByteBuffer buffer = ByteBuffer.allocate(bufferSize);

    // 1. Buffer is null - invalid
    assertFalse(channel.isBufferValid());

    // 2. Buffer is set, but position is before buffer - invalid
    setPrivateField(channel, "internalBuffer", buffer);
    setPrivateField(channel, "bufferStartPosition", bufferStart);
    setPrivateField(channel, "position", bufferStart - 1);
    assertFalse(channel.isBufferValid());

    // 3. Position at start of buffer - valid
    setPrivateField(channel, "position", bufferStart);
    assertTrue(channel.isBufferValid());

    // 4. Position inside buffer - valid
    setPrivateField(channel, "position", bufferStart + bufferSize / 2);

    assertTrue(channel.isBufferValid());

    // 5. Position at end of buffer (exclusive) - invalid
    setPrivateField(channel, "position", bufferStart + bufferSize);
    assertFalse(channel.isBufferValid());
  }

  @Test
  public void invalidateBuffer_resetsState() throws Exception {
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setMinRangeRequestSize(10).build();
    GoogleCloudStorageBidiReadChannel channel = getMockedBidiReadChannel(readOptions);

    // 1. Read from the channel to populate the internal buffer.
    channel.position(5L);
    channel.read(ByteBuffer.allocate(1));

    // 2. Seek to a position outside the current buffer's range.
    // This will trigger invalidateBuffer() internally.
    channel.position(20L);

    assertNull(getPrivateField(channel, "internalBuffer"));
    assertEquals(-1L, getPrivateField(channel, "bufferStartPosition"));
  }

  @Test
  public void refillInternalBuffer_populatesBufferCorrectly() throws Exception {
    int minRangeRequestSize = 20;
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setMinRangeRequestSize(minRangeRequestSize).build();
    GoogleCloudStorageBidiReadChannel channel = getMockedBidiReadChannel(readOptions);

    long readPosition = 15;
    channel.position(readPosition);

    channel.refillInternalBuffer();

    ByteBuffer internalBuffer = (ByteBuffer) getPrivateField(channel, "internalBuffer");
    long bufferStartPosition = (long) getPrivateField(channel, "bufferStartPosition");

    assertNotNull(internalBuffer);
    assertEquals(readPosition, bufferStartPosition);
    assertEquals(minRangeRequestSize, internalBuffer.remaining());

    byte[] bufferContent = new byte[minRangeRequestSize];
    internalBuffer.get(bufferContent);
    String actualContent = new String(bufferContent, StandardCharsets.UTF_8);
    String expectedContent =
        FakeBlobReadSession.TEST_STRING.substring(
            (int) readPosition, (int) readPosition + minRangeRequestSize);
    assertEquals(expectedContent, actualContent);
  }

  @Test
  public void refillInternalBuffer_nearEndOfFile_requestsFewerBytes() throws Exception {
    int minRangeRequestSize = 20;
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setMinRangeRequestSize(minRangeRequestSize).build();
    GoogleCloudStorageBidiReadChannel channel = getMockedBidiReadChannel(readOptions);

    long readPosition = OBJECT_SIZE - 10;
    channel.position(readPosition);

    channel.refillInternalBuffer();

    ByteBuffer internalBuffer = (ByteBuffer) getPrivateField(channel, "internalBuffer");
    long bufferStartPosition = (long) getPrivateField(channel, "bufferStartPosition");

    assertNotNull(internalBuffer);
    assertEquals(readPosition, bufferStartPosition);
    assertEquals(10, internalBuffer.remaining());

    byte[] bufferContent = new byte[10];
    internalBuffer.get(bufferContent);
    String actualContent = new String(bufferContent, StandardCharsets.UTF_8);
    String expectedContent = FakeBlobReadSession.TEST_STRING.substring((int) readPosition);
    assertEquals(expectedContent, actualContent);
  }

  @Test
  public void readBytesFromInternalBuffer_readsCorrectly() throws Exception {
    GoogleCloudStorageBidiReadChannel channel = getMockedBidiReadChannel();
    String bufferedString = "0123456789abcdefghij"; // 20 bytes
    ByteBuffer internalBuffer = ByteBuffer.wrap(bufferedString.getBytes(StandardCharsets.UTF_8));
    long bufferStartPos = 10L;

    setPrivateField(channel, "internalBuffer", internalBuffer);
    setPrivateField(channel, "bufferStartPosition", bufferStartPos);

    // Set channel position, and sync internal buffer position accordingly.
    long channelReadPos = 15L;
    channel.position(channelReadPos);
    internalBuffer.position((int) (channelReadPos - bufferStartPos));

    ByteBuffer destBuffer = ByteBuffer.allocate(8);
    int bytesRead = channel.readBytesFromInternalBuffer(destBuffer);

    assertEquals(8, bytesRead);
    assertEquals(channelReadPos + bytesRead, channel.position()); // new position 15+8=23
    assertEquals(5 + 8, internalBuffer.position()); // internal buffer new position 5+8=13

    destBuffer.flip();
    String readContent = StandardCharsets.UTF_8.decode(destBuffer).toString();
    // String starts at index 5 which is '5'. Reading 8 chars.
    assertEquals("56789abc", readContent);
  }

  @Test
  public void readBytesFromInternalBuffer_readsTillBufferEnd() throws Exception {
    GoogleCloudStorageBidiReadChannel channel = getMockedBidiReadChannel();
    String bufferedString = "0123456789"; // 10 bytes
    ByteBuffer internalBuffer = ByteBuffer.wrap(bufferedString.getBytes(StandardCharsets.UTF_8));
    long bufferStartPos = 0L;

    setPrivateField(channel, "internalBuffer", internalBuffer);
    setPrivateField(channel, "bufferStartPosition", bufferStartPos);

    long channelReadPos = 5L;
    channel.position(channelReadPos);
    internalBuffer.position((int) (channelReadPos - bufferStartPos)); // position is 5

    ByteBuffer destBuffer = ByteBuffer.allocate(20); // Dst is larger than remaining buffer
    int bytesRead = channel.readBytesFromInternalBuffer(destBuffer);

    assertEquals(5, bytesRead); // Should only read the remaining 5 bytes
    assertEquals(10L, channel.position());
    assertEquals(10, internalBuffer.position());

    destBuffer.flip();
    String readContent = StandardCharsets.UTF_8.decode(destBuffer).toString();
    assertEquals("56789", readContent);
  }

  @Test
  public void refillInternalBuffer_whenFutureFails_throwsIOException() throws IOException {
    Storage storage = mock(Storage.class);
    BlobReadSession fakeSession = new FakeBlobReadSession(FakeBlobReadSession.Behavior.FAIL_FUTURE);
    when(storage.blobReadSession(any())).thenReturn(ApiFutures.immediateFuture(fakeSession));

    GoogleCloudStorageBidiReadChannel channel =
        new GoogleCloudStorageBidiReadChannel(
            storage,
            RESOURCE_ID,
            DEFAULT_ITEM_INFO,
            GoogleCloudStorageReadOptions.builder().setMinRangeRequestSize(10).build(),
            Executors.newSingleThreadExecutor());

    IOException e = assertThrows(IOException.class, () -> channel.read(ByteBuffer.allocate(1)));

    assertThat(e).hasMessageThat().startsWith("Look ahead read failed on");
    assertThat(e).hasCauseThat().isInstanceOf(ExecutionException.class);
  }

  private String getReadVectoredData(VectoredIORange range)
      throws ExecutionException, InterruptedException, TimeoutException {
    Charset charset = StandardCharsets.UTF_8;
    return charset
        .decode(Objects.requireNonNull(range.getData()).get(3, TimeUnit.SECONDS))
        .toString();
  }

  private GoogleCloudStorageBidiReadChannel getMockedBidiReadChannel(
      GoogleCloudStorageItemInfo itemInfo, GoogleCloudStorageReadOptions readOptions)
      throws IOException {
    Storage storage = mock(Storage.class);
    when(storage.blobReadSession(any(), ArgumentMatchers.any(BlobSourceOption.class)))
        .thenReturn(ApiFutures.immediateFuture(new FakeBlobReadSession()));
    return new GoogleCloudStorageBidiReadChannel(
        storage, RESOURCE_ID, itemInfo, readOptions, Executors.newSingleThreadExecutor());
  }

  private GoogleCloudStorageBidiReadChannel getMockedBidiReadChannel(
      GoogleCloudStorageReadOptions readOptions) throws IOException {
    return getMockedBidiReadChannel(DEFAULT_ITEM_INFO, readOptions);
  }

  private GoogleCloudStorageBidiReadChannel getMockedBidiReadChannel() throws IOException {
    return getMockedBidiReadChannel(DEFAULT_ITEM_INFO, GoogleCloudStorageReadOptions.DEFAULT);
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
  }

  private GoogleCloudStorageBidiReadChannel getBidiReadChannel(
      GoogleCloudStorageReadOptions readOptions) throws IOException {
    Storage storage = mock(Storage.class);
    when(storage.blobReadSession(any()))
        .thenReturn(ApiFutures.immediateFuture(new FakeBlobReadSession()));
    return new GoogleCloudStorageBidiReadChannel(
        storage, RESOURCE_ID, DEFAULT_ITEM_INFO, readOptions, Executors.newSingleThreadExecutor());
  }

  private Object getPrivateField(Object obj, String fieldName) throws Exception {
    Field field = obj.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    return field.get(obj);
  }

  private void setPrivateField(Object obj, String fieldName, Object value) throws Exception {
    Field field = obj.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(obj, value);
  }
}
