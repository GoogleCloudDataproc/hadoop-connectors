package com.google.cloud.hadoop.gcsio;

import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.mockTransport;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.core.ApiFutures;
import com.google.cloud.storage.BlobReadSession;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobSourceOption;
import com.google.cloud.storage.StorageException;
import java.io.EOFException;
import java.io.IOException;
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

public class GoogleCloudStorageBidiReadChannelTest {

  private static final String TEST_BUCKET_NAME = "foo-bucket";

  private static final String TEST_OBJECT_NAME = "foo-object";

  private static final int OBJECT_SIZE = FakeBlobReadSession.TEST_STRING.length();
  private static final StorageResourceId RESOURCE_ID =
      new StorageResourceId(TEST_BUCKET_NAME, TEST_OBJECT_NAME);
  private static MockHttpTransport transport = mockTransport();

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
  public void readVectored_successfulRead()
      throws IOException, ExecutionException, InterruptedException, TimeoutException,
          URISyntaxException {
    GoogleCloudStorageBidiReadChannel bidiReadChannel = getMockedBidiReadChannel(DEFAULT_ITEM_INFO);
    IntFunction<ByteBuffer> allocator = ByteBuffer::allocateDirect;
    // Returns 3 ranges, with the following {Offset, Length}: {20, 10}, {50, 7}, {65, 17}
    List<VectoredIORange> ranges = getListOfVectoredIORange();

    bidiReadChannel.readVectored(ranges, allocator);

    assertEquals(getReadVectoredData(ranges.get(0)), FakeBlobReadSession.SUBSTRING_20_10);
    assertEquals(getReadVectoredData(ranges.get(1)), FakeBlobReadSession.SUBSTRING_50_7);
    assertEquals(getReadVectoredData(ranges.get(2)), FakeBlobReadSession.SUBSTRING_65_17);
  }

  @Test
  public void read_successful() throws IOException {
    GoogleCloudStorageBidiReadChannel bidiReadChannel = getMockedBidiReadChannel(DEFAULT_ITEM_INFO);
    ByteBuffer buffer = ByteBuffer.allocate(10);

    // Read first chunk
    int bytesRead = bidiReadChannel.read(buffer);
    assertEquals(10, bytesRead);
    assertEquals(10, bidiReadChannel.position());
    buffer.flip();
    assertEquals(
        FakeBlobReadSession.SUBSTRING_0_10, StandardCharsets.UTF_8.decode(buffer).toString());

    // Read second chunk
    buffer.clear();
    bytesRead = bidiReadChannel.read(buffer);
    assertEquals(10, bytesRead);
    assertEquals(20, bidiReadChannel.position());
    buffer.flip();
    assertEquals(
        FakeBlobReadSession.SUBSTRING_10_10, StandardCharsets.UTF_8.decode(buffer).toString());
  }

  @Test
  public void read_partialReadAtEnd() throws IOException {
    GoogleCloudStorageBidiReadChannel bidiReadChannel = getMockedBidiReadChannel(DEFAULT_ITEM_INFO);
    bidiReadChannel.position(OBJECT_SIZE - 5);
    ByteBuffer buffer = ByteBuffer.allocate(10);

    int bytesRead = bidiReadChannel.read(buffer);

    assertEquals(5, bytesRead);
    assertEquals(OBJECT_SIZE, bidiReadChannel.position());
    buffer.flip();
    String expectedContent = FakeBlobReadSession.TEST_STRING.substring(OBJECT_SIZE - 5);
    assertEquals(expectedContent, StandardCharsets.UTF_8.decode(buffer).toString());
  }

  @Test
  public void read_whenPositionIsAtSize_returnsNegativeOne() throws IOException {
    GoogleCloudStorageBidiReadChannel bidiReadChannel = getMockedBidiReadChannel(DEFAULT_ITEM_INFO);
    bidiReadChannel.position(OBJECT_SIZE - 1);

    // Read the last byte to advance position to OBJECT_SIZE
    bidiReadChannel.read(ByteBuffer.allocate(1));
    assertEquals(OBJECT_SIZE, bidiReadChannel.position());

    // Next read should return -1
    assertEquals(-1, bidiReadChannel.read(ByteBuffer.allocate(10)));
  }

  @Test
  public void read_whenDstIsEmpty_returnsZero() throws IOException {
    GoogleCloudStorageBidiReadChannel bidiReadChannel = getMockedBidiReadChannel(DEFAULT_ITEM_INFO);

    ByteBuffer buffer = ByteBuffer.allocate(0);

    assertEquals(0, bidiReadChannel.read(buffer));
    assertEquals(0, bidiReadChannel.position());
  }

  @Test
  public void read_onClosedChannel_throwsClosedChannelException() throws IOException {
    GoogleCloudStorageBidiReadChannel bidiReadChannel = getMockedBidiReadChannel(DEFAULT_ITEM_INFO);

    bidiReadChannel.close();

    assertThrows(ClosedChannelException.class, () -> bidiReadChannel.read(ByteBuffer.allocate(1)));
  }

  @Test
  public void read_whenReadZeroBytes_throwsIOException() throws IOException {
    // Setup a FakeBlobReadSession that returns an empty ByteString
    Storage storage = mock(Storage.class);
    BlobReadSession fakeSession =
        new FakeBlobReadSession(FakeBlobReadSession.Behavior.READ_ZERO_BYTES);
    when(storage.blobReadSession(any(), any(BlobSourceOption.class)))
        .thenReturn(ApiFutures.immediateFuture(fakeSession));

    GoogleCloudStorageBidiReadChannel bidiReadChannel =
        new GoogleCloudStorageBidiReadChannel(
            storage,
            DEFAULT_ITEM_INFO,
            GoogleCloudStorageReadOptions.builder().build(),
            Executors.newSingleThreadExecutor());

    IOException e =
        assertThrows(IOException.class, () -> bidiReadChannel.read(ByteBuffer.allocate(10)));
    assertThat(e).hasMessageThat().contains("Read 0 bytes without blocking");
  }

  @Test
  public void read_whenFutureFails_throwsIOException() throws IOException {
    // Setup a FakeBlobReadSession that returns a failed future
    Storage storage = mock(Storage.class);
    BlobReadSession fakeSession = new FakeBlobReadSession(FakeBlobReadSession.Behavior.FAIL_FUTURE);
    when(storage.blobReadSession(any(), any(BlobSourceOption.class)))
        .thenReturn(ApiFutures.immediateFuture(fakeSession));

    GoogleCloudStorageBidiReadChannel bidiReadChannel =
        new GoogleCloudStorageBidiReadChannel(
            storage,
            DEFAULT_ITEM_INFO,
            GoogleCloudStorageReadOptions.builder().build(),
            Executors.newSingleThreadExecutor());

    IOException e =
        assertThrows(IOException.class, () -> bidiReadChannel.read(ByteBuffer.allocate(10)));
    assertThat(e).hasMessageThat().startsWith("Read failed on");
    assertThat(e).hasCauseThat().isInstanceOf(ExecutionException.class);
    assertThat(e).hasCauseThat().hasCauseThat().isInstanceOf(StorageException.class);
    assertThat(e).hasCauseThat().hasCauseThat().hasMessageThat().isEqualTo("Not Found");
  }

  @Test
  public void read_whenFutureTimesOut_throwsIOException() throws IOException {
    // Setup a FakeBlobReadSession that returns a future that never completes
    Storage storage = mock(Storage.class);
    BlobReadSession fakeSession =
        new FakeBlobReadSession(FakeBlobReadSession.Behavior.TIMEOUT_FUTURE);
    when(storage.blobReadSession(any(), any(BlobSourceOption.class)))
        .thenReturn(ApiFutures.immediateFuture(fakeSession));

    GoogleCloudStorageBidiReadChannel bidiReadChannel =
        new GoogleCloudStorageBidiReadChannel(
            storage,
            DEFAULT_ITEM_INFO,
            GoogleCloudStorageReadOptions.builder()
                .setGrpcReadTimeout(java.time.Duration.ofNanos(1))
                .build(),
            Executors.newSingleThreadExecutor());

    IOException e =
        assertThrows(IOException.class, () -> bidiReadChannel.read(ByteBuffer.allocate(10)));
    assertThat(e).hasMessageThat().startsWith("Read failed on");
    assertThat(e).hasCauseThat().isInstanceOf(TimeoutException.class);
  }

  @Test
  public void position_getAndSet() throws IOException {
    GoogleCloudStorageBidiReadChannel bidiReadChannel = getMockedBidiReadChannel(DEFAULT_ITEM_INFO);
    assertEquals(0, bidiReadChannel.position());

    bidiReadChannel.position(12);
    assertEquals(12, bidiReadChannel.position());
  }

  @Test
  public void position_setToNegative_throwsEofException() throws IOException {
    GoogleCloudStorageBidiReadChannel bidiReadChannel = getMockedBidiReadChannel(DEFAULT_ITEM_INFO);
    assertThrows(EOFException.class, () -> bidiReadChannel.position(-1));
  }

  @Test
  public void position_setToBeyondSize_throwsEofException() throws IOException {
    GoogleCloudStorageBidiReadChannel bidiReadChannel = getMockedBidiReadChannel(DEFAULT_ITEM_INFO);
    assertThrows(EOFException.class, () -> bidiReadChannel.position(OBJECT_SIZE + 1));
  }

  @Test
  public void position_setToSize_throwsEofException() throws IOException {
    GoogleCloudStorageBidiReadChannel bidiReadChannel = getMockedBidiReadChannel(DEFAULT_ITEM_INFO);
    assertThrows(EOFException.class, () -> bidiReadChannel.position(OBJECT_SIZE));
  }

  @Test
  public void position_onClosedChannel_throwsClosedChannelException() throws IOException {
    GoogleCloudStorageBidiReadChannel bidiReadChannel = getMockedBidiReadChannel(DEFAULT_ITEM_INFO);

    bidiReadChannel.close();

    assertThrows(ClosedChannelException.class, () -> bidiReadChannel.position(1));
    assertThrows(ClosedChannelException.class, bidiReadChannel::position);
  }

  @Test
  public void size_successful() throws IOException {
    GoogleCloudStorageBidiReadChannel bidiReadChannel = getMockedBidiReadChannel(DEFAULT_ITEM_INFO);

    assertEquals(OBJECT_SIZE, bidiReadChannel.size());
  }

  @Test
  public void size_onClosedChannel_throwsClosedChannelException() throws IOException {
    GoogleCloudStorageBidiReadChannel bidiReadChannel = getMockedBidiReadChannel(DEFAULT_ITEM_INFO);

    bidiReadChannel.close();

    assertThrows(ClosedChannelException.class, bidiReadChannel::size);
  }

  @Test
  public void isOpen_isTrueOnNewChannel() throws IOException {
    GoogleCloudStorageBidiReadChannel bidiReadChannel = getMockedBidiReadChannel(DEFAULT_ITEM_INFO);

    assertTrue(bidiReadChannel.isOpen());
  }

  @Test
  public void isOpen_isFalseAfterClose() throws IOException {
    GoogleCloudStorageBidiReadChannel bidiReadChannel = getMockedBidiReadChannel(DEFAULT_ITEM_INFO);

    bidiReadChannel.close();

    assertFalse(bidiReadChannel.isOpen());
  }

  @Test
  public void write_unsupportedOperationException() throws IOException {
    GoogleCloudStorageBidiReadChannel bidiReadChannel = getMockedBidiReadChannel(DEFAULT_ITEM_INFO);

    assertThrows(
        UnsupportedOperationException.class,
        () -> bidiReadChannel.write(ByteBuffer.allocateDirect(0)));
  }

  @Test
  public void truncate_unsupportedOperationException() throws IOException {
    GoogleCloudStorageBidiReadChannel bidiReadChannel = getMockedBidiReadChannel(DEFAULT_ITEM_INFO);

    assertThrows(UnsupportedOperationException.class, () -> bidiReadChannel.truncate(0));
  }

  @Test
  public void initMetadata_unsupportedOperationException() throws IOException {
    GoogleCloudStorageBidiReadChannel bidiReadChannel = getMockedBidiReadChannel(DEFAULT_ITEM_INFO);

    assertThrows(
        UnsupportedOperationException.class, () -> bidiReadChannel.initMetadata("gzip", 10));
  }

  @Test
  public void size_ioException() throws IOException {
    GoogleCloudStorageItemInfo ItemInfo =
        GoogleCloudStorageItemInfo.createObject(
            RESOURCE_ID,
            /* creationTime= */ 10L,
            /* modificationTime= */ 15L,
            /* size= */ -1,
            /* contentType= */ "text/plain",
            /* contentEncoding= */ "text",
            /* metadata= */ null,
            /* contentGeneration= */ 1,
            /* metaGeneration= */ 2L,
            /* verificationAttributes= */ null);

    GoogleCloudStorageBidiReadChannel bidiReadChannel = getMockedBidiReadChannel(ItemInfo);

    assertThrows(IOException.class, bidiReadChannel::size);
  }

  private String getReadVectoredData(VectoredIORange range)
      throws ExecutionException, InterruptedException, TimeoutException {
    Charset charset = StandardCharsets.UTF_8;
    return charset
        .decode(Objects.requireNonNull(range.getData()).get(3, TimeUnit.SECONDS))
        .toString();
  }

  private GoogleCloudStorageBidiReadChannel getMockedBidiReadChannel(
      GoogleCloudStorageItemInfo ItemInfo) throws IOException {
    Storage storage = mock(Storage.class);
    when(storage.blobReadSession(any(), any()))
        .thenReturn(ApiFutures.immediateFuture(new FakeBlobReadSession()));
    return new GoogleCloudStorageBidiReadChannel(
        storage,
        ItemInfo,
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
  }
}
