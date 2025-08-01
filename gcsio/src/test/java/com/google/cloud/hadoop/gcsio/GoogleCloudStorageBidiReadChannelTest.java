package com.google.cloud.hadoop.gcsio;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFutures;
import com.google.cloud.storage.Storage;
import java.io.EOFException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
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

  private static final StorageResourceId RESOURCE_ID =
      new StorageResourceId(TEST_BUCKET_NAME, TEST_OBJECT_NAME);

  private static final GoogleCloudStorageItemInfo DEFAULT_ITEM_INFO =
      GoogleCloudStorageItemInfo.createObject(
          RESOURCE_ID,
          /* creationTime= */ 10L,
          /* modificationTime= */ 15L,
          /* size= */ FakeBlobReadSession.TEST_STRING_SIZE,
          /* contentType= */ "text/plain",
          /* contentEncoding= */ "text",
          /* metadata= */ null,
          /* contentGeneration= */ 1,
          /* metaGeneration= */ 2L,
          /* verificationAttributes= */ null);

  @Test
  public void read_readsDataAndUpdatesPosition() throws IOException {
    GoogleCloudStorageBidiReadChannel channel = getMockedBidiReadChannel();
    ByteBuffer buffer = ByteBuffer.allocate(11); // "Lorem ipsum"

    int bytesRead = channel.read(buffer);
    buffer.flip();
    String result = StandardCharsets.UTF_8.decode(buffer).toString();

    assertEquals(bytesRead, 11);
    assertEquals(channel.position(), 11);
    assertEquals(result, "Lorem ipsum");
  }

  @Test
  public void read_reachesEof() throws IOException {
    GoogleCloudStorageBidiReadChannel channel = getMockedBidiReadChannel();
    ByteBuffer buffer = ByteBuffer.allocate(FakeBlobReadSession.TEST_STRING_SIZE + 3);

    int totalBytesRead = channel.read(buffer);
    buffer.flip();
    String result = StandardCharsets.UTF_8.decode(buffer).toString();

    assertEquals(totalBytesRead, FakeBlobReadSession.TEST_STRING_SIZE);
    assertEquals(channel.position(), FakeBlobReadSession.TEST_STRING_SIZE);
    assertEquals(result, FakeBlobReadSession.TEST_STRING);
  }

  @Test
  public void position_set_updatesPositionForNextRead() throws IOException {
    GoogleCloudStorageBidiReadChannel channel = getMockedBidiReadChannel();
    long newPosition = 12; // Corresponds to starting of "dolor" in the test string

    channel.position(newPosition);
    assertEquals(channel.position(), newPosition);

    ByteBuffer buffer = ByteBuffer.allocate(5);
    channel.read(buffer);
    buffer.flip();
    String result = StandardCharsets.UTF_8.decode(buffer).toString();

    assertEquals(result, "dolor");
    assertEquals(channel.position(), newPosition + 5);
  }

  @Test
  public void position_set_negative_throwsIOException() throws IOException {
    GoogleCloudStorageBidiReadChannel channel = getMockedBidiReadChannel();
    IOException e = assertThrows(IOException.class, () -> channel.position(-1));
    assertThat(e).hasMessageThat().contains("Invalid seek position: -1");
  }

  @Test
  public void position_set_beyondEof_throwsEOFException() throws IOException {
    GoogleCloudStorageBidiReadChannel channel = getMockedBidiReadChannel();
    long invalidPosition = FakeBlobReadSession.TEST_STRING_SIZE + 3;
    EOFException e = assertThrows(EOFException.class, () -> channel.position(invalidPosition));
    assertThat(e)
        .hasMessageThat()
        .contains(
            String.format(
                "Seek position %d is beyond file size %d",
                invalidPosition, FakeBlobReadSession.TEST_STRING_SIZE));
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
  public void operations_onClosedChannel_throwException() throws IOException {
    GoogleCloudStorageBidiReadChannel channel = getMockedBidiReadChannel();
    channel.close();

    assertThat(channel.isOpen()).isFalse();
    assertThrows(ClosedChannelException.class, () -> channel.read(ByteBuffer.allocate(1)));
    assertThrows(ClosedChannelException.class, () -> channel.position());
    assertThrows(ClosedChannelException.class, () -> channel.position(1));
    assertThrows(ClosedChannelException.class, () -> channel.size());
  }

  @Test
  public void write_unsupportedOperationException() throws IOException {
    GoogleCloudStorageBidiReadChannel bidiReadChannel = getMockedBidiReadChannel();
    assertThrows(
        UnsupportedOperationException.class,
        () -> bidiReadChannel.write(ByteBuffer.allocateDirect(0)));
  }

  @Test
  public void truncaate_unsupportedOperationException() throws IOException {
    GoogleCloudStorageBidiReadChannel bidiReadChannel = getMockedBidiReadChannel();
    assertThrows(UnsupportedOperationException.class, () -> bidiReadChannel.truncate(10));
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
  }
}
