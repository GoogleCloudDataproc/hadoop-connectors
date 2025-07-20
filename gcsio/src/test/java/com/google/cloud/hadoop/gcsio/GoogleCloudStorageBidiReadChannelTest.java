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
    GoogleCloudStorageBidiReadChannel bidiReadChannel = getMockedBidiReadChannel();
    IntFunction<ByteBuffer> allocator = ByteBuffer::allocateDirect;
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
  }
}
