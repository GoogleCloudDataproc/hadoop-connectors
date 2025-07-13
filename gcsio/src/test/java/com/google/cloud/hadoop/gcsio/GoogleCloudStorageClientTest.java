package com.google.cloud.hadoop.gcsio;

import static com.google.cloud.hadoop.gcsio.MockGoogleCloudStorageImplFactory.mockedGcsClientImpl;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.mockTransport;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.core.ApiFutures;
import com.google.cloud.storage.Storage;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.IntFunction;
import org.junit.Test;

public class GoogleCloudStorageClientTest {

  private static final String TEST_BUCKET_NAME = "foo-bucket";

  private static final String TEST_OBJECT_NAME = "foo-object";

  private static MockHttpTransport transport = mockTransport();

  @Test
  public void readVectored_successfulRead()
      throws IOException, ExecutionException, InterruptedException, TimeoutException,
          URISyntaxException {
    GoogleCloudStorageClientImpl gcsClientImpl = getMockedGcsClientImpl();
    IntFunction<ByteBuffer> allocator = ByteBuffer::allocateDirect;
    List<VectoredIORange> ranges = getListOfVectoredIORange();

    VectoredIOMetrics result =
        gcsClientImpl.readVectored(
            ranges, allocator, new URI("gs://" + TEST_BUCKET_NAME + "/" + TEST_OBJECT_NAME));

    assertEquals(getReadVectoredData(ranges.get(0)), FakeBlobReadSession.SUBSTRING_20_10);
    assertEquals(getReadVectoredData(ranges.get(1)), FakeBlobReadSession.SUBSTRING_50_7);
    assertEquals(getReadVectoredData(ranges.get(2)), FakeBlobReadSession.SUBSTRING_65_17);
    assertEquals(result.getReadBytes().intValue(), 34);
  }

  private String getReadVectoredData(VectoredIORange range)
      throws ExecutionException, InterruptedException, TimeoutException {
    Charset charset = StandardCharsets.UTF_8;
    return charset.decode(range.getData().get(3, TimeUnit.SECONDS)).toString();
  }

  private GoogleCloudStorageClientImpl getMockedGcsClientImpl() throws IOException {
    Storage storage = mock(Storage.class);
    when(storage.blobReadSession(any(), any()))
        .thenReturn(ApiFutures.immediateFuture(new FakeBlobReadSession()));
    return mockedGcsClientImpl(transport, storage);
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
