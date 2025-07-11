package com.google.cloud.hadoop.gcsio;

import static com.google.cloud.hadoop.gcsio.MockGoogleCloudStorageImplFactory.mockedGcsClientImpl;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.mockTransport;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.AdditionalAnswers.answersWithDelay;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.core.ApiFutures;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import java.io.IOException;
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
      throws IOException, ExecutionException, InterruptedException, TimeoutException {
    GoogleCloudStorageClientImpl gcsClientImpl = getMockedGcsClientImpl(false);
    IntFunction<ByteBuffer> allocator = (length) -> ByteBuffer.allocateDirect(length);
    List<VectoredIORange> ranges = getListOfVectoredIORange();

    VectoredIOResult result =
        gcsClientImpl.readVectored(
            ranges, allocator, BlobId.of(TEST_BUCKET_NAME, TEST_OBJECT_NAME));

    assertEquals(getReadVectoredData(ranges.get(0)), FakeBlobReadSession.SUBSTRING_20_10);
    assertEquals(getReadVectoredData(ranges.get(1)), FakeBlobReadSession.SUBSTRING_50_7);
    assertEquals(getReadVectoredData(ranges.get(2)), FakeBlobReadSession.SUBSTRING_65_17);

    assertThat(result.getClientInitializationDuration()).isLessThan(100L);
    assertThat(result.getReadDuration()).isLessThan(100L);
  }

  @Test
  public void readVectored_returnsClientDuration()
      throws IOException, ExecutionException, InterruptedException, TimeoutException {
    GoogleCloudStorageClientImpl gcsClientImpl = getMockedGcsClientImpl(true);
    IntFunction<ByteBuffer> allocator = (length) -> ByteBuffer.allocateDirect(length);
    List<VectoredIORange> ranges = getListOfVectoredIORange();

    VectoredIOResult result =
        gcsClientImpl.readVectored(
            ranges, allocator, BlobId.of(TEST_BUCKET_NAME, TEST_OBJECT_NAME));

    assertThat(result.getClientInitializationDuration()).isLessThan(5100L);
    assertThat(result.getClientInitializationDuration()).isGreaterThan(4999L);
    assertThat(result.getReadDuration()).isLessThan(100L);
  }

  private String getReadVectoredData(VectoredIORange range)
      throws ExecutionException, InterruptedException, TimeoutException {
    Charset charset = StandardCharsets.UTF_8;
    return charset.decode(range.getData().get(3, TimeUnit.SECONDS)).toString();
  }

  private GoogleCloudStorageClientImpl getMockedGcsClientImpl(boolean withDelay)
      throws IOException {
    Storage storage = mock(Storage.class);
    if (withDelay) {
      when(storage.blobReadSession(any(), any()))
          .then(
              answersWithDelay(
                  5000L, invocation -> ApiFutures.immediateFuture(new FakeBlobReadSession())));
    } else {
      when(storage.blobReadSession(any(), any()))
          .thenReturn(ApiFutures.immediateFuture(new FakeBlobReadSession()));
    }
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
