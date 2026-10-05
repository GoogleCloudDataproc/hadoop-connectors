/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.gcsio;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import com.google.api.core.ApiFutures;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BlobReadSession;
import com.google.cloud.storage.Storage;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GoogleCloudStorageClientBidiPoolingTest {

  private Storage mockStorage;
  private GoogleCloudStorageOptions options;
  private GoogleCloudStorageClientImpl gcsClient;

  @Before
  public void setUp() throws IOException {
    mockStorage = mock(Storage.class);
    options =
        GoogleCloudStorageOptions.builder()
            .setAppName("test-app")
            .setGrpcEnabled(true)
            .setBidiEnabled(true)
            .build();

    gcsClient =
        new GoogleCloudStorageClientImpl(
            options,
            mockStorage,
            /* credentials= */ null,
            /* httpTransport= */ null,
            /* httpRequestInitializer= */ null,
            /* gRPCInterceptors= */ null,
            /* downscopedAccessTokenFn= */ null,
            /* pCUExecutorService= */ null,
            /* featureHeaderGenerator= */ null);
    gcsClient.backgroundTasksThreadPool = MoreExecutors.newDirectExecutorService();
    gcsClient.boundedThreadPool = MoreExecutors.newDirectExecutorService();
  }

  @Test
  public void testMultiOpen_prewarmsAndCachesChannel() throws Exception {
    StorageResourceId resourceId = new StorageResourceId("bucket", "object");
    BlobId blobId = BlobId.of("bucket", "object");

    BlobInfo mockBlobInfo = mock(BlobInfo.class);
    when(mockBlobInfo.getSize()).thenReturn(100L);
    when(mockBlobInfo.getGeneration()).thenReturn(123L);
    BlobReadSession customSession =
        new FakeBlobReadSession() {
          @Override
          public BlobInfo getBlobInfo() {
            return mockBlobInfo;
          }
        };

    when(mockStorage.blobReadSession(eq(blobId)))
        .thenReturn(ApiFutures.immediateFuture(customSession));

    gcsClient.multiOpen(ImmutableMap.of(resourceId, 100L));
    ConcurrentLinkedQueue<GoogleCloudStorageBidiReadChannel> queue =
        gcsClient.getChannelPool().getIfPresent(resourceId);

    SeekableByteChannel channel1 = gcsClient.open(resourceId);
    channel1.close();

    SeekableByteChannel channel2 = gcsClient.open(resourceId);
    channel2.close();

    assertThat(queue).isNotNull();
    assertThat(queue).isNotEmpty();
    assertThat(channel1).isInstanceOf(GoogleCloudStorageBidiReadChannel.class);
    assertThat(channel2).isInstanceOf(GoogleCloudStorageBidiReadChannel.class);
    // Verify blobReadSession was called exactly once (prewarming call is reused)
    verify(mockStorage, times(1)).blobReadSession(eq(blobId));
    // Verify no metadata lookups were performed on checkout
    verify(mockStorage, never()).get(any(BlobId.class), any(Storage.BlobGetOption[].class));
  }

  @Test
  public void testCloseClient_invalidatesAndClosesAllCachedChannels() throws Exception {
    StorageResourceId resourceId = new StorageResourceId("bucket", "object");
    BlobId blobId = BlobId.of("bucket", "object");

    BlobInfo mockBlobInfo = mock(BlobInfo.class);
    when(mockBlobInfo.getSize()).thenReturn(100L);
    when(mockBlobInfo.getGeneration()).thenReturn(123L);
    BlobReadSession customSession =
        new FakeBlobReadSession() {
          @Override
          public BlobInfo getBlobInfo() {
            return mockBlobInfo;
          }
        };

    when(mockStorage.blobReadSession(eq(blobId)))
        .thenReturn(ApiFutures.immediateFuture(customSession));

    gcsClient.multiOpen(ImmutableMap.of(resourceId, 100L));
    ConcurrentLinkedQueue<GoogleCloudStorageBidiReadChannel> queue =
        gcsClient.getChannelPool().getIfPresent(resourceId);
    GoogleCloudStorageBidiReadChannel prewarmedChannel = queue != null ? queue.peek() : null;

    gcsClient.close();

    assertThat(prewarmedChannel).isNotNull();
    assertThat(prewarmedChannel.isOpen()).isFalse();
    assertThat(gcsClient.getChannelPool().getIfPresent(resourceId)).isNull();
  }

  @Test
  public void testSizeMismatch_invalidatesCacheAndReopens() throws Exception {
    StorageResourceId resourceId = new StorageResourceId("bucket", "object");
    BlobId blobId = BlobId.of("bucket", "object");

    // Session 1: size 200 (mismatch with prewarm size 100)
    BlobInfo mockBlobInfo1 = mock(BlobInfo.class);
    when(mockBlobInfo1.getSize()).thenReturn(200L);
    when(mockBlobInfo1.getGeneration()).thenReturn(123L);
    BlobReadSession customSession1 =
        new FakeBlobReadSession() {
          @Override
          public BlobInfo getBlobInfo() {
            return mockBlobInfo1;
          }
        };

    // Session 2: size 200 (correct size)
    BlobInfo mockBlobInfo2 = mock(BlobInfo.class);
    when(mockBlobInfo2.getSize()).thenReturn(200L);
    when(mockBlobInfo2.getGeneration()).thenReturn(124L); // Generation might change on overwrite
    BlobReadSession customSession2 =
        new FakeBlobReadSession() {
          @Override
          public BlobInfo getBlobInfo() {
            return mockBlobInfo2;
          }
        };

    // Stub blobReadSession to return session1 then session2
    when(mockStorage.blobReadSession(any(BlobId.class)))
        .thenReturn(ApiFutures.immediateFuture(customSession1))
        .thenReturn(ApiFutures.immediateFuture(customSession2));

    gcsClient.multiOpen(ImmutableMap.of(resourceId, 100L));
    ConcurrentLinkedQueue<GoogleCloudStorageBidiReadChannel> queue =
        gcsClient.getChannelPool().getIfPresent(resourceId);
    GoogleCloudStorageBidiReadChannel channel = queue != null ? queue.peek() : null;

    assertThat(queue).isNotNull();
    assertThat(queue).isNotEmpty();
    assertThat(channel).isNotNull();
    assertThat(channel.isOpen()).isTrue();
    assertThat(channel.size()).isEqualTo(200L);
    // Since channel.size() was resolved, verify blobReadSession was called exactly TWICE
    verify(mockStorage, times(2)).blobReadSession(any(BlobId.class));
  }

  @Test
  public void testMultiOpen_unknownSize_succeedsWithoutRecovery() throws Exception {
    StorageResourceId resourceId = new StorageResourceId("bucket", "object");
    BlobId blobId = BlobId.of("bucket", "object");

    BlobInfo mockBlobInfo = mock(BlobInfo.class);
    when(mockBlobInfo.getSize()).thenReturn(150L);
    when(mockBlobInfo.getGeneration()).thenReturn(123L);
    BlobReadSession customSession =
        new FakeBlobReadSession() {
          @Override
          public BlobInfo getBlobInfo() {
            return mockBlobInfo;
          }
        };

    when(mockStorage.blobReadSession(eq(blobId)))
        .thenReturn(ApiFutures.immediateFuture(customSession));

    gcsClient.multiOpen(ImmutableMap.of(resourceId, -1L));
    ConcurrentLinkedQueue<GoogleCloudStorageBidiReadChannel> queue =
        gcsClient.getChannelPool().getIfPresent(resourceId);
    GoogleCloudStorageBidiReadChannel channel = queue != null ? queue.peek() : null;

    assertThat(queue).isNotNull();
    assertThat(queue).isNotEmpty();
    assertThat(channel).isNotNull();
    assertThat(channel.isOpen()).isTrue();
    assertThat(channel.size()).isEqualTo(150L);
    verify(mockStorage, times(1)).blobReadSession(any(BlobId.class));
  }

  @Test
  public void testDoubleClose_doesNotDuplicateChannelInPool() throws Exception {
    StorageResourceId resourceId = new StorageResourceId("bucket", "object");
    BlobId blobId = BlobId.of("bucket", "object");

    BlobInfo mockBlobInfo = mock(BlobInfo.class);
    when(mockBlobInfo.getSize()).thenReturn(100L);
    when(mockBlobInfo.getGeneration()).thenReturn(123L);
    BlobReadSession customSession =
        new FakeBlobReadSession() {
          @Override
          public BlobInfo getBlobInfo() {
            return mockBlobInfo;
          }
        };

    when(mockStorage.blobReadSession(eq(blobId)))
        .thenReturn(ApiFutures.immediateFuture(customSession));

    com.google.cloud.storage.Blob mockBlob = mock(com.google.cloud.storage.Blob.class);
    when(mockBlob.getBucket()).thenReturn(blobId.getBucket());
    when(mockBlob.getName()).thenReturn(blobId.getName());
    when(mockBlob.getSize()).thenReturn(100L);
    when(mockBlob.getGeneration()).thenReturn(123L);
    when(mockStorage.get(any(BlobId.class), any())).thenReturn(mockBlob);

    // Open channel
    GoogleCloudStorageBidiReadChannel channel =
        (GoogleCloudStorageBidiReadChannel) gcsClient.open(resourceId);

    // Close twice
    channel.close();
    channel.close();

    ConcurrentLinkedQueue<GoogleCloudStorageBidiReadChannel> queue =
        gcsClient.getChannelPool().getIfPresent(resourceId);

    assertThat(queue).isNotNull();
    assertThat(queue.size()).isEqualTo(1); // Should only be in pool once!
  }

  @Test
  public void testMultiOpen_limitsPrewarmingToMaxFiles() throws Exception {
    // Pass 25 files to prewarm
    ImmutableMap.Builder<StorageResourceId, Long> mapBuilder = ImmutableMap.builder();
    for (int i = 0; i < 25; i++) {
      StorageResourceId resourceId = new StorageResourceId("bucket", "object" + i);
      mapBuilder.put(resourceId, 100L);

      BlobId blobId = BlobId.of("bucket", "object" + i);
      BlobInfo mockBlobInfo = mock(BlobInfo.class);
      when(mockBlobInfo.getSize()).thenReturn(100L);
      when(mockBlobInfo.getGeneration()).thenReturn(123L);
      BlobReadSession customSession =
          new FakeBlobReadSession() {
            @Override
            public BlobInfo getBlobInfo() {
              return mockBlobInfo;
            }
          };

      when(mockStorage.blobReadSession(eq(blobId)))
          .thenReturn(ApiFutures.immediateFuture(customSession));
    }

    gcsClient.multiOpen(mapBuilder.build());

    // Verify cache has exactly 20 elements (since the limit is 20)
    int cachedCount = 0;
    for (int i = 0; i < 25; i++) {
      StorageResourceId resourceId = new StorageResourceId("bucket", "object" + i);
      if (gcsClient.getChannelPool().getIfPresent(resourceId) != null) {
        cachedCount++;
      }
    }

    assertThat(cachedCount).isEqualTo(20);
  }

  @Test
  public void testCheckoutChannel_prunesExpiredChannels() throws Exception {
    StorageResourceId resourceId = new StorageResourceId("bucket", "object");
    BlobId blobId = BlobId.of("bucket", "object");

    BlobInfo mockBlobInfo = mock(BlobInfo.class);
    when(mockBlobInfo.getSize()).thenReturn(100L);
    when(mockBlobInfo.getGeneration()).thenReturn(123L);
    BlobReadSession customSession1 =
        new FakeBlobReadSession() {
          @Override
          public BlobInfo getBlobInfo() {
            return mockBlobInfo;
          }
        };
    BlobReadSession customSession2 =
        new FakeBlobReadSession() {
          @Override
          public BlobInfo getBlobInfo() {
            return mockBlobInfo;
          }
        };

    when(mockStorage.blobReadSession(eq(blobId)))
        .thenReturn(ApiFutures.immediateFuture(customSession1))
        .thenReturn(ApiFutures.immediateFuture(customSession2));

    // Custom client with 1-second expiration to speed up testing
    GoogleCloudStorageOptions fastOptions =
        GoogleCloudStorageOptions.builder()
            .setAppName("test-app")
            .setGrpcEnabled(true)
            .setBidiEnabled(true)
            .setReadChannelOptions(
                GoogleCloudStorageReadOptions.builder()
                    .setBidiCacheExpireSec(1) // 1 second expiry
                    .build())
            .build();

    GoogleCloudStorageClientImpl fastClient =
        new GoogleCloudStorageClientImpl(
            fastOptions,
            mockStorage,
            /* credentials= */ null,
            /* httpTransport= */ null,
            /* httpRequestInitializer= */ null,
            /* gRPCInterceptors= */ null,
            /* downscopedAccessTokenFn= */ null,
            /* pCUExecutorService= */ null,
            /* featureHeaderGenerator= */ null);

    // Mock storage.get for metadata validation on checkout
    com.google.cloud.storage.Blob mockBlob = mock(com.google.cloud.storage.Blob.class);
    when(mockBlob.getBucket()).thenReturn(blobId.getBucket());
    when(mockBlob.getName()).thenReturn(blobId.getName());
    when(mockBlob.getSize()).thenReturn(100L);
    when(mockBlob.getGeneration()).thenReturn(123L);
    when(mockStorage.get(any(BlobId.class), any())).thenReturn(mockBlob);

    // Open two channels
    GoogleCloudStorageBidiReadChannel channel1 =
        (GoogleCloudStorageBidiReadChannel)
            fastClient.open(resourceId, fastOptions.getReadChannelOptions());
    GoogleCloudStorageBidiReadChannel channel2 =
        (GoogleCloudStorageBidiReadChannel)
            fastClient.open(resourceId, fastOptions.getReadChannelOptions());

    // Return channel1 to the pool
    channel1.close();

    // Sleep 1.5 seconds to let channel1 expire, while keeping the queue alive in the cache
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < 1500) {
      fastClient.getChannelPool().getIfPresent(resourceId);
      Thread.sleep(200);
    }

    // Perform a read on channel2 to update its lastAccessTime
    java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(1);
    channel2.read(buf);

    // Return channel2 to the pool. Now queue has [channel1, channel2]
    channel2.close();

    ConcurrentLinkedQueue<GoogleCloudStorageBidiReadChannel> queue =
        fastClient.getChannelPool().getIfPresent(resourceId);

    assertThat(queue).isNotNull();
    assertThat(queue.size()).isEqualTo(2);

    // Checkout a channel. This should prune expired channel1 and return channel2!
    GoogleCloudStorageBidiReadChannel channel3 =
        (GoogleCloudStorageBidiReadChannel)
            fastClient.open(resourceId, fastOptions.getReadChannelOptions());

    assertThat(channel3).isNotNull();
    assertThat(channel3.isOpen()).isTrue();
    assertThat(channel3).isNotSameInstanceAs(channel1);
    assertThat(channel3).isSameInstanceAs(channel2); // Should reuse channel2!

    // Verify channel1 is closed
    assertThat(channel1.isOpen()).isFalse();

    // Verify queue is now empty (since channel2 is checked out)
    ConcurrentLinkedQueue<GoogleCloudStorageBidiReadChannel> activeQueue =
        fastClient.getChannelPool().getIfPresent(resourceId);
    assertThat(activeQueue).isEmpty();

    fastClient.close();
  }

  @Test
  public void testCheckoutChannel_resetsPosition() throws Exception {
    StorageResourceId resourceId = new StorageResourceId("bucket", "object");
    BlobId blobId = BlobId.of("bucket", "object");

    BlobInfo mockBlobInfo = mock(BlobInfo.class);
    when(mockBlobInfo.getSize()).thenReturn((long) FakeBlobReadSession.TEST_STRING.length());
    when(mockBlobInfo.getGeneration()).thenReturn(123L);
    BlobReadSession customSession =
        new FakeBlobReadSession() {
          @Override
          public BlobInfo getBlobInfo() {
            return mockBlobInfo;
          }
        };

    when(mockStorage.blobReadSession(eq(blobId)))
        .thenReturn(ApiFutures.immediateFuture(customSession));

    com.google.cloud.storage.Blob mockBlob = mock(com.google.cloud.storage.Blob.class);
    when(mockBlob.getBucket()).thenReturn(blobId.getBucket());
    when(mockBlob.getName()).thenReturn(blobId.getName());
    when(mockBlob.getSize()).thenReturn((long) FakeBlobReadSession.TEST_STRING.length());
    when(mockBlob.getGeneration()).thenReturn(123L);
    when(mockStorage.get(any(BlobId.class), any())).thenReturn(mockBlob);

    // Checkout a channel
    GoogleCloudStorageBidiReadChannel channel1 =
        (GoogleCloudStorageBidiReadChannel) gcsClient.open(resourceId);

    // Read 10 bytes from it
    java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(10);
    int bytesRead = channel1.read(buf);
    assertThat(bytesRead).isEqualTo(10);
    assertThat(channel1.position()).isEqualTo(10);

    // Return to pool
    channel1.close();

    // Checkout again (should reuse channel1)
    GoogleCloudStorageBidiReadChannel channel2 =
        (GoogleCloudStorageBidiReadChannel) gcsClient.open(resourceId);

    assertThat(channel2).isSameInstanceAs(channel1);
    // The position MUST be reset to 0!
    assertThat(channel2.position()).isEqualTo(0);

    // Reading again should return the first 10 bytes again, not bytes 10-20
    buf.clear();
    bytesRead = channel2.read(buf);
    assertThat(bytesRead).isEqualTo(10);
    String readString = new String(buf.array(), java.nio.charset.StandardCharsets.UTF_8);
    assertThat(readString).isEqualTo(FakeBlobReadSession.SUBSTRING_0_10);

    channel2.close();
  }

  @Test
  public void testReturnChannel_afterClientClose_closesChannelImmediately() throws Exception {
    StorageResourceId resourceId = new StorageResourceId("bucket", "object");
    BlobId blobId = BlobId.of("bucket", "object");

    BlobInfo mockBlobInfo = mock(BlobInfo.class);
    when(mockBlobInfo.getSize()).thenReturn(100L);
    when(mockBlobInfo.getGeneration()).thenReturn(123L);
    BlobReadSession customSession =
        new FakeBlobReadSession() {
          @Override
          public BlobInfo getBlobInfo() {
            return mockBlobInfo;
          }
        };

    when(mockStorage.blobReadSession(eq(blobId)))
        .thenReturn(ApiFutures.immediateFuture(customSession));

    com.google.cloud.storage.Blob mockBlob = mock(com.google.cloud.storage.Blob.class);
    when(mockBlob.getBucket()).thenReturn(blobId.getBucket());
    when(mockBlob.getName()).thenReturn(blobId.getName());
    when(mockBlob.getSize()).thenReturn(100L);
    when(mockBlob.getGeneration()).thenReturn(123L);
    when(mockStorage.get(any(BlobId.class), any())).thenReturn(mockBlob);

    GoogleCloudStorageBidiReadChannel channel =
        (GoogleCloudStorageBidiReadChannel) gcsClient.open(resourceId);

    // Close the client first
    gcsClient.close();

    // Close the channel now. It should not go back into the pool. It should close immediately.
    channel.close();

    assertThat(channel.isOpen()).isFalse();
    // Cache should be empty or null
    assertThat(gcsClient.getChannelPool().getIfPresent(resourceId)).isNull();
  }

  @Test
  public void testCheckoutChannel_skipsClosedChannels() throws Exception {
    StorageResourceId resourceId = new StorageResourceId("bucket", "object");
    BlobId blobId = BlobId.of("bucket", "object");

    BlobInfo mockBlobInfo = mock(BlobInfo.class);
    when(mockBlobInfo.getSize()).thenReturn(100L);
    when(mockBlobInfo.getGeneration()).thenReturn(123L);

    // Set up storage mock for two sessions (one for the closed channel, one for the new one)
    BlobReadSession session1 =
        new FakeBlobReadSession() {
          @Override
          public BlobInfo getBlobInfo() {
            return mockBlobInfo;
          }
        };
    BlobReadSession session2 =
        new FakeBlobReadSession() {
          @Override
          public BlobInfo getBlobInfo() {
            return mockBlobInfo;
          }
        };
    when(mockStorage.blobReadSession(eq(blobId)))
        .thenReturn(ApiFutures.immediateFuture(session1))
        .thenReturn(ApiFutures.immediateFuture(session2));

    com.google.cloud.storage.Blob mockBlob = mock(com.google.cloud.storage.Blob.class);
    when(mockBlob.getBucket()).thenReturn(blobId.getBucket());
    when(mockBlob.getName()).thenReturn(blobId.getName());
    when(mockBlob.getSize()).thenReturn(100L);
    when(mockBlob.getGeneration()).thenReturn(123L);
    when(mockStorage.get(any(BlobId.class), any())).thenReturn(mockBlob);

    // Open channel 1 and close it to put it in the pool
    GoogleCloudStorageBidiReadChannel channel1 =
        (GoogleCloudStorageBidiReadChannel) gcsClient.open(resourceId);
    channel1.close();

    // Force-close channel 1 in the background (simulating pruning thread racing with checkout)
    channel1.actualClose();
    assertThat(channel1.isOpen()).isFalse();

    // Checkout again. It should detect channel 1 is closed, discard it, and open a new one (channel
    // 2)
    GoogleCloudStorageBidiReadChannel channel2 =
        (GoogleCloudStorageBidiReadChannel) gcsClient.open(resourceId);

    assertThat(channel2).isNotSameInstanceAs(channel1);
    assertThat(channel2.isOpen()).isTrue();

    channel2.close();
  }

  @Test
  public void testReturnChannel_defersPoolReturnUntilVectoredReadsComplete() throws Exception {
    StorageResourceId resourceId = new StorageResourceId("bucket", "object");
    BlobId blobId = BlobId.of("bucket", "object");

    BlobInfo mockBlobInfo = mock(BlobInfo.class);
    when(mockBlobInfo.getSize()).thenReturn(100L);
    when(mockBlobInfo.getGeneration()).thenReturn(123L);

    com.google.api.core.SettableApiFuture<
            com.google.cloud.storage.ZeroCopySupport.DisposableByteString>
        pendingFuture = com.google.api.core.SettableApiFuture.create();

    BlobReadSession customSession =
        new FakeBlobReadSession(FakeBlobReadSession.Behavior.TIMEOUT_FUTURE, pendingFuture) {
          @Override
          public BlobInfo getBlobInfo() {
            return mockBlobInfo;
          }
        };

    when(mockStorage.blobReadSession(eq(blobId)))
        .thenReturn(ApiFutures.immediateFuture(customSession));

    com.google.cloud.storage.Blob mockBlob = mock(com.google.cloud.storage.Blob.class);
    when(mockBlob.getBucket()).thenReturn(blobId.getBucket());
    when(mockBlob.getName()).thenReturn(blobId.getName());
    when(mockBlob.getSize()).thenReturn(100L);
    when(mockBlob.getGeneration()).thenReturn(123L);
    when(mockStorage.get(any(BlobId.class), any())).thenReturn(mockBlob);

    // Open channel
    GoogleCloudStorageBidiReadChannel channel =
        (GoogleCloudStorageBidiReadChannel) gcsClient.open(resourceId);

    // Start a vectored read range
    com.google.cloud.hadoop.gcsio.VectoredIORange range =
        com.google.cloud.hadoop.gcsio.VectoredIORange.builder()
            .setOffset(0)
            .setLength(10)
            .setData(new java.util.concurrent.CompletableFuture<>())
            .build();
    channel.readVectored(
        com.google.common.collect.ImmutableList.of(range), java.nio.ByteBuffer::allocate);

    // Close channel (should defer return as future is pending)
    channel.close();

    // Verify it is NOT yet in the pool
    assertThat(gcsClient.getChannelPool().getIfPresent(resourceId)).isNull();

    // Complete the future successfully
    pendingFuture.set(
        new com.google.cloud.storage.ZeroCopySupport.DisposableByteString() {
          @Override
          public com.google.protobuf.ByteString byteString() {
            return com.google.protobuf.ByteString.copyFrom(
                "0123456789".getBytes(java.nio.charset.StandardCharsets.UTF_8));
          }

          @Override
          public void close() {}
        });

    ConcurrentLinkedQueue<GoogleCloudStorageBidiReadChannel> queue =
        gcsClient.getChannelPool().getIfPresent(resourceId);

    // Verify it was successfully returned to the pool!
    assertThat(queue).isNotNull();
    assertThat(queue).isNotEmpty();
    assertThat(queue.peek()).isSameInstanceAs(channel);
    assertThat(channel.isOpen()).isTrue();
  }
}
