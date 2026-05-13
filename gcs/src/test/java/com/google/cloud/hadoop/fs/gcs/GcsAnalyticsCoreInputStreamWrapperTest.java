/*
 * Copyright 2026 Google Inc.
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

package com.google.cloud.hadoop.fs.gcs;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.gcs.analyticscore.client.GcsObjectRange;
import com.google.cloud.gcs.analyticscore.core.GoogleCloudStorageInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonWritableChannelException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.IntFunction;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class GcsAnalyticsCoreInputStreamWrapperTest {

  @Mock private GoogleCloudStorageInputStream mockInputStream;

  private GcsAnalyticsCoreInputStreamWrapper adapter;
  private long size = 1000L;

  @Before
  public void setUp() {
    adapter = new GcsAnalyticsCoreInputStreamWrapper(mockInputStream, size);
  }

  @Test
  public void read_returnsBytesFromInputStream() throws IOException {
    ByteBuffer dst = ByteBuffer.allocate(100);
    when(mockInputStream.read(dst)).thenReturn(50);

    int read = adapter.read(dst);

    assertThat(read).isEqualTo(50);
    verify(mockInputStream).read(dst);
  }

  @Test
  public void read_withEmptyBuffer_returnsZero() throws IOException {
    ByteBuffer dst = ByteBuffer.allocate(0);

    int read = adapter.read(dst);

    assertThat(read).isEqualTo(0);
  }

  @Test
  public void read_whenClosed_throwsClosedChannelException() throws IOException {
    adapter.close();
    ByteBuffer dst = ByteBuffer.allocate(100);

    assertThrows(ClosedChannelException.class, () -> adapter.read(dst));
  }

  @Test
  public void write_throwsNonWritableChannelException() {
    ByteBuffer src = ByteBuffer.allocate(100);

    assertThrows(NonWritableChannelException.class, () -> adapter.write(src));
  }

  @Test
  public void position_returnsInputStreamPosition() throws IOException {
    when(mockInputStream.getPos()).thenReturn(100L);

    long pos = adapter.position();

    assertThat(pos).isEqualTo(100L);
    verify(mockInputStream).getPos();
  }

  @Test
  public void position_setsInputStreamPosition() throws IOException {
    adapter.position(500L);

    verify(mockInputStream).seek(500L);
  }

  @Test
  public void position_withInvalidPosition_throwsEOFException() {
    assertThrows(EOFException.class, () -> adapter.position(-1L));
    assertThrows(EOFException.class, () -> adapter.position(size));
    assertThrows(EOFException.class, () -> adapter.position(size + 1));
  }

  @Test
  public void size_returnsConfiguredSize() throws IOException {
    assertThat(adapter.size()).isEqualTo(size);
  }

  @Test
  public void truncate_throwsNonWritableChannelException() {
    assertThrows(NonWritableChannelException.class, () -> adapter.truncate(500L));
  }

  @Test
  public void isOpen_reflectsClosedState() throws IOException {
    assertThat(adapter.isOpen()).isTrue();

    adapter.close();

    assertThat(adapter.isOpen()).isFalse();
  }

  @Test
  public void close_delegatesToInputStream() throws IOException {
    adapter.close();

    verify(mockInputStream).close();
  }

  @Test
  public void readVectored_delegatesToInputStream() throws IOException {
    CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
    GcsObjectRange range =
        GcsObjectRange.builder().setOffset(0).setLength(100).setByteBufferFuture(future).build();
    List<GcsObjectRange> ranges = Arrays.asList(range);
    IntFunction<ByteBuffer> allocate = ByteBuffer::allocate;

    adapter.readVectored(ranges, allocate);

    verify(mockInputStream).readVectored(ranges, allocate);
  }

  @Test
  public void readVectored_withSomeInvalidRanges_completesInvalidExceptionallyAndDelegatesValid()
      throws IOException {
    CompletableFuture<ByteBuffer> validFuture = new CompletableFuture<>();
    GcsObjectRange validRange =
        GcsObjectRange.builder()
            .setOffset(0)
            .setLength(100)
            .setByteBufferFuture(validFuture)
            .build();

    CompletableFuture<ByteBuffer> invalidFuture = new CompletableFuture<>();
    GcsObjectRange invalidRange =
        GcsObjectRange.builder()
            .setOffset(950)
            .setLength(100)
            .setByteBufferFuture(invalidFuture)
            .build(); // 950 + 100 = 1050 > size (1000)

    List<GcsObjectRange> ranges = Arrays.asList(validRange, invalidRange);
    IntFunction<ByteBuffer> allocate = ByteBuffer::allocate;

    adapter.readVectored(ranges, allocate);

    // Verify valid range is delegated
    verify(mockInputStream).readVectored(Arrays.asList(validRange), allocate);

    // Verify invalid range is completed exceptionally
    assertThat(invalidFuture.isCompletedExceptionally()).isTrue();
    java.util.concurrent.ExecutionException exception =
        assertThrows(java.util.concurrent.ExecutionException.class, invalidFuture::get);
    assertThat(exception.getCause()).isInstanceOf(IOException.class);
    assertThat(exception.getCause().getMessage()).contains("Range extends beyond file size");

    // Verify valid range is NOT completed exceptionally
    assertThat(validFuture.isCompletedExceptionally()).isFalse();
  }

  @Test
  public void readVectored_withAllInvalidRanges_doesNotDelegateToInputStream() throws IOException {
    CompletableFuture<ByteBuffer> invalidFuture1 = new CompletableFuture<>();
    GcsObjectRange invalidRange1 =
        GcsObjectRange.builder()
            .setOffset(1001)
            .setLength(10)
            .setByteBufferFuture(invalidFuture1)
            .build();

    CompletableFuture<ByteBuffer> invalidFuture2 = new CompletableFuture<>();
    GcsObjectRange invalidRange2 =
        GcsObjectRange.builder()
            .setOffset(950)
            .setLength(100)
            .setByteBufferFuture(invalidFuture2)
            .build();

    List<GcsObjectRange> ranges = Arrays.asList(invalidRange1, invalidRange2);
    IntFunction<ByteBuffer> allocate = ByteBuffer::allocate;

    adapter.readVectored(ranges, allocate);

    // Verify mockInputStream.readVectored was never called
    verify(mockInputStream, never()).readVectored(anyList(), any());

    // Verify both are completed exceptionally
    assertThat(invalidFuture1.isCompletedExceptionally()).isTrue();
    assertThat(invalidFuture2.isCompletedExceptionally()).isTrue();
  }

  @Test
  public void readFully_delegatesToInputStream() throws IOException {
    byte[] buffer = new byte[100];

    adapter.readFully(100L, buffer, 0, 100);

    verify(mockInputStream).readFully(100L, buffer, 0, 100);
  }
}
