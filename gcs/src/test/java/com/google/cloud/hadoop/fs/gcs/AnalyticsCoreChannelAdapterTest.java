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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.gcs.analyticscore.client.GcsObjectRange;
import com.google.cloud.gcs.analyticscore.core.GoogleCloudStorageInputStream;
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
public class AnalyticsCoreChannelAdapterTest {

  @Mock private GoogleCloudStorageInputStream mockInputStream;

  private AnalyticsCoreChannelAdapter adapter;
  private long size = 1000L;

  @Before
  public void setUp() {
    adapter = new AnalyticsCoreChannelAdapter(mockInputStream, size);
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
    assertThrows(java.io.EOFException.class, () -> adapter.position(-1L));
    assertThrows(java.io.EOFException.class, () -> adapter.position(size));
    assertThrows(java.io.EOFException.class, () -> adapter.position(size + 1));
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
  public void readFully_delegatesToInputStream() throws IOException {
    byte[] buffer = new byte[100];
    adapter.readFully(100L, buffer, 0, 100);
    verify(mockInputStream).readFully(100L, buffer, 0, 100);
  }
}
