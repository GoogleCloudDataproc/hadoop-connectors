/*
 * Copyright 2024 Google LLC
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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.statistics.DurationTracker;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Unit tests for GoogleHadoopFSInputStream class. */
@RunWith(JUnit4.class)
public class GoogleHadoopFSInputStreamTest {

  @Mock private GoogleHadoopFileSystem mockGhfs;
  @Mock private GoogleCloudStorageFileSystem mockGcsFs;
  @Mock private GhfsGlobalStorageStatistics mockStorageStatistics;
  @Mock private GhfsInstrumentation mockInstrumentation;
  @Mock private GhfsInputStreamStatistics mockStreamStatistics;

  private GoogleHadoopFSInputStream inputStream;
  private SeekableByteChannel mockChannel;
  private FileSystem.Statistics hadoopStatistics;

  @Before
  public void setUp() throws IOException {
    MockitoAnnotations.initMocks(this);

    hadoopStatistics = new FileSystem.Statistics("gs");
    mockChannel = mock(SeekableByteChannel.class);

    when(mockGhfs.getGcsFs()).thenReturn(mockGcsFs);
    when(mockGhfs.getGlobalGcsStorageStatistics()).thenReturn(mockStorageStatistics);
    when(mockGhfs.getInstrumentation()).thenReturn(mockInstrumentation);
    when(mockInstrumentation.newInputStreamStatistics(any())).thenReturn(mockStreamStatistics);
    when(mockStreamStatistics.trackDuration(anyString(), anyLong()))
        .thenReturn(new StubDurationTracker());
    when(mockStreamStatistics.trackDuration(anyString())).thenReturn(new StubDurationTracker());

    inputStream =
        new GoogleHadoopFSInputStream(
            mockGhfs,
            URI.create("gs://test-bucket/test-file"),
            null,
            mockChannel,
            hadoopStatistics);
  }

  @Test
  public void read_partialReadBeforeException_updatesStatistics() throws IOException {
    // Verify that data read before an exception occurs is correctly recorded in statistics.
    byte[] buffer = new byte[10];
    int offset = 0;
    int length = 10;

    // Mock channel to read 5 bytes then throw IOException
    when(mockChannel.read(any(ByteBuffer.class)))
        .thenAnswer(
            invocation -> {
              ByteBuffer bb = invocation.getArgument(0);
              bb.put(new byte[5]);
              throw new IOException("Simulated partial read failure");
            });

    assertThrows(IOException.class, () -> inputStream.read(buffer, offset, length));

    // Verify that the 5 bytes read before the exception were recorded
    assertThat(hadoopStatistics.getBytesRead()).isEqualTo(5);
  }

  private static class StubDurationTracker implements DurationTracker {
    @Override
    public void failed() {}

    @Override
    public void close() {}
  }
}
