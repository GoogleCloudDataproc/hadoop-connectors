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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.gcs.analyticscore.client.GcsFileInfo;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystem;
import com.google.cloud.gcs.analyticscore.client.GcsItemId;
import com.google.cloud.gcs.analyticscore.core.GoogleCloudStorageInputStream;
import com.google.cloud.hadoop.gcsio.FileInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageItemInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.cloud.hadoop.util.ITraceFactory;
import com.google.cloud.hadoop.util.ITraceOperation;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.IntFunction;
import org.apache.hadoop.fs.FileRange;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.statistics.DurationTracker;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class GoogleHadoopFSInputStreamTest {

  // TODO(user): Add test cases for when isAnalyticsCoreEnabled is false in a follow-up PR.

  private AutoCloseable mocks;

  @Mock private GoogleHadoopFileSystem mockGhfs;
  @Mock private GoogleCloudStorageFileSystem mockGcsFs;
  @Mock private GcsFileSystem mockAnalyticsGcsFs;
  @Mock private GoogleCloudStorageInputStream mockAnalyticsInputStream;
  @Mock private GhfsInstrumentation mockInstrumentation;
  @Mock private GhfsInputStreamStatistics mockInputStreamStatistics;
  @Mock private GoogleCloudStorageFileSystemOptions mockOptions;
  @Mock private GoogleCloudStorageOptions mockCloudStorageOptions;
  @Mock private GoogleCloudStorageReadOptions mockReadChannelOptions;
  @Mock private DurationTracker mockDurationTracker;
  @Mock private ITraceFactory mockTraceFactory;
  @Mock private ITraceOperation mockTraceOperation;

  private URI gcsPath;
  private FileInfo fileInfo;
  private FileSystem.Statistics statistics;
  private final GhfsGlobalStorageStatistics globalStorageStatistics =
      new GhfsGlobalStorageStatistics();

  @Before
  public void setUp() {
    gcsPath = URI.create("gs://test-bucket/test-file");
    fileInfo =
        FileInfo.fromItemInfo(
            GoogleCloudStorageItemInfo.createObject(
                new StorageResourceId("test-bucket", "test-file", /* generationId= */ 1L),
                /* creationTime= */ 0L,
                /* modificationTime= */ 0L,
                /* size= */ 1000L,
                /* contentType= */ null,
                /* contentEncoding= */ null,
                /* metadata= */ null,
                /* contentGeneration= */ 1L,
                /* metaGeneration= */ 1L,
                /* verificationAttributes= */ null));

    mocks = MockitoAnnotations.openMocks(this);

    // Real instance because FileSystem.Statistics is final
    statistics = FileSystem.getStatistics("gs", GoogleHadoopFileSystem.class);

    // Stubbing the GCS FS and Instrumentation
    when(mockGhfs.getGcsFs()).thenReturn(mockGcsFs);
    when(mockGhfs.getInstrumentation()).thenReturn(mockInstrumentation);
    when(mockInstrumentation.newInputStreamStatistics(statistics))
        .thenReturn(mockInputStreamStatistics);
    when(mockGhfs.getGlobalGcsStorageStatistics()).thenReturn(globalStorageStatistics);
    when(mockGhfs.getTraceFactory()).thenReturn(mockTraceFactory);
    when(mockTraceFactory.createRootWithLogging(any(), any())).thenReturn(mockTraceOperation);

    // Stubbing the options chain to avoid NullPointerException
    when(mockGcsFs.getOptions()).thenReturn(mockOptions);
    when(mockOptions.getCloudStorageOptions()).thenReturn(mockCloudStorageOptions);
    when(mockCloudStorageOptions.getReadChannelOptions()).thenReturn(mockReadChannelOptions);
    // fast-fail on not found is enabled by default
    when(mockReadChannelOptions.isFastFailOnNotFoundEnabled()).thenReturn(true);
    when(mockInputStreamStatistics.trackDuration(any(String.class), anyLong()))
        .thenReturn(mockDurationTracker);
    try {
      when(mockInputStreamStatistics.trackDuration(any(String.class)))
          .thenReturn(mockDurationTracker);
    } catch (Exception e) {
      // If method doesn't exist, ignore
    }
  }

  @After
  public void tearDown() throws Exception {
    if (mocks != null) {
      mocks.close();
    }
  }

  @Test
  public void create_withAnalyticsCoreEnabled_returnsInputStreamWithAnalyticsChannel()
      throws IOException {
    when(mockGhfs.isAnalyticsCoreEnabled()).thenReturn(true);
    when(mockGhfs.getAnalyticsCoreGcsFs()).thenReturn(mockAnalyticsGcsFs);
    when(mockGcsFs.getFileInfoObject(gcsPath)).thenReturn(fileInfo);

    // Instance mocking instead of static mocking
    when(mockGhfs.createAnalyticsCoreInputStream(any(GcsFileInfo.class)))
        .thenReturn(mockAnalyticsInputStream);

    GoogleHadoopFSInputStream inputStream =
        GoogleHadoopFSInputStream.create(mockGhfs, gcsPath, statistics);

    assertThat(inputStream).isNotNull();
  }

  @Test
  public void readVectored_withAnalyticsCoreEnabled_delegatesToAnalyticsChannel()
      throws IOException {
    when(mockGhfs.isAnalyticsCoreEnabled()).thenReturn(true);
    when(mockGhfs.getAnalyticsCoreGcsFs()).thenReturn(mockAnalyticsGcsFs);
    when(mockGcsFs.getFileInfoObject(gcsPath)).thenReturn(fileInfo);
    when(mockGhfs.createAnalyticsCoreInputStream(any(GcsFileInfo.class)))
        .thenReturn(mockAnalyticsInputStream);
    GoogleHadoopFSInputStream inputStream =
        GoogleHadoopFSInputStream.create(mockGhfs, gcsPath, statistics);
    FileRange range = mock(FileRange.class);
    when(range.getOffset()).thenReturn(0L);
    when(range.getLength()).thenReturn(100);
    // Ensure non-null CompletableFuture to avoid NPE in range metadata construction
    lenient().when(range.getData()).thenReturn(new java.util.concurrent.CompletableFuture<>());
    List<FileRange> ranges = Arrays.asList(range);
    IntFunction<ByteBuffer> allocate = ByteBuffer::allocate;

    inputStream.readVectored(ranges, allocate);

    verify(mockAnalyticsInputStream).readVectored(anyList(), eq(allocate));
  }

  @Test
  public void readFully_withAnalyticsCoreEnabled_delegatesToAnalyticsChannel() throws IOException {
    when(mockGhfs.isAnalyticsCoreEnabled()).thenReturn(true);
    when(mockGhfs.getAnalyticsCoreGcsFs()).thenReturn(mockAnalyticsGcsFs);
    when(mockGcsFs.getFileInfoObject(gcsPath)).thenReturn(fileInfo);
    when(mockGhfs.createAnalyticsCoreInputStream(any(GcsFileInfo.class)))
        .thenReturn(mockAnalyticsInputStream);
    GoogleHadoopFSInputStream inputStream =
        GoogleHadoopFSInputStream.create(mockGhfs, gcsPath, statistics);
    byte[] buffer = new byte[100];

    inputStream.readFully(100L, buffer, 0, 100);

    verify(mockAnalyticsInputStream).readFully(100L, buffer, 0, 100);
  }

  @Test
  public void read_withAnalyticsCoreEnabled_delegatesToAnalyticsChannel() throws IOException {
    when(mockGhfs.isAnalyticsCoreEnabled()).thenReturn(true);
    when(mockGhfs.getAnalyticsCoreGcsFs()).thenReturn(mockAnalyticsGcsFs);
    when(mockGcsFs.getFileInfoObject(gcsPath)).thenReturn(fileInfo);
    when(mockGhfs.createAnalyticsCoreInputStream(any(GcsFileInfo.class)))
        .thenReturn(mockAnalyticsInputStream);
    GoogleHadoopFSInputStream inputStream =
        GoogleHadoopFSInputStream.create(mockGhfs, gcsPath, statistics);
    when(mockAnalyticsInputStream.read(any(ByteBuffer.class)))
        .thenAnswer(
            invocation -> {
              ByteBuffer bb = invocation.getArgument(0);
              bb.put((byte) 42);
              return 1;
            });

    int result = inputStream.read();

    assertThat(result).isEqualTo(42);
    verify(mockAnalyticsInputStream).read(any(ByteBuffer.class));
  }

  @Test
  public void readBuffer_withAnalyticsCoreEnabled_delegatesToAnalyticsChannel() throws IOException {
    when(mockGhfs.isAnalyticsCoreEnabled()).thenReturn(true);
    when(mockGhfs.getAnalyticsCoreGcsFs()).thenReturn(mockAnalyticsGcsFs);
    when(mockGcsFs.getFileInfoObject(gcsPath)).thenReturn(fileInfo);
    when(mockGhfs.createAnalyticsCoreInputStream(any(GcsFileInfo.class)))
        .thenReturn(mockAnalyticsInputStream);
    GoogleHadoopFSInputStream inputStream =
        GoogleHadoopFSInputStream.create(mockGhfs, gcsPath, statistics);
    byte[] buf = new byte[10];
    when(mockAnalyticsInputStream.read(any(ByteBuffer.class)))
        .thenAnswer(
            invocation -> {
              ByteBuffer bb = invocation.getArgument(0);
              bb.put(new byte[] {1, 2, 3});
              return 3;
            });

    int numRead = inputStream.read(buf, 0, 10);

    assertThat(numRead).isEqualTo(3);
    assertThat(buf[0]).isEqualTo((byte) 1);
    assertThat(buf[1]).isEqualTo((byte) 2);
    assertThat(buf[2]).isEqualTo((byte) 3);
    verify(mockAnalyticsInputStream).read(any(ByteBuffer.class));
  }

  @Test
  public void close_withAnalyticsCoreEnabled_closesAnalyticsChannel() throws IOException {
    when(mockGhfs.isAnalyticsCoreEnabled()).thenReturn(true);
    when(mockGhfs.getAnalyticsCoreGcsFs()).thenReturn(mockAnalyticsGcsFs);
    when(mockGcsFs.getFileInfoObject(gcsPath)).thenReturn(fileInfo);
    when(mockGhfs.createAnalyticsCoreInputStream(any(GcsFileInfo.class)))
        .thenReturn(mockAnalyticsInputStream);
    when(mockInputStreamStatistics.trackDuration(any(String.class), anyLong()))
        .thenReturn(mockDurationTracker);
    GoogleHadoopFSInputStream inputStream =
        GoogleHadoopFSInputStream.create(mockGhfs, gcsPath, statistics);

    inputStream.close();

    verify(mockAnalyticsInputStream).close();
  }

  @Test
  public void create_withAnalyticsCoreEnabled_fastFailEnabled_requestsMetadataAndUsesIt()
      throws IOException {
    when(mockGhfs.isAnalyticsCoreEnabled()).thenReturn(true);
    when(mockGcsFs.getFileInfoObject(gcsPath)).thenReturn(fileInfo);
    when(mockGhfs.createAnalyticsCoreInputStream(any(GcsFileInfo.class)))
        .thenReturn(mockAnalyticsInputStream);

    GoogleHadoopFSInputStream inputStream =
        GoogleHadoopFSInputStream.create(mockGhfs, gcsPath, statistics);

    assertThat(inputStream).isNotNull();
    verify(mockGcsFs).getFileInfoObject(gcsPath);
    ArgumentCaptor<GcsFileInfo> gcsFileInfoCaptor = ArgumentCaptor.forClass(GcsFileInfo.class);
    verify(mockGhfs).createAnalyticsCoreInputStream(gcsFileInfoCaptor.capture());
    GcsFileInfo gcsFileInfo = gcsFileInfoCaptor.getValue();
    assertThat(gcsFileInfo.getUri()).isEqualTo(gcsPath);
    assertThat(gcsFileInfo.getItemInfo().getSize()).isEqualTo(1000L);
    assertThat(gcsFileInfo.getItemInfo().getItemId().getBucketName()).isEqualTo("test-bucket");
    assertThat(gcsFileInfo.getItemInfo().getItemId().getObjectName())
        .isEqualTo(Optional.of("test-file"));
    assertThat(gcsFileInfo.getItemInfo().getItemId().getContentGeneration())
        .isEqualTo(Optional.of(1L));
    verify(mockGhfs, never()).createAnalyticsCoreInputStream(any(GcsItemId.class));
  }

  @Test
  public void create_withAnalyticsCoreEnabled_fastFailEnabled_missingObject_throwsFileNotFound()
      throws IOException {
    when(mockGhfs.isAnalyticsCoreEnabled()).thenReturn(true);
    when(mockGcsFs.getFileInfoObject(gcsPath))
        .thenReturn(
            FileInfo.fromItemInfo(
                GoogleCloudStorageItemInfo.createNotFound(
                    new StorageResourceId("test-bucket", "test-file"))));

    FileNotFoundException thrown =
        assertThrows(
            FileNotFoundException.class,
            () -> GoogleHadoopFSInputStream.create(mockGhfs, gcsPath, statistics));

    assertThat(thrown).hasMessageThat().contains(gcsPath.toString());
    verify(mockGhfs, never()).createAnalyticsCoreInputStream(any(GcsFileInfo.class));
    verify(mockGhfs, never()).createAnalyticsCoreInputStream(any(GcsItemId.class));
  }

  @Test
  public void create_withAnalyticsCoreEnabled_fastFailDisabled_doesNotRequestMetadata()
      throws IOException {
    when(mockGhfs.isAnalyticsCoreEnabled()).thenReturn(true);
    when(mockReadChannelOptions.isFastFailOnNotFoundEnabled()).thenReturn(false);
    when(mockGhfs.createAnalyticsCoreInputStream(any(GcsItemId.class)))
        .thenReturn(mockAnalyticsInputStream);

    GoogleHadoopFSInputStream inputStream =
        GoogleHadoopFSInputStream.create(mockGhfs, gcsPath, statistics);

    assertThat(inputStream).isNotNull();
    verify(mockGcsFs, never()).getFileInfoObject(any());
    ArgumentCaptor<GcsItemId> gcsItemIdCaptor = ArgumentCaptor.forClass(GcsItemId.class);
    verify(mockGhfs).createAnalyticsCoreInputStream(gcsItemIdCaptor.capture());
    GcsItemId gcsItemId = gcsItemIdCaptor.getValue();
    assertThat(gcsItemId.getBucketName()).isEqualTo("test-bucket");
    assertThat(gcsItemId.getObjectName()).isEqualTo(Optional.of("test-file"));
    assertThat(gcsItemId.getContentGeneration()).isEqualTo(Optional.empty());
    verify(mockGhfs, never()).createAnalyticsCoreInputStream(any(GcsFileInfo.class));
  }

  @Test
  public void create_withAnalyticsCoreEnabled_fastFailDisabled_directoryPath_throwsIAE()
      throws IOException {
    when(mockGhfs.isAnalyticsCoreEnabled()).thenReturn(true);
    when(mockReadChannelOptions.isFastFailOnNotFoundEnabled()).thenReturn(false);
    URI directoryPath = URI.create("gs://test-bucket/test-dir/");

    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> GoogleHadoopFSInputStream.create(mockGhfs, directoryPath, statistics));

    assertThat(thrown).hasMessageThat().contains("Cannot open a directory for reading");
    verify(mockGhfs, never()).createAnalyticsCoreInputStream(any(GcsItemId.class));
  }

  @Test
  public void seekAndRead_withAnalyticsCoreEnabled_fastFailDisabled_doNotRequestMetadata()
      throws IOException {
    when(mockGhfs.isAnalyticsCoreEnabled()).thenReturn(true);
    when(mockReadChannelOptions.isFastFailOnNotFoundEnabled()).thenReturn(false);
    when(mockGhfs.createAnalyticsCoreInputStream(any(GcsItemId.class)))
        .thenReturn(mockAnalyticsInputStream);
    when(mockAnalyticsInputStream.read(any(ByteBuffer.class)))
        .thenAnswer(
            invocation -> {
              ByteBuffer bb = invocation.getArgument(0);
              bb.put((byte) 42);
              return 1;
            });
    GoogleHadoopFSInputStream inputStream =
        GoogleHadoopFSInputStream.create(mockGhfs, gcsPath, statistics);

    // The size is unknown, so seeking can't be validated against it and must not fetch it.
    inputStream.seek(5000L);
    int result = inputStream.read();

    assertThat(result).isEqualTo(42);
    verify(mockAnalyticsInputStream).seek(5000L);
    verify(mockAnalyticsInputStream).read(any(ByteBuffer.class));
    verify(mockGcsFs, never()).getFileInfoObject(any());
  }

  @Test
  public void readVectored_withAnalyticsCoreEnabled_fastFailDisabled_doesNotRequestMetadata()
      throws IOException {
    when(mockGhfs.isAnalyticsCoreEnabled()).thenReturn(true);
    when(mockReadChannelOptions.isFastFailOnNotFoundEnabled()).thenReturn(false);
    when(mockGhfs.createAnalyticsCoreInputStream(any(GcsItemId.class)))
        .thenReturn(mockAnalyticsInputStream);
    GoogleHadoopFSInputStream inputStream =
        GoogleHadoopFSInputStream.create(mockGhfs, gcsPath, statistics);
    FileRange range = FileRange.createFileRange(5000L, 100);
    IntFunction<ByteBuffer> allocate = ByteBuffer::allocate;

    inputStream.readVectored(List.of(range), allocate);

    // The range can't be validated against the unknown size, so it is delegated as-is.
    verify(mockAnalyticsInputStream).readVectored(anyList(), eq(allocate));
    assertThat(range.getData().isCompletedExceptionally()).isFalse();
    verify(mockGcsFs, never()).getFileInfoObject(any());
  }
}
