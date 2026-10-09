/*
 * Copyright 2024 Google Inc.
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

import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemTestHelper.assertObjectContent;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemTestHelper.createInMemoryGoogleHadoopFileSystem;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemTestHelper.writeObject;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.hadoop.gcsio.FileInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileRange;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.VectoredReadUtils;
import org.apache.hadoop.fs.impl.CombinedFileRange;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;

/** Unit test for readVectored implementation. */
@RunWith(JUnit4.class)
public class VectoredIOImplTest {
  private final String OBJECT_NAME = "/bar/test/object";
  // Using heap buffer for all unit Test
  // TODO: verify direct buffer as well.
  private final IntFunction<ByteBuffer> allocate = ByteBuffer::allocate;
  private final String rangeReadErrorMsgFormat =
      "Exception while reading buffer of length:%s from position: %s";
  private GoogleHadoopFileSystem ghfs;
  private GoogleCloudStorageFileSystem gcsFs;
  private VectoredReadOptions vectoredReadOptions = VectoredReadOptions.DEFAULT;
  private FileInfo fileInfo;
  private VectoredIOImpl vectoredIO;
  // stores the path of default object
  private Path path;
  private GhfsGlobalStorageStatistics ghfsStorageStatistics;
  private GhfsInputStreamStatistics streamStatistics;
  private FileSystem.Statistics statistics;
  private ConcurrentHashMap<String, Long> rangeReadThreadStats;

  @Before
  public void before() throws Exception {
    this.ghfs = createInMemoryGoogleHadoopFileSystem();
    // Write a file which will be used across test case to validate the ranges reads
    this.path = new Path(ghfs.getUri().resolve(OBJECT_NAME));
    FSDataOutputStream outputStream = ghfs.create(path);
    writeObject(outputStream, /* partSize */ 1024, /*partCount*/ 10);
    // Update fileInfo
    FileStatus fileStatus = ghfs.getFileStatus(path);
    this.fileInfo =
        fileStatus instanceof GoogleHadoopFileStatus
            ? ((GoogleHadoopFileStatus) fileStatus).getFileInfo()
            : null;
    this.gcsFs = spy(ghfs.getGcsFs());
    this.statistics = new FileSystem.Statistics(ghfs.getScheme());
    this.ghfsStorageStatistics = new GhfsGlobalStorageStatistics();
    this.vectoredIO = new VectoredIOImpl(vectoredReadOptions, ghfsStorageStatistics);
    this.streamStatistics = ghfs.getInstrumentation().newInputStreamStatistics(statistics);
    this.rangeReadThreadStats = new ConcurrentHashMap<>();
  }

  @After
  public void cleanup() {
    if (vectoredIO != null) {
      vectoredIO.close();
    }
    ghfsStorageStatistics.reset();
    streamStatistics.close();
    rangeReadThreadStats = null;
  }

  @Test
  public void testInvalidRangeRequest() throws IOException {
    List<FileRange> fileRanges = new ArrayList<>();
    // valid range
    fileRanges.add(FileRange.createFileRange(/* offset */ 0, /* length */ 10));
    // invalid length
    fileRanges.add(FileRange.createFileRange(/* offset */ 11, /* length */ -50));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            vectoredIO.readVectored(
                fileRanges,
                allocate,
                gcsFs,
                fileInfo,
                fileInfo.getPath(),
                streamStatistics,
                rangeReadThreadStats));
    fileRanges.clear();

    // invalid offset
    fileRanges.add(FileRange.createFileRange(/* offset */ -1, /* length */ 50));
    assertThrows(
        EOFException.class,
        () ->
            vectoredIO.readVectored(
                fileRanges,
                allocate,
                gcsFs,
                fileInfo,
                fileInfo.getPath(),
                streamStatistics,
                rangeReadThreadStats));
  }

  @Test
  public void testDisjointRangeReads() throws Exception {
    List<FileRange> fileRanges = new ArrayList<>();
    int rangeLength = 10;
    fileRanges.add(FileRange.createFileRange(/* offset */ 0, rangeLength));
    // offset > previous range endPosition + minSeek
    fileRanges.add(
        FileRange.createFileRange(
            /* offset */ vectoredReadOptions.getMinSeekVectoredReadSize() + 10, rangeLength));
    vectoredIO.readVectored(
        fileRanges,
        allocate,
        gcsFs,
        fileInfo,
        fileInfo.getPath(),
        streamStatistics,
        rangeReadThreadStats);
    verifyRangeContent(fileRanges);
    // callCount is equal to disjointRangeRequests
    verifyGcsFsOpenCalls(/* callCount */ 2);
    assertThat(
            ghfsStorageStatistics
                .getLong(GhfsStatistic.STREAM_READ_VECTORED_EXTRA_READ_BYTES.name())
                .longValue())
        .isEqualTo(0);
    assertThat(ghfsStorageStatistics.getLong(GhfsStatistic.STREAM_READ_BYTES.getSymbol()))
        .isEqualTo(rangeLength * 2);
    assertThat(streamStatistics.getBytesRead()).isEqualTo(rangeLength * 2);

    assertThat(
            ghfsStorageStatistics.getLong(
                GhfsStatistic.STREAM_READ_VECTORED_READ_INCOMING_RANGES.getSymbol()))
        .isEqualTo(2);

    assertThat(
            ghfsStorageStatistics.getLong(
                GhfsStatistic.STREAM_READ_VECTORED_READ_COMBINED_RANGES.getSymbol()))
        .isEqualTo(2);
  }

  @Test
  public void testMergedRangeReads() throws Exception {
    List<FileRange> fileRanges = new ArrayList<>();
    long offset = 0;
    int rangeLength = 10;
    fileRanges.add(FileRange.createFileRange(offset, rangeLength));
    offset += rangeLength;
    int discardedBytes = vectoredReadOptions.getMinSeekVectoredReadSize() - 1;
    offset += discardedBytes;
    fileRanges.add(FileRange.createFileRange(offset, rangeLength));
    vectoredIO.readVectored(
        fileRanges,
        allocate,
        gcsFs,
        fileInfo,
        fileInfo.getPath(),
        streamStatistics,
        rangeReadThreadStats);
    verifyRangeContent(fileRanges);
    // Ranges are merged, data is read from single channel
    verifyGcsFsOpenCalls(/* callCount */ 1);
    assertThat(
            ghfsStorageStatistics.getLong(
                GhfsStatistic.STREAM_READ_VECTORED_EXTRA_READ_BYTES.getSymbol()))
        .isEqualTo(discardedBytes);

    assertThat(ghfsStorageStatistics.getLong(GhfsStatistic.STREAM_READ_BYTES.getSymbol()))
        .isEqualTo(rangeLength * 2);
    assertThat(streamStatistics.getBytesRead()).isEqualTo(rangeLength * 2);

    assertThat(
            ghfsStorageStatistics.getLong(
                GhfsStatistic.STREAM_READ_VECTORED_READ_INCOMING_RANGES.getSymbol()))
        .isEqualTo(2);

    assertThat(
            ghfsStorageStatistics.getLong(
                GhfsStatistic.STREAM_READ_VECTORED_READ_COMBINED_RANGES.getSymbol()))
        .isEqualTo(1);
  }

  @Test
  public void error_disjoint_range() throws Exception {
    List<FileRange> fileRanges = new ArrayList<>();
    // valid range
    fileRanges.add(FileRange.createFileRange(/* offset */ 0, /* length */ 10));
    fileRanges.add(
        FileRange.createFileRange(
            /* offset */ vectoredReadOptions.getMinSeekVectoredReadSize() + 10, /* length */ 10));

    GoogleCloudStorageFileSystem mockedGcsFs = mock(GoogleCloudStorageFileSystem.class);
    ArgumentCaptor<GoogleCloudStorageReadOptions> readOptionsArgumentCaptor =
        ArgumentCaptor.forClass(GoogleCloudStorageReadOptions.class);
    ArgumentCaptor<FileInfo> fileInfoArgumentCaptor = ArgumentCaptor.forClass(FileInfo.class);

    when(mockedGcsFs.getOptions()).thenReturn(GoogleCloudStorageFileSystemOptions.DEFAULT);
    // for various calls return various objects
    when(mockedGcsFs.open(fileInfoArgumentCaptor.capture(), readOptionsArgumentCaptor.capture()))
        .thenReturn(new MockedReadChannel(), new MockedReadChannel());

    vectoredIO.readVectored(
        fileRanges,
        allocate,
        mockedGcsFs,
        fileInfo,
        fileInfo.getPath(),
        streamStatistics,
        rangeReadThreadStats);
    verifyRangeException(fileRanges);

    verify(mockedGcsFs, times(2)).open((FileInfo) any(), any());

    assertTrue(readOptionsArgumentCaptor.getValue().isReadExactRequestedBytesEnabled());
    assertThat(fileInfoArgumentCaptor.getValue().getPath()).isEqualTo(fileInfo.getPath());
  }

  @Test
  public void error_merged_range() throws Exception {
    List<FileRange> fileRanges = new ArrayList<>();
    // valid range
    fileRanges.add(FileRange.createFileRange(/* offset */ 0, /* length */ 10));
    fileRanges.add(
        FileRange.createFileRange(
            /* offset */ vectoredReadOptions.getMinSeekVectoredReadSize() - 2, /* length */ 10));

    GoogleCloudStorageFileSystem mockedGcsFs = mock(GoogleCloudStorageFileSystem.class);
    ArgumentCaptor<GoogleCloudStorageReadOptions> readOptionsArgumentCaptor =
        ArgumentCaptor.forClass(GoogleCloudStorageReadOptions.class);
    ArgumentCaptor<FileInfo> fileInfoArgumentCaptor = ArgumentCaptor.forClass(FileInfo.class);

    when(mockedGcsFs.getOptions()).thenReturn(GoogleCloudStorageFileSystemOptions.DEFAULT);
    // for various calls return various objects
    when(mockedGcsFs.open(fileInfoArgumentCaptor.capture(), readOptionsArgumentCaptor.capture()))
        .thenReturn(new MockedReadChannel());

    vectoredIO.readVectored(
        fileRanges,
        allocate,
        mockedGcsFs,
        fileInfo,
        fileInfo.getPath(),
        streamStatistics,
        rangeReadThreadStats);

    verifyRangeException(fileRanges);

    verify(mockedGcsFs, times(1)).open((FileInfo) any(), any());

    assertTrue(readOptionsArgumentCaptor.getValue().isReadExactRequestedBytesEnabled());
    assertFalse(
        mockedGcsFs
            .getOptions()
            .getCloudStorageOptions()
            .getReadChannelOptions()
            .isReadExactRequestedBytesEnabled());
    assertThat(fileInfoArgumentCaptor.getValue().getPath()).isEqualTo(fileInfo.getPath());
  }

  @Test
  public void overlappingRangeTest() {
    List<FileRange> fileRanges = new ArrayList<>();
    // overlapping range
    fileRanges.add(FileRange.createFileRange(/* offset */ 0, /* length */ 10));
    fileRanges.add(FileRange.createFileRange(/* offset */ 5, /* length */ 10));
    Throwable e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                vectoredIO.readVectored(
                    fileRanges,
                    allocate,
                    gcsFs,
                    fileInfo,
                    fileInfo.getPath(),
                    streamStatistics,
                    rangeReadThreadStats));
    assertThat((e.getMessage())).contains("overlapping");
  }

  /**
   * Ranges should have been merged based on minSeek value but still denied because merged range
   * size is beyond maxMergedRange.
   */
  @Test
  public void range_merge_denied() throws Exception {
    List<FileRange> fileRanges = new ArrayList<>();
    // first two ranges should be merged
    // third range shouldn't be merged as it will cause overall length > mergeRangeMaxSize
    fileRanges.add(FileRange.createFileRange(/* offset */ 0, /* length */ 10)); // length = 10;
    fileRanges.add(FileRange.createFileRange(/* offset */ 11, /* length */ 10)); //
    fileRanges.add(
        FileRange.createFileRange(
            /* offset */ 22, /* length */ vectoredReadOptions.getMergeRangeMaxSize()));

    List<CombinedFileRange> expectedCombinedRanges =
        VectoredReadUtils.mergeSortedRanges(
            fileRanges,
            1,
            vectoredReadOptions.getMinSeekVectoredReadSize(),
            vectoredReadOptions.getMergeRangeMaxSize());
    assertThat(expectedCombinedRanges.size()).isEqualTo(2);

    GoogleCloudStorageFileSystem mockedGcsFs = mock(GoogleCloudStorageFileSystem.class);
    when(mockedGcsFs.getOptions()).thenReturn(GoogleCloudStorageFileSystemOptions.DEFAULT);
    // for various calls return various objects
    MockedReadChannel channel1 = spy(new MockedReadChannel());
    MockedReadChannel channel2 = spy(new MockedReadChannel());
    when(mockedGcsFs.open((FileInfo) any(), any())).thenReturn(channel1).thenReturn(channel2);

    // Using just 1 thread will put ordering in execution of rangeRequest and make it easier to
    // verify mocks ( and also avoid flakiness of test)
    VectoredIOImpl vectoredIO =
        new VectoredIOImpl(
            vectoredReadOptions.toBuilder().setReadThreads(1).build(), ghfsStorageStatistics);
    vectoredIO.readVectored(
        fileRanges,
        allocate,
        mockedGcsFs,
        fileInfo,
        fileInfo.getPath(),
        streamStatistics,
        rangeReadThreadStats);
    verifyRangeException(fileRanges);

    // open is called only as per combinedRange and not as per request FileRange
    verify(mockedGcsFs, times(expectedCombinedRanges.size())).open((FileInfo) any(), any());

    assertThat(channel1.position()).isEqualTo(expectedCombinedRanges.get(0).getOffset());
    assertThat(channel2.position()).isEqualTo(expectedCombinedRanges.get(1).getOffset());

    assertThat(
            ghfsStorageStatistics
                .getLong(GhfsStatistic.STREAM_READ_VECTORED_EXTRA_READ_BYTES.name())
                .longValue())
        .isEqualTo(0);
  }

  @Test
  public void verifyRangeSorting() {
    List<FileRange> fileRanges = new ArrayList<>();
    fileRanges.add(FileRange.createFileRange(/* offset */ 22, /* length */ 2));
    fileRanges.add(FileRange.createFileRange(/* offset */ 18, /* length */ 2));
    fileRanges.add(FileRange.createFileRange(/* offset */ 16, /* length */ 2));
    fileRanges.add(FileRange.createFileRange(/* offset */ 1, /* length */ 2));
    fileRanges.add(FileRange.createFileRange(/* offset */ 9, /* length */ 2));
    fileRanges.add(FileRange.createFileRange(/* offset */ 3, /* length */ 2));
    List<? extends FileRange> sortedRanges =
        vectoredIO.validateNonOverlappingAndReturnSortedRanges(fileRanges);
    FileRange prev = null;
    for (FileRange current : sortedRanges) {
      if (prev != null) {
        assertThat(fileRanges).contains(current);
        assertThat(current.getOffset()).isGreaterThan(prev.getOffset());
      }
      prev = current;
    }
  }

  @Test
  public void verifyRequestedRangeSizeMetric() {
    List<FileRange> fileRanges = new ArrayList<>();
    fileRanges.add(FileRange.createFileRange(/* offset */ 0, /* length */ 1));
    fileRanges.add(FileRange.createFileRange(/* offset */ 10, /* length */ 2));
    fileRanges.add(FileRange.createFileRange(/* offset */ 20, /* length */ 3));
    fileRanges.add(FileRange.createFileRange(/* offset */ 30, /* length */ 4));
    fileRanges.add(FileRange.createFileRange(/* offset */ 40, /* length */ 5));
    fileRanges.add(FileRange.createFileRange(/* offset */ 50, /* length */ 6));
  }

  @Test
  public void rangeOverFlowMergedRange() throws Exception {
    List<FileRange> fileRanges = new ArrayList<>();
    // Ranges should be merged together
    int rangeLength = 5;
    int offset = (int) fileInfo.getSize() - 1 - rangeLength;
    FileRange validRange = FileRange.createFileRange(offset, rangeLength);
    fileRanges.add(validRange);
    int discardedBytes = 5;
    offset += rangeLength + discardedBytes;
    FileRange overFlowRange = FileRange.createFileRange(offset, rangeLength);
    fileRanges.add(overFlowRange);

    vectoredIO.readVectored(
        fileRanges,
        allocate,
        gcsFs,
        fileInfo,
        fileInfo.getPath(),
        streamStatistics,
        rangeReadThreadStats);
    verifyRangeContent(Arrays.asList(validRange));
    verifyRangeException(Arrays.asList(overFlowRange));
    assertThat(
            ghfsStorageStatistics.getLong(
                GhfsStatistic.STREAM_READ_VECTORED_EXTRA_READ_BYTES.getSymbol()))
        .isEqualTo(discardedBytes);

    assertThat(ghfsStorageStatistics.getLong(GhfsStatistic.STREAM_READ_BYTES.getSymbol()))
        .isEqualTo(validRange.getLength());
    assertThat(streamStatistics.getBytesRead()).isEqualTo(validRange.getLength());

    assertThat(
            ghfsStorageStatistics.getLong(
                GhfsStatistic.STREAM_READ_VECTORED_READ_INCOMING_RANGES.getSymbol()))
        .isEqualTo(2);

    assertThat(
            ghfsStorageStatistics.getLong(
                GhfsStatistic.STREAM_READ_VECTORED_READ_COMBINED_RANGES.getSymbol()))
        .isEqualTo(1);
  }

  @Test
  public void rangeOverFlowSingleRange() throws Exception {
    List<FileRange> fileRanges = new ArrayList<>();
    int rangeLength = 5;
    int offset = (int) fileInfo.getSize() - 1;
    FileRange overFlowRange = FileRange.createFileRange(offset, rangeLength);
    fileRanges.add(overFlowRange);
    vectoredIO.readVectored(
        fileRanges,
        allocate,
        gcsFs,
        fileInfo,
        fileInfo.getPath(),
        streamStatistics,
        rangeReadThreadStats);

    verifyRangeException(fileRanges);

    assertThat(ghfsStorageStatistics.getLong(GhfsStatistic.STREAM_READ_BYTES.getSymbol()))
        .isEqualTo(0);
    assertThat(streamStatistics.getBytesRead()).isEqualTo(0);
    assertThat(
            ghfsStorageStatistics.getLong(
                GhfsStatistic.STREAM_READ_VECTORED_READ_INCOMING_RANGES.getSymbol()))
        .isEqualTo(1);

    assertThat(
            ghfsStorageStatistics.getLong(
                GhfsStatistic.STREAM_READ_VECTORED_READ_COMBINED_RANGES.getSymbol()))
        .isEqualTo(1);
  }

  private void verifyRangeContent(List<FileRange> fileRanges) throws Exception {
    for (FileRange range : fileRanges) {
      ByteBuffer result = range.getData().get(1, TimeUnit.MINUTES);
      assertObjectContent(ghfs, path, result.duplicate(), range.getOffset());
    }
  }

  private void verifyRangeException(List<FileRange> fileRanges) {
    for (FileRange range : fileRanges) {
      Throwable e =
          assertThrows(ExecutionException.class, () -> range.getData().get(1, TimeUnit.MINUTES));
      assertThat(e.getCause()).isInstanceOf(IOException.class);
    }
  }

  private class MockedReadChannel implements SeekableByteChannel {
    private long position = 0;
    private boolean isOpen = true;

    @Override
    public int read(ByteBuffer dst) throws IOException {
      throw new IOException(
          String.format(rangeReadErrorMsgFormat, dst.limit() - dst.position(), position));
    }

    @Override
    public long position() {
      return position;
    }

    @Override
    public SeekableByteChannel position(long newPosition) {
      position = newPosition;
      return this;
    }

    @Override
    public long size() {
      return 0;
    }

    @Override
    public SeekableByteChannel truncate(long size) throws IOException {
      throw new UnsupportedOperationException("Cannot mutate read-only channel");
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
      throw new UnsupportedOperationException("Cannot mutate read-only channel");
    }

    @Override
    public boolean isOpen() {
      return isOpen;
    }

    @Override
    public void close() {
      isOpen = false;
    }
  }

  private void verifyGcsFsOpenCalls(int callCount) throws IOException {
    verify(gcsFs, times(callCount)).open((FileInfo) any(), any());
  }
}
