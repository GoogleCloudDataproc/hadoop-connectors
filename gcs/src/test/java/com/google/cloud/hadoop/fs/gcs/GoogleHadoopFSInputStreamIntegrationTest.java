/*
 * Copyright 2019 Google LLC
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

import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.STREAM_READ_BYTES;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.STREAM_READ_CLOSE_OPERATIONS;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.STREAM_READ_OPERATIONS;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.STREAM_READ_OPERATIONS_INCOMPLETE;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.STREAM_READ_SEEK_BACKWARD_OPERATIONS;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.STREAM_READ_SEEK_BYTES_BACKWARDS;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.STREAM_READ_SEEK_BYTES_SKIPPED;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.STREAM_READ_SEEK_FORWARD_OPERATIONS;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.STREAM_READ_SEEK_OPERATIONS;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.STREAM_READ_VECTORED_OPERATIONS;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemTestHelper.assertByteBuffers;
import static com.google.common.truth.Truth.assertThat;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.SUFFIX_FAILURES;
import static org.junit.Assert.assertThrows;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemIntegrationHelper;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics;
import java.io.EOFException;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.fs.FileRange;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.util.functional.FutureIO;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GoogleHadoopFSInputStreamIntegrationTest {

  private static GoogleCloudStorageFileSystemIntegrationHelper gcsFsIHelper;

  @BeforeClass
  public static void beforeClass() throws Exception {
    gcsFsIHelper = GoogleCloudStorageFileSystemIntegrationHelper.create();
    gcsFsIHelper.beforeAllTests();
  }

  @AfterClass
  public static void afterClass() {
    gcsFsIHelper.afterAllTests();
  }

  @Test
  public void testBidiVectoredRead() throws Exception {
    URI path = gcsFsIHelper.getUniqueObjectUri(getClass(), "testBidiVectoredRead");

    String testContent = "test content";
    gcsFsIHelper.writeTextFile(path, testContent);

    List<FileRange> ranges = new ArrayList<>();
    ranges.add(FileRange.createFileRange(0, 5));
    ranges.add(FileRange.createFileRange(5, 6));

    try (GoogleHadoopFileSystem ghfs =
            GoogleHadoopFileSystemIntegrationHelper.createGhfs(
                path, GoogleHadoopFileSystemIntegrationHelper.getBidiTestConfiguration());
        GoogleHadoopFSInputStream in = createGhfsInputStream(ghfs, path)) {

      in.readVectored(ranges, ByteBuffer::allocate);
      validateVectoredReadResult(ranges, path);
    }
  }

  @Test
  public void seek_illegalArgument() throws Exception {
    URI path = gcsFsIHelper.getUniqueObjectUri(getClass(), "seek_illegalArgument");

    GoogleHadoopFileSystem ghfs =
        GoogleHadoopFileSystemIntegrationHelper.createGhfs(
            path, GoogleHadoopFileSystemIntegrationHelper.getTestConfig());

    String testContent = "test content";
    gcsFsIHelper.writeTextFile(path, testContent);

    GoogleHadoopFSInputStream in = createGhfsInputStream(ghfs, path);

    Throwable exception = assertThrows(EOFException.class, () -> in.seek(testContent.length()));
    assertThat(exception).hasMessageThat().contains("Invalid seek offset");
  }

  @Test
  public void read_singleBytes() throws Exception {
    URI path = gcsFsIHelper.getUniqueObjectUri(getClass(), "read_singleBytes");

    GoogleHadoopFileSystem ghfs =
        GoogleHadoopFileSystemIntegrationHelper.createGhfs(
            path, GoogleHadoopFileSystemIntegrationHelper.getTestConfig());

    String testContent = "test content";
    gcsFsIHelper.writeTextFile(path, testContent);

    byte[] value = new byte[2];
    byte[] expected = Arrays.copyOf(testContent.getBytes(StandardCharsets.UTF_8), 2);

    FileSystem.Statistics statistics = new FileSystem.Statistics(ghfs.getScheme());
    try (GoogleHadoopFSInputStream in = GoogleHadoopFSInputStream.create(ghfs, path, statistics)) {
      assertThat(in.read(value, 0, 1)).isEqualTo(1);
      assertThat(statistics.getReadOps()).isEqualTo(1);
      assertThat(in.read(1, value, 1, 1)).isEqualTo(1);
      assertThat(statistics.getReadOps()).isEqualTo(2);
    }

    assertThat(value).isEqualTo(expected);
  }

  @Test
  public void read_multiple() throws Exception {
    URI path = gcsFsIHelper.getUniqueObjectUri(getClass(), "read_singleBytes");

    GoogleHadoopFileSystem ghfs =
        GoogleHadoopFileSystemIntegrationHelper.createGhfs(
            path, GoogleHadoopFileSystemIntegrationHelper.getTestConfig());

    String testContent = "test content";
    gcsFsIHelper.writeTextFile(path, testContent);

    byte[] value = new byte[11];
    byte[] expected = Arrays.copyOf(testContent.getBytes(StandardCharsets.UTF_8), 11);

    FileSystem.Statistics statistics = new FileSystem.Statistics(ghfs.getScheme());
    try (GoogleHadoopFSInputStream in = GoogleHadoopFSInputStream.create(ghfs, path, statistics)) {
      assertThat(in.read(value, 0, 1)).isEqualTo(1);
      assertThat(statistics.getReadOps()).isEqualTo(1);
      assertThat(in.read(1, value, 1, 10)).isEqualTo(10);
      assertThat(statistics.getReadOps()).isEqualTo(2);
    }

    assertThat(statistics.getBytesRead()).isEqualTo(11);
    assertThat(value).isEqualTo(expected);
  }

  @Test
  public void testMergedRangeRequest() throws Exception {
    URI path = gcsFsIHelper.getUniqueObjectUri(getClass(), "read_mergedRange");

    GoogleHadoopFileSystem ghfs =
        GoogleHadoopFileSystemIntegrationHelper.createGhfs(
            path, GoogleHadoopFileSystemIntegrationHelper.getTestConfig());

    // overriding the default values with lower limit to test out on smaller content size.
    VectoredReadOptions vectoredReadOptions =
        VectoredReadOptions.DEFAULT.toBuilder()
            .setMinSeekVectoredReadSize(2)
            .setMergeRangeMaxSize(10)
            .build();

    String testContent = "verify vectored read ranges are getting"; // length = 40
    gcsFsIHelper.writeTextFile(path, testContent);
    FileSystem.Statistics statistics = new FileSystem.Statistics(ghfs.getScheme());
    GoogleHadoopFSInputStream in = GoogleHadoopFSInputStream.create(ghfs, path, statistics);

    List<FileRange> fileRanges = new ArrayList<>();
    int totalBytesRead = 0;
    // below two ranges will be merged
    fileRanges.add(FileRange.createFileRange(0, 5));
    totalBytesRead += 5;
    fileRanges.add(FileRange.createFileRange(6, 2)); // read till 8
    totalBytesRead += 2;

    fileRanges.add(FileRange.createFileRange(11, 4)); // read till 15
    totalBytesRead += 4;
    fileRanges.add(FileRange.createFileRange(20, 15));
    totalBytesRead += 15;

    try (GoogleHadoopFSInputStream ignore = in) {
      in.readVectored(fileRanges, ByteBuffer::allocate);
      validateVectoredReadResult(fileRanges, path);
    }

    assertThat(statistics.getBytesRead()).isEqualTo(totalBytesRead);
  }

  @Test
  public void rangeRequestBeyondFileSize() throws Exception {
    URI path = gcsFsIHelper.getUniqueObjectUri(getClass(), "read_rangeRequestBeyondFile");

    GoogleHadoopFileSystem ghfs =
        GoogleHadoopFileSystemIntegrationHelper.createGhfs(
            path, GoogleHadoopFileSystemIntegrationHelper.getTestConfig());

    String testContent = "verify vectored read ranges are getting"; // length = 40
    gcsFsIHelper.writeTextFile(path, testContent);
    FileSystem.Statistics statistics = new FileSystem.Statistics(ghfs.getScheme());
    GoogleHadoopFSInputStream in = GoogleHadoopFSInputStream.create(ghfs, path, statistics);

    List<FileRange> fileRanges = new ArrayList<>();
    // range inside file size
    FileRange fileRange1 = FileRange.createFileRange(33, 2);
    fileRanges.add(fileRange1);
    // range going beyond filesize
    FileRange fileRange2 = FileRange.createFileRange(35, 15);
    fileRanges.add(fileRange2);

    try (GoogleHadoopFSInputStream ignore = in) {
      in.readVectored(fileRanges, ByteBuffer::allocate);
    }
    byte[] readBytes = gcsFsIHelper.readFile(path, fileRange1.getOffset(), fileRange1.getLength());
    assertByteBuffers(fileRange1.getData().get(), ByteBuffer.wrap(readBytes));

    Throwable e = assertThrows(ExecutionException.class, () -> fileRange2.getData().get());
    assertThat(e.getCause()).isInstanceOf(IOException.class);
  }

  private void validateVectoredReadResult(List<FileRange> fileRanges, URI path) throws Exception {
    for (FileRange range : fileRanges) {
      ByteBuffer resBuffer = FutureIO.awaitFuture(range.getData(), 60, TimeUnit.SECONDS);
      byte[] readBytes = gcsFsIHelper.readFile(path, range.getOffset(), range.getLength());
      assertByteBuffers(resBuffer, ByteBuffer.wrap(readBytes));
    }
  }

  @Test
  public void testAvailable() throws Exception {
    URI path = gcsFsIHelper.getUniqueObjectUri(getClass(), "testAvailable");
    GoogleHadoopFileSystem ghfs =
        GoogleHadoopFileSystemIntegrationHelper.createGhfs(
            path, GoogleHadoopFileSystemIntegrationHelper.getTestConfig());

    String testContent = "test content";
    gcsFsIHelper.writeTextFile(path, testContent);

    GoogleHadoopFSInputStream in = createGhfsInputStream(ghfs, path);
    try (GoogleHadoopFSInputStream ignore = in) {
      assertThat(in.available()).isEqualTo(0);
    }

    assertThrows(IOException.class, in::available);
  }

  @Test
  public void testReadAfterClosed() throws Exception {
    URI path = gcsFsIHelper.getUniqueObjectUri(getClass(), "read_after_closed");

    GoogleHadoopFileSystem ghfs =
        GoogleHadoopFileSystemIntegrationHelper.createGhfs(
            path, GoogleHadoopFileSystemIntegrationHelper.getTestConfig());

    String testContent = "test content";
    gcsFsIHelper.writeTextFile(path, testContent);

    FileSystem.Statistics statistics = new FileSystem.Statistics(ghfs.getScheme());
    GoogleHadoopFSInputStream in = GoogleHadoopFSInputStream.create(ghfs, path, statistics);
    in.close();
    assertThrows(IOException.class, in::read);
  }

  @Test
  public void fs_operation_durationMetric_tests() throws Exception {

    URI path = gcsFsIHelper.getUniqueObjectUri(getClass(), "seek_illegalArgument");

    GoogleHadoopFileSystem ghfs =
        GoogleHadoopFileSystemIntegrationHelper.createGhfs(
            path, GoogleHadoopFileSystemIntegrationHelper.getTestConfig());

    String testContent = "test content";
    gcsFsIHelper.writeTextFile(path, testContent);

    byte[] value = new byte[2];
    byte[] expected = Arrays.copyOf(testContent.getBytes(StandardCharsets.UTF_8), 2);
    GoogleHadoopFSInputStream in = createGhfsInputStream(ghfs, path);
    assertThat(in.read(value, 0, 1)).isEqualTo(1);
    assertThat(in.read(1, value, 1, 1)).isEqualTo(1);
    assertThat(value).isEqualTo(expected);

    IOStatistics ioStats = in.getIOStatistics();
    TestUtils.verifyDurationMetric(ioStats, STREAM_READ_OPERATIONS.getSymbol(), 2);
    TestUtils.verifyDurationMetric(ioStats, STREAM_READ_SEEK_OPERATIONS.getSymbol(), 2);

    in.seek(0);

    TestUtils.verifyDurationMetric(ioStats, STREAM_READ_SEEK_OPERATIONS.getSymbol(), 3);
    in.close();

    TestUtils.verifyDurationMetric(ioStats, STREAM_READ_CLOSE_OPERATIONS.getSymbol(), 1);

    try (GoogleHadoopFSInputStream inputStream = createGhfsInputStream(ghfs, path)) {
      Throwable exception =
          assertThrows(EOFException.class, () -> inputStream.seek(testContent.length()));
      TestUtils.verifyDurationMetric(
          inputStream.getIOStatistics(), STREAM_READ_SEEK_OPERATIONS + SUFFIX_FAILURES, 1);
    }

    TestUtils.verifyDurationMetric(
        ghfs.getInstrumentation().getIOStatistics(), STREAM_READ_CLOSE_OPERATIONS.getSymbol(), 2);
    TestUtils.verifyDurationMetric(
        ghfs.getInstrumentation().getIOStatistics(), STREAM_READ_SEEK_OPERATIONS.getSymbol(), 4);

    TestUtils.verifyDurationMetric(
        ghfs.getInstrumentation().getIOStatistics(), STREAM_READ_OPERATIONS.getSymbol(), 2);
  }

  @Test
  public void fs_operation_threadLocalMetric_tests() throws Exception {
    URI path = gcsFsIHelper.getUniqueObjectUri(getClass(), "seek_illegalArgument");

    GoogleHadoopFileSystem ghfs =
        GoogleHadoopFileSystemIntegrationHelper.createGhfs(
            path, GoogleHadoopFileSystemIntegrationHelper.getTestConfig());

    GhfsGlobalStorageStatistics globalStorageStatistics = ghfs.getGlobalGcsStorageStatistics();
    GhfsThreadLocalStatistics ghfsThreadLocalStatistics =
        globalStorageStatistics.getThreadLocalStatistics();

    String testContent = "test content";
    gcsFsIHelper.writeTextFile(path, testContent);

    globalStorageStatistics.reset();
    ghfsThreadLocalStatistics.reset();

    byte[] expected = Arrays.copyOf(testContent.getBytes(StandardCharsets.UTF_8), 10);
    GoogleHadoopFSInputStream in = createGhfsInputStream(ghfs, path);
    List<FileRange> fileRanges = new ArrayList<>();
    FileRange rangeReq1 = FileRange.createFileRange(0, 10);
    fileRanges.add(rangeReq1);

    in.seek(0);
    assertThat(in.read(new byte[1], 0, 1)).isEqualTo(1);

    in.readVectored(fileRanges, ByteBuffer::allocate);
    assertThat(expected).isEqualTo(rangeReq1.getData().get(10, TimeUnit.SECONDS).array());
    in.close();

    IOStatistics ioStats = in.getIOStatistics();
    TestUtils.verifyDurationMetric(ioStats, STREAM_READ_OPERATIONS.getSymbol(), 1);
    TestUtils.verifyDurationMetric(ioStats, STREAM_READ_VECTORED_OPERATIONS.getSymbol(), 1);

    assertThat(ghfsThreadLocalStatistics.getLong("gcsApiCount")).isEqualTo(3);

    assertThat(globalStorageStatistics.getLong(STREAM_READ_OPERATIONS.getSymbol())).isEqualTo(1);
    assertThat(
            globalStorageStatistics.getLong(
                GoogleCloudStorageStatistics.GCS_GET_MEDIA_REQUEST.getSymbol()))
        .isEqualTo(2);
    assertThat(
            globalStorageStatistics.getLong(
                GoogleCloudStorageStatistics.GCS_METADATA_REQUEST.getSymbol()))
        .isEqualTo(1);
    assertThat(
            globalStorageStatistics.getLong(
                GoogleCloudStorageStatistics.GCS_API_REQUEST_COUNT.getSymbol()))
        .isEqualTo(3);
  }

  @Test
  public void operation_durationMetric_tests() throws Exception {
    URI path = gcsFsIHelper.getUniqueObjectUri(getClass(), "seek_illegalArgument");

    GoogleHadoopFileSystem ghfs =
        GoogleHadoopFileSystemIntegrationHelper.createGhfs(
            path, GoogleHadoopFileSystemIntegrationHelper.getTestConfig());
    GhfsGlobalStorageStatistics stats = TestUtils.getStorageStatistics();

    String testContent = "test content";
    gcsFsIHelper.writeTextFile(path, testContent);

    byte[] value = new byte[2];
    byte[] expected = Arrays.copyOf(testContent.getBytes(StandardCharsets.UTF_8), 2);

    GoogleHadoopFSInputStream in = createGhfsInputStream(ghfs, path);

    assertThat(in.read(value, 0, 1)).isEqualTo(1);
    assertThat(in.read(1, value, 1, 1)).isEqualTo(1);
    assertThat(value).isEqualTo(expected);

    TestUtils.verifyCounter(stats, STREAM_READ_SEEK_OPERATIONS, 0);
    TestUtils.verifyCounter(stats, STREAM_READ_SEEK_BACKWARD_OPERATIONS, 2);
    TestUtils.verifyDurationMetric(stats, STREAM_READ_OPERATIONS.getSymbol(), 0);

    in.seek(0);

    TestUtils.verifyDurationMetric(stats, STREAM_READ_SEEK_OPERATIONS.getSymbol(), 0);
    TestUtils.verifyDurationMetric(stats, STREAM_READ_SEEK_OPERATIONS.getSymbol(), 0);
    TestUtils.verifyDurationMetric(stats, STREAM_READ_OPERATIONS.getSymbol(), 0);
    TestUtils.verifyCounter(stats, STREAM_READ_SEEK_BACKWARD_OPERATIONS, 3);
    TestUtils.verifyCounter(stats, STREAM_READ_SEEK_BYTES_BACKWARDS, 2);
    TestUtils.verifyCounter(stats, STREAM_READ_SEEK_FORWARD_OPERATIONS, 0);
    TestUtils.verifyCounter(stats, STREAM_READ_SEEK_BYTES_SKIPPED, 0);

    int expectedSeek = 5;
    in.seek(expectedSeek);
    in.close();

    TestUtils.verifyCounter(stats, STREAM_READ_SEEK_FORWARD_OPERATIONS, 1);
    TestUtils.verifyCounter(stats, STREAM_READ_SEEK_BYTES_SKIPPED, expectedSeek);
    TestUtils.verifyCounter(stats, STREAM_READ_BYTES, 2);
    TestUtils.verifyCounter(stats, STREAM_READ_OPERATIONS_INCOMPLETE, 0);
    TestUtils.verifyDurationMetric(stats, STREAM_READ_CLOSE_OPERATIONS.getSymbol(), 1);
    TestUtils.verifyDurationMetric(stats, STREAM_READ_SEEK_OPERATIONS.getSymbol(), 4);
    TestUtils.verifyDurationMetric(stats, STREAM_READ_OPERATIONS.getSymbol(), 2);
  }

  @Test
  public void readVectoredMetricsTest() throws Exception {
    URI path = gcsFsIHelper.getUniqueObjectUri(getClass(), "read_mergedRange");

    GoogleHadoopFileSystem ghfs =
        GoogleHadoopFileSystemIntegrationHelper.createGhfs(
            path, GoogleHadoopFileSystemIntegrationHelper.getTestConfig());
    GhfsGlobalStorageStatistics stats = TestUtils.getStorageStatistics();

    // overriding the default values with lower limit to test out on smaller content size.
    VectoredReadOptions vectoredReadOptions =
        VectoredReadOptions.DEFAULT.toBuilder()
            .setMinSeekVectoredReadSize(2)
            .setMergeRangeMaxSize(10)
            .build();

    String testContent = "verify vectored read ranges are getting"; // length = 40
    gcsFsIHelper.writeTextFile(path, testContent);
    FileSystem.Statistics statistics = new FileSystem.Statistics(ghfs.getScheme());
    GoogleHadoopFSInputStream in = GoogleHadoopFSInputStream.create(ghfs, path, statistics);

    List<FileRange> fileRanges = new ArrayList<>();
    fileRanges.add(FileRange.createFileRange(0, 5));

    try (GoogleHadoopFSInputStream ignore = in) {
      in.readVectored(fileRanges, ByteBuffer::allocate);
      validateVectoredReadResult(fileRanges, path);
    }
    TestUtils.verifyCounter(stats, STREAM_READ_VECTORED_OPERATIONS, 1);
    TestUtils.verifyDurationMetric(stats, STREAM_READ_VECTORED_OPERATIONS.getSymbol(), 1);
  }

  private static GoogleHadoopFSInputStream createGhfsInputStream(
      GoogleHadoopFileSystem ghfs, URI path) throws IOException {
    FileSystem.Statistics statistics = new FileSystem.Statistics(ghfs.getScheme());
    return GoogleHadoopFSInputStream.create(ghfs, path, statistics);
  }
}
