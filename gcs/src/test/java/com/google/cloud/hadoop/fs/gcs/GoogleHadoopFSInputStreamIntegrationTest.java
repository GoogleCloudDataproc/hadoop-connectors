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
import static com.google.common.truth.Truth.assertThat;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.SUFFIX_FAILURES;
import static org.junit.Assert.assertThrows;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemIntegrationHelper;
import java.io.EOFException;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.statistics.IOStatistics;
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
    try (GoogleHadoopFSInputStream in =
        GoogleHadoopFSInputStream.create(ghfs, path, VectoredReadOptions.DEFAULT, statistics)) {
      assertThat(in.read(value, 0, 1)).isEqualTo(1);
      assertThat(statistics.getReadOps()).isEqualTo(1);
      assertThat(in.read(1, value, 1, 1)).isEqualTo(1);
      assertThat(statistics.getReadOps()).isEqualTo(2);
    }

    assertThat(value).isEqualTo(expected);
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
    GoogleHadoopFSInputStream in =
        GoogleHadoopFSInputStream.create(ghfs, path, VectoredReadOptions.DEFAULT, statistics);
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

  private static GoogleHadoopFSInputStream createGhfsInputStream(
      GoogleHadoopFileSystem ghfs, URI path) throws IOException {
    return GoogleHadoopFSInputStream.create(
        ghfs, path, VectoredReadOptions.DEFAULT, new FileSystem.Statistics(ghfs.getScheme()));
  }
}
