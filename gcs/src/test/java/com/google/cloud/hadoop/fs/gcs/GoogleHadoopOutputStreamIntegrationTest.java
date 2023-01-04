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

import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_OUTPUT_STREAM_BUFFER_SIZE;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_OUTPUT_STREAM_PIPE_TYPE;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_OUTPUT_STREAM_SYNC_MIN_INTERVAL;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertThrows;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpStatusCodes;
import com.google.cloud.hadoop.gcsio.CreateFileOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemIntegrationHelper;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions.PipeType;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class GoogleHadoopOutputStreamIntegrationTest {

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

  @Parameterized.Parameters
  public static Collection<Object[]> getConstructorArguments() {
    return Arrays.asList(
        new Object[] {PipeType.IO_STREAM_PIPE}, new Object[] {PipeType.NIO_CHANNEL_PIPE});
  }

  private final PipeType pipeType;

  public GoogleHadoopOutputStreamIntegrationTest(PipeType pipeType) {
    this.pipeType = pipeType;
  }

  private Configuration getTestConfig() {
    Configuration conf = GoogleHadoopFileSystemIntegrationHelper.getTestConfig();
    conf.setEnum(GCS_OUTPUT_STREAM_PIPE_TYPE.getKey(), pipeType);
    return conf;
  }

  @Test
  public void write_withZeroBufferSize() throws Exception {
    URI testFile = gcsFsIHelper.getUniqueObjectUri("GHFSOutputStream_write_withZeroBufferSize");

    Configuration config = getTestConfig();
    config.setInt(GoogleHadoopFileSystemConfiguration.GCS_OUTPUT_STREAM_BUFFER_SIZE.getKey(), 0);

    GoogleHadoopFileSystem ghfs =
        GoogleHadoopFileSystemIntegrationHelper.createGhfs(testFile, config);

    AsyncWriteChannelOptions writeOptions =
        ghfs.getGcsFs().getOptions().getCloudStorageOptions().getWriteChannelOptions();
    assertThat(writeOptions.getBufferSize()).isEqualTo(0);

    try (GoogleHadoopOutputStream out =
        new GoogleHadoopOutputStream(
            ghfs,
            testFile,
            CreateFileOptions.DEFAULT,
            new FileSystem.Statistics(ghfs.getScheme()))) {
      out.write(1);
    }

    FileStatus fileStatus = ghfs.getFileStatus(ghfs.getHadoopPath(testFile));

    assertThat(fileStatus.getLen()).isEqualTo(1);
  }

  @Test
  public void hsync() throws Exception {
    URI path = gcsFsIHelper.getUniqueObjectUri("hsync");
    Path hadoopPath = new Path(path);
    FileSystem fs = GoogleHadoopFileSystemIntegrationHelper.createGhfs(path, getTestConfig());

    // test composing of 5 1-byte writes into 5-byte object
    byte[] expected = new byte[5];
    new Random().nextBytes(expected);

    try (FSDataOutputStream fout = fs.create(hadoopPath)) {
      for (int i = 0; i < expected.length; i++) {
        fout.write(expected[i]);
        fout.hsync();

        // Validate partly composed data
        int composedLength = i + 1;
        assertThat(fs.getFileStatus(hadoopPath).getLen()).isEqualTo(composedLength);
        assertThat(gcsFsIHelper.readFile(path)).isEqualTo(Arrays.copyOf(expected, composedLength));
      }
    }

    assertThat(fs.getFileStatus(hadoopPath).getLen()).isEqualTo(expected.length);
    assertThat(gcsFsIHelper.readFile(path)).isEqualTo(expected);
  }

  @Test
  public void hsync_noBuffer() throws Exception {
    URI path = gcsFsIHelper.getUniqueObjectUri("hsync_noBuffer");
    Path hadoopPath = new Path(path);

    Configuration config = getTestConfig();
    config.setInt(GCS_OUTPUT_STREAM_BUFFER_SIZE.getKey(), 0);
    FileSystem fs = GoogleHadoopFileSystemIntegrationHelper.createGhfs(path, config);

    String line1 = "hello\n";
    byte[] line1Bytes = line1.getBytes(UTF_8);
    String line2 = "world\n";
    byte[] line2Bytes = line2.getBytes(UTF_8);
    String line3 = "foobar\n";
    byte[] line3Bytes = line3.getBytes(UTF_8);

    StringBuilder expected = new StringBuilder();
    try (FSDataOutputStream out = fs.create(hadoopPath)) {
      // Write first line one byte at a time.
      for (byte b : line1Bytes) {
        out.write(b);
      }
      expected.append(line1);

      out.hsync();

      String readText = gcsFsIHelper.readTextFile(path);
      assertWithMessage("Expected line1 after first hsync()")
          .that(readText)
          .isEqualTo(expected.toString());

      // Write second line, sync() again.
      out.write(line2Bytes, 0, line2Bytes.length);
      expected.append(line2);
      out.hsync();
      readText = gcsFsIHelper.readTextFile(path);
      assertWithMessage("Expected line1 + line2 after second sync()")
          .that(readText)
          .isEqualTo(expected.toString());

      // Write third line, close() without sync().
      out.write(line3Bytes, 0, line3Bytes.length);
      expected.append(line3);
    }

    String readText = gcsFsIHelper.readTextFile(path);
    assertWithMessage("Expected line1 + line2 + line3 after close()")
        .that(readText)
        .isEqualTo(expected.toString());
  }

  @Test
  public void append_shouldAppendNewData() throws Exception {
    URI path = gcsFsIHelper.getUniqueObjectUri("append_shouldAppendNewData");
    Path hadoopPath = new Path(path);

    gcsFsIHelper.writeTextFile(path.getAuthority(), path.getPath(), "original-content");

    FileSystem ghfs = GoogleHadoopFileSystemIntegrationHelper.createGhfs(path, getTestConfig());

    // Test appending three 9-character strings to existing object using 20 bytes buffer size
    try (FSDataOutputStream os = ghfs.append(hadoopPath, 20, /* progress= */ () -> {})) {
      os.write("_append-1".getBytes(UTF_8));

      // Validate that file content didn't change after write call
      assertThat(gcsFsIHelper.readTextFile(path)).isEqualTo("original-content");

      os.hsync();

      // Validate that hsync persisted data
      assertThat(gcsFsIHelper.readTextFile(path)).isEqualTo("original-content_append-1");

      os.write("_append-2".getBytes(UTF_8));
      os.write("_append-3".getBytes(UTF_8));
    }

    String expectedContent = "original-content_append-1_append-2_append-3";

    assertThat(gcsFsIHelper.readTextFile(path)).isEqualTo(expectedContent);

    // Check if file after appending has right size
    assertThat(ghfs.getFileStatus(hadoopPath).getLen()).isEqualTo(expectedContent.length());
  }

  @Test
  public void append_shouldFail_whenFileDoesNotExist() throws Exception {
    URI path = gcsFsIHelper.getUniqueObjectUri("append_shouldFail_whenFileDoesNotExist");
    Path hadoopPath = new Path(path);

    FileSystem ghfs = GoogleHadoopFileSystemIntegrationHelper.createGhfs(path, getTestConfig());

    // Test appending three 9-character strings to existing object using 20 bytes buffer size
    FSDataOutputStream fsos = ghfs.append(hadoopPath, 20, /* progress= */ () -> {});
    fsos.write("_append-1".getBytes(UTF_8));

    GoogleJsonResponseException hsyncException =
        assertThrows(GoogleJsonResponseException.class, fsos::hsync);
    assertThat(hsyncException.getStatusCode()).isEqualTo(HttpStatusCodes.STATUS_CODE_NOT_FOUND);
    assertThat(hsyncException.getMessage()).contains(String.format(" %s ", hadoopPath.getName()));

    GoogleJsonResponseException closeException =
        assertThrows(GoogleJsonResponseException.class, fsos::close);
    assertThat(closeException.getStatusCode()).isEqualTo(HttpStatusCodes.STATUS_CODE_NOT_FOUND);
    assertThat(closeException.getMessage()).contains(String.format(" %s ", hadoopPath.getName()));

    // Validate that file wasn't created
    assertThat(ghfs.exists(hadoopPath)).isFalse();
  }

  @Test
  public void hflush_syncsEverything() throws Exception {
    URI path = gcsFsIHelper.getUniqueObjectUri("hflush_syncsEverything");
    Path hadoopPath = new Path(path);

    Configuration config = getTestConfig();
    FileSystem ghfs = GoogleHadoopFileSystemIntegrationHelper.createGhfs(path, config);

    byte[] testData = new byte[5];
    new Random().nextBytes(testData);

    try (FSDataOutputStream out = ghfs.create(hadoopPath)) {
      for (int i = 0; i < testData.length; i++) {
        out.write(testData[i]);
        out.hflush();

        // Validate partly composed data always just contain the first byte because only the
        // first hflush() succeeds and all subsequent hflush() calls should be rate limited.
        int composedLength = i + 1;
        assertThat(ghfs.getFileStatus(hadoopPath).getLen()).isEqualTo(composedLength);
        assertThat(gcsFsIHelper.readFile(path)).isEqualTo(Arrays.copyOf(testData, composedLength));
      }
    }

    // Assert that data was fully written after close
    assertThat(ghfs.getFileStatus(hadoopPath).getLen()).isEqualTo(testData.length);
    assertThat(gcsFsIHelper.readFile(path)).isEqualTo(testData);
  }

  @Test
  public void hflush_rateLimited_writesEverything() throws Exception {
    URI path = gcsFsIHelper.getUniqueObjectUri("hflush_rateLimited_writesEverything");
    Path hadoopPath = new Path(path);

    Configuration config = getTestConfig();
    config.setLong(GCS_OUTPUT_STREAM_SYNC_MIN_INTERVAL.getKey(), Duration.ofDays(1).toMillis());
    FileSystem ghfs = GoogleHadoopFileSystemIntegrationHelper.createGhfs(path, config);

    byte[] testData = new byte[10];
    new Random().nextBytes(testData);

    try (FSDataOutputStream out = ghfs.create(hadoopPath)) {
      for (byte testDataByte : testData) {
        out.write(testDataByte);
        out.hflush();

        // Validate partly composed data always just contain the first byte because only the
        // first hflush() succeeds and all subsequent hflush() calls should be rate limited.
        assertThat(ghfs.getFileStatus(hadoopPath).getLen()).isEqualTo(1);
        assertThat(gcsFsIHelper.readFile(path)).isEqualTo(new byte[] {testData[0]});
      }
    }

    // Assert that data was fully written after close
    assertThat(ghfs.getFileStatus(hadoopPath).getLen()).isEqualTo(testData.length);
    assertThat(gcsFsIHelper.readFile(path)).isEqualTo(testData);
  }
}
