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

package com.google.cloud.hadoop.fs.gcs;

import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_ANALYTICS_CORE_ENABLE;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_ANALYTICS_CORE_WRITE_ENABLE;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_OUTPUT_STREAM_BUFFER_SIZE;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_OUTPUT_STREAM_SYNC_MIN_INTERVAL;
import static com.google.common.truth.Truth.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertThrows;

import com.google.cloud.hadoop.gcsio.CreateFileOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemIntegrationHelper;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Integration tests for GoogleHadoopOutputStream with Analytics Core write path enabled.
 *
 * <p>TODO: In a follow up PR the existing integration tests would be parameterized to run with
 * analytics core.
 */
@RunWith(JUnit4.class)
public class GoogleHadoopOutputStreamAnalyticsIntegrationTest {

  private static GoogleCloudStorageFileSystemIntegrationHelper gcsFsIHelper;
  private static GoogleHadoopFileSystem sharedGhfs;

  @BeforeClass
  public static void beforeClass() throws Exception {
    gcsFsIHelper = GoogleCloudStorageFileSystemIntegrationHelper.create();
    gcsFsIHelper.beforeAllTests();
    URI initUri =
        gcsFsIHelper.getUniqueObjectUri(
            GoogleHadoopOutputStreamAnalyticsIntegrationTest.class, "init");
    sharedGhfs = GoogleHadoopFileSystemIntegrationHelper.createGhfs(initUri, getTestConfig());
  }

  @AfterClass
  public static void afterClass() throws IOException {
    if (sharedGhfs != null) {
      sharedGhfs.close();
    }
    if (gcsFsIHelper != null) {
      gcsFsIHelper.afterAllTests();
    }
  }

  private static Configuration getTestConfig() {
    Configuration conf = GoogleHadoopFileSystemIntegrationHelper.getTestConfig();
    conf.setBoolean(GCS_ANALYTICS_CORE_ENABLE.getKey(), true);
    conf.setBoolean(GCS_ANALYTICS_CORE_WRITE_ENABLE.getKey(), true);
    return conf;
  }

  @Test
  public void write_singleByte_writesContentCorrectly() throws Exception {
    URI path = gcsFsIHelper.getUniqueObjectUri(getClass(), "write_singleByte");
    Path hadoopPath = new Path(path);
    byte[] expected = "hello analytics core write".getBytes(UTF_8);

    try (FSDataOutputStream out = sharedGhfs.create(hadoopPath)) {
      for (byte b : expected) {
        out.write(b);
      }
    }

    assertThat(sharedGhfs.getFileStatus(hadoopPath).getLen()).isEqualTo(expected.length);
    assertThat(gcsFsIHelper.readFile(path)).isEqualTo(expected);
  }

  @Test
  public void write_byteArray_writesContentCorrectly() throws Exception {
    URI path = gcsFsIHelper.getUniqueObjectUri(getClass(), "write_byteArray");
    Path hadoopPath = new Path(path);
    byte[] expected = "hello analytics core write array".getBytes(UTF_8);

    try (FSDataOutputStream out = sharedGhfs.create(hadoopPath)) {
      out.write(expected, 0, expected.length);
    }

    assertThat(sharedGhfs.getFileStatus(hadoopPath).getLen()).isEqualTo(expected.length);
    assertThat(gcsFsIHelper.readFile(path)).isEqualTo(expected);
  }

  @Test
  public void write_byteArrayWithOffsetAndLength_writesContentCorrectly() throws Exception {
    URI path = gcsFsIHelper.getUniqueObjectUri(getClass(), "write_byteArrayWithOffsetAndLength");
    Path hadoopPath = new Path(path);
    byte[] source = "hello analytics core write slice".getBytes(UTF_8);
    int offset = 6;
    int length = 14;
    byte[] expected = Arrays.copyOfRange(source, offset, offset + length);

    try (FSDataOutputStream out = sharedGhfs.create(hadoopPath)) {
      out.write(source, offset, length);
    }

    assertThat(sharedGhfs.getFileStatus(hadoopPath).getLen()).isEqualTo(expected.length);
    assertThat(gcsFsIHelper.readFile(path)).isEqualTo(expected);
  }

  @Test
  public void write_largeData_writesContentCorrectly() throws Exception {
    URI path = gcsFsIHelper.getUniqueObjectUri(getClass(), "write_largeData");
    Path hadoopPath = new Path(path);
    byte[] expected = new byte[2 * 1024 * 1024]; // 2 MiB
    new Random().nextBytes(expected);

    try (FSDataOutputStream out = sharedGhfs.create(hadoopPath)) {
      out.write(expected);
    }

    assertThat(sharedGhfs.getFileStatus(hadoopPath).getLen()).isEqualTo(expected.length);
    assertThat(gcsFsIHelper.readFile(path)).isEqualTo(expected);
  }

  @Test
  public void write_withZeroBufferSize_writesContentCorrectly() throws Exception {
    URI path = gcsFsIHelper.getUniqueObjectUri(getClass(), "write_withZeroBufferSize");
    Path hadoopPath = new Path(path);
    Configuration config = getTestConfig();
    config.setInt(GCS_OUTPUT_STREAM_BUFFER_SIZE.getKey(), 0);
    byte[] expected = "hello analytics core unbuffered write".getBytes(UTF_8);

    try (FileSystem fs = GoogleHadoopFileSystemIntegrationHelper.createGhfs(path, config)) {
      try (FSDataOutputStream out = fs.create(hadoopPath)) {
        out.write(expected);
      }

      assertThat(fs.getFileStatus(hadoopPath).getLen()).isEqualTo(expected.length);
    }
    assertThat(gcsFsIHelper.readFile(path)).isEqualTo(expected);
  }

  @Test
  public void hsync_writesContentCorrectly() throws Exception {
    URI path = gcsFsIHelper.getUniqueObjectUri(getClass(), "hsync");
    Path hadoopPath = new Path(path);
    Configuration config = getTestConfig();
    config.setTimeDuration(GCS_OUTPUT_STREAM_SYNC_MIN_INTERVAL.getKey(), 1, SECONDS);

    byte[] part1 = "hello ".getBytes(UTF_8);
    byte[] part2 = "world".getBytes(UTF_8);
    byte[] expected = "hello world".getBytes(UTF_8);

    try (FileSystem fs = GoogleHadoopFileSystemIntegrationHelper.createGhfs(path, config)) {
      try (FSDataOutputStream fout = fs.create(hadoopPath)) {
        fout.write(part1);
        fout.hsync();

        assertThat(fs.getFileStatus(hadoopPath).getLen()).isEqualTo(part1.length);
        assertThat(gcsFsIHelper.readFile(path)).isEqualTo(part1);

        fout.write(part2);
        fout.hsync();

        assertThat(fs.getFileStatus(hadoopPath).getLen()).isEqualTo(expected.length);
        assertThat(gcsFsIHelper.readFile(path)).isEqualTo(expected);
      }

      assertThat(fs.getFileStatus(hadoopPath).getLen()).isEqualTo(expected.length);
      assertThat(gcsFsIHelper.readFile(path)).isEqualTo(expected);
    }
  }

  @Test
  public void overwrite_true_overwritesExistingFile() throws Exception {
    URI path = gcsFsIHelper.getUniqueObjectUri(getClass(), "overwrite_true");
    Path hadoopPath = new Path(path);
    byte[] initialContent = "initial content".getBytes(UTF_8);

    try (FSDataOutputStream out = sharedGhfs.create(hadoopPath, true)) {
      out.write(initialContent);
    }

    assertThat(gcsFsIHelper.readFile(path)).isEqualTo(initialContent);

    byte[] newContent = "overwritten content".getBytes(UTF_8);
    try (FSDataOutputStream out = sharedGhfs.create(hadoopPath, true)) {
      out.write(newContent);
    }
    assertThat(gcsFsIHelper.readFile(path)).isEqualTo(newContent);
  }

  @Test
  public void overwrite_false_throwsExceptionWhenFileExists() throws Exception {
    URI path = gcsFsIHelper.getUniqueObjectUri(getClass(), "overwrite_false");
    Path hadoopPath = new Path(path);
    byte[] initialContent = "initial content".getBytes(UTF_8);

    try (FSDataOutputStream out = sharedGhfs.create(hadoopPath, false)) {
      out.write(initialContent);
    }

    assertThrows(FileAlreadyExistsException.class, () -> sharedGhfs.create(hadoopPath, false));
  }

  @Test
  public void write_directOutputStream_writesContentCorrectly() throws Exception {
    URI path = gcsFsIHelper.getUniqueObjectUri(getClass(), "write_directOutputStream");

    byte[] expected = "hello analytics core direct stream write".getBytes(UTF_8);
    try (GoogleHadoopOutputStream out =
        createGhfsOutputStream(sharedGhfs, path, CreateFileOptions.DEFAULT)) {
      out.write(expected);
    }

    assertThat(gcsFsIHelper.readFile(path)).isEqualTo(expected);
  }

  @Test
  public void write_zeroBytes_createsEmptyFile() throws Exception {
    URI path = gcsFsIHelper.getUniqueObjectUri(getClass(), "write_zeroBytes");

    try (GoogleHadoopOutputStream out =
        createGhfsOutputStream(sharedGhfs, path, CreateFileOptions.DEFAULT)) {
      out.write(new byte[0], 0, 0);
    }

    assertThat(gcsFsIHelper.readFile(path)).isEqualTo(new byte[0]);
  }

  private static GoogleHadoopOutputStream createGhfsOutputStream(
      GoogleHadoopFileSystem ghfs, URI path, CreateFileOptions options) throws IOException {
    FileSystem.Statistics statistics = new FileSystem.Statistics(ghfs.getScheme());
    return new GoogleHadoopOutputStream(ghfs, path, options, statistics);
  }
}
