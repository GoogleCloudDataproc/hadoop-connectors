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

import static com.google.common.truth.Truth.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertThrows;

import com.google.cloud.hadoop.gcsio.CreateFileOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemIntegrationHelper;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.ClosedChannelException;
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

/** Integration tests for GoogleHadoopOutputStream with Analytics Core write path enabled. */
@RunWith(JUnit4.class)
public class GoogleHadoopOutputStreamAnalyticsIntegrationTest {

  private static GoogleCloudStorageFileSystemIntegrationHelper gcsFsIHelper;

  @BeforeClass
  public static void beforeClass() throws Exception {
    gcsFsIHelper = GoogleCloudStorageFileSystemIntegrationHelper.create();
    gcsFsIHelper.beforeAllTests();
  }

  @AfterClass
  public static void afterClass() {
    if (gcsFsIHelper != null) {
      gcsFsIHelper.afterAllTests();
    }
  }

  private Configuration getTestConfig() {
    Configuration conf = GoogleHadoopFileSystemIntegrationHelper.getTestConfig();
    conf.setBoolean("fs.gs.analytics.core.enable", true);
    conf.setBoolean("fs.gs.analytics.core.write.enable", true);
    return conf;
  }

  @Test
  public void write_singleByte_writesContentCorrectly() throws Exception {
    URI path = gcsFsIHelper.getUniqueObjectUri(getClass(), "write_singleByte");
    Path hadoopPath = new Path(path);
    FileSystem fs = GoogleHadoopFileSystemIntegrationHelper.createGhfs(path, getTestConfig());

    byte[] expected = "hello analytics core write".getBytes(UTF_8);
    try (FSDataOutputStream out = fs.create(hadoopPath)) {
      for (byte b : expected) {
        out.write(b);
      }
    }

    assertThat(fs.getFileStatus(hadoopPath).getLen()).isEqualTo(expected.length);
    assertThat(gcsFsIHelper.readFile(path)).isEqualTo(expected);
  }

  @Test
  public void write_byteArray_writesContentCorrectly() throws Exception {
    URI path = gcsFsIHelper.getUniqueObjectUri(getClass(), "write_byteArray");
    Path hadoopPath = new Path(path);
    FileSystem fs = GoogleHadoopFileSystemIntegrationHelper.createGhfs(path, getTestConfig());

    byte[] expected = "hello analytics core write array".getBytes(UTF_8);
    try (FSDataOutputStream out = fs.create(hadoopPath)) {
      out.write(expected, 0, expected.length);
    }

    assertThat(fs.getFileStatus(hadoopPath).getLen()).isEqualTo(expected.length);
    assertThat(gcsFsIHelper.readFile(path)).isEqualTo(expected);
  }

  @Test
  public void write_largeData_writesContentCorrectly() throws Exception {
    URI path = gcsFsIHelper.getUniqueObjectUri(getClass(), "write_largeData");
    Path hadoopPath = new Path(path);
    FileSystem fs = GoogleHadoopFileSystemIntegrationHelper.createGhfs(path, getTestConfig());

    byte[] expected = new byte[2 * 1024 * 1024]; // 2 MiB
    new Random().nextBytes(expected);

    try (FSDataOutputStream out = fs.create(hadoopPath)) {
      out.write(expected);
    }

    assertThat(fs.getFileStatus(hadoopPath).getLen()).isEqualTo(expected.length);
    assertThat(gcsFsIHelper.readFile(path)).isEqualTo(expected);
  }

  @Test
  public void write_withZeroBufferSize_writesContentCorrectly() throws Exception {
    URI path = gcsFsIHelper.getUniqueObjectUri(getClass(), "write_withZeroBufferSize");
    Path hadoopPath = new Path(path);

    Configuration config = getTestConfig();
    config.setInt("fs.gs.outputstream.buffer.size", 0);
    FileSystem fs = GoogleHadoopFileSystemIntegrationHelper.createGhfs(path, config);

    byte[] expected = "hello analytics core unbuffered write".getBytes(UTF_8);
    try (FSDataOutputStream out = fs.create(hadoopPath)) {
      out.write(expected);
    }

    assertThat(fs.getFileStatus(hadoopPath).getLen()).isEqualTo(expected.length);
    assertThat(gcsFsIHelper.readFile(path)).isEqualTo(expected);
  }

  @Test
  public void hsync() throws Exception {
    URI path = gcsFsIHelper.getUniqueObjectUri(getClass(), "hsync");
    Path hadoopPath = new Path(path);
    FileSystem fs = GoogleHadoopFileSystemIntegrationHelper.createGhfs(path, getTestConfig());

    byte[] expected = new byte[5];
    new Random().nextBytes(expected);

    try (FSDataOutputStream fout = fs.create(hadoopPath)) {
      for (int i = 0; i < expected.length; i++) {
        fout.write(expected[i]);
        fout.hsync();

        int composedLength = i + 1;
        assertThat(fs.getFileStatus(hadoopPath).getLen()).isEqualTo(composedLength);
        assertThat(gcsFsIHelper.readFile(path)).isEqualTo(Arrays.copyOf(expected, composedLength));
      }
    }

    assertThat(fs.getFileStatus(hadoopPath).getLen()).isEqualTo(expected.length);
    assertThat(gcsFsIHelper.readFile(path)).isEqualTo(expected);
  }

  @Test
  public void overwrite_true_overwritesExistingFile() throws Exception {
    URI path = gcsFsIHelper.getUniqueObjectUri(getClass(), "overwrite_true");
    Path hadoopPath = new Path(path);
    FileSystem fs = GoogleHadoopFileSystemIntegrationHelper.createGhfs(path, getTestConfig());

    byte[] initialContent = "initial content".getBytes(UTF_8);
    try (FSDataOutputStream out = fs.create(hadoopPath, true)) {
      out.write(initialContent);
    }
    assertThat(gcsFsIHelper.readFile(path)).isEqualTo(initialContent);

    byte[] newContent = "overwritten content".getBytes(UTF_8);
    try (FSDataOutputStream out = fs.create(hadoopPath, true)) {
      out.write(newContent);
    }
    assertThat(gcsFsIHelper.readFile(path)).isEqualTo(newContent);
  }

  @Test
  public void overwrite_false_throwsExceptionWhenFileExists() throws Exception {
    URI path = gcsFsIHelper.getUniqueObjectUri(getClass(), "overwrite_false");
    Path hadoopPath = new Path(path);
    FileSystem fs = GoogleHadoopFileSystemIntegrationHelper.createGhfs(path, getTestConfig());

    byte[] initialContent = "initial content".getBytes(UTF_8);
    try (FSDataOutputStream out = fs.create(hadoopPath, false)) {
      out.write(initialContent);
    }

    assertThrows(FileAlreadyExistsException.class, () -> fs.create(hadoopPath, false));
  }

  @Test
  public void write_directOutputStream_writesContentCorrectly() throws Exception {
    URI path = gcsFsIHelper.getUniqueObjectUri(getClass(), "write_directOutputStream");
    GoogleHadoopFileSystem ghfs = setupGhfs(path);

    byte[] expected = "hello analytics core direct stream write".getBytes(UTF_8);
    try (GoogleHadoopOutputStream out =
        createGhfsOutputStream(ghfs, path, CreateFileOptions.DEFAULT)) {
      out.write(expected);
    }

    assertThat(gcsFsIHelper.readFile(path)).isEqualTo(expected);
  }

  @Test
  public void write_whenClosed_throwsClosedChannelException() throws Exception {
    URI path = gcsFsIHelper.getUniqueObjectUri(getClass(), "write_after_closed");
    GoogleHadoopFileSystem ghfs = setupGhfs(path);

    GoogleHadoopOutputStream out = createGhfsOutputStream(ghfs, path, CreateFileOptions.DEFAULT);
    out.close();

    assertThrows(ClosedChannelException.class, () -> out.write(1));
  }

  @Test
  public void write_zeroBytes_createsEmptyFile() throws Exception {
    URI path = gcsFsIHelper.getUniqueObjectUri(getClass(), "write_zeroBytes");
    GoogleHadoopFileSystem ghfs = setupGhfs(path);

    try (GoogleHadoopOutputStream out =
        createGhfsOutputStream(ghfs, path, CreateFileOptions.DEFAULT)) {
      out.write(new byte[0], 0, 0);
    }

    assertThat(gcsFsIHelper.readFile(path)).isEqualTo(new byte[0]);
  }

  private GoogleHadoopFileSystem setupGhfs(URI path) throws Exception {
    return GoogleHadoopFileSystemIntegrationHelper.createGhfs(path, getTestConfig());
  }

  private static GoogleHadoopOutputStream createGhfsOutputStream(
      GoogleHadoopFileSystem ghfs, URI path, CreateFileOptions options) throws IOException {
    FileSystem.Statistics statistics = new FileSystem.Statistics(ghfs.getScheme());
    return new GoogleHadoopOutputStream(ghfs, path, options, statistics);
  }
}
