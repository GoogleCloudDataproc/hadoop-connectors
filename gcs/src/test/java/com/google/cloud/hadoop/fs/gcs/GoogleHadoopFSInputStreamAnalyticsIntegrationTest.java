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

import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemTestHelper.assertByteBuffers;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemIntegrationHelper;
import java.io.EOFException;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileRange;
import org.apache.hadoop.fs.FileSystem;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration tests for GoogleHadoopFSInputStream with Analytics Core enabled. */
@RunWith(JUnit4.class)
public class GoogleHadoopFSInputStreamAnalyticsIntegrationTest {

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

  private static class TestFixture {
    final GoogleHadoopFileSystem ghfs;
    final URI path;
    final String content;

    TestFixture(GoogleHadoopFileSystem ghfs, URI path, String content) {
      this.ghfs = ghfs;
      this.path = path;
      this.content = content;
    }
  }

  private TestFixture setupFixture(String tag, String content) throws Exception {
    URI path = gcsFsIHelper.getUniqueObjectUri(getClass(), tag);
    Configuration config = GoogleHadoopFileSystemIntegrationHelper.getTestConfig();
    config.setBoolean("fs.gs.analytics.core.enable", true);

    GoogleHadoopFileSystem ghfs = GoogleHadoopFileSystemIntegrationHelper.createGhfs(path, config);
    gcsFsIHelper.writeTextFile(path, content);
    return new TestFixture(ghfs, path, content);
  }

  @Test
  public void read_singleBytes_returnsData() throws Exception {
    String testContent = "test content";
    TestFixture fixture = setupFixture("read_singleBytes", testContent);

    byte[] value = new byte[2];
    byte[] expected = Arrays.copyOf(testContent.getBytes(StandardCharsets.UTF_8), 2);

    FileSystem.Statistics statistics = new FileSystem.Statistics(fixture.ghfs.getScheme());
    try (GoogleHadoopFSInputStream in =
        GoogleHadoopFSInputStream.create(fixture.ghfs, fixture.path, statistics)) {
      assertThat(in.read(value, 0, 1)).isEqualTo(1);
      assertThat(in.read(1, value, 1, 1)).isEqualTo(1);
    }

    assertThat(value).isEqualTo(expected);
  }

  @Test
  public void readVectored_returnsExpectedRanges() throws Exception {
    String testContent = "verify vectored read ranges are getting"; // length = 40
    TestFixture fixture = setupFixture("readVectored", testContent);

    List<FileRange> fileRanges = new ArrayList<>();
    fileRanges.add(FileRange.createFileRange(0, 5));
    fileRanges.add(FileRange.createFileRange(10, 5));

    try (GoogleHadoopFSInputStream in = createGhfsInputStream(fixture.ghfs, fixture.path)) {
      in.readVectored(fileRanges, ByteBuffer::allocate);
      validateVectoredReadResult(fileRanges, fixture.path);
    }
  }

  @Test
  public void seek_movesPointerToCorrectPosition() throws Exception {
    String testContent = "test content";
    TestFixture fixture = setupFixture("seek", testContent);

    byte[] value = new byte[2];
    byte[] expected = new byte[] {'c', 'o'}; // "content" starts at index 5

    FileSystem.Statistics statistics = new FileSystem.Statistics(fixture.ghfs.getScheme());
    try (GoogleHadoopFSInputStream in =
        GoogleHadoopFSInputStream.create(fixture.ghfs, fixture.path, statistics)) {
      in.seek(5);
      assertThat(in.read(value, 0, 2)).isEqualTo(2);
    }

    assertThat(value).isEqualTo(expected);
  }

  @Test
  public void readFully_readsDataIntoBuffer() throws Exception {
    String testContent = "test content";
    TestFixture fixture = setupFixture("readFully", testContent);

    byte[] value = new byte[4];
    byte[] expected = new byte[] {'c', 'o', 'n', 't'}; // "content" starts at index 5

    FileSystem.Statistics statistics = new FileSystem.Statistics(fixture.ghfs.getScheme());
    try (GoogleHadoopFSInputStream in =
        GoogleHadoopFSInputStream.create(fixture.ghfs, fixture.path, statistics)) {
      in.readFully(5, value);
    }

    assertThat(value).isEqualTo(expected);
  }

  @Test
  public void read_whenClosed_throwsIOException() throws Exception {
    TestFixture fixture = setupFixture("read_after_closed", "test content");

    FileSystem.Statistics statistics = new FileSystem.Statistics(fixture.ghfs.getScheme());
    GoogleHadoopFSInputStream in =
        GoogleHadoopFSInputStream.create(fixture.ghfs, fixture.path, statistics);
    in.close();
    assertThrows(IOException.class, in::read);
  }

  @Test
  public void read_zeroBytes_returnsZero() throws Exception {
    TestFixture fixture = setupFixture("read_zeroBytes", "test content");

    byte[] value = new byte[2];

    FileSystem.Statistics statistics = new FileSystem.Statistics(fixture.ghfs.getScheme());
    try (GoogleHadoopFSInputStream in =
        GoogleHadoopFSInputStream.create(fixture.ghfs, fixture.path, statistics)) {
      assertThat(in.read(value, 0, 0)).isEqualTo(0);
    }
  }

  @Test
  public void seek_beyondFileSize_throwsEOFException() throws Exception {
    String testContent = "test content";
    TestFixture fixture = setupFixture("seek_beyond_file", testContent);

    FileSystem.Statistics statistics = new FileSystem.Statistics(fixture.ghfs.getScheme());
    try (GoogleHadoopFSInputStream in =
        GoogleHadoopFSInputStream.create(fixture.ghfs, fixture.path, statistics)) {
      assertThrows(EOFException.class, () -> in.seek(testContent.length() + 1));
    }
  }

  @Test
  public void read_positional_beyondFileSize_returnsMinusOne() throws Exception {
    String testContent = "test content";
    TestFixture fixture = setupFixture("read_pos_beyond_file", testContent);

    byte[] value = new byte[2];

    FileSystem.Statistics statistics = new FileSystem.Statistics(fixture.ghfs.getScheme());
    try (GoogleHadoopFSInputStream in =
        GoogleHadoopFSInputStream.create(fixture.ghfs, fixture.path, statistics)) {
      assertThat(in.read(testContent.length(), value, 0, 1)).isEqualTo(-1);
    }
  }

  private static GoogleHadoopFSInputStream createGhfsInputStream(
      GoogleHadoopFileSystem ghfs, URI path) throws IOException {
    FileSystem.Statistics statistics = new FileSystem.Statistics(ghfs.getScheme());
    return GoogleHadoopFSInputStream.create(ghfs, path, statistics);
  }

  private void validateVectoredReadResult(List<FileRange> fileRanges, URI path)
      throws ExecutionException, InterruptedException, IOException {
    String testContent = gcsFsIHelper.readTextFile(path);
    for (FileRange fileRange : fileRanges) {
      ByteBuffer buffer = fileRange.getData().get();
      byte[] expected =
          Arrays.copyOfRange(
              testContent.getBytes(StandardCharsets.UTF_8),
              (int) fileRange.getOffset(),
              (int) (fileRange.getOffset() + fileRange.getLength()));
      assertByteBuffers(buffer, ByteBuffer.wrap(expected));
    }
  }
}
