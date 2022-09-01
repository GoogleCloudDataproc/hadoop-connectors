/*
 * Copyright 2019 Google LLC. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.fs.gcs;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemIntegrationHelper;
import java.io.EOFException;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FileSystem;
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
    try (GoogleHadoopFSInputStream in = GoogleHadoopFSInputStream.create(ghfs, path, statistics)) {
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
    GoogleHadoopFSInputStream in = GoogleHadoopFSInputStream.create(ghfs, path, statistics);
    in.close();
    assertThrows(IOException.class, in::read);
  }

  @Test
  public void testReadFully() throws Exception {
    URI path = gcsFsIHelper.getUniqueObjectUri(getClass(), "testReadFully");
    GoogleHadoopFileSystem ghfs =
        GoogleHadoopFileSystemIntegrationHelper.createGhfs(
            path, GoogleHadoopFileSystemIntegrationHelper.getTestConfig());

    String testContent = "test content";
    gcsFsIHelper.writeTextFile(path, testContent);

    byte[] value = new byte[5];
    byte[] expected = Arrays.copyOfRange(testContent.getBytes(StandardCharsets.UTF_8), 2, 7);

    GoogleHadoopFSInputStream in = createGhfsInputStream(ghfs, path);
    try (GoogleHadoopFSInputStream ignore = in) {
      in.readFully(2, value);
      assertThat(in.getPos()).isEqualTo(0);
    }
    assertThat(value).isEqualTo(expected);
  }

  @Test
  public void testReadFully_illegalSize() throws Exception {
    URI path = gcsFsIHelper.getUniqueObjectUri(getClass(), "testReadFully");
    GoogleHadoopFileSystem ghfs =
        GoogleHadoopFileSystemIntegrationHelper.createGhfs(
            path, GoogleHadoopFileSystemIntegrationHelper.getTestConfig());

    String testContent = "test content";
    gcsFsIHelper.writeTextFile(path, testContent);

    byte[] value = new byte[20];

    GoogleHadoopFSInputStream in = createGhfsInputStream(ghfs, path);
    Throwable exception = assertThrows(EOFException.class, () -> in.readFully(2, value));
    assertThat(exception).hasMessageThat().contains(FSExceptionMessages.EOF_IN_READ_FULLY);
  }

  private static GoogleHadoopFSInputStream createGhfsInputStream(
      GoogleHadoopFileSystem ghfs, URI path) throws IOException {
    return GoogleHadoopFSInputStream.create(
        ghfs, path, new FileSystem.Statistics(ghfs.getScheme()));
  }
}
