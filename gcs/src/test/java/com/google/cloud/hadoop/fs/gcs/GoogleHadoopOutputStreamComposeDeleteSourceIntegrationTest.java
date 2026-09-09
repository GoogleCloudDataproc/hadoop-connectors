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

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemIntegrationHelper;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions.ClientType;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GoogleHadoopOutputStreamComposeDeleteSourceIntegrationTest {

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

  @Test
  public void writeWithComposeDeleteSource_httpClient() throws Exception {
    testComposeDeleteSource(ClientType.HTTP_API_CLIENT);
  }

  @Test
  public void writeWithComposeDeleteSource_grpcClient() throws Exception {
    testComposeDeleteSource(ClientType.STORAGE_CLIENT);
  }

  private void testComposeDeleteSource(ClientType clientType) throws Exception {
    URI path = gcsFsIHelper.getUniqueObjectUri("compose_del_src_" + clientType);
    Path hadoopPath = new Path(path);

    Configuration config = GoogleHadoopFileSystemIntegrationHelper.getTestConfig();
    config.setEnum("fs.gs.client.type", clientType);
    config.setBoolean("fs.gs.operation.compose.delete-source.enable", true);

    FileSystem fs = GoogleHadoopFileSystemIntegrationHelper.createGhfs(path, config);

    byte[] chunk1 = "chunk1-content-data-".getBytes(UTF_8);
    byte[] chunk2 = "chunk2-content-data-".getBytes(UTF_8);
    byte[] chunk3 = "chunk3-content-data-".getBytes(UTF_8);

    byte[] expectedPart1 = chunk1;
    byte[] expectedPart2 = ("chunk1-content-data-" + "chunk2-content-data-").getBytes(UTF_8);
    byte[] expectedTotal =
        ("chunk1-content-data-" + "chunk2-content-data-" + "chunk3-content-data-").getBytes(UTF_8);

    try (FSDataOutputStream out = fs.create(hadoopPath)) {
      out.write(chunk1);
      out.hsync();

      // Validate data after first sync (no compose yet)
      assertThat(fs.getFileStatus(hadoopPath).getLen()).isEqualTo(expectedPart1.length);
      assertThat(gcsFsIHelper.readFile(path)).isEqualTo(expectedPart1);

      // Second write + hsync triggers compose with delete-source
      out.write(chunk2);
      out.hsync();

      // Validate data after second sync (composed chunk1 + chunk2)
      assertThat(fs.getFileStatus(hadoopPath).getLen()).isEqualTo(expectedPart2.length);
      assertThat(gcsFsIHelper.readFile(path)).isEqualTo(expectedPart2);

      // Third write + close triggers final compose with delete-source
      out.write(chunk3);
    }

    // Verify final file size and content
    assertThat(fs.getFileStatus(hadoopPath).getLen()).isEqualTo(expectedTotal.length);
    assertThat(gcsFsIHelper.readFile(path)).isEqualTo(expectedTotal);

    // Verify that no temporary tail files remain in the directory
    FileStatus[] statuses = fs.listStatus(hadoopPath.getParent());
    if (statuses != null) {
      for (FileStatus status : statuses) {
        assertThat(status.getPath().getName()).doesNotContain(".tmp.ghfs.");
      }
    }
  }
}
