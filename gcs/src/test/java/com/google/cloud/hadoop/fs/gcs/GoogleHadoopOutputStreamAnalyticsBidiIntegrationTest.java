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
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_APPENDABLE_OBJECTS_FINALIZE_BEFORE_CLOSE;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_CLIENT_TYPE;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_GRPC_WRITE_ENABLE;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_OPERATION_BIDI_API_ENABLE;
import static com.google.common.truth.Truth.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertThrows;

import com.google.cloud.gcs.analyticscore.client.GcsClientOptions;
import com.google.cloud.gcs.analyticscore.client.GcsClientOptions.Protocol;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemImpl;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemIntegrationHelper;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions.ClientType;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper;
import java.io.IOException;
import java.net.URI;
import java.util.Random;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Integration tests for appendable (bidi) writes through the Analytics Core write path.
 *
 * <p>Appendable writes are only supported on zonal buckets using the Rapid storage class, so this
 * suite creates its own zonal bucket. Zonal buckets do not support compose, so {@code hsync()} is
 * intentionally not exercised here; see {@link GoogleHadoopOutputStreamAnalyticsIntegrationTest}
 * for the transport-agnostic coverage.
 */
@RunWith(JUnit4.class)
public class GoogleHadoopOutputStreamAnalyticsBidiIntegrationTest {

  private static GoogleCloudStorageFileSystemIntegrationHelper gcsFsIHelper;
  private static String zonalBucketName;
  private static GoogleHadoopFileSystem ghfs;

  @BeforeClass
  public static void beforeClass() throws Exception {
    // Zonal buckets can only be created through the gRPC storage client.
    GoogleCloudStorageFileSystem zonalCapableGcsFs =
        new GoogleCloudStorageFileSystemImpl(
            GoogleCloudStorageTestHelper.getCredentials(),
            GoogleCloudStorageFileSystemOptions.builder()
                .setClientType(ClientType.STORAGE_CLIENT)
                .setBucketDeleteEnabled(true)
                .setCloudStorageOptions(
                    GoogleCloudStorageTestHelper.getStandardOptionBuilder().build())
                .build());
    gcsFsIHelper = new GoogleCloudStorageFileSystemIntegrationHelper(zonalCapableGcsFs);
    zonalBucketName =
        gcsFsIHelper.createUniqueZonalOrRegionalBucket("ac-bidi", /* isBucketZonal= */ true);
    ghfs =
        GoogleHadoopFileSystemIntegrationHelper.createGhfs(
            new URI("gs://" + zonalBucketName + "/"), getTestConfig());
  }

  @AfterClass
  public static void afterClass() throws IOException {
    if (ghfs != null) {
      ghfs.close();
    }
    if (gcsFsIHelper != null) {
      gcsFsIHelper.afterAllTests();
    }
  }

  private static Configuration getTestConfig() {
    Configuration conf = GoogleHadoopFileSystemIntegrationHelper.getTestConfig();
    conf.setBoolean(GCS_ANALYTICS_CORE_ENABLE.getKey(), true);
    conf.setBoolean(GCS_ANALYTICS_CORE_WRITE_ENABLE.getKey(), true);
    conf.setEnum(GCS_CLIENT_TYPE.getKey(), ClientType.STORAGE_CLIENT);
    conf.setBoolean(GCS_OPERATION_BIDI_API_ENABLE.getKey(), true);
    conf.setBoolean(GCS_GRPC_WRITE_ENABLE.getKey(), true);
    // Finalize on close so that readers observe the final object size right away.
    conf.setBoolean(GCS_APPENDABLE_OBJECTS_FINALIZE_BEFORE_CLOSE.getKey(), true);
    return conf;
  }

  private static Path getUniquePath(String tag) {
    return new Path(String.format("gs://%s/%s_%s", zonalBucketName, tag, UUID.randomUUID()));
  }

  private static byte[] readFile(Path path, int length) throws IOException {
    byte[] content = new byte[length];
    try (FSDataInputStream in = ghfs.open(path)) {
      in.readFully(0, content);
    }
    return content;
  }

  @Test
  public void initialize_mapsConnectorBidiConfigToAppendableWrites() {
    GcsClientOptions clientOptions =
        ghfs.getAnalyticsCoreGcsFs().getFileSystemOptions().getGcsClientOptions();

    assertThat(clientOptions.getProtocol()).isEqualTo(Protocol.BIDI);
    assertThat(clientOptions.getGcsWriteOptions().isBidiWriteEnabled()).isTrue();
    assertThat(clientOptions.getGcsWriteOptions().isBidiFinalizeOnClose()).isTrue();
  }

  @Test
  public void write_byteArray_writesContentCorrectly() throws Exception {
    Path path = getUniquePath("write_byteArray");
    byte[] expected = "hello analytics core appendable write".getBytes(UTF_8);

    try (FSDataOutputStream out = ghfs.create(path)) {
      out.write(expected);
    }

    assertThat(ghfs.getFileStatus(path).getLen()).isEqualTo(expected.length);
    assertThat(readFile(path, expected.length)).isEqualTo(expected);
  }

  @Test
  public void write_singleBytes_writesContentCorrectly() throws Exception {
    Path path = getUniquePath("write_singleBytes");
    byte[] expected = "appendable single byte writes".getBytes(UTF_8);

    try (FSDataOutputStream out = ghfs.create(path)) {
      for (byte b : expected) {
        out.write(b);
      }
    }

    assertThat(ghfs.getFileStatus(path).getLen()).isEqualTo(expected.length);
    assertThat(readFile(path, expected.length)).isEqualTo(expected);
  }

  @Test
  public void write_largeData_writesContentCorrectly() throws Exception {
    Path path = getUniquePath("write_largeData");
    byte[] expected = new byte[2 * 1024 * 1024]; // 2 MiB
    new Random().nextBytes(expected);

    try (FSDataOutputStream out = ghfs.create(path)) {
      out.write(expected);
    }

    assertThat(ghfs.getFileStatus(path).getLen()).isEqualTo(expected.length);
    assertThat(readFile(path, expected.length)).isEqualTo(expected);
  }

  @Test
  public void write_zeroBytes_createsEmptyFile() throws Exception {
    Path path = getUniquePath("write_zeroBytes");

    try (FSDataOutputStream out = ghfs.create(path)) {
      out.write(new byte[0], 0, 0);
    }

    assertThat(ghfs.getFileStatus(path).getLen()).isEqualTo(0);
  }

  @Test
  public void overwrite_true_overwritesExistingFile() throws Exception {
    Path path = getUniquePath("overwrite_true");
    byte[] initialContent = "initial content".getBytes(UTF_8);
    byte[] newContent = "overwritten content".getBytes(UTF_8);

    try (FSDataOutputStream out = ghfs.create(path, /* overwrite= */ true)) {
      out.write(initialContent);
    }
    assertThat(readFile(path, initialContent.length)).isEqualTo(initialContent);

    try (FSDataOutputStream out = ghfs.create(path, /* overwrite= */ true)) {
      out.write(newContent);
    }

    assertThat(ghfs.getFileStatus(path).getLen()).isEqualTo(newContent.length);
    assertThat(readFile(path, newContent.length)).isEqualTo(newContent);
  }

  @Test
  public void overwrite_false_throwsExceptionWhenFileExists() throws Exception {
    Path path = getUniquePath("overwrite_false");

    try (FSDataOutputStream out = ghfs.create(path, /* overwrite= */ false)) {
      out.write("initial content".getBytes(UTF_8));
    }

    assertThrows(FileAlreadyExistsException.class, () -> ghfs.create(path, /* overwrite= */ false));
  }
}
