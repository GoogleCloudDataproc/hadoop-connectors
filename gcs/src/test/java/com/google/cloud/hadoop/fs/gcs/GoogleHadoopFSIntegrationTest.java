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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.api.client.http.HttpExecuteInterceptor;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.cloud.hadoop.gcsio.CreateObjectOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemIntegrationHelper;
import com.google.api.client.http.HttpContent;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageWriteChannel;
import com.google.cloud.hadoop.gcsio.ObjectWriteConditions;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.cloud.hadoop.util.ClientRequestHelper;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.FileNotFoundException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.file.DirectoryNotEmptyException;
import java.util.EnumSet;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration tests for {@link GoogleHadoopFS} class. */
@RunWith(JUnit4.class)
public class GoogleHadoopFSIntegrationTest {

  private static GoogleCloudStorageFileSystemIntegrationHelper gcsFsIHelper;
  private static URI initUri;

  @BeforeClass
  public static void beforeClass() throws Exception {
    gcsFsIHelper = GoogleCloudStorageFileSystemIntegrationHelper.create();
    gcsFsIHelper.beforeAllTests();
    initUri = new URI("gs://" + gcsFsIHelper.sharedBucketName1);
  }

  @AfterClass
  public static void afterClass() {
    gcsFsIHelper.afterAllTests();
  }

  @After
  public void after() throws IOException {
    FileSystem.closeAll();
  }

  @Test
  public void testInitializationWithUriAndConf_shouldGiveFsStatusWithNotUsedMemory()
      throws IOException, URISyntaxException {
    Configuration config = GoogleHadoopFileSystemIntegrationHelper.getTestConfig();
    GoogleHadoopFS ghfs = new GoogleHadoopFS(initUri, config);

    assertThat(ghfs.getFsStatus().getUsed()).isEqualTo(0);
  }

  @Test
  public void testInitializationWithGhfsUriAndConf_shouldGiveFsStatusWithNotUsedMemory()
      throws IOException, URISyntaxException {
    Configuration config = GoogleHadoopFileSystemIntegrationHelper.getTestConfig();
    GoogleHadoopFS ghfs = new GoogleHadoopFS(new GoogleHadoopFileSystem(), initUri, config);

    assertThat(ghfs.getFsStatus().getUsed()).isEqualTo(0);
  }

  @Test
  public void testCreateInternal_shouldCreateParent() throws Exception {
    Configuration config = GoogleHadoopFileSystemIntegrationHelper.getTestConfig();
    GoogleHadoopFS ghfs = new GoogleHadoopFS(initUri, config);

    Path filePath =
        new Path(initUri.resolve("/testCreateInternal_shouldCreateParent/dir/file").toString());

    try (FSDataOutputStream stream =
        ghfs.createInternal(
            filePath,
            EnumSet.of(CreateFlag.CREATE),
            /* absolutePermission= */ null,
            /* bufferSize= */ 128,
            /* replication= */ (short) 1,
            /* blockSize= */ 32,
            () -> {},
            new Options.ChecksumOpt(),
            /* createParent= */ true)) {
      stream.write(1);

      assertThat(stream.size()).isEqualTo(1);
    }

    FileStatus parentStatus = ghfs.getFileStatus(filePath.getParent());
    assertThat(parentStatus.getModificationTime()).isEqualTo(0L);
  }

  @Test
  public void testCreateInternal_shouldNotCreateParent() throws Exception {
    Configuration config = GoogleHadoopFileSystemIntegrationHelper.getTestConfig();
    GoogleHadoopFS ghfs = new GoogleHadoopFS(initUri, config);

    Path filePath =
        new Path(initUri.resolve("/testCreateInternal_shouldNotCreateParent/dir/file").toString());

    try (FSDataOutputStream stream =
        ghfs.createInternal(
            filePath,
            EnumSet.of(CreateFlag.CREATE),
            /* absolutePermission= */ null,
            /* bufferSize= */ 128,
            /* replication= */ (short) 1,
            /* blockSize= */ 32,
            () -> {},
            new Options.ChecksumOpt(),
            /* createParent= */ false)) {
      stream.write(1);

      assertThat(stream.size()).isEqualTo(1);
    }

    // GoogleHadoopFS ignores 'createParent' flag and always creates parent
    FileStatus parentStatus = ghfs.getFileStatus(filePath.getParent().getParent());
    assertThat(parentStatus.getModificationTime()).isEqualTo(0);
  }

  @Test
  public void testGetUriDefaultPort_shouldBeEqualToGhfsDefaultPort()
      throws IOException, URISyntaxException {
    Configuration config = GoogleHadoopFileSystemIntegrationHelper.getTestConfig();
    GoogleHadoopFS ghfs = new GoogleHadoopFS(initUri, config);

    assertThat(ghfs.getUriDefaultPort()).isEqualTo(-1);
  }

  @Test
  public void testGetUri_shouldBeEqualToGhfsUri() throws IOException, URISyntaxException {
    Configuration config = GoogleHadoopFileSystemIntegrationHelper.getTestConfig();
    GoogleHadoopFS ghfs = new GoogleHadoopFS(initUri, config);

    assertThat(ghfs.getUri()).isEqualTo(initUri.resolve("/"));
  }

  @Test
  public void testValidName_shouldNotContainPoints() throws IOException, URISyntaxException {
    Configuration config = GoogleHadoopFileSystemIntegrationHelper.getTestConfig();
    GoogleHadoopFS ghfs = new GoogleHadoopFS(initUri, config);

    assertThat(ghfs.isValidName("gs://test//")).isTrue();
    assertThat(ghfs.isValidName("hdfs://test//")).isTrue();
    assertThat(ghfs.isValidName("gs//test/../")).isFalse();
    assertThat(ghfs.isValidName("gs//test//.")).isFalse();
  }

  @Test
  public void testCheckPath_shouldThrowExceptionForMismatchingBucket()
      throws IOException, URISyntaxException {
    Configuration config = GoogleHadoopFileSystemIntegrationHelper.getTestConfig();
    GoogleHadoopFS ghfs = new GoogleHadoopFS(initUri, config);

    Path testPath = new Path("gs://fake/file");

    InvalidPathException e =
        assertThrows(InvalidPathException.class, () -> ghfs.checkPath(testPath));

    assertThat(e).hasMessageThat().startsWith("Invalid path");
  }

  @Test
  public void getServerDefaults_shouldReturnSpecifiedConfiguration() throws Exception {
    Configuration config = GoogleHadoopFileSystemIntegrationHelper.getTestConfig();
    config.setLong("fs.gs.block.size", 1);
    GoogleHadoopFS ghfs = new GoogleHadoopFS(initUri, config);

    FsServerDefaults defaults = ghfs.getServerDefaults();

    assertThat(defaults.getBlockSize()).isEqualTo(1);
  }

  @Test
  public void testMkdirs_shouldReturnDefaultFilePermissions()
      throws IOException, URISyntaxException {
    Configuration config = GoogleHadoopFileSystemIntegrationHelper.getTestConfig();
    config.set("fs.gs.reported.permissions", "357");
    GoogleHadoopFS ghfs = new GoogleHadoopFS(initUri, config);

    FsPermission permission = new FsPermission("000");
    FsPermission expectedPermission = new FsPermission("357");

    Path path = new Path(initUri.resolve("/testMkdirs_shouldRespectFilePermissions").toString());
    ghfs.mkdir(path, permission, /* createParent= */ true);

    assertThat(ghfs.getFileStatus(path).getPermission()).isEqualTo(expectedPermission);
  }

  @Test
  public void testDeleteRecursive_shouldDeleteAllInPath() throws IOException, URISyntaxException {
    Configuration config = GoogleHadoopFileSystemIntegrationHelper.getTestConfig();
    GoogleHadoopFS ghfs = new GoogleHadoopFS(initUri, config);

    FsPermission permission = new FsPermission("000");

    URI parentDir = initUri.resolve("/testDeleteRecursive_shouldDeleteAllInPath");
    Path testDir = new Path(parentDir.resolve("test_dir").toString());
    URI testFile = parentDir.resolve("test_file");
    Path testFilePath = new Path(testFile.toString());

    ghfs.mkdir(testDir, permission, /* createParent= */ true);
    gcsFsIHelper.writeTextFile(initUri.getAuthority(), testFile.getPath(), "file data");

    assertThat(ghfs.getFileStatus(testDir)).isNotNull();
    assertThat(ghfs.getFileStatus(testFilePath)).isNotNull();
    assertThat(ghfs.getFileStatus(testDir.getParent())).isNotNull();

    ghfs.delete(testDir.getParent(), /* recursive= */ true);

    assertThrows(FileNotFoundException.class, () -> ghfs.getFileStatus(testDir));
    assertThrows(FileNotFoundException.class, () -> ghfs.getFileStatus(testFilePath));
    assertThrows(FileNotFoundException.class, () -> ghfs.getFileStatus(testDir.getParent()));
  }

  @Test
  public void testWriteWithTrailingChecksum_Enabled_MultiChunk() throws Exception {
    Configuration config = GoogleHadoopFileSystemIntegrationHelper.getTestConfig();
    // enable trailing checksum
    config.setBoolean("fs.gs.write.trailing.checksum.enable", true);
    config.setInt("fs.gs.outputstream.upload.chunk.size", 1 * 1024 * 1024);

    GoogleHadoopFileSystem ghfs = new GoogleHadoopFileSystem();
    ghfs.initialize(initUri, config);

    Path testPath = new Path(initUri.resolve("/testWriteWithTrailingChecksum_Enabled.bin"));

    int fileSize = (int) (2.5 * 1024 * 1024);
    byte[] expectedData = new byte[fileSize];
    new Random().nextBytes(expectedData);
    // write data
    try (FSDataOutputStream out = ghfs.create(testPath)) {
      out.write(expectedData);
    }

    byte[] actualData = new byte[fileSize];
    // read the data
    try (FSDataInputStream in = ghfs.open(testPath)) {
      in.readFully(actualData);
    }

    assertThat(actualData).isEqualTo(expectedData);

    ghfs.delete(testPath, false);
  }

  @Test
  public void testWriteWithTrailingChecksum_Disabled_Regression() throws Exception {
    Configuration config = GoogleHadoopFileSystemIntegrationHelper.getTestConfig();
    // enable trailing checksum
    config.setBoolean("fs.gs.write.trailing.checksum.enable", false);
    config.setInt("fs.gs.outputstream.upload.chunk.size", 1 * 1024 * 1024);
    GoogleHadoopFileSystem ghfs = new GoogleHadoopFileSystem();
    ghfs.initialize(initUri, config);

    Path testPath = new Path(initUri.resolve("/testWriteWithTrailingChecksum_Disabled.bin"));

    int fileSize = 1024 * 1024;
    byte[] expectedData = new byte[fileSize];
    new Random().nextBytes(expectedData);
    // write data
    try (FSDataOutputStream out = ghfs.create(testPath)) {
      out.write(expectedData);
    }

    byte[] actualData = new byte[fileSize];
    // read the data
    try (FSDataInputStream in = ghfs.open(testPath)) {
      in.readFully(actualData);
    }
    assertThat(actualData).isEqualTo(expectedData);

    ghfs.delete(testPath, false);
  }

  @Test
  public void testDeleteNotRecursive_shouldBeAppliedToHierarchyOfDirectories()
      throws IOException, URISyntaxException {
    Configuration config = GoogleHadoopFileSystemIntegrationHelper.getTestConfig();
    GoogleHadoopFS ghfs = new GoogleHadoopFS(initUri, config);

    FsPermission permission = new FsPermission("000");

    URI parentDir = initUri.resolve("/testDeleteRecursive_shouldDeleteAllInPath");
    Path testDir = new Path(parentDir.resolve("test_dir").toString());

    ghfs.mkdir(testDir, permission, /* createParent= */ true);

    assertThrows(
        DirectoryNotEmptyException.class,
        () -> ghfs.delete(testDir.getParent(), /* recursive= */ false));
  }

  @Test
  public void testGetFileStatus_shouldReturnDetails() throws IOException, URISyntaxException {
    Configuration config = GoogleHadoopFileSystemIntegrationHelper.getTestConfig();
    GoogleHadoopFS ghfs = new GoogleHadoopFS(initUri, config);

    URI testFile = initUri.resolve("/testGetFileStatus_shouldReturnDetails");
    Path testFilePath = new Path(testFile.toString());

    gcsFsIHelper.writeTextFile(testFile.getAuthority(), testFile.getPath(), "file content");

    FileStatus fileStatus = ghfs.getFileStatus(testFilePath);
    assertThat(fileStatus.getReplication()).isEqualTo(3);
  }

  @Test
  public void testGetFileBlockLocations_shouldReturnLocalhost()
      throws IOException, URISyntaxException {
    Configuration config = GoogleHadoopFileSystemIntegrationHelper.getTestConfig();
    GoogleHadoopFS ghfs = new GoogleHadoopFS(initUri, config);

    URI testFile = initUri.resolve("/testGetFileBlockLocations_shouldReturnLocalhost");
    Path testFilePath = new Path(testFile.toString());

    gcsFsIHelper.writeTextFile(testFile.getAuthority(), testFile.getPath(), "file content");

    BlockLocation[] fileBlockLocations = ghfs.getFileBlockLocations(testFilePath, 1, 1);
    assertThat(fileBlockLocations).hasLength(1);
    assertThat(fileBlockLocations[0].getHosts()).isEqualTo(new String[] {"localhost"});
  }

  @Test
  public void testListStatus_shouldReturnOneStatus() throws IOException, URISyntaxException {
    Configuration config = GoogleHadoopFileSystemIntegrationHelper.getTestConfig();
    GoogleHadoopFS ghfs = new GoogleHadoopFS(initUri, config);

    URI testFile = initUri.resolve("/testListStatus_shouldReturnOneStatus");
    Path testFilePath = new Path(testFile.toString());

    assertThrows(FileNotFoundException.class, () -> ghfs.listStatus(testFilePath));

    gcsFsIHelper.writeTextFile(testFile.getAuthority(), testFile.getPath(), "file content");

    assertThat(ghfs.listStatus(testFilePath)).hasLength(1);
  }

  @Test
  public void testRenameInternal_shouldMakeOldPathNotFound()
      throws IOException, URISyntaxException {
    Configuration config = GoogleHadoopFileSystemIntegrationHelper.getTestConfig();
    GoogleHadoopFS ghfs = new GoogleHadoopFS(initUri, config);

    URI srcFile = initUri.resolve("/testRenameInternal_shouldMakeOldPathNotFound/src");
    Path srcPath = new Path(srcFile);
    URI dstFile = initUri.resolve("/testRenameInternal_shouldMakeOldPathNotFound/dst");
    Path dstPath = new Path(dstFile);

    gcsFsIHelper.writeTextFile(srcFile.getAuthority(), srcFile.getPath(), "file content");

    assertThat(ghfs.listStatus(srcPath)).hasLength(1);
    assertThrows(FileNotFoundException.class, () -> ghfs.listStatus(dstPath));

    ghfs.renameInternal(srcPath, dstPath);

    assertThrows(FileNotFoundException.class, () -> ghfs.listStatus(srcPath));
    assertThat(ghfs.listStatus(dstPath)).hasLength(1);
  }

  @Test
  public void testWriteWithTrailingChecksum_SimulatedDataCorruption() throws Exception {
    // 1. Setup Configuration
    Configuration config = GoogleHadoopFileSystemIntegrationHelper.getTestConfig();
    GoogleHadoopFileSystem ghfs = new GoogleHadoopFileSystem();
    ghfs.initialize(initUri, config);

    // 2. Build a "Sabotaging" GCS Client
    // We want to keep all standard behaviors (like your Checksum Interceptor)
    // but add one final step to corrupt the body.
    HttpRequestInitializer sabotageInitializer = request -> {
      // Initialize normal credentials and interceptors first
      ghfs.getGcsFs().getGcs().getRequestFactory().getInitializer().initialize(request);

      // Add our corruptor as the LAST step
      final HttpExecuteInterceptor existingInterceptor = request.getInterceptor();
      request.setInterceptor(req -> {
        if (existingInterceptor != null) {
          existingInterceptor.intercept(req);
        }

        // Only corrupt the FINAL upload chunk (the one with the checksum header)
        if (req.getHeaders().containsKey("x-goog-hash") && "PUT".equals(req.getRequestMethod())) {
          // Wrap the content to corrupt it on the wire
          HttpContent originalContent = req.getContent();
          req.setContent(new CorruptingHttpContent(originalContent));
        }
      });
    };

    GoogleCloudStorageOptions gcsOptions = GoogleCloudStorageOptions.builder()
        .setAppName("GHFS-Corruption-Test")
        .build();
    GoogleCloudStorageImpl gcs = new GoogleCloudStorageImpl(gcsOptions, sabotageInitializer);

    // 3. Configure Write Channel
    String bucket = initUri.getAuthority();
    String objectName = "corruption_test_" + System.currentTimeMillis() + ".bin";
    StorageResourceId resourceId = new StorageResourceId(bucket, objectName);

    AsyncWriteChannelOptions writeOptions = AsyncWriteChannelOptions.builder()
        .setTrailingChecksumEnabled(true) // Crucial: Enable the header generation
        .setUploadChunkSize(1024 * 1024)  // Small chunks
        .build();

    GoogleCloudStorageWriteChannel channel = new GoogleCloudStorageWriteChannel(
        gcs.getStorage(),
        new ClientRequestHelper<>(),
        Executors.newCachedThreadPool(),
        writeOptions,
        resourceId,
        CreateObjectOptions.DEFAULT_OVERWRITE,
        ObjectWriteConditions.NONE
    );

    // 4. Run the Test
    channel.initialize();

    byte[] data = new byte[2 * 1024 * 1024]; // 2MB data
    new Random().nextBytes(data);
    channel.write(ByteBuffer.wrap(data));

    // 5. Assert: Server must REJECT the upload
    // We are sending a Valid Checksum Header + Corrupted Data Body.
    // GCS should detect this mismatch and throw 400 Bad Request.
    IOException e = assertThrows(IOException.class, channel::close);

    // 6. Verify the Error
    // The error message from GCS is typically: "Provided CRC32C ... doesn't match calculated ..."
    assertThat(e.getMessage()).contains("400");
    assertThat(e.getMessage()).contains("CRC32C");

    // Cleanup
    try { ghfs.getGcsFs().delete(resourceId); } catch (Exception ignored) {}
  }

  // Helper class to corrupt data on the fly
  class CorruptingHttpContent implements HttpContent {
    private final HttpContent delegate;

    public CorruptingHttpContent(HttpContent delegate) {
      this.delegate = delegate;
    }

    @Override
    public long getLength() throws IOException {
      return delegate.getLength();
    }

    @Override
    public String getType() {
      return delegate.getType();
    }

    @Override
    public boolean retrySupported() {
      return delegate.retrySupported();
    }

    @Override
    public void writeTo(OutputStream out) throws IOException {
      // Wrap the output stream to corrupt the first byte written
      delegate.writeTo(new FilterOutputStream(out) {
        private boolean corrupted = false;

        @Override
        public void write(int b) throws IOException {
          if (!corrupted) {
            // FLIP A BIT: XOR with 1 to change the value
            super.write(b ^ 1);
            corrupted = true;
          } else {
            super.write(b);
          }
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
          if (!corrupted && len > 0) {
            // Corrupt the first byte in the array
            b[off] = (byte) (b[off] ^ 1);
            super.write(b, off, len);
            // Restore it so we don't corrupt the source array permanently
            b[off] = (byte) (b[off] ^ 1);
            corrupted = true;
          } else {
            super.write(b, off, len);
          }
        }
      });
    }
  }
}
