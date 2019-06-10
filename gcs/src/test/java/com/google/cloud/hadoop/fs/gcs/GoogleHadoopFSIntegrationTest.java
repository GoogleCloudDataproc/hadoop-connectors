/*
 * Copyright 2019 Google Inc. All Rights Reserved.
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

import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.DELEGATION_TOKEN_BINDING_CLASS;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.PERMISSIONS_TO_REPORT;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemTestHelper.createInMemoryGoogleHadoopFileSystem;
import static com.google.common.truth.Truth.assertThat;
import static org.apache.hadoop.fs.FileSystemTestHelper.createFile;
import static org.junit.Assert.*;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemBase.GcsFileChecksumType;
import com.google.cloud.hadoop.fs.gcs.auth.TestDelegationTokenBindingImpl;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.DirectoryNotEmptyException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration tests for GoogleHadoopFS class. */
@RunWith(JUnit4.class)
public class GoogleHadoopFSIntegrationTest {
  private static URI initUri;
  private GoogleHadoopFileSystem fs;
  private FsPermission permissions;

  @Before
  public void before() throws IOException {
    initUri = new Path("gs://test/").toUri();
    fs = createInMemoryGoogleHadoopFileSystem();
    fs.initialize(initUri, loadConfig());
    permissions = new FsPermission((short) 000);
  }

  @After
  public void after() throws IOException {
    FileSystem.closeAll();
  }

  @Test
  public void testInitializationWithUriAndConf_shouldGiveFsStatusWithNotUsedMemory()
      throws IOException, URISyntaxException {
    GoogleHadoopFS googleHadoopFS = new GoogleHadoopFS(initUri, loadConfig());
    assertThat(googleHadoopFS.getFsStatus().getUsed()).isEqualTo(0);
  }

  @Test
  public void testInitializationWithGhfsUriAndConf_shouldGiveFsStatusWithNotUsedMemory()
      throws IOException, URISyntaxException {
    GoogleHadoopFS googleHadoopFS = new GoogleHadoopFS(fs, initUri, loadConfig());
    assertThat(googleHadoopFS.getFsStatus().getUsed()).isEqualTo(0);
  }

  @Test
  public void testCreateInternal_shouldCreateNewGhfs() throws IOException, URISyntaxException {
    GoogleHadoopFS googleHadoopFS = new GoogleHadoopFS(fs, initUri, loadConfig());
    Path file = new Path(initUri + "file");
    EnumSet<CreateFlag> flag = EnumSet.noneOf(CreateFlag.class);
    flag.add(CreateFlag.CREATE);
    FsPermission absolutePermission = null;
    short replication = 1;
    int bufferSize = 128;
    long blockSize = 32;
    Progressable progress = null;
    Options.ChecksumOpt checksumOpt = new Options.ChecksumOpt();
    boolean createParent = true;
    FSDataOutputStream fsDataOutputStream =
        googleHadoopFS.createInternal(
            file,
            flag,
            absolutePermission,
            bufferSize,
            replication,
            blockSize,
            progress,
            checksumOpt,
            createParent);
    assertThat(fsDataOutputStream).isNotNull();
    createParent = false;
    fsDataOutputStream =
        googleHadoopFS.createInternal(
            file,
            flag,
            absolutePermission,
            bufferSize,
            replication,
            blockSize,
            progress,
            checksumOpt,
            createParent);
    assertThat(fsDataOutputStream).isNotNull();
  }

  @Test
  public void testGetUriDefaultPort_shouldBeEqualToGhfsDefaultPort()
      throws IOException, URISyntaxException {
    GoogleHadoopFS googleHadoopFS = new GoogleHadoopFS(fs, initUri, loadConfig());
    assertThat(googleHadoopFS.getUriDefaultPort()).isEqualTo(fs.getDefaultPort());
  }

  @Test
  public void testGetUri_shouldBeEqualToGhfsUri() throws IOException, URISyntaxException {
    GoogleHadoopFS googleHadoopFS = new GoogleHadoopFS(fs, initUri, loadConfig());
    assertThat(googleHadoopFS.getUri()).isEqualTo(fs.getUri());
  }

  @Test
  public void testValidName_shouldNotContainPoints() throws IOException, URISyntaxException {
    GoogleHadoopFS googleHadoopFS = new GoogleHadoopFS(fs, initUri, loadConfig());
    assertThat(googleHadoopFS.isValidName("gs://test//")).isTrue();
    assertThat(googleHadoopFS.isValidName("hdfs://test//")).isTrue();
    assertThat(googleHadoopFS.isValidName("gs//test/../")).isFalse();
    assertThat(googleHadoopFS.isValidName("gs//test//.")).isFalse();
  }

  @Test
  public void testCheckPath_shouldThrowExceptionForMismatchingBucket()
      throws IOException, URISyntaxException {
    GoogleHadoopFS googleHadoopFS = new GoogleHadoopFS(fs, initUri, loadConfig());
    Path file = new Path("gs://fake/" + "file");
    InvalidPathException e =
        assertThrows(InvalidPathException.class, () -> googleHadoopFS.checkPath(file));
    assertThat(e.getLocalizedMessage()).startsWith("Invalid path");
  }

  @Test
  public void testGetUserDefault_shouldReturnSpecifiedConfiguration()
      throws IOException, URISyntaxException {
    Path file = new Path("gs://fake/" + "file");
    Configuration conf = new Configuration();
    conf.setLong(GoogleHadoopFileSystemConfiguration.BLOCK_SIZE.getKey(), 1);
    conf.set(
        GoogleHadoopFileSystemConfiguration.GCS_FILE_CHECKSUM_TYPE.getKey(),
        String.valueOf(GcsFileChecksumType.MD5));
    GoogleHadoopFS googleHadoopFS = new GoogleHadoopFS(fs, initUri, conf);

    assertThat(googleHadoopFS.getServerDefaults(file).getBlockSize()).isEqualTo(1);
  }

  @Test
  public void testMkdirs_shouldRespectFilePermissions() throws IOException, URISyntaxException {
    Path path = new Path("gs://test/1/2");
    GoogleHadoopFS googleHadoopFS = prepareGoogleHadoopFS(path);

    googleHadoopFS.mkdir(path, permissions, true);
    googleHadoopFS.mkdir(path, permissions, false);

    assertThat(googleHadoopFS.getFileStatus(path).getPermission()).isEqualTo(permissions);
    assertThat(googleHadoopFS.getFileStatus(path).getPermission()).isEqualTo(permissions);
  }

  @Test
  public void testDeleteRecursive_shouldDeleteAllInPath() throws IOException, URISyntaxException {
    Path path = new Path("gs://test/1/2");
    GoogleHadoopFS googleHadoopFS = prepareGoogleHadoopFS(path);

    googleHadoopFS.mkdir(path, permissions, true);
    googleHadoopFS.delete(new Path(new URI("gs://test")), true);

    assertFalse(googleHadoopFS.delete(path, true));
  }

  @Test
  public void testDeleteNotRecursive_shouldBeAppliedToNotEmptyDirectories()
      throws IOException, URISyntaxException {
    Path path = new Path("gs://test/1/2");
    GoogleHadoopFS googleHadoopFS = prepareGoogleHadoopFS(path);

    googleHadoopFS.mkdir(path, permissions, true);

    assertThrows(
        DirectoryNotEmptyException.class,
        () -> googleHadoopFS.delete(new Path(new URI("gs://test")), false));
    assertTrue(googleHadoopFS.delete(path, false));
  }

  @Test
  public void testGetFileStatus_shouldReturnDetails() throws IOException, URISyntaxException {
    Path path = new Path("gs://test/1/2");
    GoogleHadoopFS googleHadoopFS = prepareGoogleHadoopFS(path);

    createFile(fs, path);

    assertThat(googleHadoopFS.getFileStatus(path).getReplication()).isEqualTo((short) 3);
  }

  @Test
  public void testGetFileBlockLocations_shouldReturnLocalhost()
      throws IOException, URISyntaxException {
    Path path = new Path("gs://test/1/2");
    GoogleHadoopFS googleHadoopFS = prepareGoogleHadoopFS(path);

    createFile(fs, path);

    assertThat(googleHadoopFS.getFileBlockLocations(path, (long) 1, (long) 1).clone()[0].getHosts())
        .isEqualTo(new String[] {"localhost"});
  }

  @Test
  public void testGetFsStatus_shouldReturnStatisticsWith() throws IOException, URISyntaxException {
    Path path = new Path("gs://test/1/2");
    GoogleHadoopFS googleHadoopFS = prepareGoogleHadoopFS(path);

    assertThat(googleHadoopFS.getFsStatus().getUsed()).isEqualTo(0);
  }

  @Test
  public void testListStatus_shouldReturnOneStatus() throws IOException, URISyntaxException {
    Path path = new Path("gs://test/1/2");
    GoogleHadoopFS googleHadoopFS = prepareGoogleHadoopFS(path);

    createFile(fs, path);

    assertThat(googleHadoopFS.listStatus(path).length).isEqualTo(1);
  }

  @Test
  public void testRenameInternal_shouldMakeOldPathNotFound()
      throws IOException, URISyntaxException {
    Path path = new Path("gs://test/1/2");
    Path renamedPath = new Path("gs://test/2/");
    GoogleHadoopFS googleHadoopFS = prepareGoogleHadoopFS(path);
    createFile(fs, path);

    assertThat(googleHadoopFS.listStatus(path).length).isEqualTo(1);
    googleHadoopFS.renameInternal(path, renamedPath);
    assertThrows(FileNotFoundException.class, () -> googleHadoopFS.listStatus(path));
    assertThat(googleHadoopFS.listStatus(renamedPath).length).isEqualTo(1);
  }

  private Configuration loadConfig() {
    Configuration config = new Configuration();

    config.set(GoogleHadoopFileSystemConfiguration.GCS_PROJECT_ID.getKey(), "test_project");
    config.setInt(GoogleHadoopFileSystemConfiguration.GCS_INPUT_STREAM_BUFFER_SIZE.getKey(), 512);
    config.setLong(GoogleHadoopFileSystemConfiguration.BLOCK_SIZE.getKey(), 1024);
    // Token binding config
    config.set(
        DELEGATION_TOKEN_BINDING_CLASS.getKey(), TestDelegationTokenBindingImpl.class.getName());
    config.set(
        TestDelegationTokenBindingImpl.TestAccessTokenProviderImpl.TOKEN_CONFIG_PROPERTY_NAME,
        "qWDAWFA3WWFAWFAWFAW3FAWF3AWF3WFAF33GR5G5"); // Bogus auth token

    return config;
  }

  private GoogleHadoopFS prepareGoogleHadoopFS(Path path) throws IOException, URISyntaxException {
    Configuration configuration = new Configuration();
    configuration.set(PERMISSIONS_TO_REPORT.getKey(), String.valueOf(this.permissions));
    return new GoogleHadoopFS(fs, path.toUri(), configuration);
  }
}
