/*
 * Copyright 2014 Google Inc. All Rights Reserved.
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

import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.STREAM_READ_OPERATIONS;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.STREAM_WRITE_OPERATIONS;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_CONFIG_PREFIX;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_LAZY_INITIALIZATION_ENABLE;
import static com.google.cloud.hadoop.gcsio.testing.InMemoryGoogleCloudStorage.getInMemoryGoogleCloudStorageOptions;
import static com.google.cloud.hadoop.util.HadoopCredentialConfiguration.GROUP_IMPERSONATION_SERVICE_ACCOUNT_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialConfiguration.IMPERSONATION_SERVICE_ACCOUNT_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialConfiguration.SERVICE_ACCOUNT_JSON_KEYFILE_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialConfiguration.USER_IMPERSONATION_SERVICE_ACCOUNT_SUFFIX;
import static com.google.common.truth.Truth.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertThrows;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions;
import com.google.cloud.hadoop.gcsio.MethodOutcome;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.cloud.hadoop.gcsio.UriPaths;
import com.google.cloud.hadoop.gcsio.testing.InMemoryGoogleCloudStorage;
import com.google.cloud.hadoop.util.AccessTokenProvider;
import com.google.cloud.hadoop.util.testing.TestingAccessTokenProvider;
import com.google.common.io.ByteStreams;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Unit tests for {@link GoogleHadoopFileSystem} class. */
@RunWith(Parameterized.class)
public class GoogleHadoopFileSystemTest extends GoogleHadoopFileSystemIntegrationTest {

  @Before
  public void before() throws IOException {
    // Disable logging.
    // Normally you would need to keep a strong reference to any logger used for
    // configuration, but the "root" logger is always present.
    Logger.getLogger("").setLevel(Level.OFF);

    ghfs = GoogleHadoopFileSystemTestHelper.createInMemoryGoogleHadoopFileSystem();
    ghfsFileSystemDescriptor = (FileSystemDescriptor) ghfs;

    postCreateInit();
  }

  @Test
  public void testVersionString() {
    assertThat(GoogleHadoopFileSystemBase.VERSION).isNotNull();
    assertThat(
            GoogleHadoopFileSystemBase.UNKNOWN_VERSION.equals(GoogleHadoopFileSystemBase.VERSION))
        .isFalse();
  }

  @Test
  public void lazyInitialization_succeeds_withInvalidCredentialsConfiguration() throws Exception {
    new GoogleHadoopFileSystem();
    Configuration lazyConf = new Configuration();
    lazyConf.setBoolean(GCS_LAZY_INITIALIZATION_ENABLE.getKey(), true);
    lazyConf.set(
        GCS_CONFIG_PREFIX + SERVICE_ACCOUNT_JSON_KEYFILE_SUFFIX.getKey(), "non-existent.json");
    lazyConf.setEnum("fs.gs.client.type", storageClientType);
    GoogleHadoopFileSystem lazyFs = new GoogleHadoopFileSystem();

    lazyFs.initialize(new URI("gs://test-non-existent/"), lazyConf);
    lazyFs.close();
  }

  @Test
  public void lazyInitialization_deleteCall_fails_withInvalidCredentialsConfiguration()
      throws Exception {
    Configuration lazyConf = new Configuration();
    lazyConf.setBoolean(GCS_LAZY_INITIALIZATION_ENABLE.getKey(), true);
    lazyConf.set(
        GCS_CONFIG_PREFIX + SERVICE_ACCOUNT_JSON_KEYFILE_SUFFIX.getKey(), "non-existent.json");
    lazyConf.setEnum("fs.gs.client.type", storageClientType);
    GoogleHadoopFileSystem lazyFs = new GoogleHadoopFileSystem();

    lazyFs.initialize(new URI("gs://test-non-existent"), lazyConf);

    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () -> lazyFs.delete(new Path("gs://test-non-existent/dir"), false));

    assertThat(exception).hasMessageThat().isEqualTo("Failed to create GCS FS");
    assertThat(exception).hasCauseThat().isInstanceOf(FileNotFoundException.class);
    assertThat(exception)
        .hasCauseThat()
        .hasMessageThat()
        .isAnyOf(
            "non-existent.json (No such file or directory)",
            "non-existent.json (The system cannot find the file specified)");

    lazyFs.close();
  }

  @Test
  public void eagerInitialization_fails_withInvalidCredentialsConfiguration() {
    new GoogleHadoopFileSystem();
    Configuration eagerConf = new Configuration();
    eagerConf.setBoolean(GCS_LAZY_INITIALIZATION_ENABLE.getKey(), false);
    eagerConf.set(
        GCS_CONFIG_PREFIX + SERVICE_ACCOUNT_JSON_KEYFILE_SUFFIX.getKey(), "non-existent.json");
    eagerConf.setEnum("fs.gs.client.type", storageClientType);

    FileSystem eagerFs = new GoogleHadoopFileSystem();

    FileNotFoundException exception =
        assertThrows(
            FileNotFoundException.class,
            () -> eagerFs.initialize(new URI("gs://test-non-existent"), eagerConf));

    assertThat(exception)
        .hasMessageThat()
        .isAnyOf(
            "non-existent.json (No such file or directory)",
            "non-existent.json (The system cannot find the file specified)");
  }

  // -----------------------------------------------------------------
  // Tests that exercise behavior defined in HdfsBehavior.
  // -----------------------------------------------------------------

  /** Validates {@link GoogleHadoopFileSystem#rename(Path, Path)}. */
  @Test
  @Override
  public void testRename() throws Exception {
    renameHelper(
        new HdfsBehavior() {
          /**
           * Returns the {@link MethodOutcome} of trying to rename an existing file into the root
           * directory.
           */
          @Override
          public MethodOutcome renameFileIntoRootOutcome() {
            return new MethodOutcome(MethodOutcome.Type.RETURNS_TRUE);
          }
        });
  }

  @Test
  public void testCouldUseFlatGlob() throws IOException, URISyntaxException {
    Configuration lazyConf = new Configuration();
    lazyConf.setBoolean(GCS_LAZY_INITIALIZATION_ENABLE.getKey(), true);
    try (GoogleHadoopFileSystem lazyFs = new GoogleHadoopFileSystem()) {
      assertThat(lazyFs.couldUseFlatGlob(new Path(new URI("gs://**/test/")))).isFalse();
    }
  }

  @Test
  public void testTrimToPrefixWithoutGlob() {
    GoogleHadoopFileSystem lazyFs = new GoogleHadoopFileSystem();
    lazyFs.trimToPrefixWithoutGlob("gs://**/test");
    assertThat(lazyFs.trimToPrefixWithoutGlob("gs://**/test")).isEqualTo("gs://");
  }

  @Override
  @Test
  public void testGetGcsPath() throws URISyntaxException {
    GoogleHadoopFileSystem myghfs = (GoogleHadoopFileSystem) ghfs;
    URI gcsPath = new URI("gs://" + myghfs.getUri().getAuthority() + "/dir/obj");
    assertThat(myghfs.getGcsPath(new Path(gcsPath))).isEqualTo(gcsPath);
  }

  @Test
  public void testGetDefaultPortIndicatesPortsAreNotUsed() throws Exception {
    Configuration config = new Configuration();
    config.set("fs.gs.auth.access.token.provider.impl", TestingAccessTokenProvider.class.getName());
    config.setEnum("fs.gs.client.type", storageClientType);
    URI gsUri = new URI("gs://foobar/");

    GoogleHadoopFileSystem ghfs = new GoogleHadoopFileSystem();
    ghfs.initialize(gsUri, config);

    assertThat(ghfs.getDefaultPort()).isEqualTo(-1);
  }

  @Test
  public void testImpsersonationServiceAccountUsed() throws Exception {
    Configuration config = new Configuration();
    config.setClass(
        "fs.gs.auth.access.token.provider.impl",
        TestingAccessTokenProvider.class,
        AccessTokenProvider.class);
    config.set(
        GCS_CONFIG_PREFIX + IMPERSONATION_SERVICE_ACCOUNT_SUFFIX.getKey(), "test-service-account");

    URI gsUri = new URI("gs://foobar/");
    GoogleHadoopFileSystem ghfs = new GoogleHadoopFileSystem();

    Exception exception =
        assertThrows(GoogleJsonResponseException.class, () -> ghfs.initialize(gsUri, config));
    assertThat(exception).hasMessageThat().startsWith("401 Unauthorized");
  }

  @Test
  public void testImpsersonationUserNameIdentifierUsed() throws Exception {
    Configuration config = new Configuration();
    config.setClass(
        "fs.gs.auth.access.token.provider.impl",
        TestingAccessTokenProvider.class,
        AccessTokenProvider.class);
    config.set(
        GCS_CONFIG_PREFIX
            + USER_IMPERSONATION_SERVICE_ACCOUNT_SUFFIX.getKey()
            + UserGroupInformation.getCurrentUser().getShortUserName(),
        "test-service-account");

    URI gsUri = new URI("gs://foobar/");
    GoogleHadoopFileSystem ghfs = new GoogleHadoopFileSystem();

    Exception exception =
        assertThrows(
            GoogleJsonResponseException.class,
            () -> {
              ghfs.initialize(gsUri, config);
            });
    assertThat(exception).hasMessageThat().startsWith("401 Unauthorized");
  }

  @Test
  public void testImpsersonationGroupNameIdentifierUsed() throws Exception {
    Configuration config = new Configuration();
    config.setClass(
        "fs.gs.auth.access.token.provider.impl",
        TestingAccessTokenProvider.class,
        AccessTokenProvider.class);
    config.set(
        GCS_CONFIG_PREFIX
            + GROUP_IMPERSONATION_SERVICE_ACCOUNT_SUFFIX.getKey()
            + UserGroupInformation.getCurrentUser().getGroupNames()[0],
        "test-service-account");

    URI gsUri = new URI("gs://foobar/");
    GoogleHadoopFileSystem ghfs = new GoogleHadoopFileSystem();

    Exception exception =
        assertThrows(
            GoogleJsonResponseException.class,
            () -> {
              ghfs.initialize(gsUri, config);
            });
    assertThat(exception).hasMessageThat().startsWith("401 Unauthorized");
  }

  @Test
  public void testImpsersonationUserAndGroupNameIdentifiersUsed() throws Exception {
    Configuration config = new Configuration();
    config.setClass(
        "fs.gs.auth.access.token.provider.impl",
        TestingAccessTokenProvider.class,
        AccessTokenProvider.class);
    config.set(
        GCS_CONFIG_PREFIX
            + USER_IMPERSONATION_SERVICE_ACCOUNT_SUFFIX.getKey()
            + UserGroupInformation.getCurrentUser().getShortUserName(),
        "test-service-account1");
    config.set(
        GCS_CONFIG_PREFIX
            + GROUP_IMPERSONATION_SERVICE_ACCOUNT_SUFFIX.getKey()
            + UserGroupInformation.getCurrentUser().getGroupNames()[0],
        "test-service-account2");

    URI gsUri = new URI("gs://foobar/");
    GoogleHadoopFileSystem ghfs = new GoogleHadoopFileSystem();

    Exception exception =
        assertThrows(
            GoogleJsonResponseException.class,
            () -> {
              ghfs.initialize(gsUri, config);
            });
    assertThat(exception).hasMessageThat().startsWith("401 Unauthorized");
  }

  @Test
  public void testImpsersonationServiceAccountAndUserAndGroupNameIdentifierUsed() throws Exception {
    Configuration config = new Configuration();
    config.setClass(
        "fs.gs.auth.access.token.provider.impl",
        TestingAccessTokenProvider.class,
        AccessTokenProvider.class);
    config.set(
        GCS_CONFIG_PREFIX + IMPERSONATION_SERVICE_ACCOUNT_SUFFIX.getKey(), "test-service-account1");
    config.set(
        GCS_CONFIG_PREFIX
            + USER_IMPERSONATION_SERVICE_ACCOUNT_SUFFIX.getKey()
            + UserGroupInformation.getCurrentUser().getShortUserName(),
        "test-service-account2");
    config.set(
        GCS_CONFIG_PREFIX
            + GROUP_IMPERSONATION_SERVICE_ACCOUNT_SUFFIX.getKey()
            + UserGroupInformation.getCurrentUser().getGroupNames()[0],
        "test-service-account3");

    URI gsUri = new URI("gs://foobar/");
    GoogleHadoopFileSystem ghfs = new GoogleHadoopFileSystem();

    Exception exception =
        assertThrows(
            GoogleJsonResponseException.class,
            () -> {
              ghfs.initialize(gsUri, config);
            });
    assertThat(exception).hasMessageThat().startsWith("401 Unauthorized");
  }

  @Test
  public void testImpsersonationInvalidUserNameIdentifierUsed() throws Exception {
    Configuration config = new Configuration();
    config.setClass(
        "fs.gs.auth.access.token.provider.impl",
        TestingAccessTokenProvider.class,
        AccessTokenProvider.class);
    config.set(
        GCS_CONFIG_PREFIX + USER_IMPERSONATION_SERVICE_ACCOUNT_SUFFIX.getKey() + "invalid-user",
        "test-service-account");

    URI gsUri = new URI("gs://foobar/");
    GoogleHadoopFileSystem ghfs = new GoogleHadoopFileSystem();
    ghfs.initialize(gsUri, config);
  }

  @Test
  public void testTotalTimeStatistics() throws IOException {
    GhfsStorageStatistics stats = new GhfsStorageStatistics();
    stats.updateStats(STREAM_READ_OPERATIONS, 10, 100, 200, 10, new Object());
    stats.addTotalTimeStatistic(STREAM_READ_OPERATIONS.getSymbol() + "_duration");
    assertThat(stats.getLong(STREAM_READ_OPERATIONS.getSymbol() + "_duration")).isEqualTo(200);

    stats.updateStats(STREAM_WRITE_OPERATIONS, 10, 100, 200, 10, new Object());
    stats.addTotalTimeStatistic(STREAM_WRITE_OPERATIONS.getSymbol() + "_duration");
    assertThat(stats.getLong(STREAM_WRITE_OPERATIONS.getSymbol() + "_duration")).isEqualTo(200);
  }

  @Test
  public void testRenameDirectory_rejectsPathTraversalObject() throws Exception {
    for (boolean moveEnabled : new boolean[] {true, false}) {
      GoogleCloudStorageFileSystem memoryGcsFs =
          new GoogleCloudStorageFileSystem(
              InMemoryGoogleCloudStorage::new,
              GoogleCloudStorageFileSystemOptions.builder()
                  .setCloudStorageOptions(
                      getInMemoryGoogleCloudStorageOptions()
                          .toBuilder()
                          .setMoveOperationEnabled(moveEnabled)
                          .build())
                  .build());
      try (GoogleHadoopFileSystem myghfs = new GoogleHadoopFileSystem(memoryGcsFs)) {
        URI initUri = new URI("gs://traversal-ghfs-bucket-" + moveEnabled);
        myghfs.initialize(initUri, new Configuration());
        myghfs.mkdirs(new Path(initUri));
        String bucket = myghfs.getUri().getAuthority();

        try (WritableByteChannel ch =
            myghfs.getGcsFs().getGcs().create(new StorageResourceId(bucket, "staging/legit.txt"))) {
          ch.write(ByteBuffer.wrap("legit".getBytes(UTF_8)));
        }
        try (WritableByteChannel ch =
            myghfs.getGcsFs().getGcs().create(new StorageResourceId(bucket, "victim/existing"))) {
          ch.write(ByteBuffer.wrap("ORIGINAL".getBytes(UTF_8)));
        }
        try (WritableByteChannel ch =
            myghfs
                .getGcsFs()
                .getGcs()
                .create(new StorageResourceId(bucket, "staging/../victim/existing"))) {
          ch.write(ByteBuffer.wrap("ATTACKER".getBytes(UTF_8)));
        }

        boolean renamed =
            myghfs.rename(
                new Path("gs://" + bucket + "/staging"), new Path("gs://" + bucket + "/final"));

        assertThat(renamed).isFalse();
        assertThat(
                myghfs
                    .getGcsFs()
                    .getGcs()
                    .getItemInfo(new StorageResourceId(bucket, "final/legit.txt"))
                    .exists())
            .isFalse();
        try (SeekableByteChannel ch =
            myghfs
                .getGcsFs()
                .getGcs()
                .open(
                    new StorageResourceId(bucket, "victim/existing"),
                    GoogleCloudStorageReadOptions.DEFAULT)) {
          ByteBuffer buf = ByteBuffer.allocate((int) ch.size());
          while (buf.hasRemaining()) {
            if (ch.read(buf) == -1) {
              break;
            }
          }
          assertThat(new String(buf.array(), UTF_8)).isEqualTo("ORIGINAL");
        }
      }
    }
  }

  @Test
  public void testListStatusAndGlobStatus_dotSegmentErrorContractAndModeConsistency()
      throws Exception {
    for (String globAlgo : new String[] {"CONCURRENT", "FLAT", "DEFAULT"}) {
      GoogleCloudStorageFileSystem memoryGcsFs =
          new GoogleCloudStorageFileSystem(
              InMemoryGoogleCloudStorage::new,
              GoogleCloudStorageFileSystemOptions.builder()
                  .setCloudStorageOptions(getInMemoryGoogleCloudStorageOptions())
                  .build());
      try (GoogleHadoopFileSystem myghfs = new GoogleHadoopFileSystem(memoryGcsFs)) {
        Configuration conf = new Configuration();
        conf.set("fs.gs.glob.algorithm", globAlgo);
        URI initUri = new URI("gs://glob-dot-bucket-" + globAlgo.toLowerCase());
        myghfs.initialize(initUri, conf);
        myghfs.mkdirs(new Path(initUri));
        String bucket = myghfs.getUri().getAuthority();

        try (WritableByteChannel ch =
            myghfs
                .getGcsFs()
                .getGcs()
                .create(new StorageResourceId(bucket, "globroot/good/a.txt"))) {
          ch.write(ByteBuffer.wrap("GOOD".getBytes(UTF_8)));
        }
        try (WritableByteChannel ch =
            myghfs
                .getGcsFs()
                .getGcs()
                .create(new StorageResourceId(bucket, "globroot/bad/../evil.txt"))) {
          ch.write(ByteBuffer.wrap("ATTACKER".getBytes(UTF_8)));
        }

        URI badUri = new URI("gs", bucket, "/globroot/bad/../evil.txt", null, null);
        assertThat(assertThrows(IllegalArgumentException.class, () -> myghfs.getHadoopPath(badUri)))
            .hasMessageThat()
            .contains("must not contain '.' or '..' path segments");

        // Remote GCS dot-segment objects surface as checked IOException on FileSystem APIs.
        Path badDirPath = new Path("gs://" + bucket + "/globroot/bad");
        assertThat(assertThrows(IOException.class, () -> myghfs.listStatus(badDirPath)))
            .hasMessageThat()
            .contains("must not contain '.' or '..' path segments");

        // Matching glob descending into poisoned directory throws IOException across all modes.
        Path wildcardGlob = new Path("gs://" + bucket + "/globroot/*/*.txt");
        assertThat(assertThrows(IOException.class, () -> myghfs.globStatus(wildcardGlob)))
            .hasMessageThat()
            .contains("must not contain '.' or '..' path segments");

        // Unrelated sibling dot-segment prefix does not break glob targeting a clean directory.
        FileStatus[] goodMatches =
            myghfs.globStatus(new Path("gs://" + bucket + "/globroot/{good}/*.txt"));
        assertThat(goodMatches).hasLength(1);
        assertThat(goodMatches[0].getPath().toString())
            .isEqualTo("gs://" + bucket + "/globroot/good/a.txt");
      }
    }
  }

  @Test
  public void testPercentEncodedDotSegments_rejectedInGetGcsPathCreateAndRename() throws Exception {
    try (FileSystem localFs =
        GoogleHadoopFileSystemTestHelper.createInMemoryGoogleHadoopFileSystem()) {
      GoogleHadoopFileSystem myghfs = (GoogleHadoopFileSystem) localFs;
      String bucket = myghfs.getUri().getAuthority();
      Path srcFile = new Path("gs://" + bucket + "/src_file.txt");
      try (FSDataOutputStream out = myghfs.create(srcFile)) {
        out.write("hello".getBytes(UTF_8));
      }
      myghfs.mkdirs(new Path("gs://" + bucket + "/staging"));

      Path encodedDotDotDst = new Path(URI.create("gs://" + bucket + "/staging/%2e%2e"));
      Path encodedDotFile = new Path(URI.create("gs://" + bucket + "/staging/%2e/f"));

      assertThat(
              assertThrows(
                  IllegalArgumentException.class, () -> myghfs.getGcsPath(encodedDotDotDst)))
          .hasMessageThat()
          .contains("must not contain '.' or '..' path segments");
      assertThat(assertThrows(IllegalArgumentException.class, () -> myghfs.create(encodedDotFile)))
          .hasMessageThat()
          .contains("must not contain '.' or '..' path segments");
      assertThat(
              assertThrows(
                  IllegalArgumentException.class, () -> myghfs.rename(srcFile, encodedDotDotDst)))
          .hasMessageThat()
          .contains("must not contain '.' or '..' path segments");
    }
  }

  @Test
  public void testGetHadoopPathAndRoundTrip_validDotsColonsBackslashesAndBucketWithoutSlash()
      throws Exception {
    try (FileSystem localFs =
        GoogleHadoopFileSystemTestHelper.createInMemoryGoogleHadoopFileSystem()) {
      GoogleHadoopFileSystem myghfs = (GoogleHadoopFileSystem) localFs;
      String bucket = myghfs.getUri().getAuthority();

      // Bucket URI without trailing slash returns fsRoot instead of throwing NPE/IAE.
      assertThat(myghfs.getHadoopPath(new URI("gs://" + bucket)))
          .isEqualTo(new Path(myghfs.getUri()));

      String[] validObjectNames = {
        "dir/.hidden",
        "dir/..leading",
        "dir/trailing..",
        "dir/part:001.parquet",
        "dir/2026-11-20T04:00:00Z.parquet",
        "dir/%2e%2e/literal.txt",
        "dir/query?a=1#frag",
        "dir/my file.txt",
        "dir/staging\\..\\literal_backslash",
        "dir/c:\\..\\literal_drive",
      };

      for (String obj : validObjectNames) {
        try (WritableByteChannel ch =
            myghfs.getGcsFs().getGcs().create(new StorageResourceId(bucket, obj))) {
          ch.write(ByteBuffer.wrap(("payload:" + obj).getBytes(UTF_8)));
        }
        URI gcsUri = UriPaths.fromStringPathComponents(bucket, obj, false);
        Path hadoopPath = myghfs.getHadoopPath(gcsUri);
        assertThat(myghfs.getGcsPath(hadoopPath)).isEqualTo(gcsUri);
        try (FSDataInputStream in = myghfs.open(hadoopPath)) {
          byte[] bytes = ByteStreams.toByteArray(in);
          assertThat(new String(bytes, UTF_8)).isEqualTo("payload:" + obj);
        }
      }

      FileStatus[] listed = myghfs.listStatus(new Path("gs://" + bucket + "/dir"));
      assertThat(listed.length).isAtLeast(9);
    }
  }

  // -----------------------------------------------------------------
  // Inherited tests that we suppress because their behavior differs
  // from the base class.
  // TODO(user): Add tests for subtleties of how global-rooted
  // initialization differs from bucket-rooted initialization.
  // -----------------------------------------------------------------
  @Override
  public void testInitializeSuccess() {}

  @Override
  public void testInitializeSucceedsWhenNoProjectIdConfigured() {}

  @Override
  public void testInitializeWithWorkingDirectory() {}

  @Override
  public void testIOExceptionIsThrowAfterClose() {}

  @Override
  public void testFileSystemIsRemovedFromCacheOnClose() {}

  @Override
  public void testConfigurablePermissions() {}

  @Override
  public void testFileStatusUser() {}

  @Override
  public void testCrc32cFileChecksum() {}

  @Override
  public void testMd5FileChecksum() {}

  @Override
  public void testConcurrentCreationWithoutOverwrite_onlyOneSucceeds() {}

  @Override
  public void testInitializeCompatibleWithHadoopCredentialProvider() {}

  @Override
  public void testRenameHnBucket() {}

  @Override
  public void testRenameWithMoveDisabled() {}

  @Override
  public void testHnBucketRecursiveDeleteOperationOnDirectory() {}

  @Override
  public void testHnBucketRecursiveDeleteOperationOnBucket() {}

  @Override
  public void testHnBucketNonRecursiveDeleteOperation() {}

  @Override
  public void testHnBucketDeleteOperationOnNonExistingFolder() {}
}
