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

import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.FILES_CREATED;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.INVOCATION_CREATE;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.INVOCATION_CREATE_NON_RECURSIVE;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.INVOCATION_DELETE;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.INVOCATION_EXISTS;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.INVOCATION_GET_FILE_CHECKSUM;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.INVOCATION_GET_FILE_STATUS;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.INVOCATION_GLOB_STATUS;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.INVOCATION_LIST_FILES;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.INVOCATION_LIST_LOCATED_STATUS;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.INVOCATION_LIST_STATUS;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.INVOCATION_MKDIRS;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.INVOCATION_OPEN;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.INVOCATION_OP_XATTR_LIST;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.INVOCATION_RENAME;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.INVOCATION_XATTR_GET_MAP;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.INVOCATION_XATTR_GET_NAMED;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.INVOCATION_XATTR_GET_NAMED_MAP;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.DELEGATION_TOKEN_BINDING_CLASS;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_CONFIG_PREFIX;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_REPAIR_IMPLICIT_DIRECTORIES_ENABLE;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemTestHelper.createInMemoryGoogleHadoopFileSystem;
import static com.google.cloud.hadoop.gcsio.testing.InMemoryGoogleCloudStorage.getInMemoryGoogleCloudStorageOptions;
import static com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.AUTHENTICATION_TYPE_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.GROUP_IMPERSONATION_SERVICE_ACCOUNT_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.IMPERSONATION_SERVICE_ACCOUNT_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.USER_IMPERSONATION_SERVICE_ACCOUNT_SUFFIX;
import static com.google.common.base.StandardSystemProperty.USER_NAME;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.stream;
import static org.junit.Assert.assertThrows;

import com.google.api.client.http.HttpResponseException;
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem.GcsFileChecksumType;
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem.GlobAlgorithm;
import com.google.cloud.hadoop.fs.gcs.auth.TestDelegationTokenBindingImpl;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemImpl;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemIntegrationTest;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.MethodOutcome;
import com.google.cloud.hadoop.gcsio.testing.InMemoryGoogleCloudStorage;
import com.google.cloud.hadoop.util.AccessTokenProvider;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.AuthenticationType;
import com.google.cloud.hadoop.util.testing.TestingAccessTokenProvider;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Ints;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.Service;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration tests for GoogleHadoopFileSystem class. */
@RunWith(JUnit4.class)
public class GoogleHadoopFileSystemIntegrationTest extends GoogleHadoopFileSystemTestBase {

  @ClassRule
  public static NotInheritableExternalResource storageResource =
      new NotInheritableExternalResource(GoogleHadoopFileSystemIntegrationTest.class) {
        @Override
        public void before() throws Throwable {
          GoogleHadoopFileSystem testInstance = new GoogleHadoopFileSystem();
          ghfs = testInstance;

          // loadConfig needs ghfsHelper, which is normally created in
          // postCreateInit. Create one here for it to use.
          ghfsHelper = new HadoopFileSystemIntegrationHelper(ghfs);

          URI initUri = new URI("gs://" + ghfsHelper.getUniqueBucketName("init"));
          ghfs.initialize(initUri, loadConfig());

          if (GoogleHadoopFileSystemConfiguration.GCS_LAZY_INITIALIZATION_ENABLE.get(
              ghfs.getConf(), ghfs.getConf()::getBoolean)) {
            testInstance.getGcsFs();
          }

          HadoopFileSystemTestBase.postCreateInit();
        }

        @Override
        public void after() {
          GoogleHadoopFileSystemTestBase.storageResource.after();
        }
      };

  @Before
  public void clearFileSystemCache() throws IOException {
    FileSystem.closeAll();
  }

  // -----------------------------------------------------------------
  // Tests that exercise behavior defined in HdfsBehavior.
  // -----------------------------------------------------------------

  /** Validates rename(). */
  @Test
  @Override
  public void testRename() throws Exception {
    renameHelper(
        new HdfsBehavior() {
          /**
           * Returns the MethodOutcome of trying to rename an existing file into the root directory.
           */
          @Override
          public MethodOutcome renameFileIntoRootOutcome() {
            return new MethodOutcome(MethodOutcome.Type.RETURNS_TRUE);
          }
        });
  }

  @Test
  public void testInitializePath_success() throws Exception {
    List<String> validPaths = Arrays.asList("gs://foo", "gs://foo/bar");
    for (String path : validPaths) {
      try (GoogleHadoopFileSystem testGhfs = createInMemoryGoogleHadoopFileSystem()) {
        testGhfs.initialize(new URI(path), new Configuration());
      }
    }
  }

  @Test
  public void testInitializePath_failure_notSupportedSchema() throws Exception {
    List<String> invalidPaths =
        Arrays.asList("http://foo", "gsg://foo", "hdfs:/", "hdfs:/foo", "hdfs://foo");
    for (String path : invalidPaths) {
      URI uri = new URI(path);
      try (GoogleHadoopFileSystem testGhfs = createInMemoryGoogleHadoopFileSystem()) {
        IllegalArgumentException e =
            assertThrows(
                "Path '" + path + "' should be invalid",
                IllegalArgumentException.class,
                () -> testGhfs.initialize(uri, new Configuration()));
        assertThat(e).hasMessageThat().startsWith("URI scheme not supported:");
      }
    }
  }

  @Test
  public void testInitializePath_failure_bucketNotSpecified() throws Exception {
    List<String> invalidPaths = Arrays.asList("gs:/", "gs:/foo", "gs:/foo/bar", "gs:///");
    for (String path : invalidPaths) {
      URI uri = new URI(path);
      try (GoogleHadoopFileSystem testGhfs = createInMemoryGoogleHadoopFileSystem()) {
        IllegalArgumentException e =
            assertThrows(
                "Path '" + path + "' should be invalid",
                IllegalArgumentException.class,
                () -> testGhfs.initialize(uri, new Configuration()));
        assertThat(e).hasMessageThat().startsWith("No bucket specified in GCS URI:");
      }
    }
  }

  @Test
  public void initialize_throwsExceptionWhenPathNull() throws Exception {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> myGhfs.initialize(null, new Configuration()));
    assertThat(exception).hasMessageThat().startsWith("path must not be null");
  }

  @Test
  public void initialize_throwsExceptionWhenConfigNull() throws Exception {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    URI correctUri = new URI("s:/foo/bar");
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> myGhfs.initialize(correctUri, null));
    assertThat(exception).hasMessageThat().startsWith("config must not be null");
  }

  @Test
  public void initialize_throwsExceptionWhenPathSchemeNull() throws Exception {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    URI incorrectUri = new URI("foo/bar");
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> myGhfs.initialize(incorrectUri, new Configuration()));
    assertThat(exception).hasMessageThat().startsWith("scheme of path must not be null");
  }

  @Test
  public void initialize_delegationTokensServiceLifecycle() throws Exception {
    Configuration config = new Configuration();

    // Token binding config
    config.set(
        DELEGATION_TOKEN_BINDING_CLASS.getKey(), TestDelegationTokenBindingImpl.class.getName());
    config.set(
        TestDelegationTokenBindingImpl.TestAccessTokenProviderImpl.TOKEN_CONFIG_PROPERTY_NAME,
        "qWDAWFA3WWFAWFAWFAW3FAWF3AWF3WFAF33GR5G5"); // Bogus auth token

    GoogleHadoopFileSystem fs = new GoogleHadoopFileSystem();
    fs.initialize(new URI("gs://test/init-uri"), config);
    assertThat(Service.STATE.STARTED).isEqualTo(fs.delegationTokens.getServiceState());

    fs.close();
    assertThat(Service.STATE.STOPPED).isEqualTo(fs.delegationTokens.getServiceState());
  }

  @Test
  public void getDefaultPort() throws IOException {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    assertThat(myGhfs.getDefaultPort()).isEqualTo(-1);
  }

  @Test
  public void getCanonicalServiceName_delegationTokensNotNull() throws Exception {
    Configuration config = new Configuration();

    // Token binding config
    config.set(
        DELEGATION_TOKEN_BINDING_CLASS.getKey(), TestDelegationTokenBindingImpl.class.getName());
    config.set(
        TestDelegationTokenBindingImpl.TestAccessTokenProviderImpl.TOKEN_CONFIG_PROPERTY_NAME,
        "qWDAWFA3WWFAWFAWFAW3FAWF3AWF3WFAF33GR5G5"); // Bogus auth token

    GoogleHadoopFileSystem fs = new GoogleHadoopFileSystem();
    fs.initialize(new URI("gs://test/init-uri"), config);

    assertThat(fs.getCanonicalServiceName()).isEqualTo(fs.delegationTokens.getService().toString());
  }

  @Test
  public void open_throwsExceptionWhenHadoopPathNull() {
    GoogleHadoopFileSystem myGhfs = new GoogleHadoopFileSystem();
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> myGhfs.open((Path) null, 1));
    assertThat(exception).hasMessageThat().startsWith("hadoopPath must not be null");
  }

  @Test
  public void create_throwsExceptionWhenHadoopPathIsNull() throws Exception {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> myGhfs.create(null, true, 1));
    assertThat(exception).hasMessageThat().startsWith("hadoopPath must not be null");
  }

  @Test
  public void create_throwsExceptionWhenReplicationIsNotPositiveInteger() throws Exception {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    URI fileUri = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path filePath = ghfsHelper.castAsHadoopPath(fileUri);
    short replicationSmallerThanZero = -1;

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> myGhfs.create(filePath, true, 1, replicationSmallerThanZero, 1));
    assertThat(exception).hasMessageThat().startsWith("replication must be a positive integer");
  }

  @Test
  public void create_throwsExceptionWhenBlockSizeIsNotPositiveInteger() throws Exception {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    URI fileUri = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path filePath = ghfsHelper.castAsHadoopPath(fileUri);
    long blockSizeSmallerThanZero = -1;
    IllegalArgumentException blockSmallerThanZeroException =
        assertThrows(
            IllegalArgumentException.class,
            () -> myGhfs.create(filePath, true, 1, (short) 1, blockSizeSmallerThanZero));
    assertThat(blockSmallerThanZeroException)
        .hasMessageThat()
        .startsWith("blockSize must be a positive integer");
  }

  @Test
  public void create_IOstatistics() throws IOException {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    assertThat(myGhfs.getIOStatistics().counters().get(INVOCATION_CREATE.getSymbol())).isEqualTo(0);
    assertThat(myGhfs.getIOStatistics().counters().get(FILES_CREATED.getSymbol())).isEqualTo(0);

    try (FSDataOutputStream fout = myGhfs.create(new Path("/file1"))) {
      fout.writeBytes("Test Content");
    }
    assertThat(myGhfs.getIOStatistics().counters().get(INVOCATION_CREATE.getSymbol())).isEqualTo(1);
    assertThat(myGhfs.getIOStatistics().counters().get(FILES_CREATED.getSymbol())).isEqualTo(1);
    assertThat(myGhfs.delete(new Path("/file1"))).isTrue();
  }

  @Test
  public void listLocatedStatus_IOStatistics() throws IOException {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();

    try (FSDataOutputStream fout = myGhfs.create(new Path("/file1"))) {
      fout.writeBytes("Test Content");
      fout.close();
      myGhfs.listLocatedStatus(new Path("/file1"));
      assertThat(
              myGhfs.getIOStatistics().counters().get(INVOCATION_LIST_LOCATED_STATUS.getSymbol()))
          .isEqualTo(1);
    }
    assertThat(myGhfs.delete(new Path("/file1"))).isTrue();
  }

  @Test
  public void createNonRecursive_throwsExceptionWhenHadoopPathNull() throws IOException {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    NullPointerException exception =
        assertThrows(
            NullPointerException.class,
            () -> myGhfs.createNonRecursive(/* f= */ null, true, 1, (short) 1, 1, () -> {}));
    assertThat(exception).hasMessageThat().startsWith("hadoopPath must not be null");
  }

  @Test
  public void createNonRecursive() throws IOException {
    GoogleHadoopFileSystem myGhfs = (GoogleHadoopFileSystem) ghfs;
    URI fileUri = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path filePath = ghfsHelper.castAsHadoopPath(fileUri);
    try (FSDataOutputStream createNonRecursiveOutputStream =
        myGhfs.createNonRecursive(filePath, true, 1, (short) 1, 1, () -> {})) {
      createNonRecursiveOutputStream.write(1);

      assertThat(createNonRecursiveOutputStream.size()).isEqualTo(1);
      assertThat(createNonRecursiveOutputStream.getPos()).isEqualTo(1);
    }
  }

  @Test
  public void createNonRecursive_throwsExceptionWhenParentFolderNoExists() {
    GoogleHadoopFileSystem myGhfs = (GoogleHadoopFileSystem) ghfs;
    Path filePath = new Path("bad/path");
    FileNotFoundException exception =
        assertThrows(
            FileNotFoundException.class,
            () -> myGhfs.createNonRecursive(filePath, true, 1, (short) 1, 1, () -> {}));
    assertThat(exception).hasMessageThat().startsWith("Can not create");
  }

  @Test
  public void delete_throwsExceptionWhenHadoopPathNull() throws IOException {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> myGhfs.delete(null, true));
    assertThat(exception).hasMessageThat().startsWith("hadoopPath must not be null");
  }

  @Test
  public void open_IOstatistics() throws IOException {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    Path testRoot = new Path("/directory1/");
    myGhfs.mkdirs(testRoot);
    FSDataOutputStream fout = myGhfs.create(new Path("/directory1/file1"));
    fout.writeBytes("data");
    fout.close();
    myGhfs.open(new Path("/directory1/file1"));
    assertThat(myGhfs.getIOStatistics().counters().get(INVOCATION_OPEN.getSymbol())).isEqualTo(1);
    assertThat(myGhfs.delete(testRoot, /* recursive= */ true)).isTrue();
  }

  @Test
  public void delete_IOstatistics() throws IOException {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    FSDataOutputStream fout = myGhfs.create(new Path("/file1"));
    fout.writeBytes("data");
    fout.close();
    myGhfs.delete(new Path("/file1"));
    assertThat(myGhfs.getIOStatistics().counters().get(INVOCATION_DELETE.getSymbol())).isEqualTo(1);
  }

  @Test
  public void listStatus_throwsExceptionWhenHadoopPathNull() throws IOException {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> myGhfs.listStatus((Path) null));
    assertThat(exception).hasMessageThat().startsWith("hadoopPath must not be null");
  }

  @Test
  public void setWorkingDirectory_throwsExceptionWhenHadoopPathNull() throws IOException {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> myGhfs.setWorkingDirectory(null));
    assertThat(exception).hasMessageThat().startsWith("hadoopPath must not be null");
  }

  @Test
  public void mkdirs_throwsExceptionWhenHadoopPathNull() throws IOException {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> myGhfs.mkdirs(null));
    assertThat(exception).hasMessageThat().startsWith("hadoopPath must not be null");
  }

  @Test
  public void mkdirs_IOstatistics() throws IOException {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    assertThat(myGhfs.getIOStatistics().counters().get(INVOCATION_MKDIRS.getSymbol())).isEqualTo(1);
  }

  @Test
  @Override
  public void testCheckPathSuccess() {
    String rootBucket = ghfs.getWorkingDirectory().toUri().getAuthority();
    List<String> validPaths =
        ImmutableList.of(
            "/",
            "/foo",
            "/foo/bar",
            "gs:/",
            "gs:/foo",
            "gs:/foo/bar",
            "gs://",
            "gs://" + rootBucket,
            "gs://" + rootBucket + "/bar");
    for (String validPath : validPaths) {
      Path path = new Path(validPath);
      ((GoogleHadoopFileSystem) ghfs).checkPath(path);
    }
  }

  /** Validates failure path in checkPath(). */
  @Test
  @Override
  public void testCheckPathFailure() {
    String rootBucket = ghfs.getWorkingDirectory().toUri().getAuthority();
    List<String> invalidSchemePaths =
        ImmutableList.of(
            "gsg:/",
            "hdfs:/",
            "gsg:/foo/bar",
            "hdfs:/foo/bar",
            "gsg://",
            "hdfs://",
            "gsg://" + rootBucket,
            "gsg://" + rootBucket + "/bar");
    GoogleHadoopFileSystem myGhfs = (GoogleHadoopFileSystem) HadoopFileSystemTestBase.ghfs;
    for (String invalidPath : invalidSchemePaths) {
      Path path = new Path(invalidPath);
      IllegalArgumentException e =
          assertThrows(
              "Path '" + path + "' should be invalid",
              IllegalArgumentException.class,
              () -> myGhfs.checkPath(path));
      assertThat(e).hasMessageThat().startsWith("Wrong scheme: ");
    }

    List<String> invalidBucketPaths = new ArrayList<>();
    String notRootBucket = "not-" + rootBucket;
    invalidBucketPaths.add("gs://" + notRootBucket);
    invalidBucketPaths.add("gs://" + notRootBucket + "/bar");
    for (String invalidPath : invalidBucketPaths) {
      Path path = new Path(invalidPath);
      IllegalArgumentException e =
          assertThrows(IllegalArgumentException.class, () -> myGhfs.checkPath(path));
      assertThat(e.getLocalizedMessage()).startsWith("Wrong bucket:");
    }
  }
  /** Verify that default constructor does not throw. */
  @Test
  public void testDefaultConstructor() {
    new GoogleHadoopFileSystem();
  }

  /** Verify that getHomeDirectory() returns expected value. */
  @Test
  public void testGetHomeDirectory() {
    URI homeDir = ghfs.getHomeDirectory().toUri();
    String scheme = homeDir.getScheme();
    String bucket = homeDir.getAuthority();
    String path = homeDir.getPath();
    assertWithMessage("Unexpected home directory scheme: " + scheme).that(scheme).isEqualTo("gs");
    assertWithMessage("Unexpected home directory bucket: " + bucket)
        .that(bucket)
        .isEqualTo(ghfs.getWorkingDirectory().toUri().getAuthority());
    assertWithMessage("Unexpected home directory path: " + path)
        .that(path.startsWith("/user/"))
        .isTrue();
  }

  /** Test getHadoopPath() invalid args. */
  @Test
  public void testGetHadoopPathInvalidArgs() throws URISyntaxException {
    IllegalArgumentException expected =
        assertThrows(
            IllegalArgumentException.class,
            () -> ((GoogleHadoopFileSystem) ghfs).getHadoopPath(new URI("gs://foobucket/bar")));
    assertThat(expected).hasMessageThat().startsWith("Authority of URI");
  }

  /** Validates that we correctly build our Options object from a Hadoop config. */
  @Test
  public void testBuildOptionsFromConfig() {
    Configuration config = loadConfig("projectId", "path/to/serviceAccountKeyFile.json");

    GoogleCloudStorageFileSystemOptions.Builder optionsBuilder =
        GoogleHadoopFileSystemConfiguration.getGcsFsOptionsBuilder(config);
    GoogleCloudStorageFileSystemOptions options = optionsBuilder.build();
    GoogleCloudStorageOptions gcsOptions = options.getCloudStorageOptions();

    assertThat(gcsOptions.isAutoRepairImplicitDirectoriesEnabled()).isTrue();

    config.setBoolean(GCS_REPAIR_IMPLICIT_DIRECTORIES_ENABLE.getKey(), false);

    optionsBuilder = GoogleHadoopFileSystemConfiguration.getGcsFsOptionsBuilder(config);
    options = optionsBuilder.build();

    gcsOptions = options.getCloudStorageOptions();
    assertThat(gcsOptions.isAutoRepairImplicitDirectoriesEnabled()).isFalse();
  }

  /** Validates success path in initialize(). */
  @Test
  @Override
  public void testInitializeSuccess() throws IOException {
    // Reuse loadConfig() to initialize auth related settings.
    Configuration config = loadConfig();

    // Set up remaining settings to known test values.
    long blockSize = 1024;
    config.setLong(GoogleHadoopFileSystemConfiguration.BLOCK_SIZE.getKey(), blockSize);
    String rootBucketName = ghfsHelper.getUniqueBucketName("initialize-root");

    URI initUri = new Path("gs://" + rootBucketName).toUri();
    GoogleHadoopFileSystem fs = new GoogleHadoopFileSystem();
    fs.initialize(initUri, config);
    GoogleCloudStorageOptions cloudStorageOptions =
        fs.getGcsFs().getOptions().getCloudStorageOptions();

    // Verify that config settings were set correctly.
    assertThat(fs.getDefaultBlockSize()).isEqualTo(blockSize);
    assertThat(fs.initUri).isEqualTo(initUri);
    assertThat(fs.getWorkingDirectory().toUri().getAuthority()).isEqualTo(rootBucketName);
  }

  @Test
  public void testInitializeSucceedsWhenNoProjectIdConfigured()
      throws URISyntaxException, IOException {
    Configuration config = loadConfig();
    // Unset Project ID
    config.unset(GoogleHadoopFileSystemConfiguration.GCS_PROJECT_ID.getKey());

    URI gsUri = new Path("gs://foo").toUri();
    new GoogleHadoopFileSystem().initialize(gsUri, config);
  }

  @Test
  public void testInitializeThrowsWhenWrongSchemeConfigured()
      throws URISyntaxException, IOException {
    // Verify that we cannot initialize using URI with a wrong scheme.
    URI wrongScheme = new URI("http://foo/bar");

    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> new GoogleHadoopFileSystem().initialize(wrongScheme, new Configuration()));
    assertThat(thrown).hasMessageThat().contains("URI scheme not supported");
  }

  @Test
  public void testInitializeThrowsWhenCredentialsNotFound() throws URISyntaxException, IOException {
    URI gsUri = new URI("gs://foobar/");
    String fakeProjectId = "123456";
    Configuration config = new Configuration();
    config.set(GCS_CONFIG_PREFIX + AUTHENTICATION_TYPE_SUFFIX.getKey(), "INVALID_AUTH_TYPE");
    // Set project ID.
    config.set(GoogleHadoopFileSystemConfiguration.GCS_PROJECT_ID.getKey(), fakeProjectId);

    GoogleHadoopFileSystem ghfs = new GoogleHadoopFileSystem();

    IllegalArgumentException thrown;
    if (GoogleHadoopFileSystemConfiguration.GCS_LAZY_INITIALIZATION_ENABLE.get(
        config, config::getBoolean)) {
      ghfs.initialize(gsUri, config);
      thrown = assertThrows(IllegalArgumentException.class, ghfs::getGcsFs);
    } else {
      thrown = assertThrows(IllegalArgumentException.class, () -> ghfs.initialize(gsUri, config));
    }

    assertThat(thrown).hasMessageThat().startsWith("No enum constant ");
    assertThat(thrown).hasMessageThat().contains("AuthenticationType.INVALID_AUTH_TYPE");
  }

  /** Validates initialize() with configuration key fs.gs.working.dir set. */
  @Override
  @Test
  public void testInitializeWithWorkingDirectory() throws Exception {
    // We can just test by calling initialize multiple times (for each test condition) because
    // there is nothing in initialize() which must be run only once. If this changes, this test
    // method will need to resort to using a new GoogleHadoopFileSystem() for each item
    // in the for-loop.
    GoogleHadoopFileSystem myGhfs = (GoogleHadoopFileSystem) ghfs;

    Configuration config = loadConfig();
    ghfs.initialize(myGhfs.initUri, config);

    // setUpWorkingDirectoryTest() depends on getFileSystemRoot(), which in turn depends on
    // having initialized with the desired systemBucket. If we tried to call this before
    // ghfs.initialize on the preceding line, the test may or may not succeed depending on
    // whether the last test case happened to set systemBucket to bucketName already.
    List<WorkingDirData> wddList = setUpWorkingDirectoryTest();
    String rootBucketName = ghfs.getWorkingDirectory().toUri().getAuthority();
    for (WorkingDirData wdd : wddList) {
      Path path = wdd.path;
      Path expectedWorkingDir = wdd.expectedPath;
      Path currentWorkingDir = ghfs.getWorkingDirectory();
      config.set(
          GoogleHadoopFileSystemConfiguration.GCS_WORKING_DIRECTORY.getKey(), path.toString());
      ghfs.initialize(myGhfs.initUri, config);
      Path newWorkingDir = ghfs.getWorkingDirectory();
      if (expectedWorkingDir != null) {
        assertThat(newWorkingDir).isEqualTo(expectedWorkingDir);
      } else {
        assertThat(newWorkingDir).isEqualTo(currentWorkingDir);
      }
    }
    assertThat(ghfs.getHomeDirectory().toString()).startsWith("gs://" + rootBucketName);
  }

  /** Validates success path in configureBuckets(). */
  @Test
  @Override
  public void testConfigureBucketsSuccess() throws IOException {
    String rootBucketName = "gs://" + ghfsHelper.getUniqueBucketName("configure-root");

    URI initUri = new Path(rootBucketName).toUri();

    // To test configureBuckets which occurs after GCSFS initialization in configure(), while
    // still being reusable by derived unittests (we can't call loadConfig in a test case which
    // is inherited by a derived test), we will use the constructor which already provides a (fake)
    // GCSFS and skip the portions of the config specific to GCSFS.

    GoogleCloudStorageFileSystem fakeGcsFs =
        new GoogleCloudStorageFileSystemImpl(
            InMemoryGoogleCloudStorage::new,
            GoogleCloudStorageFileSystemOptions.builder()
                .setCloudStorageOptions(getInMemoryGoogleCloudStorageOptions())
                .build());

    Configuration config = new Configuration();
    GoogleHadoopFileSystem fs = new GoogleHadoopFileSystem(fakeGcsFs);
    fs.initialize(initUri, config);

    // Verify that config settings were set correctly.
    assertThat(fs.initUri).isEqualTo(initUri);

    initUri = new Path("gs://" + ghfsHelper.sharedBucketName1 + "/foo").toUri();
    fs = new GoogleHadoopFileSystem(fakeGcsFs);
    fs.initialize(initUri, config);

    // Verify that config settings were set correctly.
    assertThat(fs.initUri).isEqualTo(initUri);

    assertThat(fs.getWorkingDirectory().toUri().getAuthority()).isEqualTo(initUri.getAuthority());
  }

  /** Validates success path when there is a root bucket but no system bucket is specified. */
  @Test
  @Override
  public void testConfigureBucketsWithRootBucketButNoSystemBucket() throws IOException {
    String rootBucketName = ghfsHelper.getUniqueBucketName("configure-root");
    URI initUri = new Path("gs://" + rootBucketName).toUri();
    GoogleCloudStorageFileSystem fakeGcsFs =
        new GoogleCloudStorageFileSystemImpl(
            InMemoryGoogleCloudStorage::new,
            GoogleCloudStorageFileSystemOptions.builder()
                .setCloudStorageOptions(getInMemoryGoogleCloudStorageOptions())
                .build());
    GoogleHadoopFileSystem fs = new GoogleHadoopFileSystem(fakeGcsFs);
    fs.initialize(initUri, new Configuration());

    // Verify that config settings were set correctly.
    assertThat(fs.initUri).isEqualTo(initUri);
  }

  @Test
  public void create_storage_statistics() throws IOException {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    GhfsStorageStatistics StorageStats = new GhfsStorageStatistics(myGhfs.getIOStatistics());
    Path testRoot = new Path("/directory1/");
    myGhfs.mkdirs(testRoot);
    FSDataOutputStream fout = myGhfs.create(new Path("/directory1/file1"));
    fout.writeBytes("Test Content");
    fout.close();
    assertThat(StorageStats.isTracked("op_create")).isTrue();
    assertThat(StorageStats.getLong("op_create")).isEqualTo(1);
    assertThat(myGhfs.delete(testRoot, /* recursive= */ true)).isTrue();
  }

  @Test
  public void listLocatedStatus_storage_statistics() throws IOException {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    GhfsStorageStatistics StorageStats = new GhfsStorageStatistics(myGhfs.getIOStatistics());
    Path testRoot = new Path("/directory1/");
    myGhfs.mkdirs(testRoot);
    FSDataOutputStream fout = myGhfs.create(new Path("/directory1/file1"));
    fout.writeBytes("Test Content");
    fout.close();
    myGhfs.listLocatedStatus(testRoot);
    assertThat(StorageStats.isTracked("op_list_located_status")).isTrue();
    assertThat(StorageStats.getLong("op_list_located_status")).isEqualTo(1);
    assertThat(myGhfs.delete(testRoot, /* recursive= */ true)).isTrue();
  }

  @Test
  public void open_storage_statistics() throws IOException {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    GhfsStorageStatistics StorageStats = new GhfsStorageStatistics(myGhfs.getIOStatistics());
    Path testRoot = new Path("/directory1/");
    myGhfs.mkdirs(testRoot);
    FSDataOutputStream fout = myGhfs.create(new Path("/directory1/file1"));
    fout.writeBytes("data");
    fout.close();
    myGhfs.open(new Path("/directory1/file1"));
    assertThat(StorageStats.isTracked("op_open")).isTrue();
    assertThat(StorageStats.getLong("op_open")).isEqualTo(1);
    assertThat(myGhfs.delete(testRoot, /* recursive= */ true)).isTrue();
  }

  @Test
  public void copy_from_local_file_storage_statistics() throws IOException {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    GhfsStorageStatistics StorageStats = new GhfsStorageStatistics(myGhfs.getIOStatistics());
    Path testRoot = new Path("/directory1/");
    myGhfs.mkdirs(testRoot);
    FSDataOutputStream fout = myGhfs.create(new Path("/directory1/file1"));
    fout.writeBytes("data");
    fout.close();
    // Temporary file in local FS.
    File localTempFile = File.createTempFile("ghfs-test-", null);
    Path localTempFilePath = new Path(localTempFile.getPath());

    myGhfs.copyFromLocalFile(false, true, localTempFilePath, testRoot);

    assertThat((StorageStats.isTracked("op_copy_from_local_file"))).isTrue();
    assertThat((StorageStats.getLong("op_copy_from_local_file"))).isEqualTo(1);
    assertThat(myGhfs.delete(testRoot, /* recursive= */ true)).isTrue();
  }

  @Test
  public void delete_storage_statistics() throws IOException {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    GhfsStorageStatistics StorageStats = new GhfsStorageStatistics(myGhfs.getIOStatistics());
    Path filePath = new Path("/file1");
    FSDataOutputStream fout = myGhfs.create(filePath);
    fout.writeBytes("Test Content");
    fout.close();
    assertThat(myGhfs.delete(filePath)).isTrue();
    assertThat(StorageStats.isTracked("op_delete")).isTrue();
    assertThat(StorageStats.getLong("op_delete")).isEqualTo(1);
  }

  @Test
  public void mkdirs_storage_statistics() throws IOException {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    GhfsStorageStatistics StorageStats = new GhfsStorageStatistics(myGhfs.getIOStatistics());
    assertThat(StorageStats.isTracked("op_mkdirs")).isTrue();
    assertThat(StorageStats.getLong("op_mkdirs")).isEqualTo(1);
  }

  @Test
  public void GlobStatus_storage_statistics() throws IOException {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    GhfsStorageStatistics StorageStats = new GhfsStorageStatistics(myGhfs.getIOStatistics());
    Path testRoot = new Path("/directory1/");
    myGhfs.mkdirs(testRoot);
    myGhfs.mkdirs(new Path("/directory1/subdirectory1"));
    myGhfs.create(new Path("/directory1/subdirectory1/file1")).writeBytes("data");
    myGhfs.globStatus(new Path("/d*"));
    assertThat(StorageStats.isTracked("op_glob_status")).isTrue();
    assertThat(StorageStats.getLong("op_glob_status")).isEqualTo(1);
    assertThat(myGhfs.delete(testRoot, /* recursive= */ true)).isTrue();
  }

  @Test
  public void getFileStatus_storage_statistics() throws IOException {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    GhfsStorageStatistics StorageStats = new GhfsStorageStatistics(myGhfs.getIOStatistics());
    Path testRoot = new Path("/directory1/");
    myGhfs.mkdirs(testRoot);
    FSDataOutputStream fout = myGhfs.create(new Path("/directory1/file1"));
    fout.writeBytes("data");
    fout.close();
    myGhfs.getFileStatus(new Path("/directory1/file1"));
    assertThat(StorageStats.isTracked("op_get_file_status")).isTrue();
    assertThat(StorageStats.getLong("op_get_file_status")).isEqualTo(1);
    assertThat(myGhfs.delete(testRoot, /* recursive= */ true)).isTrue();
  }

  @Test
  public void createNonRecursive_storage_statistics() throws IOException {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    GhfsStorageStatistics StorageStats = new GhfsStorageStatistics(myGhfs.getIOStatistics());
    Path testRoot = new Path("/directory1/");
    myGhfs.mkdirs(testRoot);
    Path filePath = new Path("/directory1/file1");
    try (FSDataOutputStream createNonRecursiveOutputStream =
        myGhfs.createNonRecursive(filePath, true, 1, (short) 1, 1, () -> {})) {
      createNonRecursiveOutputStream.write(1);
    }
    assertThat(StorageStats.isTracked("op_create_non_recursive")).isTrue();
    assertThat(StorageStats.getLong("op_create_non_recursive")).isEqualTo(1);
    assertThat(myGhfs.delete(testRoot, /* recursive= */ true)).isTrue();
  }

  @Test
  public void getFileChecksum_storage_statistics() throws IOException {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    GhfsStorageStatistics StorageStats = new GhfsStorageStatistics(myGhfs.getIOStatistics());
    Path testRoot = new Path("/directory1/");
    myGhfs.mkdirs(testRoot);
    Path filePath = new Path("/directory1/file1");
    FSDataOutputStream fout = myGhfs.create(new Path("/directory1/file1"));
    fout.writeBytes("data");
    fout.close();
    myGhfs.getFileChecksum(filePath);
    assertThat(StorageStats.isTracked("op_get_file_checksum")).isTrue();
    assertThat(StorageStats.getLong("op_get_file_checksum")).isEqualTo(1);
    assertThat(myGhfs.delete(testRoot, /* recursive= */ true)).isTrue();
  }

  @Test
  public void rename_storage_statistics() throws IOException {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    GhfsStorageStatistics StorageStats = new GhfsStorageStatistics(myGhfs.getIOStatistics());
    Path testRoot = new Path("/directory1/");
    myGhfs.mkdirs(testRoot);
    Path source = new Path("/directory1/file1");
    myGhfs.create(source).writeBytes("data");
    Path dest = new Path("/directory1/file2");
    myGhfs.rename(source, dest);
    assertThat(StorageStats.isTracked("op_rename")).isTrue();
    assertThat(StorageStats.getLong("op_rename")).isEqualTo(1);
    assertThat(myGhfs.delete(testRoot, /* recursive= */ true)).isTrue();
  }

  @Test
  public void exists_storage_statistics() throws IOException {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    GhfsStorageStatistics StorageStats = new GhfsStorageStatistics(myGhfs.getIOStatistics());
    Path testRoot = new Path("/directory1/");
    myGhfs.mkdirs(testRoot);
    Path filePath = new Path("/directory1/file1");
    myGhfs.create(filePath).writeBytes("data");
    myGhfs.exists(filePath);
    assertThat(StorageStats.isTracked("op_exists")).isTrue();
    assertThat(StorageStats.getLong("op_exists")).isEqualTo(1);
    assertThat(myGhfs.delete(testRoot, /* recursive= */ true)).isTrue();
  }

  @Test
  public void xattr_storage_statistics() throws IOException {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    Path testRoot = new Path("/directory1/");
    myGhfs.mkdirs(testRoot);
    Path filePath = new Path("/directory1/file1");
    GhfsStorageStatistics StorageStats = new GhfsStorageStatistics(myGhfs.getIOStatistics());
    myGhfs.create(filePath).writeBytes("data");

    myGhfs.getXAttrs(filePath);
    assertThat(StorageStats.isTracked("op_xattr_get_map")).isTrue();
    assertThat(StorageStats.getLong("op_xattr_get_map")).isEqualTo(1);

    myGhfs.getXAttr(filePath, "test-xattr_statistics");
    assertThat(StorageStats.isTracked("op_xattr_get_named")).isTrue();
    assertThat(StorageStats.getLong("op_xattr_get_named")).isEqualTo(1);

    myGhfs.getXAttrs(
        filePath,
        ImmutableList.of("test-xattr-statistics", "test-xattr-statistics1", "test-xattr"));

    assertThat(StorageStats.isTracked("op_xattr_get_named_map")).isTrue();
    assertThat(StorageStats.getLong("op_xattr_get_named_map")).isEqualTo(1);

    myGhfs.listXAttrs(filePath);
    assertThat(StorageStats.isTracked("op_xattr_list")).isTrue();
    assertThat(StorageStats.getLong("op_xattr_list")).isEqualTo(1);

    assertThat(myGhfs.delete(testRoot, true)).isTrue();
  }

  /** Validates that exception thrown if no root bucket is specified. */
  @Test
  @Override
  public void testConfigureBucketsWithNeitherRootBucketNorSystemBucket() throws IOException {
    URI initUri = new Path("gs://").toUri();
    GoogleCloudStorageFileSystem fakeGcsFs =
        new GoogleCloudStorageFileSystemImpl(
            InMemoryGoogleCloudStorage::new,
            GoogleCloudStorageFileSystemOptions.builder()
                .setCloudStorageOptions(getInMemoryGoogleCloudStorageOptions())
                .build());
    GoogleHadoopFileSystem fs = new GoogleHadoopFileSystem(fakeGcsFs);
    Configuration config = new Configuration();

    IllegalArgumentException thrown =
        assertThrows(IllegalArgumentException.class, () -> fs.initialize(initUri, config));

    assertThat(thrown).hasMessageThat().isEqualTo("No bucket specified in GCS URI: gs:/");
  }

  private static Configuration getConfigurationWithImplementation() {
    Configuration conf = loadConfig();
    conf.set("fs.gs.impl", GoogleHadoopFileSystem.class.getCanonicalName());
    return conf;
  }

  @Test
  public void testFileSystemIsRemovedFromCacheOnClose() throws IOException, URISyntaxException {
    Configuration conf = getConfigurationWithImplementation();

    URI fsUri = new URI(String.format("gs://%s/", sharedBucketName1));

    FileSystem fs1 = FileSystem.get(fsUri, conf);
    FileSystem fs2 = FileSystem.get(fsUri, conf);

    assertThat(fs2).isSameInstanceAs(fs1);

    fs1.close();

    FileSystem fs3 = FileSystem.get(fsUri, conf);
    assertThat(fs3).isNotSameInstanceAs(fs1);

    fs3.close();
  }

  @Test
  public void testIOExceptionIsThrowAfterClose() throws IOException, URISyntaxException {
    Configuration conf = getConfigurationWithImplementation();

    URI fsUri = new URI(String.format("gs://%s/", sharedBucketName1));

    FileSystem fs1 = FileSystem.get(fsUri, conf);
    FileSystem fs2 = FileSystem.get(fsUri, conf);

    assertThat(fs2).isSameInstanceAs(fs1);

    fs1.close();

    assertThrows(IOException.class, () -> fs2.exists(new Path("/SomePath/That/Doesnt/Matter")));
  }

  public void createFile(Path filePath, byte[] data) throws IOException {
    try (FSDataOutputStream output = ghfs.create(filePath)) {
      output.write(data);
    }
  }

  @Test
  public void testGlobStatusPathExpansionAndFilter() throws IOException {
    Path testRoot = new Path(ghfs.getWorkingDirectory(), "testGlobStatusPathExpansionAndFilter");

    byte[] data = "testGlobStatusPathExpansionAndFilter_data".getBytes(UTF_8);

    createFile(testRoot.suffix("/date/2020/07/17/0/file1.xml"), data);
    createFile(testRoot.suffix("/date/2020/07/17/0/file1.json"), data);
    createFile(testRoot.suffix("/date/2020/07/18/0/file2.xml"), data);
    createFile(testRoot.suffix("/date/2020/07/18/0/file2.json"), data);
    createFile(testRoot.suffix("/date/2020/07/19/0/file3.xml"), data);
    createFile(testRoot.suffix("/date/2020/07/19/0/file3.json"), data);
    createFile(testRoot.suffix("/date/2020/07/20/0/file4.xml"), data);
    createFile(testRoot.suffix("/date/2020/07/20/0/file4.json"), data);

    FileStatus[] files =
        ghfs.globStatus(
            testRoot.suffix("/*/{2020/07/17,2020/07/18,2020/07/19}/*/*"),
            path -> path.getName().endsWith(".json"));

    Path workingDirRoot = new Path(ghfs.getWorkingDirectory(), testRoot);

    assertThat(stream(files).map(FileStatus::getPath).collect(toImmutableList()))
        .containsExactly(
            workingDirRoot.suffix("/date/2020/07/17/0/file1.json"),
            workingDirRoot.suffix("/date/2020/07/18/0/file2.json"),
            workingDirRoot.suffix("/date/2020/07/19/0/file3.json"));

    assertThat(ghfs.delete(testRoot, /* recursive= */ true)).isTrue();
  }

  @Test
  public void GlobStatus_statistics() throws IOException {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    Path testRoot = new Path("/directory1/");
    myGhfs.mkdirs(testRoot);
    myGhfs.mkdirs(new Path("/directory1/subdirectory1"));
    myGhfs.create(new Path("/directory1/subdirectory1/file1")).writeBytes("data");
    myGhfs.globStatus(new Path("/d*"));
    assertThat(myGhfs.getIOStatistics().counters().get(INVOCATION_GLOB_STATUS.getSymbol()))
        .isEqualTo(1);
    assertThat(myGhfs.delete(testRoot, /* recursive= */ true)).isTrue();
  }

  @Test
  public void testGlobStatus() throws IOException {
    Path testRoot = new Path("/directory1/");
    ghfs.mkdirs(testRoot);
    ghfs.mkdirs(new Path("/directory1/subdirectory1"));
    ghfs.mkdirs(new Path("/directory1/subdirectory2"));

    byte[] data = "data".getBytes(UTF_8);

    createFile(new Path("/directory1/subdirectory1/file1"), data);
    createFile(new Path("/directory1/subdirectory1/file2"), data);
    createFile(new Path("/directory1/subdirectory2/file1"), data);
    createFile(new Path("/directory1/subdirectory2/file2"), data);

    FileStatus[] rootDirectories = ghfs.globStatus(new Path("/d*"));
    assertThat(stream(rootDirectories).map(d -> d.getPath().getName()).collect(toImmutableList()))
        .containsExactly("directory1");

    FileStatus[] subDirectories = ghfs.globStatus(new Path("/directory1/s*"));
    assertThat(subDirectories).hasLength(2);

    FileStatus[] subDirectory1Files = ghfs.globStatus(new Path("/directory1/subdirectory1/*"));
    assertThat(subDirectory1Files).hasLength(2);
    assertThat(subDirectory1Files[0].getPath().getName()).isEqualTo("file1");
    assertThat(subDirectory1Files[1].getPath().getName()).isEqualTo("file2");

    FileStatus[] subDirectory2Files = ghfs.globStatus(new Path("/directory1/subdirectory2/f*"));
    assertThat(subDirectory2Files).hasLength(2);
    assertThat(subDirectory2Files[0].getPath().getName()).isEqualTo("file1");
    assertThat(subDirectory2Files[1].getPath().getName()).isEqualTo("file2");

    FileStatus[] subDirectory2Files2 = ghfs.globStatus(new Path("/directory1/subdirectory2/file?"));
    assertThat(subDirectory2Files2).hasLength(2);
    assertThat(subDirectory2Files2[0].getPath().getName()).isEqualTo("file1");
    assertThat(subDirectory2Files2[1].getPath().getName()).isEqualTo("file2");

    FileStatus[] subDirectory2Files3 =
        ghfs.globStatus(new Path("/directory1/subdirectory2/file[0-9]"));
    assertThat(subDirectory2Files3).hasLength(2);
    assertThat(subDirectory2Files3[0].getPath().getName()).isEqualTo("file1");
    assertThat(subDirectory2Files3[1].getPath().getName()).isEqualTo("file2");

    FileStatus[] subDirectory2Files4 =
        ghfs.globStatus(new Path("/directory1/subdirectory2/file[^1]"));
    assertThat(subDirectory2Files4).hasLength(1);
    assertThat(subDirectory2Files4[0].getPath().getName()).isEqualTo("file2");

    FileStatus[] subDirectory2Files5 =
        ghfs.globStatus(new Path("/directory1/subdirectory2/file{1,2}"));
    assertThat(subDirectory2Files5).hasLength(2);
    assertThat(subDirectory2Files5[0].getPath().getName()).isEqualTo("file1");
    assertThat(subDirectory2Files5[1].getPath().getName()).isEqualTo("file2");

    assertThat(ghfs.delete(testRoot, /* recursive= */ true)).isTrue();
  }

  @Test
  public void getFileStatus_statistics() throws IOException {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    Path testRoot = new Path("/directory1/");
    myGhfs.mkdirs(testRoot);
    FSDataOutputStream fout = myGhfs.create(new Path("/directory1/file1"));
    fout.writeBytes("data");
    fout.close();
    myGhfs.getFileStatus(new Path("/directory1/file1"));
    assertThat(myGhfs.getIOStatistics().counters().get(INVOCATION_GET_FILE_STATUS.getSymbol()))
        .isEqualTo(1);
    assertThat(myGhfs.delete(testRoot, /* recursive= */ true)).isTrue();
  }

  @Test
  public void createNonRecursive_statistics() throws IOException {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    Path testRoot = new Path("/directory1/");
    myGhfs.mkdirs(testRoot);
    Path filePath = new Path("/directory1/file1");
    try (FSDataOutputStream createNonRecursiveOutputStream =
        myGhfs.createNonRecursive(filePath, true, 1, (short) 1, 1, () -> {})) {
      createNonRecursiveOutputStream.write(1);
    }
    assertThat(myGhfs.getIOStatistics().counters().get(INVOCATION_CREATE_NON_RECURSIVE.getSymbol()))
        .isEqualTo(1);
    assertThat(myGhfs.delete(testRoot, /* recursive= */ true)).isTrue();
  }

  @Test
  public void getFileChecksum_statistics() throws IOException {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    Path testRoot = new Path("/directory1/");
    myGhfs.mkdirs(testRoot);
    Path filePath = new Path("/directory1/file1");
    FSDataOutputStream fout = myGhfs.create(new Path("/directory1/file1"));
    fout.writeBytes("data");
    fout.close();
    myGhfs.getFileChecksum(filePath);
    assertThat(myGhfs.getIOStatistics().counters().get(INVOCATION_GET_FILE_CHECKSUM.getSymbol()))
        .isEqualTo(1);
    assertThat(myGhfs.delete(testRoot, /* recursive= */ true)).isTrue();
  }

  @Test
  public void getFileStatus_throwsExceptionWhenHadoopPathNull() {
    GoogleHadoopFileSystem myGhfs = new GoogleHadoopFileSystem();
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> myGhfs.getFileStatus(null));
    assertThat(exception).hasMessageThat().contains("hadoopPath must not be null");
  }

  @Test
  public void getFileStatus_throwsExceptionWhenFileInfoDontExists() throws IOException {
    URI fileUri = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path filePath = ghfsHelper.castAsHadoopPath(fileUri);
    FileNotFoundException e =
        assertThrows(FileNotFoundException.class, () -> ghfs.getFileStatus(filePath));
    assertThat(e).hasMessageThat().startsWith("File not found");
  }

  /** Tests getFileStatus() with non-default permissions. */
  @Test
  public void testConfigurablePermissions() throws IOException {
    String testPermissions = "777";
    Configuration conf = getConfigurationWithImplementation();
    conf.set(GoogleHadoopFileSystemConfiguration.PERMISSIONS_TO_REPORT.getKey(), testPermissions);
    GoogleHadoopFileSystem myGhfs = new GoogleHadoopFileSystem();
    myGhfs.initialize(ghfs.getUri(), conf);
    URI fileUri = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path filePath = ghfsHelper.castAsHadoopPath(fileUri);
    ghfsHelper.writeFile(filePath, "foo", 1, /* overwrite= */ true);

    FileStatus status = myGhfs.getFileStatus(filePath);
    assertThat(status.getPermission()).isEqualTo(new FsPermission(testPermissions));

    // Cleanup.
    assertThat(ghfs.delete(filePath, /* recursive= */ true)).isTrue();
  }

  /** Test getFileStatus() uses the user reported by UGI */
  @Test
  public void testFileStatusUser() throws Exception {
    String ugiUser = UUID.randomUUID().toString();
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(ugiUser);
    Configuration conf = getConfigurationWithImplementation();
    GoogleHadoopFileSystem myGhfs = new GoogleHadoopFileSystem();
    myGhfs.initialize(ghfs.getUri(), conf);
    URI fileUri = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path filePath = ghfsHelper.castAsHadoopPath(fileUri);
    ghfsHelper.writeFile(filePath, "foo", 1, /* overwrite= */ true);

    FileStatus status =
        ugi.doAs((PrivilegedExceptionAction<FileStatus>) () -> myGhfs.getFileStatus(filePath));

    assertThat(status.getOwner()).isEqualTo(ugiUser);
    assertThat(status.getGroup()).isEqualTo(ugiUser);

    // Cleanup.
    assertThat(ghfs.delete(filePath, true)).isTrue();
  }

  @Test
  public void append_throwsExceptionWhenHadooptPathNull() {
    GoogleHadoopFileSystem myGhfs = new GoogleHadoopFileSystem();
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                myGhfs.append(
                    /* hadoopPath= */ null,
                    GoogleHadoopFileSystemConfiguration.GCS_OUTPUT_STREAM_BUFFER_SIZE.getDefault(),
                    () -> {}));
    assertThat(exception).hasMessageThat().contains("hadoopPath must not be null");
  }

  @Test
  public void testConcat() throws IOException {
    Path directory = new Path(ghfs.getWorkingDirectory(), "testConcat");

    long expectedLength = 0;
    List<Path> files = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      Path file = new Path(directory, String.format("file-%s", UUID.randomUUID()));
      ghfsHelper.writeFile(file, "data_" + file, 1, /* overwrite= */ false);
      files.add(file);
      expectedLength += ghfs.getFileStatus(file).getLen();
    }

    // Create target file
    Path target = new Path(directory, "target");
    ghfsHelper.writeFile(target, new byte[0], /* numWrites= */ 1, /* overwrite= */ false);

    ghfs.concat(target, files.toArray(new Path[0]));

    assertThat(ghfs.getFileStatus(target).getLen()).isEqualTo(expectedLength);

    // cleanup
    assertThat(ghfs.delete(directory, true)).isTrue();
  }

  @Test
  public void concat_throwsExceptionWhenSourceAreEmpty() {
    String bucket = ghfs.getWorkingDirectory().toUri().getAuthority();
    Path target = new Path(String.format("gs://%s/testConcat_exception/target", bucket));
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> ghfs.concat(target, new Path[0]));
    assertThat(exception).hasMessageThat().contains("srcs must have at least one source");
  }

  @Test
  public void concat_throwsExceptionWhenTargetDirectoryInSources() throws IOException {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    Path directory =
        new Path(
            String.format(
                "gs://%s/testConcat_exception/",
                myGhfs.getWorkingDirectory().toUri().getAuthority()));
    Path target = new Path(directory, "target");
    Path[] srcsWithTarget = {target};
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> myGhfs.concat(target, srcsWithTarget));
    assertThat(exception).hasMessageThat().contains("target must not be contained in sources");
  }

  @Test
  public void rename_throwExceptionWhenDstNul() {
    String bucket = ghfs.getWorkingDirectory().toUri().getAuthority();
    Path src = new Path(String.format("gs://%s/testRename/", bucket));
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> ghfs.rename(src, /* dst= */ null));
    assertThat(exception).hasMessageThat().contains("dst must not be null");
  }

  @Test
  public void rename_throwExceptionWhenSrcNull() throws IOException {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    Path directory =
        new Path(
            String.format(
                "gs://%s/testRename/", myGhfs.getWorkingDirectory().toUri().getAuthority()));
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> myGhfs.rename(null, directory));
    assertThat(exception).hasMessageThat().contains("src must not be null");
  }

  @Test
  public void rename_statistics() throws IOException {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    Path testRoot = new Path("/directory1/");
    myGhfs.mkdirs(testRoot);
    Path source = new Path("/directory1/file1");
    myGhfs.create(source).writeBytes("data");
    Path dest = new Path("/directory1/file2");
    myGhfs.rename(source, dest);
    assertThat(myGhfs.getIOStatistics().counters().get(INVOCATION_RENAME.getSymbol())).isEqualTo(1);
    assertThat(myGhfs.delete(testRoot, /* recursive= */ true)).isTrue();
  }

  @Test
  public void exists_statistics() throws IOException {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    Path testRoot = new Path("/directory1/");
    myGhfs.mkdirs(testRoot);
    Path filePath = new Path("/directory1/file1");
    myGhfs.create(filePath).writeBytes("data");
    myGhfs.exists(filePath);
    assertThat(myGhfs.getIOStatistics().counters().get(INVOCATION_EXISTS.getSymbol())).isEqualTo(1);
    assertThat(myGhfs.delete(testRoot, /* recursive= */ true)).isTrue();
  }

  @Test
  public void xattr_statistics() throws IOException {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    Path testRoot = new Path("/directory1/");
    myGhfs.mkdirs(testRoot);
    Path filePath = new Path("/directory1/file1");
    myGhfs.create(filePath).writeBytes("data");

    myGhfs.getXAttrs(filePath);
    assertThat(myGhfs.getIOStatistics().counters().get(INVOCATION_XATTR_GET_MAP.getSymbol()))
        .isEqualTo(1);

    myGhfs.getXAttr(filePath, "test-xattr_statistics");
    assertThat(myGhfs.getIOStatistics().counters().get(INVOCATION_XATTR_GET_NAMED.getSymbol()))
        .isEqualTo(1);

    myGhfs.getXAttrs(
        filePath,
        ImmutableList.of("test-xattr-statistics", "test-xattr-statistics1", "test-xattr"));
    assertThat(myGhfs.getIOStatistics().counters().get(INVOCATION_XATTR_GET_NAMED_MAP.getSymbol()))
        .isEqualTo(1);

    myGhfs.listXAttrs(filePath);
    assertThat(myGhfs.getIOStatistics().counters().get(INVOCATION_OP_XATTR_LIST.getSymbol()))
        .isEqualTo(1);

    assertThat(myGhfs.delete(testRoot, true)).isTrue();
  }

  @Test
  @Ignore("Test is failing")
  public void http_IOStatistics() throws IOException {
    FSDataOutputStream fout = ghfs.create(new Path("/file1"));
    fout.writeBytes("Test Content");
    fout.close();
    // evaluating the iostatistics by extracting the values set for the iostatistics key after each
    // file operation
    assertThat(
            ((GoogleHadoopFileSystem) ghfs)
                .getIOStatistics()
                .counters()
                .get(INVOCATION_CREATE.getSymbol()))
        .isEqualTo(1);
    // The create and write methods are expected to trigger requests of types GET, PUT and PATCH
    assertThat(
            ((GoogleHadoopFileSystem) ghfs)
                .getIOStatistics()
                .counters()
                .get(GhfsStatistic.ACTION_HTTP_GET_REQUEST.getSymbol()))
        .isEqualTo(2);
    assertThat(
            ((GoogleHadoopFileSystem) ghfs)
                .getIOStatistics()
                .counters()
                .get(GhfsStatistic.ACTION_HTTP_PUT_REQUEST.getSymbol()))
        .isEqualTo(1);
    assertThat(
            ((GoogleHadoopFileSystem) ghfs)
                .getIOStatistics()
                .counters()
                .get(GhfsStatistic.ACTION_HTTP_PATCH_REQUEST.getSymbol()))
        .isEqualTo(1);
    assertThat(ghfs.delete(new Path("/file1"))).isTrue();
    // Delete operation triggers the DELETE type request
    assertThat(
            ((GoogleHadoopFileSystem) ghfs)
                .getIOStatistics()
                .counters()
                .get(GhfsStatistic.ACTION_HTTP_DELETE_REQUEST.getSymbol()))
        .isEqualTo(1);
  }

  @Test
  public void fileChecksum_throwsExceptionWHenHadoopPathAsNull() {
    GoogleHadoopFileSystem myGhfs = new GoogleHadoopFileSystem();
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> myGhfs.getFileChecksum(null));
    assertThat(exception).hasMessageThat().contains("hadoopPath must not be null");
  }

  @Test
  public void fileChecksum_throwsExceptionWhenFileNotFound() throws Exception {
    URI fileUri = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path filePath = ghfsHelper.castAsHadoopPath(fileUri);
    FileNotFoundException e =
        assertThrows(FileNotFoundException.class, () -> ghfs.getFileChecksum(filePath));
    assertThat(e).hasMessageThat().startsWith("File not found");
  }

  @Test
  public void testCrc32cFileChecksum() throws Exception {
    testFileChecksum(
        GcsFileChecksumType.CRC32C,
        content -> Ints.toByteArray(Hashing.crc32c().hashString(content, UTF_8).asInt()));
  }

  @Test
  public void testMd5FileChecksum() throws Exception {
    testFileChecksum(
        GcsFileChecksumType.MD5, content -> Hashing.md5().hashString(content, UTF_8).asBytes());
  }

  private static void testFileChecksum(
      GcsFileChecksumType checksumType, Function<String, byte[]> checksumFn) throws Exception {
    Configuration config = getConfigurationWithImplementation();
    config.set("fs.gs.checksum.type", checksumType.name());

    GoogleHadoopFileSystem myGhfs = new GoogleHadoopFileSystem();
    myGhfs.initialize(ghfs.getUri(), config);

    URI fileUri = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path filePath = ghfsHelper.castAsHadoopPath(fileUri);
    String fileContent = "foo-testFileChecksum-" + checksumType;
    ghfsHelper.writeFile(filePath, fileContent, 1, /* overwrite= */ true);

    FileChecksum fileChecksum = myGhfs.getFileChecksum(filePath);

    assertThat(fileChecksum.getAlgorithmName()).isEqualTo(checksumType.getAlgorithmName());
    assertThat(fileChecksum.getLength()).isEqualTo(checksumType.getByteLength());
    assertThat(fileChecksum.getBytes()).isEqualTo(checksumFn.apply(fileContent));
    assertThat(fileChecksum.toString())
        .contains(String.format("%s: ", checksumType.getAlgorithmName()));

    // Cleanup.
    assertThat(ghfs.delete(filePath, /* recursive= */ true)).isTrue();
  }

  @Test
  public void testInitializeWithEmptyWorkingDirectory_shouldHaveUserSpecificWorkingDirectory()
      throws IOException {
    GoogleHadoopFileSystem myGhfs = (GoogleHadoopFileSystem) ghfs;
    Configuration config = myGhfs.getConf();
    config.unset(GoogleHadoopFileSystemConfiguration.GCS_WORKING_DIRECTORY.getKey());
    ghfs.initialize(myGhfs.initUri, config);

    String expectedHomeDir =
        String.format(
            "gs://%s/user/%s",
            myGhfs.getWorkingDirectory().toUri().getAuthority(), USER_NAME.value());

    assertThat(ghfs.getHomeDirectory().toString()).startsWith(expectedHomeDir);
  }

  @Test
  public void testGlobStatusOptions_directoriesNamesShouldBeConsistent() throws IOException {
    testGlobStatusFlatConcurrent(GlobAlgorithm.CONCURRENT);
    testGlobStatusFlatConcurrent(GlobAlgorithm.DEFAULT);
    testGlobStatusFlatConcurrent(GlobAlgorithm.FLAT);
  }

  private void testGlobStatusFlatConcurrent(GlobAlgorithm globAlgorithm) throws IOException {
    Configuration configuration = ghfs.getConf();
    configuration.setEnum(
        GoogleHadoopFileSystemConfiguration.GCS_GLOB_ALGORITHM.getKey(), globAlgorithm);
    ghfs.initialize(ghfs.getUri(), configuration);

    Path testRoot = new Path("/directory1/");
    ghfs.mkdirs(testRoot);
    ghfs.mkdirs(new Path("/directory1/subdirectory1"));
    createFile(new Path("/directory1/subdirectory1/file1"), "data".getBytes(UTF_8));

    FileStatus[] rootDirStatuses = ghfs.globStatus(new Path("/d*"));
    List<String> rootDirs =
        stream(rootDirStatuses).map(d -> d.getPath().toString()).collect(toImmutableList());

    assertThat(rootDirs).containsExactly(ghfs.getWorkingDirectory() + "directory1");

    // Cleanup.
    assertThat(ghfs.delete(testRoot, /* recursive= */ true)).isTrue();
  }

  @Test
  public void testCreateFSDataOutputStream() {
    assertThrows(
        "hadoopPath must not be null", IllegalArgumentException.class, () -> ghfs.create(null));

    assertThrows(
        "replication must be a positive integer: -1",
        IllegalArgumentException.class,
        () -> ghfs.create(new Path("/directory1/"), (short) -1));

    assertThrows(
        "blockSize must be a positive integer: -1",
        IllegalArgumentException.class,
        () -> ghfs.create(new Path("/directory1/"), true, -1, (short) 1, -1));
  }

  @Test
  public void testRenameNullFile() {
    Path path = new Path("/directory1/");

    assertThrows(
        "src must not be null", IllegalArgumentException.class, () -> ghfs.rename(null, path));
    assertThrows(
        "dst must not be null", IllegalArgumentException.class, () -> ghfs.rename(path, null));
  }

  @Test
  public void testListStatusNull() {
    assertThrows(
        "hadoopPath must not be null",
        IllegalArgumentException.class,
        () -> ghfs.listStatus((Path) null));
  }

  @Test
  public void testSetWorkingDirectoryNull() {
    assertThrows(
        "hadoopPath must not be null",
        IllegalArgumentException.class,
        () -> ghfs.setWorkingDirectory(null));
  }

  @Test
  public void testGlobStatusWithNewUriScheme() throws IOException {
    Path globRoot = new Path("/newuriencoding_globs/");
    ghfs.mkdirs(globRoot);
    ghfs.mkdirs(new Path("/newuriencoding_globs/subdirectory1"));
    ghfs.mkdirs(new Path("/newuriencoding_globs/#this#is#a&subdir/"));

    byte[] data = new byte[10];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) i;
    }

    createFile(new Path("/newuriencoding_globs/subdirectory1/file1"), data);
    createFile(new Path("/newuriencoding_globs/subdirectory1/file2"), data);
    createFile(new Path("/newuriencoding_globs/#this#is#a&subdir/file1"), data);
    createFile(new Path("/newuriencoding_globs/#this#is#a&subdir/file2"), data);
    createFile(new Path("/newuriencoding_globs/#this#is#a&subdir/file2"), data);

    FileStatus[] rootDirectories = ghfs.globStatus(new Path("/new*"));
    assertThat(rootDirectories).hasLength(1);
    assertThat(rootDirectories[0].getPath().getName()).isEqualTo("newuriencoding_globs");

    FileStatus[] subDirectories = ghfs.globStatus(new Path("/newuriencoding_globs/s*"));
    assertThat(subDirectories).hasLength(1);
    assertThat(subDirectories[0].getPath().getName()).isEqualTo("subdirectory1");

    FileStatus[] subDirectories2 = ghfs.globStatus(new Path("/newuriencoding_globs/#this*"));
    assertThat(subDirectories2).hasLength(1);
    assertThat(subDirectories2[0].getPath().getName()).isEqualTo("#this#is#a&subdir");

    FileStatus[] subDirectories3 = ghfs.globStatus(new Path("/newuriencoding_globs/#this?is?a&*"));
    assertThat(subDirectories3).hasLength(1);
    assertThat(subDirectories3[0].getPath().getName()).isEqualTo("#this#is#a&subdir");

    FileStatus[] subDirectory1Files =
        ghfs.globStatus(new Path("/newuriencoding_globs/subdirectory1/*"));
    assertThat(subDirectory1Files).hasLength(2);
    assertThat(subDirectory1Files[0].getPath().getName()).isEqualTo("file1");
    assertThat(subDirectory1Files[1].getPath().getName()).isEqualTo("file2");

    FileStatus[] subDirectory2Files =
        ghfs.globStatus(new Path("/newuriencoding_globs/#this#is#a&subdir/f*"));
    assertThat(subDirectory2Files).hasLength(2);
    assertThat(subDirectory2Files[0].getPath().getName()).isEqualTo("file1");
    assertThat(subDirectory2Files[1].getPath().getName()).isEqualTo("file2");

    FileStatus[] subDirectory2Files2 =
        ghfs.globStatus(new Path("/newuriencoding_globs/#this#is#a&subdir/file?"));
    assertThat(subDirectory2Files2).hasLength(2);
    assertThat(subDirectory2Files2[0].getPath().getName()).isEqualTo("file1");
    assertThat(subDirectory2Files2[1].getPath().getName()).isEqualTo("file2");

    FileStatus[] subDirectory2Files3 =
        ghfs.globStatus(new Path("/newuriencoding_globs/#this#is#a&subdir/file[0-9]"));
    assertThat(subDirectory2Files3).hasLength(2);
    assertThat(subDirectory2Files3[0].getPath().getName()).isEqualTo("file1");
    assertThat(subDirectory2Files3[1].getPath().getName()).isEqualTo("file2");

    FileStatus[] subDirectory2Files4 =
        ghfs.globStatus(new Path("/newuriencoding_globs/#this#is#a&subdir/file[^1]"));
    assertThat(subDirectory2Files4).hasLength(1);
    assertThat(subDirectory2Files4[0].getPath().getName()).isEqualTo("file2");

    FileStatus[] subDirectory2Files5 =
        ghfs.globStatus(new Path("/newuriencoding_globs/#this#is#a&subdir/file{1,2}"));
    assertThat(subDirectory2Files5).hasLength(2);
    assertThat(subDirectory2Files5[0].getPath().getName()).isEqualTo("file1");
    assertThat(subDirectory2Files5[1].getPath().getName()).isEqualTo("file2");

    ghfs.delete(globRoot, true);
  }

  @Test
  public void testPathsOnlyValidInNewUriScheme() throws IOException {
    GoogleHadoopFileSystem typedFs = (GoogleHadoopFileSystem) ghfs;

    Path directory =
        new Path(
            String.format(
                "gs://%s/testPathsOnlyValidInNewUriScheme/",
                typedFs.getWorkingDirectory().toUri().getAuthority()));
    Path p = new Path(directory, "foo#bar#baz");
    assertThrows(FileNotFoundException.class, () -> ghfs.getFileStatus(p));

    ghfsHelper.writeFile(p, "SomeText", 100, /* overwrite= */ false);

    FileStatus status = ghfs.getFileStatus(p);
    assertThat(status.getPath()).isEqualTo(p);
    ghfs.delete(directory, true);
  }

  @Override
  public void testGetGcsPath() throws URISyntaxException {
    GoogleHadoopFileSystem myghfs = (GoogleHadoopFileSystem) ghfs;
    URI gcsPath =
        new URI("gs://" + myghfs.getWorkingDirectory().toUri().getAuthority() + "/dir/obj");
    URI convertedPath = myghfs.getGcsPath(new Path(gcsPath));
    assertThat(convertedPath).isEqualTo(gcsPath);

    // When using the LegacyPathCodec this will fail, but it's perfectly fine to encode
    // this in the UriPathCodec. Note that new Path("/buck^et", "object")
    // isn't actually using bucket as a bucket, but instead as a part of the path...
    myghfs.getGcsPath(new Path("/buck^et", "object"));

    // Validate that authorities can't be crazy:
    assertThrows(
        IllegalArgumentException.class, () -> myghfs.getGcsPath(new Path("gs://buck^et/object")));
  }

  @Test
  public void testInvalidCredentialsFromAccessTokenProvider() throws Exception {
    Configuration config = new Configuration();
    config.setEnum("fs.gs.auth.type", AuthenticationType.ACCESS_TOKEN_PROVIDER);
    config.setClass(
        "fs.gs.auth.access.token.provider",
        TestingAccessTokenProvider.class,
        AccessTokenProvider.class);
    URI gsUri = new URI("gs://foobar/");

    GoogleHadoopFileSystem ghfs = new GoogleHadoopFileSystem();
    ghfs.initialize(gsUri, config);

    IOException thrown = assertThrows(IOException.class, () -> ghfs.exists(new Path("gs://")));

    assertThat(thrown).hasCauseThat().hasMessageThat().contains("Invalid Credentials");
  }

  @Test
  public void testImpersonationServiceAccountUsed() throws Exception {
    Configuration config = new Configuration();
    config.setEnum("fs.gs.auth.type", AuthenticationType.ACCESS_TOKEN_PROVIDER);
    config.setClass(
        "fs.gs.auth.access.token.provider",
        TestingAccessTokenProvider.class,
        AccessTokenProvider.class);
    config.set(
        GCS_CONFIG_PREFIX + IMPERSONATION_SERVICE_ACCOUNT_SUFFIX.getKey(), "test-service-account");

    Path gcsPath = new Path("gs://foobar/");
    GoogleHadoopFileSystem ghfs = new GoogleHadoopFileSystem();
    ghfs.initialize(gcsPath.toUri(), config);

    IOException thrown = assertThrows(IOException.class, () -> ghfs.listStatus(gcsPath));
    HttpResponseException httpException = ApiErrorExtractor.getHttpResponseException(thrown);
    assertThat(httpException).hasMessageThat().startsWith("401 Unauthorized");
  }

  @Test
  public void testImpersonationUserNameIdentifierUsed() throws Exception {
    Configuration config = new Configuration();
    config.setEnum("fs.gs.auth.type", AuthenticationType.ACCESS_TOKEN_PROVIDER);
    config.setClass(
        "fs.gs.auth.access.token.provider",
        TestingAccessTokenProvider.class,
        AccessTokenProvider.class);
    config.set(
        GCS_CONFIG_PREFIX
            + USER_IMPERSONATION_SERVICE_ACCOUNT_SUFFIX.getKey()
            + UserGroupInformation.getCurrentUser().getShortUserName(),
        "test-service-account");

    Path gcsPath = new Path("gs://foobar/");
    GoogleHadoopFileSystem ghfs = new GoogleHadoopFileSystem();
    ghfs.initialize(gcsPath.toUri(), config);

    IOException thrown = assertThrows(IOException.class, () -> ghfs.listStatus(gcsPath));
    HttpResponseException httpException = ApiErrorExtractor.getHttpResponseException(thrown);
    assertThat(httpException).hasMessageThat().startsWith("401 Unauthorized");
  }

  @Test
  public void testImpersonationGroupNameIdentifierUsed() throws Exception {
    Configuration config = new Configuration();
    config.setEnum("fs.gs.auth.type", AuthenticationType.ACCESS_TOKEN_PROVIDER);
    config.setClass(
        "fs.gs.auth.access.token.provider",
        TestingAccessTokenProvider.class,
        AccessTokenProvider.class);
    config.set(
        GCS_CONFIG_PREFIX
            + GROUP_IMPERSONATION_SERVICE_ACCOUNT_SUFFIX.getKey()
            + UserGroupInformation.getCurrentUser().getGroupNames()[0],
        "test-service-account");

    Path gcsPath = new Path("gs://foobar/");
    GoogleHadoopFileSystem ghfs = new GoogleHadoopFileSystem();
    ghfs.initialize(gcsPath.toUri(), config);

    IOException thrown = assertThrows(IOException.class, () -> ghfs.listStatus(gcsPath));
    HttpResponseException httpException = ApiErrorExtractor.getHttpResponseException(thrown);
    assertThat(httpException).hasMessageThat().startsWith("401 Unauthorized");
  }

  @Test
  public void testImpersonationUserAndGroupNameIdentifiersUsed() throws Exception {
    Configuration config = new Configuration();
    config.setEnum("fs.gs.auth.type", AuthenticationType.ACCESS_TOKEN_PROVIDER);
    config.setClass(
        "fs.gs.auth.access.token.provider",
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

    Path gcsPath = new Path("gs://foobar/");
    GoogleHadoopFileSystem ghfs = new GoogleHadoopFileSystem();
    ghfs.initialize(gcsPath.toUri(), config);

    IOException thrown = assertThrows(IOException.class, () -> ghfs.listStatus(gcsPath));
    HttpResponseException httpException = ApiErrorExtractor.getHttpResponseException(thrown);
    assertThat(httpException).hasMessageThat().startsWith("401 Unauthorized");
  }

  @Test
  public void testImpersonationServiceAccountAndUserAndGroupNameIdentifierUsed() throws Exception {
    Configuration config = new Configuration();
    config.setEnum("fs.gs.auth.type", AuthenticationType.ACCESS_TOKEN_PROVIDER);
    config.setClass(
        "fs.gs.auth.access.token.provider",
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

    Path gcsPath = new Path("gs://foobar/");
    GoogleHadoopFileSystem ghfs = new GoogleHadoopFileSystem();
    ghfs.initialize(gcsPath.toUri(), config);

    IOException thrown = assertThrows(IOException.class, () -> ghfs.listStatus(gcsPath));
    HttpResponseException httpException = ApiErrorExtractor.getHttpResponseException(thrown);
    assertThat(httpException).hasMessageThat().startsWith("401 Unauthorized");
  }

  @Test
  public void testImpersonationInvalidUserNameIdentifierUsed() throws Exception {
    Configuration config = new Configuration();
    config.setEnum("fs.gs.auth.type", AuthenticationType.ACCESS_TOKEN_PROVIDER);
    config.setClass(
        "fs.gs.auth.access.token.provider",
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
  public void unauthenticatedAccessToPublicBuckets_fsGsProperties() throws Exception {
    String publicBucket = "gs://gcp-public-data-landsat";

    Configuration config = new Configuration();
    config.setEnum("fs.gs.auth.type", AuthenticationType.UNAUTHENTICATED);

    FileSystem fs = FileSystem.get(new URI(publicBucket), config);

    FileStatus[] fileStatuses = fs.listStatus(new Path(publicBucket));

    assertThat(
            ((GoogleHadoopFileSystem) fs)
                .getIOStatistics()
                .counters()
                .get(INVOCATION_LIST_FILES.getSymbol()))
        .isEqualTo(1);
    assertThat(
            ((GoogleHadoopFileSystem) fs)
                .getIOStatistics()
                .counters()
                .get(INVOCATION_LIST_STATUS.getSymbol()))
        .isEqualTo(1);

    assertThat(fileStatuses).isNotEmpty();
  }

  @Test
  public void unauthenticatedAccessToPublicBuckets_googleCloudProperties() throws Exception {
    String publicBucket = "gs://gcp-public-data-landsat";

    Configuration config = new Configuration();
    config.setEnum("google.cloud.auth.type", AuthenticationType.UNAUTHENTICATED);

    FileSystem fs = FileSystem.get(new URI(publicBucket), config);

    FileStatus[] fileStatuses = fs.listStatus(new Path(publicBucket));

    assertThat(fileStatuses).isNotEmpty();
  }
}
