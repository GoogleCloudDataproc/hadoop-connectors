/*
 * Copyright 2014 Google Inc.
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

import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.ACTION_HTTP_DELETE_REQUEST;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.ACTION_HTTP_GET_REQUEST;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.ACTION_HTTP_PATCH_REQUEST;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.ACTION_HTTP_POST_REQUEST;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.ACTION_HTTP_PUT_REQUEST;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.FILES_CREATED;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.INVOCATION_COPY_FROM_LOCAL_FILE;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.INVOCATION_CREATE;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.INVOCATION_CREATE_NON_RECURSIVE;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.INVOCATION_DELETE;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.INVOCATION_EXISTS;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.INVOCATION_GET_FILE_CHECKSUM;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.INVOCATION_GET_FILE_STATUS;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.INVOCATION_GLOB_STATUS;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.INVOCATION_HFLUSH;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.INVOCATION_HSYNC;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.INVOCATION_LIST_LOCATED_STATUS;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.INVOCATION_LIST_STATUS;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.INVOCATION_LIST_STATUS_RESULT_SIZE;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.INVOCATION_MKDIRS;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.INVOCATION_OPEN;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.INVOCATION_OP_XATTR_LIST;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.INVOCATION_RENAME;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.INVOCATION_XATTR_GET_MAP;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.INVOCATION_XATTR_GET_NAMED;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.INVOCATION_XATTR_GET_NAMED_MAP;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.STREAM_READ_BYTES;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.STREAM_READ_OPERATIONS;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.STREAM_WRITE_BYTES;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.STREAM_WRITE_OPERATIONS;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_OUTPUT_STREAM_BUFFER_SIZE;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_PROJECT_ID;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemTestHelper.createInMemoryGoogleHadoopFileSystem;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.GCS_GET_MEDIA_REQUEST;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.GCS_LIST_DIR_REQUEST;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.GCS_LIST_FILE_REQUEST;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.GCS_METADATA_REQUEST;
import static com.google.cloud.hadoop.gcsio.testing.InMemoryGoogleCloudStorage.getInMemoryGoogleCloudStorageOptions;
import static com.google.common.base.StandardSystemProperty.USER_NAME;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.stream;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_CREDENTIAL_PROVIDER_PATH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

import com.google.api.client.http.HttpResponseException;
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem.GcsFileChecksumType;
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem.GlobAlgorithm;
import com.google.cloud.hadoop.fs.gcs.auth.AbstractDelegationTokenBinding;
import com.google.cloud.hadoop.fs.gcs.auth.TestDelegationTokenBindingImpl;
import com.google.cloud.hadoop.gcsio.*;
import com.google.cloud.hadoop.gcsio.CreateBucketOptions;
import com.google.cloud.hadoop.gcsio.FolderInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemIntegrationHelper;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions.ClientType;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.ListFolderOptions;
import com.google.cloud.hadoop.gcsio.MethodOutcome;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.cloud.hadoop.gcsio.testing.InMemoryGoogleCloudStorage;
import com.google.cloud.hadoop.util.AccessTokenProvider;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.cloud.hadoop.util.GoogleCloudStorageEventBus;
import com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.AuthenticationType;
import com.google.cloud.hadoop.util.LoggingFormatter;
import com.google.cloud.hadoop.util.testing.TestingAccessTokenProvider;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Ints;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.logging.*;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.Service;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/** Integration tests for GoogleHadoopFileSystem class. */
public abstract class GoogleHadoopFileSystemIntegrationTest extends GoogleHadoopFileSystemTestBase {

  private static final String PUBLIC_BUCKET = "gs://gcp-public-data-landsat";

  private static HashSet<String> EXPECTED_DURATION_METRICS = getExpectedDurationMetrics();

  private static final String GCS_API_COUNT = "gcsApiCount";
  private static final String HADOOP_API_COUNT = "hadoopApiCount";

  private static final long BUCKET_DELETION_TIMEOUT_MS = 30_000;
  private static final long RETRY_INTERVAL_MS = 1_000;

  @Before
  public void before() throws Exception {

    GoogleHadoopFileSystem testInstance = new GoogleHadoopFileSystem();
    ghfs = testInstance;

    // loadConfig needs ghfsHelper, which is normally created in
    // postCreateInit. Create one here for it to use.
    ghfsHelper = new HadoopFileSystemIntegrationHelper(ghfs);

    URI initUri = new URI("gs://" + ghfsHelper.getUniqueBucketName("init"));
    ghfs.initialize(initUri, loadConfig(storageClientType));

    if (GoogleHadoopFileSystemConfiguration.GCS_LAZY_INITIALIZATION_ENABLE.get(
        ghfs.getConf(), ghfs.getConf()::getBoolean)) {
      testInstance.getGcsFs();
    }

    super.postCreateInit();
  }

  @After
  public void after() throws IOException {
    ghfsHelper.afterAllTests();
    super.after();
  }

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
  public void testRenameHnBucket() throws Exception {
    String bucketName = this.gcsiHelper.getUniqueBucketName("hn");
    GoogleHadoopFileSystem googleHadoopFileSystem = new GoogleHadoopFileSystem();

    URI initUri = new URI("gs://" + bucketName);
    Configuration config = loadConfig();
    config.setBoolean("fs.gs.hierarchical.namespace.folders.enable", true);
    googleHadoopFileSystem.initialize(initUri, config);

    GoogleCloudStorage theGcs = googleHadoopFileSystem.getGcsFs().getGcs();
    theGcs.createBucket(
        bucketName, CreateBucketOptions.builder().setHierarchicalNamespaceEnabled(true).build());

    try {
      GoogleCloudStorageFileSystemIntegrationHelper helper =
          new HadoopFileSystemIntegrationHelper(googleHadoopFileSystem);

      renameHelper(
          new HdfsBehavior() {
            /**
             * Returns the MethodOutcome of trying to rename an existing file into the root
             * directory.
             */
            @Override
            public MethodOutcome renameFileIntoRootOutcome() {
              return new MethodOutcome(MethodOutcome.Type.RETURNS_TRUE);
            }
          },
          bucketName,
          bucketName,
          helper);
    } finally {
      googleHadoopFileSystem.delete(new Path(initUri));
      googleHadoopFileSystem.close();
    }
  }

  @Test
  public void testRenameWithMoveDisabled() throws Exception {
    String bucketName = this.gcsiHelper.getUniqueBucketName("move");
    GoogleHadoopFileSystem googleHadoopFileSystem = new GoogleHadoopFileSystem();

    URI initUri = new URI("gs://" + bucketName);
    Configuration config = loadConfig();
    config.setBoolean("fs.gs.operation.move.enable", false);
    googleHadoopFileSystem.initialize(initUri, config);

    GoogleCloudStorage theGcs = googleHadoopFileSystem.getGcsFs().getGcs();
    theGcs.createBucket(bucketName);

    try {
      GoogleCloudStorageFileSystemIntegrationHelper helper =
          new HadoopFileSystemIntegrationHelper(googleHadoopFileSystem);

      renameHelper(
          new HdfsBehavior() {
            /**
             * Returns the MethodOutcome of trying to rename an existing file into the root
             * directory.
             */
            @Override
            public MethodOutcome renameFileIntoRootOutcome() {
              return new MethodOutcome(MethodOutcome.Type.RETURNS_TRUE);
            }
          },
          bucketName,
          bucketName,
          helper);
    } finally {
      googleHadoopFileSystem.delete(new Path(initUri));
      googleHadoopFileSystem.close();
    }
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
    config.setClass(
        "fs.gs.delegation.token.binding",
        TestDelegationTokenBindingImpl.class,
        AbstractDelegationTokenBinding.class);
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
    config.setClass(
        "fs.gs.delegation.token.binding",
        TestDelegationTokenBindingImpl.class,
        AbstractDelegationTokenBinding.class);
    config.set(
        TestDelegationTokenBindingImpl.TestAccessTokenProviderImpl.TOKEN_CONFIG_PROPERTY_NAME,
        "qWDAWFA3WWFAWFAWFAW3FAWF3AWF3WFAF33GR5G5"); // Bogus auth token

    GoogleHadoopFileSystem fs = new GoogleHadoopFileSystem();
    fs.initialize(new URI("gs://test/init-uri"), config);

    assertThat(fs.getCanonicalServiceName()).isEqualTo(fs.delegationTokens.getService().toString());

    fs.close();
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
    URI fileUri = getTempFilePath();
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
    URI fileUri = getTempFilePath();
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
    StorageStatistics stats = TestUtils.getStorageStatistics();

    assertThat(myGhfs.getIOStatistics().counters().get(INVOCATION_CREATE.getSymbol())).isEqualTo(0);
    assertThat(myGhfs.getIOStatistics().counters().get(FILES_CREATED.getSymbol())).isEqualTo(0);

    assertThat(getMetricValue(stats, INVOCATION_CREATE)).isEqualTo(0);
    assertThat(getMetricValue(stats, FILES_CREATED)).isEqualTo(0);

    try (FSDataOutputStream fout = myGhfs.create(new Path("/file1"))) {
      fout.writeBytes("Test Content");
    }

    assertThat(myGhfs.getIOStatistics().counters().get(INVOCATION_CREATE.getSymbol())).isEqualTo(1);
    assertThat(myGhfs.getIOStatistics().counters().get(FILES_CREATED.getSymbol())).isEqualTo(1);

    assertThat(getMetricValue(stats, INVOCATION_CREATE)).isEqualTo(1);
    assertThat(getMetricValue(stats, FILES_CREATED)).isEqualTo(1);

    TestUtils.verifyDurationMetric(myGhfs.getIOStatistics(), INVOCATION_CREATE.getSymbol(), 1);

    assertThat(myGhfs.delete(new Path("/file1"))).isTrue();
    TestUtils.verifyDurationMetric(
        (GhfsGlobalStorageStatistics) stats, INVOCATION_CREATE.getSymbol(), 1);
  }

  @Test
  public void create_IOstatistics_statistics_check() throws IOException {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    StorageStatistics stats = TestUtils.getStorageStatistics();

    assertThat(myGhfs.getIOStatistics().counters().get(INVOCATION_CREATE.getSymbol())).isEqualTo(0);
    assertThat(myGhfs.getIOStatistics().counters().get(FILES_CREATED.getSymbol())).isEqualTo(0);

    assertThat(getMetricValue(stats, INVOCATION_CREATE)).isEqualTo(0);
    assertThat(getMetricValue(stats, FILES_CREATED)).isEqualTo(0);

    try (FSDataOutputStream fout = myGhfs.create(new Path("/file1"))) {
      fout.writeBytes("Test Content");
    }

    assertThat(myGhfs.getIOStatistics().counters().get(INVOCATION_CREATE.getSymbol())).isEqualTo(1);
    assertThat(myGhfs.getIOStatistics().counters().get(FILES_CREATED.getSymbol())).isEqualTo(1);

    assertThat(getMetricValue(stats, INVOCATION_CREATE)).isEqualTo(1);
    assertThat(getMetricValue(stats, FILES_CREATED)).isEqualTo(1);

    TestUtils.verifyDurationMetric(myGhfs.getIOStatistics(), INVOCATION_CREATE.getSymbol(), 1);

    assertThat(myGhfs.delete(new Path("/file1"))).isTrue();
    TestUtils.verifyDurationMetric(
        (GhfsGlobalStorageStatistics) stats, INVOCATION_CREATE.getSymbol(), 1);

    myGhfs = createInMemoryGoogleHadoopFileSystem();

    assertThat(myGhfs.getIOStatistics().counters().get(INVOCATION_CREATE.getSymbol())).isEqualTo(0);
    assertThat(myGhfs.getIOStatistics().counters().get(FILES_CREATED.getSymbol())).isEqualTo(0);

    assertThat(getMetricValue(stats, INVOCATION_CREATE)).isEqualTo(1);
    assertThat(getMetricValue(stats, FILES_CREATED)).isEqualTo(1);

    try (FSDataOutputStream fout = myGhfs.create(new Path("/file1"))) {
      fout.writeBytes("Test Content");
    }

    assertThat(myGhfs.getIOStatistics().counters().get(INVOCATION_CREATE.getSymbol())).isEqualTo(1);
    assertThat(myGhfs.getIOStatistics().counters().get(FILES_CREATED.getSymbol())).isEqualTo(1);
    TestUtils.verifyDurationMetric(myGhfs.getIOStatistics(), INVOCATION_CREATE.getSymbol(), 1);

    assertThat(getMetricValue(stats, INVOCATION_CREATE)).isEqualTo(2);
    assertThat(getMetricValue(stats, FILES_CREATED)).isEqualTo(2);

    assertThat(myGhfs.delete(new Path("/file1"))).isTrue();
    TestUtils.verifyDurationMetric(
        (GhfsGlobalStorageStatistics) stats, INVOCATION_CREATE.getSymbol(), 2);
  }

  @Test
  public void listLocatedStatus_IOStatistics() throws IOException {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    StorageStatistics stats = TestUtils.getStorageStatistics();

    try (FSDataOutputStream fout = myGhfs.create(new Path("/file1"))) {
      fout.writeBytes("Test Content");
      fout.close();
      myGhfs.listLocatedStatus(new Path("/file1"));

      assertThat(
              myGhfs.getIOStatistics().counters().get(INVOCATION_LIST_LOCATED_STATUS.getSymbol()))
          .isEqualTo(1);

      TestUtils.verifyCounter(
          (GhfsGlobalStorageStatistics) stats, INVOCATION_LIST_LOCATED_STATUS, 1);
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
    URI fileUri = getTempFilePath();
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
    StorageStatistics stats = TestUtils.getStorageStatistics();

    Path testRoot = new Path("/directory1/");
    myGhfs.mkdirs(testRoot);
    FSDataOutputStream fout = myGhfs.create(new Path("/directory1/file1"));
    fout.writeBytes("data");
    fout.close();
    myGhfs.open(new Path("/directory1/file1"));
    assertThat(getMetricValue(stats, INVOCATION_OPEN)).isEqualTo(1);
    assertThat(myGhfs.getIOStatistics().counters().get(INVOCATION_OPEN.getSymbol())).isEqualTo(1);

    assertThat(myGhfs.delete(testRoot, /* recursive= */ true)).isTrue();

    TestUtils.verifyDurationMetric(myGhfs.getIOStatistics(), INVOCATION_OPEN.getSymbol(), 1);

    TestUtils.verifyDurationMetric(
        (GhfsGlobalStorageStatistics) stats, INVOCATION_OPEN.getSymbol(), 1);
  }

  @Test
  public void delete_IOstatistics() throws IOException {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    StorageStatistics stats = TestUtils.getStorageStatistics();

    FSDataOutputStream fout = myGhfs.create(new Path("/file1"));
    fout.writeBytes("data");
    fout.close();
    myGhfs.delete(new Path("/file1"));
    assertThat(getMetricValue(stats, INVOCATION_DELETE)).isEqualTo(1);

    TestUtils.verifyDurationMetric(myGhfs.getIOStatistics(), INVOCATION_DELETE.getSymbol(), 1);

    TestUtils.verifyDurationMetric(
        (GhfsGlobalStorageStatistics) stats, INVOCATION_DELETE.getSymbol(), 1);
  }

  @Test
  public void statistics_check_get_list_status_result_size() throws IOException {

    // first filesystem object
    GoogleHadoopFileSystem myGhfs1 = createInMemoryGoogleHadoopFileSystem();
    StorageStatistics stats = TestUtils.getStorageStatistics();

    Path testRoot = new Path("/directory1/");

    // first file created in ghfs1
    myGhfs1.mkdirs(testRoot);
    FSDataOutputStream fout = myGhfs1.create(new Path("/directory1/file1"));
    fout.writeBytes("data");
    fout.close();
    myGhfs1.listStatus(testRoot);
    assertThat(
            (myGhfs1)
                .getIOStatistics()
                .counters()
                .get(INVOCATION_LIST_STATUS_RESULT_SIZE.getSymbol()))
        .isEqualTo(1);

    TestUtils.verifyCounter(
        (GhfsGlobalStorageStatistics) stats, INVOCATION_LIST_STATUS_RESULT_SIZE, 1);
    assertThat(myGhfs1.delete(testRoot, /* recursive= */ true)).isTrue();

    // create another FileSystem Object
    GoogleHadoopFileSystem myGhfs2 = createInMemoryGoogleHadoopFileSystem();

    // first file created in ghfs2
    fout = myGhfs2.create(new Path("/directory1/file1"));
    fout.writeBytes("data");
    fout.close();

    // first file created in ghfs3
    fout = myGhfs2.create(new Path("/directory1/file2"));
    fout.writeBytes("data");
    fout.close();

    myGhfs2.listStatus(testRoot);

    assertThat(
            (myGhfs2)
                .getIOStatistics()
                .counters()
                .get(INVOCATION_LIST_STATUS_RESULT_SIZE.getSymbol()))
        .isEqualTo(2);

    TestUtils.verifyCounter(
        (GhfsGlobalStorageStatistics) stats, INVOCATION_LIST_STATUS_RESULT_SIZE, 3);

    assertThat(myGhfs2.delete(testRoot, /* recursive= */ true)).isTrue();
  }

  @Test
  public void statistics_check_read_twice() throws Exception {
    GoogleHadoopFileSystem fs1 = createInMemoryGoogleHadoopFileSystem();
    StorageStatistics stats = TestUtils.getStorageStatistics();

    Path testFileToIO = new Path("/test-random.bin");
    createFile(fs1, testFileToIO, 1024, 10 * 1024);
    readFile(fs1, testFileToIO, 1024);
    fs1.delete(testFileToIO);

    // per FS based statistics
    assertThat(fs1.getIOStatistics().counters().get(STREAM_READ_BYTES.getSymbol()))
        .isEqualTo(10240);
    assertThat(fs1.getIOStatistics().counters().get(STREAM_READ_OPERATIONS.getSymbol()))
        .isEqualTo(11);
    assertThat(fs1.getIOStatistics().counters().get(GhfsStatistic.INVOCATION_DELETE.getSymbol()))
        .isEqualTo(1);

    // Global Statistics
    TestUtils.verifyDurationMetric(
        (GhfsGlobalStorageStatistics) stats, STREAM_READ_BYTES.getSymbol(), 10240);
    TestUtils.verifyDurationMetric(
        (GhfsGlobalStorageStatistics) stats, STREAM_READ_OPERATIONS.getSymbol(), 10);
    TestUtils.verifyDurationMetric(
        (GhfsGlobalStorageStatistics) stats, INVOCATION_DELETE.getSymbol(), 1);

    GoogleHadoopFileSystem fs2 = createInMemoryGoogleHadoopFileSystem();
    createFile(fs2, testFileToIO, 1024, 10 * 1024);
    readFile(fs2, testFileToIO, 1024);
    fs2.delete(testFileToIO);

    // per FS based statistics
    assertThat(fs2.getIOStatistics().counters().get(STREAM_READ_BYTES.getSymbol()))
        .isEqualTo(10240);
    assertThat(fs2.getIOStatistics().counters().get(STREAM_READ_OPERATIONS.getSymbol()))
        .isEqualTo(11);
    assertThat(fs1.getIOStatistics().counters().get(GhfsStatistic.INVOCATION_DELETE.getSymbol()))
        .isEqualTo(1);

    assertThat(fs1.getIOStatistics().counters().get(STREAM_READ_BYTES.getSymbol()))
        .isEqualTo(10240);
    assertThat(fs1.getIOStatistics().counters().get(STREAM_READ_OPERATIONS.getSymbol()))
        .isEqualTo(11);
    assertThat(fs1.getIOStatistics().counters().get(GhfsStatistic.INVOCATION_DELETE.getSymbol()))
        .isEqualTo(1);

    // Global Statistics
    TestUtils.verifyDurationMetric(
        (GhfsGlobalStorageStatistics) stats, STREAM_READ_BYTES.getSymbol(), 20480);
    TestUtils.verifyDurationMetric(
        (GhfsGlobalStorageStatistics) stats, STREAM_READ_OPERATIONS.getSymbol(), 20);
    TestUtils.verifyDurationMetric(
        (GhfsGlobalStorageStatistics) stats, INVOCATION_DELETE.getSymbol(), 2);

    fs1.close();
    fs2.close();
  }

  @Test
  public void statistics_check_write_twice() throws Exception {

    GoogleHadoopFileSystem fs1 = createInMemoryGoogleHadoopFileSystem();
    StorageStatistics stats = TestUtils.getStorageStatistics();

    Path testFileToIO = new Path("/test-random.bin");
    createFile(fs1, testFileToIO, 1024, 10 * 1024);
    fs1.delete(testFileToIO);

    // per FS based statistics
    assertThat(fs1.getIOStatistics().counters().get(STREAM_WRITE_BYTES.getSymbol()))
        .isEqualTo(10240);
    assertThat(fs1.getIOStatistics().counters().get(STREAM_WRITE_OPERATIONS.getSymbol()))
        .isEqualTo(10);
    assertThat(fs1.getIOStatistics().counters().get(GhfsStatistic.INVOCATION_DELETE.getSymbol()))
        .isEqualTo(1);

    // Global Statistics
    TestUtils.verifyDurationMetric(
        (GhfsGlobalStorageStatistics) stats, STREAM_WRITE_BYTES.getSymbol(), 10240);
    TestUtils.verifyDurationMetric(
        (GhfsGlobalStorageStatistics) stats, STREAM_WRITE_OPERATIONS.getSymbol(), 10);
    TestUtils.verifyDurationMetric(
        (GhfsGlobalStorageStatistics) stats, INVOCATION_DELETE.getSymbol(), 1);

    GoogleHadoopFileSystem fs2 = createInMemoryGoogleHadoopFileSystem();
    createFile(fs2, testFileToIO, 1024, 10 * 1024);
    fs2.delete(testFileToIO);

    // per FS based statistics
    assertThat(fs2.getIOStatistics().counters().get(STREAM_WRITE_BYTES.getSymbol()))
        .isEqualTo(10240);
    assertThat(fs2.getIOStatistics().counters().get(STREAM_WRITE_OPERATIONS.getSymbol()))
        .isEqualTo(10);
    assertThat(fs2.getIOStatistics().counters().get(GhfsStatistic.INVOCATION_DELETE.getSymbol()))
        .isEqualTo(1);
    assertThat(fs1.getIOStatistics().counters().get(STREAM_WRITE_BYTES.getSymbol()))
        .isEqualTo(10240);
    assertThat(fs1.getIOStatistics().counters().get(STREAM_WRITE_OPERATIONS.getSymbol()))
        .isEqualTo(10);
    assertThat(fs1.getIOStatistics().counters().get(GhfsStatistic.INVOCATION_DELETE.getSymbol()))
        .isEqualTo(1);

    // Global Statistics
    TestUtils.verifyDurationMetric(
        (GhfsGlobalStorageStatistics) stats, STREAM_WRITE_BYTES.getSymbol(), 20480);
    TestUtils.verifyDurationMetric(
        (GhfsGlobalStorageStatistics) stats, STREAM_WRITE_OPERATIONS.getSymbol(), 20);
    TestUtils.verifyDurationMetric(
        (GhfsGlobalStorageStatistics) stats, INVOCATION_DELETE.getSymbol(), 2);

    fs1.close();
    fs2.close();
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
    StorageStatistics stats = TestUtils.getStorageStatistics();
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    assertThat(myGhfs.getIOStatistics().counters().get(INVOCATION_MKDIRS.getSymbol())).isEqualTo(1);

    TestUtils.verifyCounter((GhfsGlobalStorageStatistics) stats, INVOCATION_MKDIRS, 1);
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
    GoogleHadoopFileSystem myGhfs = (GoogleHadoopFileSystem) ghfs;
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
  @SuppressWarnings("CheckReturnValue")
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
    Configuration config = loadConfig(new Configuration(), storageClientType);
    config.set(GCS_PROJECT_ID.toString(), "projectId");
    config.setEnum("fs.gs.auth.type", AuthenticationType.SERVICE_ACCOUNT_JSON_KEYFILE);
    config.set("fs.gs.auth.service.account.json.keyfile", "path/to/serviceAccountKeyFile.json");

    GoogleCloudStorageFileSystemOptions.Builder optionsBuilder =
        GoogleHadoopFileSystemConfiguration.getGcsFsOptionsBuilder(config);
    GoogleCloudStorageFileSystemOptions options = optionsBuilder.build();
    GoogleCloudStorageOptions gcsOptions = options.getCloudStorageOptions();

    assertThat(gcsOptions.isAutoRepairImplicitDirectoriesEnabled()).isTrue();

    config.setBoolean("fs.gs.implicit.dir.repair.enable", false);

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
    Configuration config = loadConfig(storageClientType);

    // Set up remaining settings to known test values.
    long blockSize = 1024;
    config.setLong("fs.gs.block.size", blockSize);
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

    fs.close();
  }

  @Test
  public void testInitializeSucceedsWhenNoProjectIdConfigured()
      throws URISyntaxException, IOException {
    Configuration config = loadConfig(storageClientType);
    // Unset Project ID
    config.unset("fs.gs.project.id");

    URI gsUri = new Path("gs://foo").toUri();
    GoogleHadoopFileSystem ghfs = new GoogleHadoopFileSystem();
    ghfs.initialize(gsUri, config);

    ghfs.close();
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
    config.set("fs.gs.auth.type", "INVALID_AUTH_TYPE");
    // Set project ID.
    config.set("fs.gs.project.id", fakeProjectId);

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
    ghfs.close();
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

    Configuration config = loadConfig(storageClientType);
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
      config.set("fs.gs.working.dir", path.toString());
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

    fs.close();
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

    fs.close();
  }

  @Test
  public void create_storage_statistics() throws IOException {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    GhfsStorageStatistics FSStorageStats = new GhfsStorageStatistics(myGhfs.getIOStatistics());

    StorageStatistics GlobalStorageStats = TestUtils.getStorageStatistics();
    Path testRoot = new Path("/directory1/");
    myGhfs.mkdirs(testRoot);
    FSDataOutputStream fout = myGhfs.create(new Path("/directory1/file1"));
    fout.writeBytes("Test Content");
    fout.close();

    assertThat(FSStorageStats.isTracked("op_create")).isTrue();
    assertThat(FSStorageStats.getLong("op_create")).isEqualTo(1);

    assertThat(GlobalStorageStats.isTracked("op_create")).isTrue();
    TestUtils.verifyCounter((GhfsGlobalStorageStatistics) GlobalStorageStats, INVOCATION_CREATE, 1);
    assertThat(FSStorageStats.getLong("op_create")).isEqualTo(1);
    assertThat(myGhfs.delete(testRoot, /* recursive= */ true)).isTrue();
  }

  @Test
  public void listLocatedStatus_storage_statistics() throws IOException {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    GhfsStorageStatistics StorageStats = new GhfsStorageStatistics(myGhfs.getIOStatistics());

    StorageStatistics stats = TestUtils.getStorageStatistics();
    Path testRoot = new Path("/directory1/");
    myGhfs.mkdirs(testRoot);
    FSDataOutputStream fout = myGhfs.create(new Path("/directory1/file1"));
    fout.writeBytes("Test Content");
    fout.close();
    myGhfs.listLocatedStatus(testRoot);

    assertThat(StorageStats.isTracked("op_list_located_status")).isTrue();
    assertThat(StorageStats.getLong("op_list_located_status")).isEqualTo(1);

    assertThat(stats.isTracked("op_list_located_status")).isTrue();
    TestUtils.verifyCounter((GhfsGlobalStorageStatistics) stats, INVOCATION_LIST_LOCATED_STATUS, 1);
    assertThat(StorageStats.getLong("op_list_located_status")).isEqualTo(1);
    assertThat(myGhfs.delete(testRoot, /* recursive= */ true)).isTrue();
  }

  @Test
  public void open_storage_statistics() throws IOException {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    GhfsStorageStatistics StorageStats = new GhfsStorageStatistics(myGhfs.getIOStatistics());

    StorageStatistics stats = TestUtils.getStorageStatistics();
    Path testRoot = new Path("/directory1/");
    myGhfs.mkdirs(testRoot);
    FSDataOutputStream fout = myGhfs.create(new Path("/directory1/file1"));
    fout.writeBytes("data");
    fout.close();
    myGhfs.open(new Path("/directory1/file1"));
    assertThat(getMetricValue(stats, INVOCATION_OPEN)).isEqualTo(1);
    TestUtils.verifyDurationMetric(
        (GhfsGlobalStorageStatistics) stats, INVOCATION_OPEN.getSymbol(), 1);

    assertThat(StorageStats.isTracked("op_open")).isTrue();
    assertThat(StorageStats.getLong("op_open")).isEqualTo(1);

    assertThat(myGhfs.delete(testRoot, /* recursive= */ true)).isTrue();
  }

  @Test
  public void copy_from_local_file_storage_statistics() throws IOException {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    StorageStatistics stats = TestUtils.getStorageStatistics();
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

    TestUtils.verifyCounter(
        (GhfsGlobalStorageStatistics) stats, INVOCATION_COPY_FROM_LOCAL_FILE, 1);
    assertThat(myGhfs.delete(testRoot, /* recursive= */ true)).isTrue();
  }

  @Test
  public void delete_storage_statistics() throws IOException {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    StorageStatistics stats = TestUtils.getStorageStatistics();
    GhfsStorageStatistics StorageStats = new GhfsStorageStatistics(myGhfs.getIOStatistics());

    Path filePath = new Path("/file1");
    FSDataOutputStream fout = myGhfs.create(filePath);
    fout.writeBytes("Test Content");
    fout.close();
    assertThat(myGhfs.delete(filePath)).isTrue();
    assertThat(StorageStats.isTracked("op_delete")).isTrue();
    assertThat(StorageStats.getLong("op_delete")).isEqualTo(1);

    TestUtils.verifyCounter((GhfsGlobalStorageStatistics) stats, INVOCATION_DELETE, 1);
  }

  @Test
  public void mkdirs_storage_statistics() throws IOException {
    StorageStatistics stats = TestUtils.getStorageStatistics();
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    GhfsStorageStatistics StorageStats = new GhfsStorageStatistics(myGhfs.getIOStatistics());
    assertThat(StorageStats.isTracked("op_mkdirs")).isTrue();
    assertThat(StorageStats.getLong("op_mkdirs")).isEqualTo(1);

    assertThat(stats.isTracked("op_mkdirs")).isTrue();
    TestUtils.verifyCounter((GhfsGlobalStorageStatistics) stats, INVOCATION_MKDIRS, 1);
  }

  @Test
  public void GlobStatus_storage_statistics() throws IOException {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    StorageStatistics stats = TestUtils.getStorageStatistics();
    GhfsStorageStatistics StorageStats = new GhfsStorageStatistics(myGhfs.getIOStatistics());

    Path testRoot = new Path("/directory1/");
    myGhfs.mkdirs(testRoot);
    myGhfs.mkdirs(new Path("/directory1/subdirectory1"));
    myGhfs.create(new Path("/directory1/subdirectory1/file1")).writeBytes("data");
    myGhfs.globStatus(new Path("/d*"));
    TestUtils.verifyCounter((GhfsGlobalStorageStatistics) stats, INVOCATION_GLOB_STATUS, 1);

    assertThat(StorageStats.isTracked("op_glob_status")).isTrue();
    assertThat(StorageStats.getLong("op_glob_status")).isEqualTo(1);

    assertThat(myGhfs.delete(testRoot, /* recursive= */ true)).isTrue();
  }

  @Test
  public void getFileStatus_storage_statistics() throws IOException {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    StorageStatistics stats = TestUtils.getStorageStatistics();
    GhfsStorageStatistics StorageStats = new GhfsStorageStatistics(myGhfs.getIOStatistics());

    Path testRoot = new Path("/directory1/");
    myGhfs.mkdirs(testRoot);
    FSDataOutputStream fout = myGhfs.create(new Path("/directory1/file1"));
    fout.writeBytes("data");
    fout.close();
    myGhfs.getFileStatus(new Path("/directory1/file1"));
    TestUtils.verifyCounter((GhfsGlobalStorageStatistics) stats, INVOCATION_GET_FILE_STATUS, 1);

    assertThat(StorageStats.isTracked("op_get_file_status")).isTrue();
    assertThat(StorageStats.getLong("op_get_file_status")).isEqualTo(1);

    assertThat(myGhfs.delete(testRoot, /* recursive= */ true)).isTrue();
  }

  @Test
  public void createNonRecursive_storage_statistics() throws IOException {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    StorageStatistics stats = TestUtils.getStorageStatistics();
    GhfsStorageStatistics StorageStats = new GhfsStorageStatistics(myGhfs.getIOStatistics());

    Path testRoot = new Path("/directory1/");
    myGhfs.mkdirs(testRoot);
    Path filePath = new Path("/directory1/file1");
    try (FSDataOutputStream createNonRecursiveOutputStream =
        myGhfs.createNonRecursive(filePath, true, 1, (short) 1, 1, () -> {})) {
      createNonRecursiveOutputStream.write(1);
    }
    TestUtils.verifyCounter(
        (GhfsGlobalStorageStatistics) stats, INVOCATION_CREATE_NON_RECURSIVE, 1);

    assertThat(StorageStats.isTracked("op_create_non_recursive")).isTrue();
    assertThat(StorageStats.getLong("op_create_non_recursive")).isEqualTo(1);

    assertThat(myGhfs.delete(testRoot, /* recursive= */ true)).isTrue();
  }

  @Test
  public void getFileChecksum_storage_statistics() throws IOException {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    StorageStatistics stats = TestUtils.getStorageStatistics();
    GhfsStorageStatistics StorageStats = new GhfsStorageStatistics(myGhfs.getIOStatistics());

    Path testRoot = new Path("/directory1/");
    myGhfs.mkdirs(testRoot);
    Path filePath = new Path("/directory1/file1");
    FSDataOutputStream fout = myGhfs.create(new Path("/directory1/file1"));
    fout.writeBytes("data");
    fout.close();
    myGhfs.getFileChecksum(filePath);
    TestUtils.verifyCounter((GhfsGlobalStorageStatistics) stats, INVOCATION_GET_FILE_CHECKSUM, 1);

    assertThat(StorageStats.isTracked("op_get_file_checksum")).isTrue();
    assertThat(StorageStats.getLong("op_get_file_checksum")).isEqualTo(1);

    assertThat(myGhfs.delete(testRoot, /* recursive= */ true)).isTrue();
  }

  @Test
  public void rename_storage_statistics() throws IOException {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    StorageStatistics stats = TestUtils.getStorageStatistics();
    GhfsStorageStatistics StorageStats = new GhfsStorageStatistics(myGhfs.getIOStatistics());

    Path testRoot = new Path("/directory1/");
    myGhfs.mkdirs(testRoot);
    Path source = new Path("/directory1/file1");
    myGhfs.create(source).writeBytes("data");
    Path dest = new Path("/directory1/file2");
    myGhfs.rename(source, dest);
    TestUtils.verifyCounter((GhfsGlobalStorageStatistics) stats, INVOCATION_RENAME, 1);

    assertThat(StorageStats.isTracked("op_rename")).isTrue();
    assertThat(StorageStats.getLong("op_rename")).isEqualTo(1);

    assertThat(myGhfs.delete(testRoot, /* recursive= */ true)).isTrue();
  }

  @Test
  public void exists_storage_statistics() throws IOException {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    StorageStatistics stats = TestUtils.getStorageStatistics();
    GhfsStorageStatistics StorageStats = new GhfsStorageStatistics(myGhfs.getIOStatistics());

    Path testRoot = new Path("/directory1/");
    myGhfs.mkdirs(testRoot);
    Path filePath = new Path("/directory1/file1");
    myGhfs.create(filePath).writeBytes("data");
    myGhfs.exists(filePath);

    assertThat(StorageStats.isTracked("op_exists")).isTrue();
    assertThat(StorageStats.getLong("op_exists")).isEqualTo(1);

    TestUtils.verifyCounter((GhfsGlobalStorageStatistics) stats, INVOCATION_EXISTS, 1);
    assertThat(myGhfs.delete(testRoot, /* recursive= */ true)).isTrue();
  }

  @Test
  public void xattr_storage_statistics() throws IOException {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    Path testRoot = new Path("/directory1/");
    myGhfs.mkdirs(testRoot);
    Path filePath = new Path("/directory1/file1");
    StorageStatistics stats = TestUtils.getStorageStatistics();
    GhfsStorageStatistics StorageStats = new GhfsStorageStatistics(myGhfs.getIOStatistics());

    myGhfs.create(filePath).writeBytes("data");

    myGhfs.getXAttrs(filePath);
    assertThat(StorageStats.isTracked("op_xattr_get_map")).isTrue();
    assertThat(StorageStats.getLong("op_xattr_get_map")).isEqualTo(1);

    TestUtils.verifyCounter((GhfsGlobalStorageStatistics) stats, INVOCATION_XATTR_GET_MAP, 1);

    myGhfs.getXAttr(filePath, "test-xattr_statistics");
    assertThat(StorageStats.isTracked("op_xattr_get_named")).isTrue();
    assertThat(StorageStats.getLong("op_xattr_get_named")).isEqualTo(1);

    TestUtils.verifyCounter((GhfsGlobalStorageStatistics) stats, INVOCATION_XATTR_GET_NAMED, 1);

    myGhfs.getXAttrs(
        filePath,
        ImmutableList.of("test-xattr-statistics", "test-xattr-statistics1", "test-xattr"));
    assertThat(StorageStats.isTracked("op_xattr_get_named_map")).isTrue();
    assertThat(StorageStats.getLong("op_xattr_get_named_map")).isEqualTo(1);

    TestUtils.verifyCounter((GhfsGlobalStorageStatistics) stats, INVOCATION_XATTR_GET_NAMED_MAP, 1);

    myGhfs.listXAttrs(filePath);

    assertThat(StorageStats.isTracked("op_xattr_list")).isTrue();
    assertThat(StorageStats.getLong("op_xattr_list")).isEqualTo(1);

    TestUtils.verifyCounter((GhfsGlobalStorageStatistics) stats, INVOCATION_OP_XATTR_LIST, 1);

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

  private Configuration getConfigurationWithImplementation() {
    Configuration conf = loadConfig(storageClientType);
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
    StorageStatistics stats = TestUtils.getStorageStatistics();

    Path testRoot = new Path("/directory1/");
    myGhfs.mkdirs(testRoot);
    myGhfs.mkdirs(new Path("/directory1/subdirectory1"));
    myGhfs.create(new Path("/directory1/subdirectory1/file1")).writeBytes("data");
    myGhfs.globStatus(new Path("/d*"));

    assertThat(myGhfs.getIOStatistics().counters().get(INVOCATION_GLOB_STATUS.getSymbol()))
        .isEqualTo(1);

    TestUtils.verifyCounter((GhfsGlobalStorageStatistics) stats, INVOCATION_GLOB_STATUS, 1);
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

    FileStatus[] rootDirectories = ghfs.globStatus(new Path("/directory1*"));
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
    StorageStatistics stats = TestUtils.getStorageStatistics();

    Path testRoot = new Path("/directory1/");
    myGhfs.mkdirs(testRoot);
    FSDataOutputStream fout = myGhfs.create(new Path("/directory1/file1"));
    fout.writeBytes("data");
    fout.close();
    myGhfs.getFileStatus(new Path("/directory1/file1"));
    TestUtils.verifyCounter((GhfsGlobalStorageStatistics) stats, INVOCATION_GET_FILE_STATUS, 1);
    assertThat(myGhfs.getIOStatistics().counters().get(INVOCATION_GET_FILE_STATUS.getSymbol()))
        .isEqualTo(1);

    assertThat(myGhfs.delete(testRoot, /* recursive= */ true)).isTrue();
  }

  @Test
  public void createNonRecursive_statistics() throws IOException {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    StorageStatistics stats = TestUtils.getStorageStatistics();

    Path testRoot = new Path("/directory1/");
    myGhfs.mkdirs(testRoot);
    Path filePath = new Path("/directory1/file1");
    try (FSDataOutputStream createNonRecursiveOutputStream =
        myGhfs.createNonRecursive(filePath, true, 1, (short) 1, 1, () -> {})) {
      createNonRecursiveOutputStream.write(1);
    }

    assertThat(myGhfs.getIOStatistics().counters().get(INVOCATION_CREATE_NON_RECURSIVE.getSymbol()))
        .isEqualTo(1);

    TestUtils.verifyCounter(
        (GhfsGlobalStorageStatistics) stats, INVOCATION_CREATE_NON_RECURSIVE, 1);
    assertThat(myGhfs.delete(testRoot, /* recursive= */ true)).isTrue();

    TestUtils.verifyDurationMetric(
        myGhfs.getIOStatistics(), INVOCATION_CREATE_NON_RECURSIVE.getSymbol(), 1);

    TestUtils.verifyDurationMetric(
        (GhfsGlobalStorageStatistics) stats, INVOCATION_CREATE_NON_RECURSIVE.getSymbol(), 1);
  }

  @Test
  public void getFileChecksum_statistics() throws IOException {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    StorageStatistics stats = TestUtils.getStorageStatistics();

    Path testRoot = new Path("/directory1/");
    myGhfs.mkdirs(testRoot);
    Path filePath = new Path("/directory1/file1");
    FSDataOutputStream fout = myGhfs.create(new Path("/directory1/file1"));
    fout.writeBytes("data");
    fout.close();
    myGhfs.getFileChecksum(filePath);
    assertThat(myGhfs.getIOStatistics().counters().get(INVOCATION_GET_FILE_CHECKSUM.getSymbol()))
        .isEqualTo(1);
    TestUtils.verifyCounter((GhfsGlobalStorageStatistics) stats, INVOCATION_GET_FILE_CHECKSUM, 1);
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
    URI fileUri = getTempFilePath();
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
    conf.set("fs.gs.reported.permissions", testPermissions);
    GoogleHadoopFileSystem myGhfs = new GoogleHadoopFileSystem();
    myGhfs.initialize(ghfs.getUri(), conf);
    URI fileUri = getTempFilePath();
    Path filePath = ghfsHelper.castAsHadoopPath(fileUri);
    ghfsHelper.writeFile(filePath, "foo", 1, /* overwrite= */ true);

    FileStatus status = myGhfs.getFileStatus(filePath);
    assertThat(status.getPermission()).isEqualTo(new FsPermission(testPermissions));

    // Cleanup.
    assertThat(ghfs.delete(filePath, /* recursive= */ true)).isTrue();

    myGhfs.close();
  }

  /** Test getFileStatus() uses the user reported by UGI */
  @Test
  public void testFileStatusUser() throws Exception {
    String ugiUser = UUID.randomUUID().toString();
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(ugiUser);
    Configuration conf = getConfigurationWithImplementation();
    GoogleHadoopFileSystem myGhfs = new GoogleHadoopFileSystem();
    myGhfs.initialize(ghfs.getUri(), conf);
    URI fileUri = getTempFilePath();
    Path filePath = ghfsHelper.castAsHadoopPath(fileUri);
    ghfsHelper.writeFile(filePath, "foo", 1, /* overwrite= */ true);

    FileStatus status =
        ugi.doAs((PrivilegedExceptionAction<FileStatus>) () -> myGhfs.getFileStatus(filePath));

    assertThat(status.getOwner()).isEqualTo(ugiUser);
    assertThat(status.getGroup()).isEqualTo(ugiUser);

    // Cleanup.
    assertThat(ghfs.delete(filePath, true)).isTrue();

    myGhfs.close();
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
                    toIntExact(GCS_OUTPUT_STREAM_BUFFER_SIZE.getDefault()),
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
    StorageStatistics stats = TestUtils.getStorageStatistics();

    Path testRoot = new Path("/directory1/");
    myGhfs.mkdirs(testRoot);
    Path source = new Path("/directory1/file1");
    myGhfs.create(source).writeBytes("data");
    Path dest = new Path("/directory1/file2");
    myGhfs.rename(source, dest);
    assertThat(myGhfs.delete(testRoot, /* recursive= */ true)).isTrue();

    TestUtils.verifyDurationMetric(myGhfs.getIOStatistics(), INVOCATION_RENAME.getSymbol(), 1);

    TestUtils.verifyDurationMetric(
        (GhfsGlobalStorageStatistics) stats, INVOCATION_RENAME.getSymbol(), 1);
  }

  @Test
  public void exists_statistics() throws IOException {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    StorageStatistics stats = TestUtils.getStorageStatistics();

    Path testRoot = new Path("/directory1/");
    myGhfs.mkdirs(testRoot);
    Path filePath = new Path("/directory1/file1");
    myGhfs.create(filePath).writeBytes("data");
    myGhfs.exists(filePath);
    TestUtils.verifyDurationMetric(
        (GhfsGlobalStorageStatistics) stats, INVOCATION_EXISTS.getSymbol(), 1);

    assertThat(myGhfs.getIOStatistics().counters().get(INVOCATION_EXISTS.getSymbol())).isEqualTo(1);

    assertThat(myGhfs.delete(testRoot, /* recursive= */ true)).isTrue();
  }

  @Test
  public void xattr_statistics() throws IOException {
    GoogleHadoopFileSystem myGhfs = createInMemoryGoogleHadoopFileSystem();
    StorageStatistics stats = TestUtils.getStorageStatistics();

    Path testRoot = new Path("/directory1/");
    myGhfs.mkdirs(testRoot);
    Path filePath = new Path("/directory1/file1");
    myGhfs.create(filePath).writeBytes("data");

    myGhfs.getXAttrs(filePath);

    IOStatistics ioStats = myGhfs.getIOStatistics();
    TestUtils.verifyDurationMetric(ioStats, INVOCATION_XATTR_GET_MAP.getSymbol(), 1);

    TestUtils.verifyDurationMetric(
        (GhfsGlobalStorageStatistics) stats, INVOCATION_XATTR_GET_MAP.getSymbol(), 1);

    myGhfs.getXAttr(filePath, "test-xattr_statistics");

    TestUtils.verifyDurationMetric(ioStats, INVOCATION_XATTR_GET_NAMED.getSymbol(), 1);

    TestUtils.verifyDurationMetric(
        (GhfsGlobalStorageStatistics) stats, INVOCATION_XATTR_GET_NAMED.getSymbol(), 1);

    myGhfs.getXAttrs(
        filePath,
        ImmutableList.of("test-xattr-statistics", "test-xattr-statistics1", "test-xattr"));

    TestUtils.verifyDurationMetric(ioStats, INVOCATION_XATTR_GET_NAMED_MAP.getSymbol(), 1);
    TestUtils.verifyDurationMetric(ioStats, INVOCATION_XATTR_GET_MAP.getSymbol(), 2);

    TestUtils.verifyDurationMetric(
        (GhfsGlobalStorageStatistics) stats, INVOCATION_XATTR_GET_NAMED_MAP.getSymbol(), 1);
    TestUtils.verifyDurationMetric(
        (GhfsGlobalStorageStatistics) stats, INVOCATION_XATTR_GET_MAP.getSymbol(), 2);

    myGhfs.listXAttrs(filePath);
    TestUtils.verifyDurationMetric(
        (GhfsGlobalStorageStatistics) stats, INVOCATION_OP_XATTR_LIST.getSymbol(), 1);
    TestUtils.verifyDurationMetric(ioStats, INVOCATION_OP_XATTR_LIST.getSymbol(), 1);

    assertThat(myGhfs.delete(testRoot, true)).isTrue();

    TestUtils.verifyDurationMetric(ioStats, INVOCATION_XATTR_GET_NAMED.getSymbol(), 1);
    TestUtils.verifyDurationMetric(ioStats, INVOCATION_XATTR_GET_MAP.getSymbol(), 2);
    TestUtils.verifyDurationMetric(ioStats, INVOCATION_XATTR_GET_NAMED_MAP.getSymbol(), 1);

    TestUtils.verifyDurationMetric(
        (GhfsGlobalStorageStatistics) stats, INVOCATION_XATTR_GET_NAMED.getSymbol(), 1);
    TestUtils.verifyDurationMetric(
        (GhfsGlobalStorageStatistics) stats, INVOCATION_XATTR_GET_MAP.getSymbol(), 2);
    TestUtils.verifyDurationMetric(
        (GhfsGlobalStorageStatistics) stats, INVOCATION_XATTR_GET_NAMED_MAP.getSymbol(), 1);
  }

  @Test
  @Ignore("Test is failing")
  public void http_IOStatistics() throws IOException {
    FSDataOutputStream fout = ghfs.create(new Path("/file1"));
    StorageStatistics stats = TestUtils.getStorageStatistics();
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

    TestUtils.verifyCounter((GhfsGlobalStorageStatistics) stats, INVOCATION_CREATE, 1);

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

    TestUtils.verifyCounter((GhfsGlobalStorageStatistics) stats, ACTION_HTTP_GET_REQUEST, 2);

    TestUtils.verifyCounter((GhfsGlobalStorageStatistics) stats, ACTION_HTTP_PUT_REQUEST, 1);

    TestUtils.verifyCounter((GhfsGlobalStorageStatistics) stats, ACTION_HTTP_PATCH_REQUEST, 1);

    assertThat(ghfs.delete(new Path("/file1"))).isTrue();
    // Delete operation triggers the DELETE type request
    assertThat(
            ((GoogleHadoopFileSystem) ghfs)
                .getIOStatistics()
                .counters()
                .get(GhfsStatistic.ACTION_HTTP_DELETE_REQUEST.getSymbol()))
        .isEqualTo(1);

    TestUtils.verifyCounter((GhfsGlobalStorageStatistics) stats, ACTION_HTTP_DELETE_REQUEST, 1);
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
    URI fileUri = getTempFilePath();
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

  private void testFileChecksum(
      GcsFileChecksumType checksumType, Function<String, byte[]> checksumFn) throws Exception {
    Configuration config = getConfigurationWithImplementation();
    config.set("fs.gs.checksum.type", checksumType.name());

    GoogleHadoopFileSystem myGhfs = new GoogleHadoopFileSystem();
    myGhfs.initialize(ghfs.getUri(), config);

    URI fileUri = getTempFilePath();
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
    myGhfs.close();
  }

  @Test
  public void testInitializeWithEmptyWorkingDirectory_shouldHaveUserSpecificWorkingDirectory()
      throws IOException {
    GoogleHadoopFileSystem myGhfs = (GoogleHadoopFileSystem) ghfs;
    Configuration config = myGhfs.getConf();
    config.unset("fs.gs.working.dir");
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
    configuration.setEnum("fs.gs.glob.algorithm", globAlgorithm);
    ghfs.initialize(ghfs.getUri(), configuration);

    Path testRoot = new Path("/directory1/");
    ghfs.mkdirs(testRoot);
    ghfs.mkdirs(new Path("/directory1/subdirectory1"));
    createFile(new Path("/directory1/subdirectory1/file1"), "data".getBytes(UTF_8));

    FileStatus[] rootDirStatuses = ghfs.globStatus(new Path("/directory1*"));
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
    Configuration config = loadConfig(storageClientType);
    config.setEnum("fs.gs.auth.type", AuthenticationType.ACCESS_TOKEN_PROVIDER);
    config.setClass(
        "fs.gs.auth.access.token.provider",
        TestingAccessTokenProvider.class,
        AccessTokenProvider.class);
    URI gsUri = new URI("gs://foobar/");

    GoogleHadoopFileSystem ghfs = new GoogleHadoopFileSystem();
    ghfs.initialize(gsUri, config);

    IOException thrown = assertThrows(IOException.class, () -> ghfs.exists(new Path("gs://")));

    if (storageClientType == ClientType.STORAGE_CLIENT) {
      assertThat(thrown)
          .hasCauseThat()
          .hasMessageThat()
          .contains("invalid authentication credentials");
    } else {
      assertThat(thrown).hasCauseThat().hasMessageThat().contains("Invalid Credentials");
    }

    ghfs.close();
  }

  @Test
  public void testImpersonationServiceAccountUsed() throws Exception {
    Configuration config = loadConfig(storageClientType);
    config.setEnum("fs.gs.auth.type", AuthenticationType.ACCESS_TOKEN_PROVIDER);
    config.setClass(
        "fs.gs.auth.access.token.provider",
        TestingAccessTokenProvider.class,
        AccessTokenProvider.class);
    config.set("fs.gs.auth.impersonation.service.account", "test-service-account");

    Path gcsPath = new Path("gs://foobar/");
    GoogleHadoopFileSystem ghfs = new GoogleHadoopFileSystem();
    ghfs.initialize(gcsPath.toUri(), config);

    IOException thrown = assertThrows(IOException.class, () -> ghfs.listStatus(gcsPath));
    HttpResponseException httpException = ApiErrorExtractor.getHttpResponseException(thrown);
    assertThat(httpException).hasMessageThat().startsWith("401 Unauthorized");

    ghfs.close();
  }

  @Test
  public void testImpersonationUserNameIdentifierUsed() throws Exception {
    Configuration config = loadConfig(storageClientType);
    config.setEnum("fs.gs.auth.type", AuthenticationType.ACCESS_TOKEN_PROVIDER);
    config.setClass(
        "fs.gs.auth.access.token.provider",
        TestingAccessTokenProvider.class,
        AccessTokenProvider.class);
    config.set(
        "fs.gs.auth.impersonation.service.account.for.user."
            + UserGroupInformation.getCurrentUser().getShortUserName(),
        "test-service-account");

    Path gcsPath = new Path("gs://foobar/");
    GoogleHadoopFileSystem ghfs = new GoogleHadoopFileSystem();
    ghfs.initialize(gcsPath.toUri(), config);

    IOException thrown = assertThrows(IOException.class, () -> ghfs.listStatus(gcsPath));
    HttpResponseException httpException = ApiErrorExtractor.getHttpResponseException(thrown);
    assertThat(httpException).hasMessageThat().startsWith("401 Unauthorized");

    ghfs.close();
  }

  @Test
  public void testImpersonationGroupNameIdentifierUsed() throws Exception {
    Configuration config = loadConfig(storageClientType);
    config.setEnum("fs.gs.auth.type", AuthenticationType.ACCESS_TOKEN_PROVIDER);
    config.setClass(
        "fs.gs.auth.access.token.provider",
        TestingAccessTokenProvider.class,
        AccessTokenProvider.class);
    config.set(
        "fs.gs.auth.impersonation.service.account.for.group."
            + UserGroupInformation.getCurrentUser().getGroupNames()[0],
        "test-service-account");

    Path gcsPath = new Path("gs://foobar/");
    GoogleHadoopFileSystem ghfs = new GoogleHadoopFileSystem();
    ghfs.initialize(gcsPath.toUri(), config);

    IOException thrown = assertThrows(IOException.class, () -> ghfs.listStatus(gcsPath));
    HttpResponseException httpException = ApiErrorExtractor.getHttpResponseException(thrown);
    assertThat(httpException).hasMessageThat().startsWith("401 Unauthorized");

    ghfs.close();
  }

  @Test
  public void testImpersonationUserAndGroupNameIdentifiersUsed() throws Exception {
    Configuration config = loadConfig(storageClientType);
    config.setEnum("fs.gs.auth.type", AuthenticationType.ACCESS_TOKEN_PROVIDER);
    config.setClass(
        "fs.gs.auth.access.token.provider",
        TestingAccessTokenProvider.class,
        AccessTokenProvider.class);
    config.set(
        "fs.gs.auth.impersonation.service.account.for.user."
            + UserGroupInformation.getCurrentUser().getShortUserName(),
        "test-service-account1");
    config.set(
        "fs.gs.auth.impersonation.service.account.for.group."
            + UserGroupInformation.getCurrentUser().getGroupNames()[0],
        "test-service-account2");

    Path gcsPath = new Path("gs://foobar/");
    GoogleHadoopFileSystem ghfs = new GoogleHadoopFileSystem();
    ghfs.initialize(gcsPath.toUri(), config);

    IOException thrown = assertThrows(IOException.class, () -> ghfs.listStatus(gcsPath));
    HttpResponseException httpException = ApiErrorExtractor.getHttpResponseException(thrown);
    assertThat(httpException).hasMessageThat().startsWith("401 Unauthorized");

    ghfs.close();
  }

  @Test
  public void testImpersonationServiceAccountAndUserAndGroupNameIdentifierUsed() throws Exception {
    Configuration config = loadConfig(storageClientType);
    config.setEnum("fs.gs.auth.type", AuthenticationType.ACCESS_TOKEN_PROVIDER);
    config.setClass(
        "fs.gs.auth.access.token.provider",
        TestingAccessTokenProvider.class,
        AccessTokenProvider.class);
    config.set("fs.gs.auth.impersonation.service.account", "test-service-account1");
    config.set(
        "fs.gs.auth.impersonation.service.account.for.user."
            + UserGroupInformation.getCurrentUser().getShortUserName(),
        "test-service-account2");
    config.set(
        "fs.gs.auth.impersonation.service.account.for.group."
            + UserGroupInformation.getCurrentUser().getGroupNames()[0],
        "test-service-account3");

    Path gcsPath = new Path("gs://foobar/");
    GoogleHadoopFileSystem ghfs = new GoogleHadoopFileSystem();
    ghfs.initialize(gcsPath.toUri(), config);

    IOException thrown = assertThrows(IOException.class, () -> ghfs.listStatus(gcsPath));
    HttpResponseException httpException = ApiErrorExtractor.getHttpResponseException(thrown);
    assertThat(httpException).hasMessageThat().startsWith("401 Unauthorized");

    ghfs.close();
  }

  @Test
  public void testImpersonationInvalidUserNameIdentifierUsed() throws Exception {
    Configuration config = loadConfig(storageClientType);
    config.setEnum("fs.gs.auth.type", AuthenticationType.ACCESS_TOKEN_PROVIDER);
    config.setClass(
        "fs.gs.auth.access.token.provider",
        TestingAccessTokenProvider.class,
        AccessTokenProvider.class);
    config.set(
        "fs.gs.auth.impersonation.service.account.for.user.invalid-user", "test-service-account");

    URI gsUri = new URI("gs://foobar/");
    GoogleHadoopFileSystem ghfs = new GoogleHadoopFileSystem();
    ghfs.initialize(gsUri, config);

    ghfs.close();
  }

  @Test
  public void unauthenticatedAccessToPublicBuckets_fsGsProperties() throws Exception {
    Configuration config = loadConfig(storageClientType);
    StorageStatistics stats = TestUtils.getStorageStatistics();

    config.setEnum("fs.gs.auth.type", AuthenticationType.UNAUTHENTICATED);

    FileSystem fs = FileSystem.get(new URI(PUBLIC_BUCKET), config);

    FileStatus[] fileStatuses = fs.listStatus(new Path(PUBLIC_BUCKET));

    TestUtils.verifyCounter((GhfsGlobalStorageStatistics) stats, INVOCATION_LIST_STATUS, 1);

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
    Configuration config = loadConfig(storageClientType);
    config.setEnum("google.cloud.auth.type", AuthenticationType.UNAUTHENTICATED);

    FileSystem fs = FileSystem.get(new URI(PUBLIC_BUCKET), config);

    FileStatus[] fileStatuses = fs.listStatus(new Path(PUBLIC_BUCKET));

    assertThat(fileStatuses).isNotEmpty();
  }

  @Test
  public void testInitializeCompatibleWithHadoopCredentialProvider() throws Exception {
    Configuration config = loadConfig(storageClientType);

    // This does not need to refer to a real bucket/path for the test.
    config.set(HADOOP_SECURITY_CREDENTIAL_PROVIDER_PATH, "jceks://gs@foobar/test.jceks");

    FileSystem.get(new URI(PUBLIC_BUCKET), config);
    // Initialization successful with no exception thrown.
  }

  @Test
  public void testThreadTraceEnabledRename() throws Exception {
    Configuration config = ghfs.getConf();
    config.set("fs.gs.application.tracelog.enable", "true");
    ghfs.initialize(ghfs.getUri(), config);

    Path testRoot = new Path(sharedBucketName1, "/directory1/");
    ghfs.mkdirs(testRoot);
    assertThat(ghfs.exists(testRoot)).isTrue();

    Path source = new Path(sharedBucketName1, "/directory1/file1");
    ghfs.mkdirs(source);
    assertThat(ghfs.exists(source)).isTrue();

    Path dest = new Path(sharedBucketName1, "/directory2/");
    assertThat(ghfs.exists(dest)).isFalse();
    ghfs.rename(testRoot, dest);
    assertThat(ghfs.exists(dest)).isTrue();
  }

  @Test
  public void testHnBucketNonRecursiveDeleteOperation() throws Exception {
    String bucketName = this.gcsiHelper.getUniqueBucketName("hn");
    GoogleHadoopFileSystem googleHadoopFileSystem = createHnEnabledBucket(bucketName);
    String bucketPath = "gs://" + bucketName;
    try {
      googleHadoopFileSystem.mkdirs(new Path("/A/"));
      assertThrows(
          "Cannot delete a non-empty directory",
          java.nio.file.DirectoryNotEmptyException.class,
          () -> googleHadoopFileSystem.delete(new Path(bucketPath), false));

      // verify only "A/" folder exists
      assertThat(getSubFolderCount(googleHadoopFileSystem, bucketPath + "/A/")).isEqualTo(1);

      // delete A/ non recursively
      googleHadoopFileSystem.delete(new Path(bucketPath + "/A"), false);

      // check that on listing we get no folders for folder "A/"
      assertThat(getSubFolderCount(googleHadoopFileSystem, bucketPath + "/A/")).isEqualTo(0);
    } finally {
      googleHadoopFileSystem.delete(new Path(bucketPath));
      googleHadoopFileSystem.close();
    }
  }

  @Test
  public void testHnBucketRecursiveDeleteOperationOnBucket() throws Exception {
    String bucketName = this.gcsiHelper.getUniqueBucketName("hn");
    String bucketPath = "gs://" + bucketName;
    GoogleHadoopFileSystem googleHadoopFileSystem = createHnEnabledBucket(bucketName);
    createResources(googleHadoopFileSystem);
    assertThat(getSubFolderCount(googleHadoopFileSystem, "gs://" + bucketName + "/")).isEqualTo(22);
    assertThrows(
        "Cannot delete a non-empty directory",
        java.nio.file.DirectoryNotEmptyException.class,
        () -> googleHadoopFileSystem.delete(new Path(bucketPath), false));

    // delete bucket
    googleHadoopFileSystem.delete(new Path(bucketPath), true);
    assertThat(
            googleHadoopFileSystem
                .getGcsFs()
                .getGcs()
                .getItemInfo(new StorageResourceId(bucketName))
                .exists())
        .isFalse();

    // Bucket delete is an eventually consistent operation can take upto 30 seconds
    // More details on consistency https://cloud.google.com/storage/docs/consistency.
    long deadline = System.currentTimeMillis() + BUCKET_DELETION_TIMEOUT_MS;
    boolean notFound = false;
    while (System.currentTimeMillis() < deadline) {
      try {
        getSubFolderCount(googleHadoopFileSystem, bucketPath);
        // If the call succeeds, the bucket is not yet fully deleted from this view.
        // Wait a bit before retrying.
        Thread.sleep(RETRY_INTERVAL_MS);
      } catch (com.google.api.gax.rpc.NotFoundException e) {
        notFound = true;
        break;
      }
    }

    if (!notFound) {
      fail("getSubFolderCount should have thrown NotFoundException after bucket deletion.");
    }

    googleHadoopFileSystem.close();
  }

  @Test
  public void testHnBucketRecursiveDeleteOperationOnDirectory() throws Exception {
    String bucketName = this.gcsiHelper.getUniqueBucketName("hn");
    String bucketPath = "gs://" + bucketName;
    GoogleHadoopFileSystem googleHadoopFileSystem = createHnEnabledBucket(bucketName);
    try {
      createResources(googleHadoopFileSystem);
      assertThat(getSubFolderCount(googleHadoopFileSystem, bucketPath + "/A/")).isEqualTo(21);
      assertThrows(
          "Cannot delete a non-empty directory",
          java.nio.file.DirectoryNotEmptyException.class,
          () -> googleHadoopFileSystem.delete(new Path(bucketPath + "/A"), false));

      // rename A/ to B/
      googleHadoopFileSystem.rename(new Path(bucketPath + "/A/"), new Path(bucketPath + "/B/"));
      assertThat(getSubFolderCount(googleHadoopFileSystem, bucketPath + "/B/")).isEqualTo(21);

      // delete B/
      googleHadoopFileSystem.delete(new Path("/B"), true);
      assertThat(getSubFolderCount(googleHadoopFileSystem, bucketPath + "/B/")).isEqualTo(0);

      // rename C/ to B/
      googleHadoopFileSystem.rename(new Path(bucketPath + "/C/"), new Path(bucketPath + "/B/"));
      assertThat(getSubFolderCount(googleHadoopFileSystem, bucketPath + "/B/")).isEqualTo(1);
    } finally {
      googleHadoopFileSystem.delete(new Path(bucketPath));
      googleHadoopFileSystem.close();
    }
  }

  @Test
  public void testHnBucketDeleteOperationOnNonExistingFolder() throws Exception {
    String bucketName = this.gcsiHelper.getUniqueBucketName("hn");
    String bucketPath = "gs://" + bucketName;
    GoogleHadoopFileSystem googleHadoopFileSystem = createHnEnabledBucket(bucketName);

    try {
      googleHadoopFileSystem.mkdirs(new Path("/A/"));
      googleHadoopFileSystem.mkdirs(new Path("/A/C/"));
      assertThat(getSubFolderCount(googleHadoopFileSystem, bucketPath + "/A/")).isEqualTo(2);

      assertThrows(
          "Cannot delete a non-empty directory",
          java.nio.file.DirectoryNotEmptyException.class,
          () -> googleHadoopFileSystem.delete(new Path(bucketPath + "/A"), false));
      assertThat(getSubFolderCount(googleHadoopFileSystem, bucketPath + "/A/")).isEqualTo(2);

      // try to delete a non existing folder
      List<FolderInfo> folderInfoList = new ArrayList<>();
      folderInfoList.add(new FolderInfo(FolderInfo.createFolderInfoObject(bucketName, "A/")));
      folderInfoList.add(new FolderInfo(FolderInfo.createFolderInfoObject(bucketName, "A/B/")));
      assertThrows(
          "The folder you tried to delete is not empty.",
          java.io.IOException.class,
          () -> googleHadoopFileSystem.getGcsFs().getGcs().deleteFolders(folderInfoList));
      assertThat(getSubFolderCount(googleHadoopFileSystem, bucketPath + "/A/")).isEqualTo(2);

      // delete A/
      googleHadoopFileSystem.delete(new Path("/A"), true);
      assertThat(getSubFolderCount(googleHadoopFileSystem, bucketPath + "/A/")).isEqualTo(0);
    } finally {
      googleHadoopFileSystem.delete(new Path(bucketPath));
      googleHadoopFileSystem.close();
    }
  }

  @Test
  public void testGcsJsonAPIMetrics() throws IOException {
    Configuration config = loadConfig(storageClientType);
    config.setBoolean(
        "fs.gs.status.parallel.enable", false); // to make the test results predictable
    config.setBoolean("fs.gs.implicit.dir.repair.enable", false);

    Path parentPath = ghfsHelper.castAsHadoopPath(getTempFilePath());
    Path subdirPath = new Path(parentPath, "foo-subdir");
    GoogleHadoopFileSystem myghfs = new GoogleHadoopFileSystem();
    myghfs.initialize(subdirPath.toUri(), config);

    GhfsGlobalStorageStatistics stats = myghfs.getGlobalGcsStorageStatistics();
    stats.reset();

    Stopwatch stopwatch = Stopwatch.createStarted();
    myghfs.mkdirs(subdirPath);
    stopwatch.stop();

    Map<String, Long> expected =
        ImmutableMap.<String, Long>builder()
            // create object using directUpload
            .put(GhfsStatistic.ACTION_HTTP_POST_REQUEST.getSymbol(), 1L)
            .put(GhfsStatistic.DIRECTORIES_CREATED.getSymbol(), 1L)
            // Check for each parent dirs fails due to NOT FOUND - expected
            .put(
                GoogleCloudStorageStatistics.GCS_API_CLIENT_NOT_FOUND_RESPONSE_COUNT.getSymbol(),
                3L)
            // Check for each parent dirs fails due to NOT FOUND - expected
            .put(GoogleCloudStorageStatistics.GCS_API_CLIENT_SIDE_ERROR_COUNT.getSymbol(), 3L)
            // 3 metadata + 1 POST
            .put(GoogleCloudStorageStatistics.GCS_API_REQUEST_COUNT.getSymbol(), 4L)
            // Check for each parent dirs
            .put(GoogleCloudStorageStatistics.GCS_METADATA_REQUEST.getSymbol(), 3L)
            .put(GhfsStatistic.INVOCATION_MKDIRS.getSymbol(), 1L)
            .build();

    verifyMetrics(stats, expected, stopwatch.elapsed().toMillis());

    stats.reset();
    stopwatch = Stopwatch.createStarted();
    Path fileToCreate = new Path(subdirPath, "foo.txt");
    int expectedLength = ThreadLocalRandom.current().nextInt(1, 10 * 1024 * 1024);
    try (FSDataOutputStream outStream = myghfs.create(fileToCreate)) {
      byte[] toWrite = new byte[expectedLength];
      ThreadLocalRandom.current().nextBytes(toWrite);
      outStream.write(toWrite);
    }

    expected =
        ImmutableMap.<String, Long>builder()
            // create resumable upload
            .put(GhfsStatistic.ACTION_HTTP_POST_REQUEST.getSymbol(), 1L) // create resumable upload
            .put(GhfsStatistic.ACTION_HTTP_PUT_REQUEST.getSymbol(), 1L) // complete resumable upload
            .put(GhfsStatistic.FILES_CREATED.getSymbol(), 1L)
            // Check for each parent dirs fails due to NOT FOUND - expected
            .put(
                GoogleCloudStorageStatistics.GCS_API_CLIENT_NOT_FOUND_RESPONSE_COUNT.getSymbol(),
                4L)
            // Check for each parent dirs fails due to NOT FOUND - expected
            .put(GoogleCloudStorageStatistics.GCS_API_CLIENT_SIDE_ERROR_COUNT.getSymbol(), 4L)
            .put(
                GoogleCloudStorageStatistics.GCS_API_REQUEST_COUNT.getSymbol(),
                7L) // 4 metadata + 1 POST + 1 PUT + 1 LIST_FILE
            // check if director with filename exists
            .put(GoogleCloudStorageStatistics.GCS_LIST_FILE_REQUEST.getSymbol(), 1L)
            .put(
                GoogleCloudStorageStatistics.GCS_METADATA_REQUEST.getSymbol(),
                4L) // Check for each parent dirs
            .put(GhfsStatistic.INVOCATION_CREATE.getSymbol(), 1L)
            .put(GhfsStatistic.STREAM_WRITE_BYTES.getSymbol(), (long) expectedLength)
            .put(GhfsStatistic.STREAM_WRITE_OPERATIONS.getSymbol(), 1L)
            .put(GhfsStatistic.STREAM_WRITE_CLOSE_OPERATIONS.getSymbol(), 1L)
            .build();

    verifyMetrics(stats, expected, stopwatch.elapsed().toMillis());

    stats.reset();
    stopwatch = Stopwatch.createStarted();

    FileStatus fileStatus = myghfs.getFileStatus(fileToCreate);

    assertThat(fileStatus.getLen()).isEqualTo(expectedLength);

    expected =
        ImmutableMap.<String, Long>builder()
            .put(GoogleCloudStorageStatistics.GCS_API_REQUEST_COUNT.getSymbol(), 1L) // 1 metadata
            .put(GhfsStatistic.INVOCATION_GET_FILE_STATUS.getSymbol(), 1L) //
            .put(
                GoogleCloudStorageStatistics.GCS_METADATA_REQUEST.getSymbol(),
                1L) // GET for file metadata
            .build();

    verifyMetrics(stats, expected, stopwatch.elapsed().toMillis());

    stats.reset();
    stopwatch = Stopwatch.createStarted();
    try (FSDataInputStream inStream = myghfs.open(fileToCreate)) {
      byte[] inBuffer = new byte[expectedLength];
      int bytesRead = inStream.read(inBuffer);

      assertThat(bytesRead).isEqualTo(expectedLength);
    }

    expected =
        ImmutableMap.<String, Long>builder()
            .put(
                GoogleCloudStorageStatistics.GCS_API_REQUEST_COUNT.getSymbol(),
                2L) // 1 metadata; 1 media
            .put(GoogleCloudStorageStatistics.GCS_GET_MEDIA_REQUEST.getSymbol(), 1L)
            .put(GoogleCloudStorageStatistics.GCS_METADATA_REQUEST.getSymbol(), 1L)
            .put(GhfsStatistic.STREAM_READ_BYTES.getSymbol(), (long) expectedLength)
            .put(GhfsStatistic.STREAM_READ_OPERATIONS.getSymbol(), 1L)
            .put(GhfsStatistic.STREAM_READ_CLOSE_OPERATIONS.getSymbol(), 1L)
            .put(GhfsStatistic.INVOCATION_OPEN.getSymbol(), 1L)
            .build();

    verifyMetrics(stats, expected, stopwatch.elapsed().toMillis());

    Path renamedirPath = new Path(parentPath, "foo-subdir-rename");

    stats.reset();
    stopwatch = Stopwatch.createStarted();

    myghfs.rename(subdirPath, renamedirPath);

    expected =
        ImmutableMap.<String, Long>builder()
            .put(GhfsStatistic.ACTION_HTTP_DELETE_REQUEST.getSymbol(), 1L) // 1 for directory.
            .put(GhfsStatistic.ACTION_HTTP_POST_REQUEST.getSymbol(), 1L) // move file;
            .put(
                GoogleCloudStorageStatistics.GCS_API_CLIENT_NOT_FOUND_RESPONSE_COUNT.getSymbol(),
                2L) // Check for each parent dirs fails due to NOT FOUND - expected
            .put(
                GoogleCloudStorageStatistics.GCS_API_CLIENT_SIDE_ERROR_COUNT.getSymbol(),
                2L) // Check for each parent dirs fails due to NOT FOUND - expected
            .put(
                GoogleCloudStorageStatistics.GCS_API_REQUEST_COUNT.getSymbol(),
                8L) // 1 delete + 3 metadata + 2 POST + 1 listDir + 3 listFile
            .put(
                GoogleCloudStorageStatistics.GCS_LIST_DIR_REQUEST.getSymbol(),
                1L) // list src files to copy/delete
            // One for src and dst. One due Auto repair checking grandparent.
            .put(GoogleCloudStorageStatistics.GCS_LIST_FILE_REQUEST.getSymbol(), 3L)
            .put(GoogleCloudStorageStatistics.GCS_METADATA_REQUEST.getSymbol(), 2L) // 1 dst; 1 src;
            .put(GhfsStatistic.INVOCATION_RENAME.getSymbol(), 1L)
            .build();

    verifyMetrics(stats, expected, stopwatch.elapsed().toMillis());

    stats.reset();
    stopwatch = Stopwatch.createStarted();
    myghfs.delete(renamedirPath, true);

    expected =
        ImmutableMap.<String, Long>builder()
            .put(GhfsStatistic.ACTION_HTTP_DELETE_REQUEST.getSymbol(), 1L) // delete src file
            .put(GhfsStatistic.FILES_DELETED.getSymbol(), 1L)
            .put(
                GoogleCloudStorageStatistics.GCS_API_CLIENT_NOT_FOUND_RESPONSE_COUNT.getSymbol(),
                1L) // Check for each parent dirs fails due to NOT FOUND - expected
            .put(
                GoogleCloudStorageStatistics.GCS_API_CLIENT_SIDE_ERROR_COUNT.getSymbol(),
                1L) // Check for each parent dirs fails due to NOT FOUND - expected
            .put(
                GoogleCloudStorageStatistics.GCS_API_REQUEST_COUNT.getSymbol(),
                4L) // 1 delete + 1 metadata + 1 listDir + 1 listFile
            .put(
                GoogleCloudStorageStatistics.GCS_LIST_DIR_REQUEST.getSymbol(),
                1L) // to find src files to delete
            .put(
                GoogleCloudStorageStatistics.GCS_LIST_FILE_REQUEST.getSymbol(),
                1L) // check if file exist for path
            .put(
                GoogleCloudStorageStatistics.GCS_METADATA_REQUEST.getSymbol(),
                1L) // check if directory exists for path
            .put(GhfsStatistic.INVOCATION_DELETE.getSymbol(), 1L)
            .build();

    verifyMetrics(stats, expected, stopwatch.elapsed().toMillis());

    myghfs.close();
  }

  @Test
  public void testGcsThreadLocalMetrics() throws IOException {
    Configuration config = loadConfig(storageClientType);
    config.setBoolean(
        "fs.gs.status.parallel.enable", false); // to make the test results predictable
    config.setBoolean("fs.gs.implicit.dir.repair.enable", false);
    config.setBoolean("fs.gs.create.items.conflict.check.enable", false);

    Path parentPath = ghfsHelper.castAsHadoopPath(getTempFilePath());
    Path subdirPath = new Path(parentPath, "foo-subdir");
    GoogleHadoopFileSystem myghfs = new GoogleHadoopFileSystem();
    myghfs.initialize(subdirPath.toUri(), config);

    GhfsThreadLocalStatistics stats =
        (GhfsThreadLocalStatistics)
            GlobalStorageStatistics.INSTANCE.get(GhfsThreadLocalStatistics.NAME);

    // TODO: Some of the GCS API related operations, which are run in a separate thread are not
    // tracked.
    // It will be fixed in a separate change
    runTest(subdirPath, myghfs, stats);

    myghfs.close();
  }

  @Test
  public void multiThreadTest() throws IOException {
    Configuration config = loadConfig(storageClientType);
    config.setBoolean(
        "fs.gs.status.parallel.enable", false); // to make the test results predictable
    config.setBoolean("fs.gs.implicit.dir.repair.enable", false);
    config.setBoolean("fs.gs.create.items.conflict.check.enable", false);

    Path parentPath = ghfsHelper.castAsHadoopPath(getTempFilePath());

    GhfsThreadLocalStatistics stats =
        (GhfsThreadLocalStatistics)
            GlobalStorageStatistics.INSTANCE.get(GhfsThreadLocalStatistics.NAME);

    IntStream.range(0, 10)
        .parallel()
        .forEach(
            i -> {
              try (GoogleHadoopFileSystem myghfs = new GoogleHadoopFileSystem()) {
                Path subdirPath = new Path(parentPath, "foo-subdir" + i);
                myghfs.initialize(subdirPath.toUri(), config);
                runTest(subdirPath, myghfs, stats);
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });
  }

  private static Map<String, Long> getThreadLocalMetrics(GhfsThreadLocalStatistics statistics) {
    Map<String, Long> values = new HashMap<>();
    statistics
        .getLongStatistics()
        .forEachRemaining(theStat -> values.put(theStat.getName(), theStat.getValue()));
    return values;
  }

  private static void runTest(
      Path subdirPath, GoogleHadoopFileSystem myghfs, GhfsThreadLocalStatistics stats)
      throws IOException {
    Map<String, Long> metrics = getThreadLocalMetrics(stats);
    myghfs.mkdirs(subdirPath);

    verify(metrics, 1L, stats);

    myghfs.getFileStatus(subdirPath);
    verify(metrics, 2L, stats);

    Path testFilePath = subdirPath.suffix("/test.empty");
    try (FSDataOutputStream outStream = myghfs.create(testFilePath)) {
      verify(metrics, 1L, stats);

      outStream.hflush();
      verify(metrics, 1L, stats);

      outStream.hsync();
      verify(metrics, 3L, stats);
    }

    metrics.merge(GCS_API_COUNT, 1L, Long::sum);
    verify(metrics, stats);

    try (FSDataInputStream ignored = myghfs.open(testFilePath)) {
      ignored.read();
      verify(metrics, 1L, stats);
    }

    Path dst = subdirPath.suffix("/test.rename.empty");
    myghfs.rename(testFilePath, dst);
    // TODO: Operations done async in a separate thread are not tracked. This will be fixed in a
    // separate change.
    verify(metrics, 1L, stats);

    myghfs.delete(dst);
    verify(metrics, 2L, stats);

    myghfs.delete(subdirPath, true);
    verify(metrics, 4L, stats);
  }

  private static void verify(
      Map<String, Long> metrics, long gcsApiCount, GhfsThreadLocalStatistics stats) {
    metrics.merge(HADOOP_API_COUNT, 1L, Long::sum);
    metrics.merge(GCS_API_COUNT, gcsApiCount, Long::sum);
    verify(metrics, stats);
  }

  private static void verify(
      Map<String, Long> expectedMetrics, GhfsThreadLocalStatistics statistics) {
    expectedMetrics.forEach((key, value) -> checkTracked(key, value, statistics));
    assertThat(getThreadLocalMetrics(statistics).size()).isEqualTo(expectedMetrics.size());
  }

  private static void checkTracked(
      String metric, long expected, GhfsThreadLocalStatistics statistics) {
    assertThat(statistics.isTracked(metric)).isTrue();

    if (!metric.toLowerCase().contains("time")) {
      // time metrics are not deterministic
      assertThat(statistics.getLong(metric)).isEqualTo(expected);
    } else if (!metric.toLowerCase().contains("time")) {
      // back off is not deterministic
      assertThat(statistics.getLong(metric)).isAtLeast(expected);
    }
  }

  private void verifyMetrics(
      GhfsGlobalStorageStatistics stats, Map<String, Long> expected, long elapsed) {
    Map<String, List<String>> expectedSum =
        ImmutableMap.<String, List<String>>builder()
            .put(
                GoogleCloudStorageStatistics.GCS_API_TIME.getSymbol(),
                List.of(
                    GhfsStatistic.ACTION_HTTP_POST_REQUEST.getSymbol(),
                    GhfsStatistic.ACTION_HTTP_DELETE_REQUEST.getSymbol(),
                    GhfsStatistic.ACTION_HTTP_PUT_REQUEST.getSymbol(),
                    GoogleCloudStorageStatistics.GCS_METADATA_REQUEST.getSymbol(),
                    GoogleCloudStorageStatistics.GCS_GET_MEDIA_REQUEST.getSymbol(),
                    GoogleCloudStorageStatistics.GCS_LIST_FILE_REQUEST.getSymbol(),
                    GoogleCloudStorageStatistics.GCS_LIST_DIR_REQUEST.getSymbol()))
            .build();

    for (Iterator<StorageStatistics.LongStatistic> it = stats.getLongStatistics(); it.hasNext(); ) {
      StorageStatistics.LongStatistic stat = it.next();

      String name = stat.getName();
      Long value = stat.getValue();

      if (expected.containsKey(name)) {
        assertWithMessage(name).that(value).isEqualTo(expected.get(name));
      } else if (expectedSum.containsKey(name)) {
        long expectedValue = 0L;
        int roundOff = 0;

        for (String subMetric : expectedSum.get(name)) {
          String meanKey = subMetric + "_mean";
          Long metricCount = stats.getLong(subMetric);
          expectedValue += stats.getLong(meanKey) * metricCount;
          roundOff += metricCount;
        }

        assertWithMessage(name).that(value).isAtLeast(expectedValue);
        assertWithMessage(name).that(value).isLessThan(expectedValue + roundOff + 1);
      } else if (!toIgnore(name)) {
        assertWithMessage(name).that(value).isEqualTo(0L);
      }

      if (name.endsWith("_min")) {
        verifyDurationMetricValues(name, stats);
      }
    }

    assertThat(stats.getLong(GhfsStatistic.GCS_CONNECTOR_TIME.getSymbol())).isLessThan(elapsed + 1);
  }

  private void verifyDurationMetricValues(String minMetricName, GhfsGlobalStorageStatistics stats) {
    if (minMetricName.equals("stream_write_operations_min")
        || minMetricName.equals("stream_read_close_operations_min")) {
      // For stream write operation, the actual writing can happen when the stream is closed and
      // hence these could be zero
      return;
    }

    String metricName = minMetricName.replace("_min", "");
    long maxValue = stats.getLong(metricName + "_max");
    long meanValue = stats.getLong(metricName + "_mean");
    long minValue = stats.getLong(minMetricName);
    long count = stats.getLong(metricName);

    if (count != 0) {
      assertWithMessage(metricName).that(maxValue).isGreaterThan(0);
      assertWithMessage(metricName).that(minValue).isGreaterThan(0);
      assertWithMessage(metricName).that(maxValue).isGreaterThan(minValue - 1);
      assertWithMessage(metricName).that(meanValue).isGreaterThan(minValue - 1);
      assertWithMessage(metricName).that(maxValue).isGreaterThan(meanValue - 1);

      if (EXPECTED_DURATION_METRICS.contains(metricName)) {
        long durationValue = stats.getLong(metricName + "_duration");
        assertWithMessage(metricName).that(durationValue).isGreaterThan(0);
        assertWithMessage(metricName).that(durationValue).isGreaterThan(meanValue * count - 1);
        if (meanValue > 0.0) {
          assertWithMessage(metricName).that(durationValue).isLessThan(meanValue * (count + 1));
        }
      }
    }
  }

  private boolean toIgnore(String name) {
    if ("gcs_connector_time".equals(name)
        || "stream_write_operations_duration".equals(name)
        || "stream_read_operations_duration".equals(name)
        || name.contains("backoff")) {
      return true;
    }

    return name.endsWith("_min")
        || name.endsWith("_mean")
        || name.endsWith("_max")
        || name.endsWith("_duration");
  }

  @Test
  public void durationMetrics() throws Exception {
    Path parentPath = ghfsHelper.castAsHadoopPath(getTempFilePath());

    GoogleHadoopFileSystem myghfs = new GoogleHadoopFileSystem();
    myghfs.initialize(parentPath.toUri(), new Configuration());

    GhfsGlobalStorageStatistics stats = myghfs.getGlobalGcsStorageStatistics();

    Set<String> metrics = getDurationConnectorMetrics(stats);

    assertEquals(EXPECTED_DURATION_METRICS, metrics);

    myghfs.close();
  }

  private Set<String> getDurationConnectorMetrics(GhfsGlobalStorageStatistics stats) {
    HashSet<String> metrics = new HashSet<>();
    for (Iterator<StorageStatistics.LongStatistic> it = stats.getLongStatistics(); it.hasNext(); ) {
      metrics.add(it.next().getName());
    }

    HashSet<String> result = new HashSet<>();
    for (String metric : metrics) {
      if (metric.endsWith("_mean")) {
        String metricName = metric.substring(0, metric.length() - "_mean".length());
        String durationMetricName = metricName + "_duration";

        if (metrics.contains(durationMetricName)) {
          result.add(metricName);
        }
      }
    }

    return result;
  }

  private void createFile(GoogleHadoopFileSystem googleHadoopFileSystem, Path path)
      throws Exception {
    try (FSDataOutputStream fout = googleHadoopFileSystem.create(path)) {
      fout.writeBytes("data");
    }
  }

  private GoogleHadoopFileSystem createHnEnabledBucket(String bucketName) throws Exception {
    GoogleHadoopFileSystem googleHadoopFileSystem = new GoogleHadoopFileSystem();
    URI initUri = new URI("gs://" + bucketName);
    Configuration config = loadConfig();
    config.setBoolean("fs.gs.hierarchical.namespace.folders.enable", true);
    googleHadoopFileSystem.initialize(initUri, config);
    GoogleCloudStorage theGcs = googleHadoopFileSystem.getGcsFs().getGcs();
    theGcs.createBucket(
        bucketName, CreateBucketOptions.builder().setHierarchicalNamespaceEnabled(true).build());
    assertThat(theGcs.isHnBucket(new Path(initUri + "/").toUri())).isTrue();
    return googleHadoopFileSystem;
  }

  /** Pathlocation should end with "/" prefix */
  private Integer getSubFolderCount(
      GoogleHadoopFileSystem googleHadoopFileSystem, String pathLocation)
      throws IOException, URISyntaxException {
    List<FolderInfo> initialListOfFolders =
        googleHadoopFileSystem
            .getGcsFs()
            .listFoldersInfoForPrefixPage(
                new URI(pathLocation), ListFolderOptions.builder().build(), null)
            .getItems();
    return initialListOfFolders.size();
  }

  private void createResources(GoogleHadoopFileSystem googleHadoopFileSystem) throws Exception {
    googleHadoopFileSystem.mkdirs(new Path("A/"));
    googleHadoopFileSystem.mkdirs(new Path("A/dir1/"));
    googleHadoopFileSystem.mkdirs(new Path("A/dir2/"));
    for (int i = 0; i < 15; i++) {
      Random r = new Random();
      googleHadoopFileSystem.mkdirs(new Path("A/dir1/" + r.nextInt() + "/"));
    }
    googleHadoopFileSystem.mkdirs(new Path("A/dir1/subdir1/"));
    googleHadoopFileSystem.mkdirs(new Path("A/dir1/subdir2/"));
    googleHadoopFileSystem.mkdirs(new Path("A/dir2/subdir3/"));
    createFile(googleHadoopFileSystem, new Path("A/1"));
    createFile(googleHadoopFileSystem, new Path("A/2"));

    googleHadoopFileSystem.mkdirs(new Path("C/"));
    createFile(googleHadoopFileSystem, new Path("C/1"));
    createFile(googleHadoopFileSystem, new Path("C/2"));
    createFile(googleHadoopFileSystem, new Path("6"));
  }

  private void checkMetric(
      String name, StorageStatistics statistics, HashSet<String> metricNames, String statsString) {
    assertThat(metricNames.contains(name)).isTrue();
    assertThat(statistics.isTracked(name)).isTrue();
    assertThat(statsString.contains(name + "=")).isTrue();
    assertEquals(0, statistics.getLong(name).longValue());
  }

  @Test
  public void register_subscriber_multiple_time() throws Exception {
    GoogleHadoopFileSystem myGhfs =
        createInMemoryGoogleHadoopFileSystem(); // registers the subscriber class first time in
    // myGhfs1
    StorageStatistics stats = TestUtils.getStorageStatistics();

    GoogleCloudStorageEventBus.register(
        GoogleCloudStorageEventSubscriber.getInstance(
            (GhfsGlobalStorageStatistics)
                stats)); // registers the same subscriber class second time

    assertThat(getMetricValue(stats, INVOCATION_CREATE)).isEqualTo(0);
    assertThat(getMetricValue(stats, FILES_CREATED)).isEqualTo(0);

    try (FSDataOutputStream fout = myGhfs.create(new Path("/file1"))) {
      fout.writeBytes("Test Content");
    }
    assertThat(getMetricValue(stats, INVOCATION_CREATE)).isEqualTo(1);
    assertThat(getMetricValue(stats, FILES_CREATED)).isEqualTo(1);
    assertThat(myGhfs.delete(new Path("/file1"))).isTrue();

    TestUtils.verifyDurationMetric(
        (GhfsGlobalStorageStatistics) stats, INVOCATION_CREATE.getSymbol(), 1);
  }

  private static Long getMetricValue(StorageStatistics stats, GhfsStatistic invocationCreate) {
    return stats.getLong(invocationCreate.getSymbol());
  }

  private void createFile(FileSystem fs, Path testFile, int writeSize, long totalSize)
      throws Exception {
    byte[] writeBuffer = new byte[writeSize];
    try (FSDataOutputStream output = fs.create(testFile)) {
      long fileBytesWrite = 0;
      do {
        output.write(writeBuffer);
        fileBytesWrite += writeSize;
      } while (fileBytesWrite < totalSize);
    }
  }

  private void readFile(FileSystem fs, Path testFile, int readSize) throws Exception {
    byte[] readBuffer = new byte[readSize];
    try (FSDataInputStream input = fs.open(testFile)) {
      long fileBytesRead = 0;
      int bytesRead;
      do {
        bytesRead = input.read(readBuffer);
        if (bytesRead > 0) {
          fileBytesRead += bytesRead;
        }
      } while (bytesRead >= 0);
    }
  }

  private static HashSet<String> getExpectedDurationMetrics() {
    return new HashSet<String>(
        List.of(
            ACTION_HTTP_DELETE_REQUEST.getSymbol(),
            ACTION_HTTP_POST_REQUEST.getSymbol(),
            ACTION_HTTP_PUT_REQUEST.getSymbol(),
            GCS_GET_MEDIA_REQUEST.getSymbol(),
            GCS_LIST_DIR_REQUEST.getSymbol(),
            GCS_LIST_FILE_REQUEST.getSymbol(),
            GCS_METADATA_REQUEST.getSymbol(),
            INVOCATION_CREATE.getSymbol(),
            INVOCATION_DELETE.getSymbol(),
            INVOCATION_GET_FILE_STATUS.getSymbol(),
            INVOCATION_GLOB_STATUS.getSymbol(),
            INVOCATION_HFLUSH.getSymbol(),
            INVOCATION_HSYNC.getSymbol(),
            INVOCATION_LIST_STATUS.getSymbol(),
            INVOCATION_MKDIRS.getSymbol(),
            INVOCATION_OPEN.getSymbol(),
            INVOCATION_RENAME.getSymbol(),
            STREAM_READ_OPERATIONS.getSymbol(),
            STREAM_WRITE_OPERATIONS.getSymbol()));
  }

  @Test
  public void testLogsContainInvocationId() throws Exception {
    Logger logger = Logger.getLogger(GoogleHadoopFileSystem.class.getName());
    logger.setUseParentHandlers(false);
    logger.setLevel(Level.ALL);

    // Temporarily remove other handlers on this logger
    Handler[] otherHandlers = logger.getHandlers();
    for (Handler h : otherHandlers) {
      logger.removeHandler(h);
    }

    // Create a handler to capture log output with LoggingFormatter
    ByteArrayOutputStream logOutput = new ByteArrayOutputStream();
    Handler testLogHandler =
        new StreamHandler(logOutput, new LoggingFormatter(new SimpleFormatter()));
    testLogHandler.setLevel(Level.ALL);
    logger.addHandler(testLogHandler);

    try {
      // Perform a rename operation, which should trigger logging with an invocation ID
      Path testRoot = new Path(ghfs.getWorkingDirectory(), "testLogsContainInvocationId");
      ghfs.mkdirs(testRoot);
      Path src = new Path(testRoot, "src.txt");
      createFile(src, "test-data".getBytes(UTF_8));
      Path dst = new Path(testRoot, "dst.txt");
      ghfs.rename(src, dst);

      // Flush the handler to ensure all log records are written to the StringWriter
      testLogHandler.flush();
      String logs = logOutput.toString(UTF_8.name());

      // Verify that the logs match the SimpleFormatter pattern with an invocation ID.
      // It should match a two-line pattern: a header with timestamp and method,
      // followed by the log message with level and invocation ID.
      String expectedLogPattern =
          "([\\w\\s,:]+ com\\.google\\.cloud\\.hadoop\\..+?\\n"
              + "(FINE|FINER|INFO): \\[gccl-invocation-id/.*?]: .*?\\n)+";
      assertThat(logs).matches(Pattern.compile(expectedLogPattern, Pattern.DOTALL));
    } finally {
      // Clean up the handler
      logger.removeHandler(testLogHandler);
      testLogHandler.close();
      logger.setLevel(Level.INFO); // reset level
      // Optional: Restore other handlers
      for (Handler h : otherHandlers) {
        if (h != testLogHandler) logger.addHandler(h);
      }
    }
  }
}
