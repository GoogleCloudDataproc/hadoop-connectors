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

import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.STREAM_READ_OPERATIONS;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.STREAM_WRITE_OPERATIONS;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_CLIENT_TYPE;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemTestHelper.createInMemoryGoogleHadoopFileSystem;
import static com.google.cloud.hadoop.gcsio.testing.InMemoryGoogleCloudStorage.getInMemoryGoogleCloudStorageOptions;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.hadoop.gcsio.FileInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemImpl;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageItemInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions;
import com.google.cloud.hadoop.gcsio.MethodOutcome;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.cloud.hadoop.gcsio.testing.InMemoryGoogleCloudStorage;
import com.google.cloud.hadoop.util.AccessTokenProvider;
import com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.AuthenticationType;
import com.google.cloud.hadoop.util.interceptors.LoggingInterceptor;
import com.google.cloud.hadoop.util.testing.TestingAccessTokenProvider;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.Arrays;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.StreamHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Unit tests for {@link GoogleHadoopFileSystem} class. */
@RunWith(Parameterized.class)
public class GoogleHadoopFileSystemTest extends GoogleHadoopFileSystemIntegrationTest {

  private LoggingInterceptor mockInterceptor;
  private StreamHandler testLogHandler;
  private Logger ghfsLogger;

  @Before
  public void before() throws IOException {
    // Disable logging.
    // Normally you would need to keep a strong reference to any logger used for
    // configuration, but the "root" logger is always present.
    Logger.getLogger("").setLevel(Level.OFF);

    super.ghfs = createInMemoryGoogleHadoopFileSystem();

    postCreateInit();
  }

  @After
  public void after() throws IOException {
    if (mockInterceptor != null) {
      // Ensure handler is removed even if test fails, to not affect other tests
      Logger.getLogger("").removeHandler(mockInterceptor);
      mockInterceptor = null;
    }
    if (testLogHandler != null) {
      assertThat(ghfsLogger).isNotNull();
      ghfsLogger.removeHandler(testLogHandler);
      ghfsLogger.setLevel(Level.OFF);
      testLogHandler = null;
      ghfsLogger = null;
    }
  }

  @Test
  public void verifyHadoopPath() throws Exception {
    GoogleHadoopFileSystem eagerFs = createInMemoryGoogleHadoopFileSystem();
    String fileNameWithColon = "empty:file";
    eagerFs.create(new Path(/* schema= */ null, /* authority= */ null, fileNameWithColon)).close();
    FileStatus[] fileStatus =
        eagerFs.listStatus(new Path(GoogleHadoopFileSystemTestHelper.IN_MEMORY_TEST_BUCKET));
    assertThat(fileStatus).hasLength(1);
    assertThat(fileStatus[0].getPath().getName()).isEqualTo(fileNameWithColon);
  }

  @Test
  public void testVersionString() {
    assertThat(GoogleHadoopFileSystem.VERSION).isNotNull();
    assertThat(GoogleHadoopFileSystem.UNKNOWN_VERSION.equals(GoogleHadoopFileSystem.VERSION))
        .isFalse();
  }

  @Test
  @SuppressWarnings("CheckReturnValue")
  public void lazyInitialization_succeeds_withInvalidCredentialsConfiguration() throws Exception {
    new GoogleHadoopFileSystem();
    Configuration lazyConf = new Configuration();
    lazyConf.setBoolean("fs.gs.lazy.init.enable", true);
    lazyConf.set("fs.gs.auth.service.account.json.keyfile", "non-existent.json");
    lazyConf.setEnum(GCS_CLIENT_TYPE.toString(), storageClientType);
    GoogleHadoopFileSystem lazyFs = new GoogleHadoopFileSystem();

    lazyFs.initialize(new URI("gs://test-non-existent/"), lazyConf);
    lazyFs.close();
  }

  @Test
  public void lazyInitialization_deleteCall_fails_withInvalidCredentialsConfiguration()
      throws Exception {
    Configuration lazyConf = new Configuration();
    lazyConf.setBoolean("fs.gs.lazy.init.enable", true);
    lazyConf.setEnum("fs.gs.auth.type", AuthenticationType.SERVICE_ACCOUNT_JSON_KEYFILE);
    lazyConf.set("fs.gs.auth.service.account.json.keyfile", "non-existent.json");
    lazyConf.setEnum(GCS_CLIENT_TYPE.toString(), storageClientType);
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
  @SuppressWarnings("CheckReturnValue")
  public void eagerInitialization_fails_withInvalidCredentialsConfiguration() {
    new GoogleHadoopFileSystem();
    Configuration eagerConf = new Configuration();
    eagerConf.setBoolean("fs.gs.lazy.init.enable", false);
    eagerConf.setEnum("fs.gs.auth.type", AuthenticationType.SERVICE_ACCOUNT_JSON_KEYFILE);
    eagerConf.set("fs.gs.auth.service.account.json.keyfile", "non-existent.json");
    eagerConf.setEnum(GCS_CLIENT_TYPE.toString(), storageClientType);
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

  @Test
  public void read_throws_exception() throws Exception {
    String rootBucketName = ghfsHelper.getUniqueBucketName("read-throws-exception");
    URI initUri = new Path("gs://" + rootBucketName).toUri();
    GoogleCloudStorageFileSystem fakeGcsFs =
        new GoogleCloudStorageFileSystemImpl(
            CustomInMemoryGoogleCloudStorage::new,
            GoogleCloudStorageFileSystemOptions.builder()
                .setCloudStorageOptions(getInMemoryGoogleCloudStorageOptions())
                .build());
    GoogleHadoopFileSystem fs = new GoogleHadoopFileSystem(fakeGcsFs);
    fs.initialize(initUri, new Configuration());

    try (FSDataInputStream inputStream = fs.open(new Path("read-throws-exception-file"))) {
      IOException exception =
          assertThrows(IOException.class, () -> inputStream.read(new byte[2], 0, 1));
      assertThat(exception)
          .hasMessageThat()
          .isEqualTo("read_throws_exception test : read call throws exception");
    }
  }

  @Test
  public void read_single_byte_throws_exception() throws Exception {
    String rootBucketName = ghfsHelper.getUniqueBucketName("read-throws-exception");
    URI initUri = new Path("gs://" + rootBucketName).toUri();
    GoogleCloudStorageFileSystem fakeGcsFs =
        new GoogleCloudStorageFileSystemImpl(
            CustomInMemoryGoogleCloudStorage::new,
            GoogleCloudStorageFileSystemOptions.builder()
                .setCloudStorageOptions(getInMemoryGoogleCloudStorageOptions())
                .build());
    GoogleHadoopFileSystem fs = new GoogleHadoopFileSystem(fakeGcsFs);
    fs.initialize(initUri, new Configuration());

    try (FSDataInputStream inputStream = fs.open(new Path("read-throws-exception-file"))) {
      IOException exception = assertThrows(IOException.class, () -> inputStream.read());
      assertThat(exception)
          .hasMessageThat()
          .isEqualTo("read_throws_exception test : read call throws exception");
    }
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
  public void testCouldUseFlatGlob() throws IOException {
    try (GoogleHadoopFileSystem lazyFs = new InMemoryGoogleHadoopFileSystem()) {
      assertThat(lazyFs.couldUseFlatGlob(new Path("gs://**/test/"))).isFalse();
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
    config.setEnum("fs.gs.auth.type", AuthenticationType.ACCESS_TOKEN_PROVIDER);
    config.setClass(
        "fs.gs.auth.access.token.provider",
        TestingAccessTokenProvider.class,
        AccessTokenProvider.class);
    config.setEnum(GCS_CLIENT_TYPE.toString(), storageClientType);
    URI gsUri = new URI("gs://foobar/");

    GoogleHadoopFileSystem ghfs = new GoogleHadoopFileSystem();
    ghfs.initialize(gsUri, config);

    assertThat(ghfs.getDefaultPort()).isEqualTo(-1);
  }

  @Test
  public void testFileOpenWithStatus() throws Exception {
    URI bucketName = new URI("gs://read-test-bucket/");
    URI failureBucketName = new URI("gs://read-test-bucket-other/");

    FileInfo fileInfo =
        FileInfo.fromItemInfo(
            GoogleCloudStorageItemInfo.createObject(
                new StorageResourceId(bucketName.getAuthority(), "bar/test/object"),
                /* creationTime= */ 10L,
                /* modificationTime= */ 15L,
                /* size= */ 200L,
                "text/plain",
                /* contentEncoding= */ "lzma",
                /* metadata= */ ImmutableMap.of("foo-meta", new byte[] {5, 66, 56}),
                /* contentGeneration= */ 312432L,
                /* metaGeneration= */ 2L,
                /* verificationAttributes= */ null));

    GoogleHadoopFileStatus fileStatus =
        new GoogleHadoopFileStatus(
            fileInfo, new Path(fileInfo.getPath()), 1, 2, FsPermission.getFileDefault(), "foo");
    try (GoogleHadoopFileSystem fs = new GoogleHadoopFileSystem()) {
      fs.initialize(bucketName, new Configuration());
      fs.open(fileStatus);

      fs.initialize(failureBucketName, new Configuration());

      IllegalArgumentException exception =
          assertThrows(IllegalArgumentException.class, () -> fs.open(fileStatus));
      assertThat(exception.getMessage())
          .isEqualTo(
              "Wrong bucket: read-test-bucket, in path: gs://read-test-bucket/bar/test/object, expected bucket: read-test-bucket-other");
    }
  }

  @Test
  public void testFileOpenWithStatusInvalidType() throws Exception {
    try (GoogleHadoopFileSystem fs = new GoogleHadoopFileSystem()) {
      fs.initialize(new URI("gs://read-test-bucket/"), new Configuration());

      IllegalArgumentException exception =
          assertThrows(IllegalArgumentException.class, () -> fs.open(new FileStatus()));
      assertThat(exception.getMessage())
          .isEqualTo(
              "Expected status to be of type GoogleHadoopFileStatus, but found class org.apache.hadoop.fs.FileStatus");
    }
  }

  @Test
  public void testTotalTimeStatistics() throws IOException {
    GhfsGlobalStorageStatistics stats = new GhfsGlobalStorageStatistics();
    stats.updateStats(STREAM_READ_OPERATIONS, 10, 100, 200, 10, new Object());
    stats.addTotalTimeStatistic(STREAM_READ_OPERATIONS.getSymbol() + "_duration");
    assertThat(stats.getLong(STREAM_READ_OPERATIONS.getSymbol() + "_duration")).isEqualTo(200);

    stats.updateStats(STREAM_WRITE_OPERATIONS, 10, 100, 200, 10, new Object());
    stats.addTotalTimeStatistic(STREAM_WRITE_OPERATIONS.getSymbol() + "_duration");
    assertThat(stats.getLong(STREAM_WRITE_OPERATIONS.getSymbol() + "_duration")).isEqualTo(200);
  }

  @Test
  public void close_whenCloudLoggingEnabled_loggingInterceptorIsClosedAndRemoved()
      throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(GoogleHadoopFileSystemConfiguration.GCS_CLOUD_LOGGING_ENABLE.getKey(), true);
    // Provide credentials config to satisfy initialize()
    conf.setEnum("fs.gs.auth.type", AuthenticationType.ACCESS_TOKEN_PROVIDER);
    conf.setClass(
        "fs.gs.auth.access.token.provider",
        TestingAccessTokenProvider.class,
        AccessTokenProvider.class);
    conf.setEnum(GCS_CLIENT_TYPE.toString(), storageClientType);

    LoggingInterceptor mockInterceptor = mock(LoggingInterceptor.class);
    // Anonymous subclass to inject the mock
    GoogleHadoopFileSystem fs =
        new GoogleHadoopFileSystem() {
          @Override
          LoggingInterceptor createLoggingInterceptor(
              GoogleCredentials credentials, String suffix) {
            // Return our mock instead of a real one
            return mockInterceptor;
          }
        };

    try {
      fs.initialize(new URI("gs://foobar/"), conf);

      // Verify handler was added
      Logger rootLogger = Logger.getLogger("");
      assertThat(Arrays.asList(rootLogger.getHandlers())).contains(mockInterceptor);

      fs.close();

      // Verify close was called and handler was removed
      verify(mockInterceptor, times(1)).close();
      assertThat(Arrays.asList(rootLogger.getHandlers())).doesNotContain(mockInterceptor);
    } finally {
      // Ensure handler is removed even if test fails, to not affect other tests
      Logger.getLogger("").removeHandler(mockInterceptor);
    }
  }

  @Test
  public void
      close_whenCloudLoggingEnabled_loggingInterceptorCloseFails_exceptionIsLoggedAndNotThrown()
          throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(GoogleHadoopFileSystemConfiguration.GCS_CLOUD_LOGGING_ENABLE.getKey(), true);
    // Provide credentials config to satisfy initialize()
    conf.setEnum("fs.gs.auth.type", AuthenticationType.ACCESS_TOKEN_PROVIDER);
    conf.setClass(
        "fs.gs.auth.access.token.provider",
        TestingAccessTokenProvider.class,
        AccessTokenProvider.class);
    conf.setEnum(GCS_CLIENT_TYPE.toString(), storageClientType);

    mockInterceptor = mock(LoggingInterceptor.class);
    doThrow(new RuntimeException("Close failed!")).when(mockInterceptor).close();

    // Setup log capturing
    ghfsLogger = Logger.getLogger(GoogleHadoopFileSystem.class.getName());
    ghfsLogger.setLevel(Level.ALL);
    ByteArrayOutputStream logOutput = new ByteArrayOutputStream();
    testLogHandler =
        new StreamHandler(
            logOutput,
            new Formatter() {
              @Override
              public String format(LogRecord record) {
                return record.getMessage() + "\n";
              }
            });
    ghfsLogger.addHandler(testLogHandler);

    // Anonymous subclass to inject the mock
    GoogleHadoopFileSystem fs =
        new GoogleHadoopFileSystem() {
          @Override
          LoggingInterceptor createLoggingInterceptor(
              GoogleCredentials credentials, String suffix) {
            // Return our mock instead of a real one
            return mockInterceptor;
          }
        };

    fs.initialize(new URI("gs://foobar/"), conf);

    // Verify handler was added
    Logger rootLogger = Logger.getLogger("");
    assertThat(Arrays.asList(rootLogger.getHandlers())).contains(mockInterceptor);

    // Close should not throw an exception, even if the interceptor's close fails.
    fs.close();

    // Verify close was called and handler was removed
    verify(mockInterceptor, times(1)).close();
    assertThat(Arrays.asList(rootLogger.getHandlers())).doesNotContain(mockInterceptor);

    // Verify exception was logged
    testLogHandler.flush();
    assertThat(logOutput.toString()).contains("Failed to stop cloud logging service");
  }

  @Test
  public void close_closesAndNullifiesInstrumentation() throws Exception {
    // Get the instrumentation field
    java.lang.reflect.Field instrumentationField =
        GoogleHadoopFileSystem.class.getDeclaredField("instrumentation");
    instrumentationField.setAccessible(true);

    // Assert that instrumentation is not null before close
    assertThat(instrumentationField.get(ghfs)).isNotNull();

    // Close the file system
    ghfs.close();

    // Assert that instrumentation is null after close
    assertThat(instrumentationField.get(ghfs)).isNull();
  }

  // -----------------------------------------------------------------
  // Inherited tests that we suppress because their behavior differs
  // from the base class.
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
  public void testInvalidCredentialsFromAccessTokenProvider() {}

  @Override
  public void testImpersonationServiceAccountUsed() {}

  @Override
  public void testImpersonationUserNameIdentifierUsed() {}

  @Override
  public void testImpersonationGroupNameIdentifierUsed() {}

  @Override
  public void testImpersonationUserAndGroupNameIdentifiersUsed() {}

  @Override
  public void testImpersonationServiceAccountAndUserAndGroupNameIdentifierUsed() {}

  @Override
  public void testImpersonationInvalidUserNameIdentifierUsed() {}

  @Override
  public void unauthenticatedAccessToPublicBuckets_fsGsProperties() {}

  @Override
  public void unauthenticatedAccessToPublicBuckets_googleCloudProperties() {}

  @Override
  public void testInitializeCompatibleWithHadoopCredentialProvider() {}

  @Override
  public void testRenameHnBucket() {}

  @Override
  public void testRenameWithMoveDisabled() {}

  @Override
  public void testGcsJsonAPIMetrics() {}

  @Override
  public void testGcsThreadLocalMetrics() {}

  @Override
  public void multiThreadTest() {}

  /* Custom InMemoryGoogleCloudStorage object which throws exception when reading */
  private class CustomInMemoryGoogleCloudStorage extends InMemoryGoogleCloudStorage {
    private IOException exceptionThrown =
        new IOException("read_throws_exception test : read call throws exception");

    CustomInMemoryGoogleCloudStorage(GoogleCloudStorageOptions storageOptions) {
      super(storageOptions);
    }

    @Override
    public SeekableByteChannel open(
        GoogleCloudStorageItemInfo itemInfo, GoogleCloudStorageReadOptions readOptions) {
      return returnChannel();
    }

    public SeekableByteChannel returnChannel() {
      return new SeekableByteChannel() {
        private long position = 0;
        private boolean isOpen = true;

        @Override
        public long position() {
          return position;
        }

        @CanIgnoreReturnValue
        @Override
        public SeekableByteChannel position(long newPosition) {
          position = newPosition;
          return this;
        }

        @Override
        public int read(ByteBuffer dst) throws IOException {
          throw exceptionThrown;
        }

        @Override
        public long size() throws IOException {
          throw exceptionThrown;
        }

        @Override
        public SeekableByteChannel truncate(long size) {
          throw new UnsupportedOperationException("Cannot mutate read-only channel");
        }

        @Override
        public int write(ByteBuffer src) {
          throw new UnsupportedOperationException("Cannot mutate read-only channel");
        }

        @Override
        public void close() {
          isOpen = false;
        }

        @Override
        public boolean isOpen() {
          return isOpen;
        }
      };
    }
  }

  @Override
  public void testHnBucketRecursiveDeleteOperationOnDirectory() {}

  @Override
  public void testHnBucketRecursiveDeleteOperationOnBucket() {}

  @Override
  public void testHnBucketNonRecursiveDeleteOperation() {}

  @Override
  public void testHnBucketDeleteOperationOnNonExistingFolder() {}

  @Override
  public void testGetFileStatusWithHint() {}

  @Test
  public void close_canBeCalledMultipleTimes() throws Exception {
    // Close the file system
    ghfs.close();
    // Close again - should not throw.
    ghfs.close();
  }
}
