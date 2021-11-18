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

import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.INVOCATION_COPY_FROM_LOCAL_FILE;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCE_BUCKET_DELETE_ENABLE;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_CONFIG_PREFIX;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_PROJECT_ID;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_REPAIR_IMPLICIT_DIRECTORIES_ENABLE;
import static com.google.cloud.hadoop.util.HadoopCredentialConfiguration.SERVICE_ACCOUNT_JSON_KEYFILE_SUFFIX;
import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemIntegrationTest;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.cloud.hadoop.gcsio.testing.TestConfiguration;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Abstract base class for test suites targeting variants of GoogleHadoopFileSystem via the Hadoop
 * FileSystem interface. Includes general HadoopFileSystemTestBase cases plus some behavior only
 * visible at the GHFS level.
 */
public abstract class GoogleHadoopFileSystemTestBase extends HadoopFileSystemTestBase {

  /**
   * Helper to load all the GHFS-specific config values from environment variables, such as those
   * needed for setting up the credentials of a real GoogleCloudStorage.
   */
  protected static Configuration loadConfig() {
    TestConfiguration testConf = TestConfiguration.getInstance();
    return loadConfig(testConf.getProjectId(), testConf.getServiceAccountJsonKeyFile());
  }

  /** Helper to load GHFS-specific config values other than those from the environment. */
  protected static Configuration loadConfig(String projectId, String serviceAccountJsonKeyFile) {
    assertWithMessage("Expected value for env var %s", TestConfiguration.GCS_TEST_PROJECT_ID)
        .that(projectId)
        .isNotNull();
    Configuration config = new Configuration();
    config.set(GCS_PROJECT_ID.getKey(), projectId);
    if (serviceAccountJsonKeyFile != null) {
      config.set(
          GCS_CONFIG_PREFIX + SERVICE_ACCOUNT_JSON_KEYFILE_SUFFIX.getKey(),
          serviceAccountJsonKeyFile);
    }
    config.setBoolean(GCS_REPAIR_IMPLICIT_DIRECTORIES_ENABLE.getKey(), true);
    // Allow buckets to be deleted in test cleanup:
    config.setBoolean(GCE_BUCKET_DELETE_ENABLE.getKey(), true);
    return config;
  }

  @ClassRule
  public static NotInheritableExternalResource storageResource =
      new NotInheritableExternalResource(GoogleHadoopFileSystemTestBase.class) {
        /** Perform clean-up once after all tests are turn. */
        @Override
        public void after() {
          HadoopFileSystemTestBase.storageResource.after();
        }
      };

  // -----------------------------------------------------------------------------------------
  // Tests that vary according to the GHFS variant, but which we want to make sure get tested.
  // -----------------------------------------------------------------------------------------

  @Test
  public abstract void testCheckPathSuccess();

  @Test
  public abstract void testCheckPathFailure();

  @Test
  public abstract void testInitializeSuccess() throws IOException, URISyntaxException;

  @Test
  public abstract void testInitializeWithWorkingDirectory() throws Exception;

  @Test
  public abstract void testConfigureBucketsSuccess() throws URISyntaxException, IOException;

  @Test
  public abstract void testConfigureBucketsWithRootBucketButNoSystemBucket() throws IOException;

  @Test
  public abstract void testConfigureBucketsWithNeitherRootBucketNorSystemBucket()
      throws IOException;

  // -----------------------------------------------------------------------------------------
  // Tests that aren't supported by all configurations of GHFS.
  // -----------------------------------------------------------------------------------------

  /** Tests getGcsPath(). */
  @Test
  public void testGetGcsPath() throws URISyntaxException {
    GoogleHadoopFileSystem myghfs = (GoogleHadoopFileSystem) ghfs;

    URI gcsPath = new URI("gs://" + myghfs.getUri().getAuthority() + "/dir/obj");
    assertThat(myghfs.getGcsPath(new Path(gcsPath))).isEqualTo(gcsPath);

    assertThat(myghfs.getGcsPath(new Path("/buck^et", "object")))
        .isEqualTo(new URI("gs://" + myghfs.getUri().getAuthority() + "/buck%5Eet/object"));
  }

  /** Verifies that test config can be accessed through the FS instance. */
  @Test
  public void testConfig() {
    GoogleHadoopFileSystem myghfs = (GoogleHadoopFileSystem) ghfs;
    GoogleCloudStorageOptions cloudStorageOptions =
        myghfs.getGcsFs().getOptions().getCloudStorageOptions();

    assertThat(cloudStorageOptions.getReadChannelOptions().getInplaceSeekLimit())
        .isEqualTo(
            GoogleHadoopFileSystemConfiguration.GCS_INPUT_STREAM_INPLACE_SEEK_LIMIT.getDefault());
    assertThat(myghfs.getDefaultBlockSize())
        .isEqualTo(GoogleHadoopFileSystemConfiguration.BLOCK_SIZE.getDefault());
  }

  /** Tests getCanonicalServiceName(). */
  @Test
  public void testGetCanonicalServiceName() {
    assertThat(ghfs.getCanonicalServiceName()).isNull();
  }

  /** Test implicit directories. */
  @Test
  public void testImplicitDirectory() throws IOException {
    String bucketName = sharedBucketName1;
    GoogleHadoopFileSystem myghfs = (GoogleHadoopFileSystem) ghfs;
    GoogleCloudStorageFileSystem gcsfs = myghfs.getGcsFs();
    URI seedUri = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path parentPath = ghfsHelper.castAsHadoopPath(seedUri);
    URI parentUri = myghfs.getGcsPath(parentPath);

    // A subdir path that looks like gs://<bucket>/<generated-tempdir>/foo-subdir where
    // neither the subdir nor gs://<bucket>/<generated-tempdir> exist yet.
    Path subdirPath = new Path(parentPath, "foo-subdir");
    URI subdirUri = myghfs.getGcsPath(subdirPath);

    Path leafPath = new Path(subdirPath, "bar-subdir");
    URI leafUri = myghfs.getGcsPath(leafPath);
    gcsfs.mkdir(leafUri);

    assertDirectory(gcsfs, leafUri, /* exists= */ true);
    assertDirectory(gcsfs, subdirUri, /* exists= */ true);
    assertDirectory(gcsfs, parentUri, /* exists= */ true);

    ghfsHelper.clearBucket(bucketName);
  }

  @Test
  public void testRepairDirectory_afterFileDelete() throws IOException {
    GoogleHadoopFileSystem myghfs = (GoogleHadoopFileSystem) ghfs;
    GoogleCloudStorageFileSystem gcsfs = myghfs.getGcsFs();
    GoogleCloudStorage gcs = gcsfs.getGcs();
    URI seedUri = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path dirPath = ghfsHelper.castAsHadoopPath(seedUri);
    URI dirUri = myghfs.getGcsPath(dirPath);

    // A subdir path that looks like gs://<bucket>/<generated-tempdir>/foo-subdir where
    // neither the subdir nor gs://<bucket>/<generated-tempdir> exist yet.
    Path emptyObject = new Path(dirPath, "empty-object");
    URI objUri = myghfs.getGcsPath(emptyObject);
    StorageResourceId resource = StorageResourceId.fromUriPath(objUri, false);
    gcs.createEmptyObject(resource);

    boolean autoRepairImplicitDirectories =
        gcsfs.getOptions().getCloudStorageOptions().isAutoRepairImplicitDirectoriesEnabled();

    assertDirectory(gcsfs, dirUri, /* exists= */ true);

    gcsfs.delete(objUri, false);

    // Implicit directory created after deletion of the sole object in the directory
    assertDirectory(gcsfs, dirUri, /* exists= */ autoRepairImplicitDirectories);

    ghfsHelper.clearBucket(resource.getBucketName());
  }

  @Test
  public void testRepairDirectory_afterSubdirectoryDelete() throws IOException {
    GoogleHadoopFileSystem myghfs = (GoogleHadoopFileSystem) ghfs;
    GoogleCloudStorageFileSystem gcsfs = myghfs.getGcsFs();
    GoogleCloudStorage gcs = gcsfs.getGcs();

    URI seedUri = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path dirPath = ghfsHelper.castAsHadoopPath(seedUri);
    URI dirUri = myghfs.getGcsPath(dirPath);
    Path subDir = new Path(dirPath, "subdir");
    URI subdirUri = myghfs.getGcsPath(subDir);

    // A subdir path that looks like gs://<bucket>/<generated-tempdir>/foo-subdir where
    // neither the subdir nor gs://<bucket>/<generated-tempdir> exist yet.
    Path emptyObject = new Path(subDir, "empty-object");
    URI objUri = myghfs.getGcsPath(emptyObject);
    StorageResourceId resource = StorageResourceId.fromUriPath(objUri, false);
    gcs.createEmptyObject(resource);

    boolean autoRepairImplicitDirectories =
        gcsfs.getOptions().getCloudStorageOptions().isAutoRepairImplicitDirectoriesEnabled();

    assertDirectory(gcsfs, dirUri, /* exists= */ true);
    assertDirectory(gcsfs, subdirUri, /* exists= */ true);

    gcsfs.delete(subdirUri, true);

    // Implicit directory created after deletion of the sole object in the directory
    assertDirectory(gcsfs, dirUri, /* exists= */ autoRepairImplicitDirectories);

    ghfsHelper.clearBucket(resource.getBucketName());
  }

  @Test
  public void testRepairDirectory_afterFileRename() throws IOException {
    GoogleHadoopFileSystem myghfs = (GoogleHadoopFileSystem) ghfs;
    GoogleCloudStorageFileSystem gcsfs = myghfs.getGcsFs();
    GoogleCloudStorage gcs = gcsfs.getGcs();

    URI seedUri = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path dirPath = ghfsHelper.castAsHadoopPath(seedUri);
    URI dirUri = myghfs.getGcsPath(dirPath);

    // A subdir path that looks like gs://<bucket>/<generated-tempdir>/foo-subdir where
    // neither the subdir nor gs://<bucket>/<generated-tempdir> exist yet.
    Path emptyObject = new Path(dirPath, "empty-object");
    URI objUri = myghfs.getGcsPath(emptyObject);
    StorageResourceId resource = StorageResourceId.fromUriPath(objUri, false);
    gcs.createEmptyObject(resource);

    boolean autoRepairImplicitDirectories =
        gcsfs.getOptions().getCloudStorageOptions().isAutoRepairImplicitDirectoriesEnabled();

    assertDirectory(gcsfs, dirUri, /* exists= */ true);

    gcsfs.rename(objUri, objUri.resolve(".."));

    // Implicit directory created after deletion of the sole object in the directory
    assertDirectory(gcsfs, dirUri, /* exists= */ autoRepairImplicitDirectories);

    ghfsHelper.clearBucket(resource.getBucketName());
  }

  @Test
  public void testRepairDirectory_afterSubdirectoryRename() throws IOException {
    String bucketName = sharedBucketName1;
    GoogleHadoopFileSystem myghfs = (GoogleHadoopFileSystem) ghfs;
    GoogleCloudStorageFileSystem gcsfs = myghfs.getGcsFs();
    GoogleCloudStorage gcs = gcsfs.getGcs();

    Path dirPath =
        ghfsHelper.castAsHadoopPath(GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath());
    URI dirUri = myghfs.getGcsPath(dirPath);
    Path subDir = new Path(dirPath, "subdir");
    URI subdirUri = myghfs.getGcsPath(subDir);

    // A subdir path that looks like gs://<bucket>/<generated-tempdir>/foo-subdir where
    // neither the subdir nor gs://<bucket>/<generated-tempdir> exist yet.
    Path emptyObject = new Path(subDir, "empty-object");
    URI objUri = myghfs.getGcsPath(emptyObject);
    StorageResourceId resource = StorageResourceId.fromUriPath(objUri, false);
    gcs.createEmptyObject(resource);

    boolean autoRepairImplicitDirectories =
        gcsfs.getOptions().getCloudStorageOptions().isAutoRepairImplicitDirectoriesEnabled();

    assertDirectory(gcsfs, dirUri, /* exists= */ true);
    assertDirectory(gcsfs, subdirUri, /* exists= */ true);

    gcsfs.rename(subdirUri, dirUri.resolve("."));

    // Implicit directory created after deletion of the sole object in the directory
    assertDirectory(gcsfs, dirUri, /* exists= */ autoRepairImplicitDirectories);

    ghfsHelper.clearBucket(bucketName);
  }

  private static void assertDirectory(GoogleCloudStorageFileSystem gcsfs, URI path, boolean exists)
      throws IOException {
    assertWithMessage("Expected to %s: %s", exists ? "exist" : "not exist", path)
        .that(gcsfs.exists(path))
        .isEqualTo(exists);
    if (exists) {
      assertWithMessage("Expected to be a directory: %s", path)
          .that(gcsfs.getFileInfo(path).isDirectory())
          .isTrue();
    } else {
      assertWithMessage("Expected to have requested ID: %s", path)
          .that(gcsfs.getFileInfo(path).getPath())
          .isEqualTo(path);
    }
  }

  /** Validates makeQualified() when working directory is not root. */
  @Test
  public void testMakeQualifiedNotRoot() {
    GoogleHadoopFileSystem myGhfs = (GoogleHadoopFileSystem) ghfs;
    URI fsRootUri = myGhfs.getUri();
    String fsRoot = fsRootUri.toString();
    String workingParent = fsRoot + "working/";
    String workingDir = workingParent + "dir";
    myGhfs.setWorkingDirectory(new Path(workingDir));
    Map<String, String> qualifiedPaths = new HashMap<>();
    qualifiedPaths.put("/", fsRoot);
    qualifiedPaths.put("/foo", fsRoot + "foo");
    qualifiedPaths.put("/foo/bar", fsRoot + "foo/bar");
    qualifiedPaths.put(".", workingDir);
    qualifiedPaths.put("foo", workingDir + "/foo");
    qualifiedPaths.put("foo/bar", workingDir + "/foo/bar");
    qualifiedPaths.put(fsRoot, fsRoot);
    qualifiedPaths.put(fsRoot + "foo", fsRoot + "foo");
    qualifiedPaths.put(fsRoot + "foo/bar", fsRoot + "foo/bar");
    qualifiedPaths.put("/foo/../foo", fsRoot + "foo");
    qualifiedPaths.put("/foo/bar/../../foo/bar", fsRoot + "foo/bar");
    qualifiedPaths.put("foo/../foo", workingDir + "/foo");
    qualifiedPaths.put("foo/bar/../../foo/bar", workingDir + "/foo/bar");
    qualifiedPaths.put(fsRoot + "foo/../foo", fsRoot + "foo");
    qualifiedPaths.put(fsRoot + "foo/bar/../../foo/bar", fsRoot + "foo/bar");
    qualifiedPaths.put("..", workingParent);
    qualifiedPaths.put("../..", fsRoot);
    qualifiedPaths.put("../foo", workingParent + "/foo");
    qualifiedPaths.put("../foo/bar", workingParent + "/foo/bar");
    qualifiedPaths.put("../foo/../foo", workingParent + "/foo");
    qualifiedPaths.put("../foo/bar/../../foo/bar", workingParent + "/foo/bar");
    qualifiedPaths.put(workingDir + "/../foo/../foo", workingParent + "/foo");
    qualifiedPaths.put(workingDir + "/../foo/bar/../../foo/bar", workingParent + "/foo/bar");
    qualifiedPaths.put(fsRoot + "..foo/bar", fsRoot + "..foo/bar");
    qualifiedPaths.put("..foo/bar", workingDir + "/..foo/bar");

    // GHFS specific behavior where root is its own parent.
    qualifiedPaths.put("/..", fsRoot);
    qualifiedPaths.put("/../../..", fsRoot);
    qualifiedPaths.put("/../foo/", fsRoot + "foo");
    qualifiedPaths.put("/../../../foo/bar", fsRoot + "foo/bar");
    qualifiedPaths.put("../../..", fsRoot);
    qualifiedPaths.put(fsRoot + "..", fsRoot);
    qualifiedPaths.put(fsRoot + "../foo", fsRoot + "foo");
    qualifiedPaths.put(fsRoot + "../foo/bar", fsRoot + "foo/bar");
    qualifiedPaths.put("../../../foo/../foo", fsRoot + "foo");
    qualifiedPaths.put("../../../foo/bar/../../foo/bar", fsRoot + "foo/bar");

    // Skip for authority-less gsg paths.
    if (fsRootUri.getAuthority() != null) {
      // When the path to qualify is of the form gs://somebucket, we want to qualify
      // it as gs://someBucket/
      qualifiedPaths.put(fsRoot.substring(0, fsRoot.length() - 1), fsRoot);
    }

    for (String unqualifiedString : qualifiedPaths.keySet()) {
      Path unqualifiedPath = new Path(unqualifiedString);
      Path qualifiedPath = new Path(qualifiedPaths.get(unqualifiedString));
      assertThat(qualifiedPath).isEqualTo(myGhfs.makeQualified(unqualifiedPath));
    }
  }

  /** Validates makeQualified() when working directory is root. */
  @Test
  public void testMakeQualifiedRoot() {
    GoogleHadoopFileSystem myGhfs = (GoogleHadoopFileSystem) ghfs;
    myGhfs.setWorkingDirectory(new Path(ghfs.getUri()));
    URI fsRootUri = ghfs.getUri();
    String fsRoot = fsRootUri.toString();
    Map<String, String> qualifiedPaths = new HashMap<>();
    qualifiedPaths.put("/", fsRoot);
    qualifiedPaths.put("/foo", fsRoot + "foo");
    qualifiedPaths.put("/foo/bar", fsRoot + "foo/bar");
    qualifiedPaths.put(".", fsRoot);
    qualifiedPaths.put("foo", fsRoot + "foo");
    qualifiedPaths.put("foo/bar", fsRoot + "foo/bar");
    qualifiedPaths.put(fsRoot, fsRoot);
    qualifiedPaths.put(fsRoot + "foo", fsRoot + "foo");
    qualifiedPaths.put(fsRoot + "foo/bar", fsRoot + "foo/bar");
    qualifiedPaths.put("/foo/../foo", fsRoot + "foo");
    qualifiedPaths.put("/foo/bar/../../foo/bar", fsRoot + "foo/bar");
    qualifiedPaths.put("foo/../foo", fsRoot + "foo");
    qualifiedPaths.put("foo/bar/../../foo/bar", fsRoot + "foo/bar");
    qualifiedPaths.put(fsRoot + "foo/../foo", fsRoot + "foo");
    qualifiedPaths.put(fsRoot + "foo/bar/../../foo/bar", fsRoot + "foo/bar");
    qualifiedPaths.put(fsRoot + "..foo/bar", fsRoot + "..foo/bar");
    qualifiedPaths.put("..foo/bar", fsRoot + "..foo/bar");

    // GHFS specific behavior where root is its own parent.
    qualifiedPaths.put("/..", fsRoot);
    qualifiedPaths.put("/../../..", fsRoot);
    qualifiedPaths.put("/../foo/", fsRoot + "foo");
    qualifiedPaths.put("/../../../foo/bar", fsRoot + "foo/bar");
    qualifiedPaths.put("..", fsRoot);
    qualifiedPaths.put("../..", fsRoot);
    qualifiedPaths.put("../foo", fsRoot + "foo");
    qualifiedPaths.put("../foo/bar", fsRoot + "foo/bar");
    qualifiedPaths.put(fsRoot + "..", fsRoot);
    qualifiedPaths.put(fsRoot + "../foo", fsRoot + "foo");
    qualifiedPaths.put(fsRoot + "../foo/bar", fsRoot + "foo/bar");
    qualifiedPaths.put("../../../foo/bar/../../foo/bar", fsRoot + "foo/bar");
    qualifiedPaths.put("../foo/../foo", fsRoot + "foo");
    qualifiedPaths.put("../foo/bar/../../foo/bar", fsRoot + "foo/bar");
    qualifiedPaths.put(fsRoot + "../foo/../foo", fsRoot + "foo");
    qualifiedPaths.put(fsRoot + "../foo/bar/../../foo/bar", fsRoot + "foo/bar");
    qualifiedPaths.put(fsRoot + "foo/../../../../foo", fsRoot + "foo");
    qualifiedPaths.put(fsRoot + "foo/bar/../../../../../foo/bar", fsRoot + "foo/bar");

    // Skip for authority-less gsg paths.
    if (fsRootUri.getAuthority() != null) {
      // When the path to qualify is of the form gs://somebucket, we want to qualify
      // it as gs://someBucket/
      qualifiedPaths.put(fsRoot.substring(0, fsRoot.length() - 1), fsRoot);
    }

    for (String unqualifiedString : qualifiedPaths.keySet()) {
      Path unqualifiedPath = new Path(unqualifiedString);
      Path qualifiedPath = new Path(qualifiedPaths.get(unqualifiedString));
      assertThat(qualifiedPath).isEqualTo(myGhfs.makeQualified(unqualifiedPath));
    }
  }

  @Test
  public void CopyFromLocalFileIOStatisticsTest() throws IOException {
    // Temporary file in GHFS.
    URI tempFileUri = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path tempFilePath = ghfsHelper.castAsHadoopPath(tempFileUri);
    Path tempDirPath = tempFilePath.getParent();
    String text = "Hello World!";
    ghfsHelper.writeFile(tempFilePath, text, 1, /* overwrite= */ false);

    // Temporary file in local FS.
    File localTempFile = File.createTempFile("ghfs-test-", null);
    Path localTempFilePath = new Path(localTempFile.getPath());

    // Test the IOStatitsics of copyFromLocalFile(delSrc,overwrite,src,dst)
    ghfs.copyFromLocalFile(false, true, localTempFilePath, tempDirPath);

    assertThat(
            ((GoogleHadoopFileSystem) ghfs)
                .getIOStatistics()
                .counters()
                .get(INVOCATION_COPY_FROM_LOCAL_FILE.getSymbol()))
        .isEqualTo(1);

    // Test the IOStatitsics of copyFromLocalFile(delSrc,overwrite,[] srcs,dst)
    ghfs.copyFromLocalFile(false, true, new Path[] {localTempFilePath}, tempDirPath);
    assertThat(
            ((GoogleHadoopFileSystem) ghfs)
                .getIOStatistics()
                .counters()
                .get(INVOCATION_COPY_FROM_LOCAL_FILE.getSymbol()))
        .isEqualTo(2);

    if (localTempFile.exists()) {
      localTempFile.delete();
    }
  }

  /**
   * We override certain methods in FileSystem simply to provide debug tracing. (Search for
   * "Overridden functions for debug tracing" in GoogleHadoopFileSystem.java). We do not add or
   * update any functionality for such methods. The following tests simply exercise that path to
   * ensure coverage. Consequently, they do not really test any functionality.
   *
   * <p>Having coverage for these methods lets us easily determine the amount of coverage that is
   * missing in the rest of the code.
   */
  @Test
  public void provideCoverageForUnmodifiedMethods() throws IOException {
    // -------------------------------------------------------
    // Create test data.

    // Temporary file in GHFS.
    URI tempFileUri = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path tempFilePath = ghfsHelper.castAsHadoopPath(tempFileUri);
    Path tempDirPath = tempFilePath.getParent();
    String text = "Hello World!";
    ghfsHelper.writeFile(tempFilePath, text, 1, /* overwrite= */ false);

    // Another temporary file in GHFS.
    URI tempFileUri2 = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path tempFilePath2 = ghfsHelper.castAsHadoopPath(tempFileUri2);

    // Temporary file in local FS.
    File localTempFile = File.createTempFile("ghfs-test-", null);
    File localCopiedFile = null;
    Path localTempFilePath = new Path(localTempFile.getPath());
    Path localTempDirPath = localTempFilePath.getParent();

    // -------------------------------------------------------
    // Call methods to provide coverage for. Note that we do not attempt to
    // test their functionality as we are not testing Hadoop engine here.
    try {
      ghfs.deleteOnExit(tempFilePath);
      ghfs.getContentSummary(tempFilePath);
      ghfs.getDelegationToken("foo");
      ghfs.copyFromLocalFile(false, true, localTempFilePath, tempDirPath);
      ghfs.copyFromLocalFile(false, true, new Path[] {localTempFilePath}, tempDirPath);
      assertThat(localTempFile.delete()).isTrue();
      ghfs.copyToLocalFile(true, tempFilePath, localTempDirPath);
      localCopiedFile = new File(localTempDirPath.toString(), tempFilePath.getName());
      assertThat(localCopiedFile.delete()).isTrue();
      Path localOutputPath = ghfs.startLocalOutput(tempFilePath2, localTempFilePath);
      try (Writer writer = Files.newBufferedWriter(Paths.get(localOutputPath.toString()), UTF_8)) {
        writer.write(text);
      }
      ghfs.completeLocalOutput(tempFilePath2, localOutputPath);
      ghfs.getUsed();
      ghfs.setVerifyChecksum(false);
      ghfs.getFileChecksum(tempFilePath2);
      ghfs.setPermission(tempFilePath2, FsPermission.getDefault());
      try {
        ghfs.setOwner(tempFilePath2, "foo-user", "foo-group");
      } catch (IOException ioe) {
        // Some filesystems (like the LocalFileSystem) are strict about existence of owners.
        // TODO(user): Abstract out the behaviors around owners/permissions and properly test
        //  the different behaviors between different filesystems.
      }
      ghfs.setTimes(tempFilePath2, 0, 0);
    } finally {
      // We do not need to separately delete the temp files created in GHFS because
      // we delete all test buckets recursively at the end of the tests.
      if (localTempFile.exists()) {
        assertThat(localTempFile.delete()).isTrue();
      }
      if (localCopiedFile != null && localCopiedFile.exists()) {
        assertThat(localCopiedFile.delete()).isTrue();
      }
    }
  }
}
