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

package com.google.cloud.hadoop.gcsio;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertThrows;

import com.google.api.client.auth.oauth2.Credential;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.Fadvise;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper;
import com.google.cloud.hadoop.gcsio.testing.TestConfiguration;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.flogger.GoogleLogger;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.model.Statement;

/** Integration tests for GoogleCloudStorageFileSystem class. */
@RunWith(JUnit4.class)
public class GoogleCloudStorageFileSystemIntegrationTest {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  // hack to make tests pass until JUnit 4.13 regression will be fixed:
  // https://github.com/junit-team/junit4/issues/1509
  // TODO: refactor or delete this hack
  public static class NotInheritableExternalResource extends ExternalResource {
    private final Class<?> testClass;

    public NotInheritableExternalResource(Class<?> testClass) {
      this.testClass = testClass;
    }

    @Override
    public Statement apply(Statement base, Description description) {
      if (testClass.equals(description.getTestClass())) {
        return super.apply(base, description);
      }
      return base;
    }

    @Override
    public void before() throws Throwable {}

    @Override
    public void after() {}
  }

  // GCS FS test access instance.
  protected static GoogleCloudStorageFileSystem gcsfs;

  // GCS instance used for cleanup
  protected static GoogleCloudStorage gcs;

  protected static GoogleCloudStorageFileSystemIntegrationHelper gcsiHelper;

  // Time when test started. Used for determining which objects got
  // created after the test started.
  protected static Instant testStartTime;

  protected static String sharedBucketName1;
  protected static String sharedBucketName2;

  // Name of the test object.
  protected static String objectName = "gcsio-test.txt";

  protected static final int UPLOAD_CHUNK_SIZE_DEFAULT = 64 * 1024 * 1024;

  @ClassRule
  public static NotInheritableExternalResource storageResource =
      new NotInheritableExternalResource(GoogleCloudStorageFileSystemIntegrationTest.class) {
        /** Perform initialization once before tests are run. */
        @Override
        public void before() throws Throwable {
          if (gcsfs == null) {
            Credential credential = GoogleCloudStorageTestHelper.getCredential();
            String appName = GoogleCloudStorageIntegrationHelper.APP_NAME;
            String projectId = TestConfiguration.getInstance().getProjectId();
            assertThat(projectId).isNotNull();

            GoogleCloudStorageFileSystemOptions.Builder optionsBuilder =
                GoogleCloudStorageFileSystemOptions.builder()
                    .setMarkerFilePattern("_(FAILURE|SUCCESS)");

            optionsBuilder
                .setBucketDeleteEnabled(true)
                .setCloudStorageOptions(
                    GoogleCloudStorageOptions.builder()
                        .setAppName(appName)
                        .setProjectId(projectId)
                        .setWriteChannelOptions(
                            AsyncWriteChannelOptions.builder()
                                .setUploadChunkSize(UPLOAD_CHUNK_SIZE_DEFAULT)
                                .build())
                        .build());

            gcsfs = new GoogleCloudStorageFileSystem(credential, optionsBuilder.build());

            gcs = gcsfs.getGcs();

            postCreateInit();
          }
        }

        /** Perform clean-up once after all tests are turn. */
        @Override
        public void after() {
          if (gcs != null) {
            gcsiHelper.afterAllTests();
            gcsiHelper = null;
          }
          if (gcsfs != null) {
            gcsfs.close();
            gcsfs = null;
          }
        }
      };

  public static void postCreateInit() throws IOException {
    postCreateInit(new GoogleCloudStorageFileSystemIntegrationHelper(gcsfs));
  }

  /**
   * Perform initialization after creating test instances.
   */
  public static void postCreateInit(
      GoogleCloudStorageFileSystemIntegrationHelper helper)
      throws IOException {
    testStartTime = Instant.now();

    gcsiHelper = helper;
    gcsiHelper.beforeAllTests();
    sharedBucketName1 = gcsiHelper.sharedBucketName1;
    sharedBucketName2 = gcsiHelper.sharedBucketName2;
  }

  // -----------------------------------------------------------------
  // Overridden methods to ensure that GCS FS functionality is used
  // instead of GCS functionality where applicable.
  // -----------------------------------------------------------------

  /**
   * Actual logic for validating the GoogleCloudStorageFileSystem-specific FileInfo returned by
   * getItemInfo() or listFileInfo().
   */
  private void validateFileInfoInternal(
      String bucketName, String objectName, boolean expectedToExist, FileInfo fileInfo)
      throws IOException {
    assertThat(fileInfo.exists()).isEqualTo(expectedToExist);

    long expectedSize = gcsiHelper.getExpectedObjectSize(objectName, expectedToExist);
    if (expectedSize != Long.MIN_VALUE) {
      assertThat(fileInfo.getSize()).isEqualTo(expectedSize);
    }

    boolean expectedDirectory = (objectName == null) || StringPaths.isDirectoryPath(objectName);
    assertWithMessage("isDirectory for bucketName '%s' objectName '%s'", bucketName, objectName)
        .that(fileInfo.isDirectory())
        .isEqualTo(expectedDirectory);

    if (expectedToExist) {
      Instant currentTime = Instant.now();
      Instant fileCreationTime = Instant.ofEpochMilli(fileInfo.getCreationTime());

      assertWithMessage(
              "stale file? testStartTime: %s, fileCreationTime: %s",
              testStartTime, fileCreationTime)
          .that(fileCreationTime)
          .isAtLeast(testStartTime);
      assertWithMessage(
              "unexpected creation-time: clock skew? currentTime: %s, fileCreationTime: %s",
              currentTime, fileCreationTime)
          .that(fileCreationTime)
          .isAtMost(currentTime);
    } else {
      assertThat(fileInfo.getCreationTime()).isEqualTo(0);
    }

    assertThat(fileInfo.toString()).isNotEmpty();
  }

  /**
   * Validates FileInfo for the given item.
   *
   * <p>See {@link #testGetAndListFileInfo()} for more info.
   *
   * <p>Note: The test initialization code creates objects as text files. Each text file contains
   * name of its associated object. Therefore, size of an object ==
   * objectName.getBytes("UTF-8").length.
   */
  protected void validateGetFileInfo(String bucketName, String objectName, boolean expectedToExist)
      throws IOException {
    URI path = gcsiHelper.getPath(bucketName, objectName);
    FileInfo fileInfo = gcsfs.getFileInfo(path);
    assertThat(fileInfo.getPath()).isEqualTo(path);
    validateFileInfoInternal(bucketName, objectName, expectedToExist, fileInfo);
  }

  /**
   * Validates FileInfo returned by listFileInfo().
   *
   * <p>See {@link #testGetAndListFileInfo()} for more info.
   */
  protected void validateListFileInfo(
      String bucketName,
      String objectNamePrefix,
      boolean pathExpectedToExist,
      String... expectedListedNames)
      throws IOException {

    boolean childPathsExpectedToExist =
        pathExpectedToExist && (expectedListedNames != null);
    boolean listRoot = bucketName == null;

    // Prepare list of expected paths.
    List<URI> expectedPaths = new ArrayList<>();
    // Also maintain a backwards mapping to keep track of which of "expectedListedNames" and
    // "bucketName" is associated with each path, so that we can supply validateFileInfoInternal
    // with the objectName and thus enable it to lookup the internally stored expected size,
    // directory status, etc., of the associated FileStatus.
    Map<URI, String[]> pathToComponents = new HashMap<>();
    if (childPathsExpectedToExist) {
      for (String expectedListedName : expectedListedNames) {
        String[] pathComponents = new String[2];
        if (listRoot) {
          pathComponents[0] = expectedListedName;
          pathComponents[1] = null;
        } else {
          pathComponents[0] = bucketName;
          pathComponents[1] = expectedListedName;
        }
        URI expectedPath = gcsiHelper.getPath(pathComponents[0], pathComponents[1]);
        expectedPaths.add(expectedPath);
        pathToComponents.put(expectedPath, pathComponents);
      }
    }

    // Get list of actual paths.
    URI path = gcsiHelper.getPath(bucketName, objectNamePrefix);
    List<FileInfo> fileInfos;
    if (pathExpectedToExist) {
      fileInfos = gcsfs.listFileInfo(path);
    } else {
      assertThrows(FileNotFoundException.class, () -> gcsfs.listFileInfo(path));
      fileInfos = new ArrayList<>();
    }

    List<URI> actualPaths = new ArrayList<>();
    for (FileInfo fileInfo : fileInfos) {
      assertWithMessage("File exists? : " + fileInfo.getPath())
          .that(fileInfo.exists())
          .isEqualTo(childPathsExpectedToExist);
      if (fileInfo.exists()) {
        actualPaths.add(fileInfo.getPath());
        String[] uriComponents = pathToComponents.get(fileInfo.getPath());
        if (uriComponents != null) {
          // Only do fine-grained validation for the explicitly expected paths.
          validateFileInfoInternal(uriComponents[0], uriComponents[1], true, fileInfo);
        }
      }
    }

    if (listRoot) {
      assertThat(actualPaths).containsAtLeastElementsIn(expectedPaths);
    } else {
      assertThat(actualPaths).containsExactlyElementsIn(expectedPaths);
    }
  }

  // -----------------------------------------------------------------
  // Tests added by this class.
  // -----------------------------------------------------------------

  /**
   * Contains data needed for testing the delete() operation.
   */
  private static class DeleteData {

    // Description of test case.
    String description;

    // Bucket component of the path to delete.
    String bucketName;

    // Object component of the path to delete.
    String objectName;

    // Indicates whether it is a recursive delete.
    boolean recursive;

    // Expected outcome; can return true, return false, or return exception of a certain type.
    MethodOutcome expectedOutcome;

    // Objects expected to exist after the operation.
    List<String> objectsExpectedToExist;

    // Objects expected to be deleted after the operation.
    List<String> objectsExpectedToBeDeleted;

    /**
     * Constructs an instance of the DeleteData class.
     */
    DeleteData(
        String description,
        String bucketName, String objectName, boolean recursive,
        MethodOutcome expectedOutcome,
        List<String> objectsExpectedToExist,
        List<String> objectsExpectedToBeDeleted) {

      this.description = description;
      this.bucketName = bucketName;
      this.objectName = objectName;
      this.recursive = recursive;
      this.expectedOutcome = expectedOutcome;
      this.objectsExpectedToExist = objectsExpectedToExist;
      this.objectsExpectedToBeDeleted = objectsExpectedToBeDeleted;
    }
  }

  /** Validates delete(). */
  @Test
  public void testDelete() throws Exception {
    deleteHelper(new DeletionBehavior() {
      @Override
      public MethodOutcome nonEmptyDeleteOutcome() {
        // GCSFS throws IOException on non-recursive delete of non-empty dir.
        return new MethodOutcome(
            MethodOutcome.Type.THROWS_EXCEPTION, IOException.class);
      }

      @Override
      public MethodOutcome nonExistentDeleteOutcome() {
        // GCSFS throws FileNotFoundException if deleting a non-existent file.
        return new MethodOutcome(
            MethodOutcome.Type.THROWS_EXCEPTION, FileNotFoundException.class);
      }
    });
  }

  /**
   * Tests {@link GoogleCloudStorageFileSystem#getFileInfo(URI)} and {@link
   * GoogleCloudStorageFileSystem#listFileInfo(URI)}.
   *
   * <p>The data required for the 2 tests is expensive to create therefore we combine the tests into
   * one.
   */
  @Test
  public void testGetAndListFileInfo() throws Exception {

    // Objects created for this test.
    String[] objectNames = {
        "o1",
        "o2",
        "d0/",
        "d1/o11",
        "d1/o12",
        "d1/d10/",
        "d1/d11/o111",
        "d2/o21",
        "d2/o22",
    };

    String dirDoesNotExist = "does-not-exist/";
    String objDoesNotExist = "does-not-exist";

    // -------------------------------------------------------
    // Create test objects.
    String tempTestBucket = gcsiHelper.createUniqueBucket("list");
    gcsiHelper.createObjectsWithSubdirs(tempTestBucket, objectNames);

    // -------------------------------------------------------
    // Tests for getItemInfo().
    // -------------------------------------------------------

    // Verify that getItemInfo() returns correct info for each object.
    for (String objectName : objectNames) {
      validateGetFileInfo(tempTestBucket, objectName, true);
    }

    // Verify that getItemInfo() returns correct info for bucket.
    validateGetFileInfo(tempTestBucket, null, true);

    // Verify that getItemInfo() returns correct info for a non-existent object.
    validateGetFileInfo(tempTestBucket, dirDoesNotExist, false);

    // Verify that getItemInfo() returns correct info for a non-existent bucket.
    validateGetFileInfo(tempTestBucket, objDoesNotExist, false);

    // -------------------------------------------------------
    // Tests for listFileInfo().
    // -------------------------------------------------------

    // Verify that listFileInfo() returns correct result for each case below.

    // At root.
    validateListFileInfo(tempTestBucket, null, true, "o1", "o2", "d0/", "d1/", "d2/");
    validateListFileInfo(tempTestBucket, "", true, "o1", "o2", "d0/", "d1/", "d2/");

    // At d0.
    validateListFileInfo(tempTestBucket, "d0/", true);

    // At o1.
    validateListFileInfo(tempTestBucket, "o1", true, "o1");

    // TODO(user) : bug in GCS? fails only when running gcsfs tests?
    // validateListFileInfo(bucketName, "d0", true, "d0/");

    // At d1.
    validateListFileInfo(tempTestBucket, "d1/", true, "d1/o11", "d1/o12", "d1/d10/", "d1/d11/");

    // TODO(user) : bug in GCS? fails only when running gcsfs tests?
    // validateListFileInfo(bucketName, "d1", true, "d1/");

    // At d1/d11.
    validateListFileInfo(tempTestBucket, "d1/d11/", true, "d1/d11/o111");

    // TODO(user) : bug in GCS? fails only when running gcsfs tests?
    // validateListFileInfo(bucketName, "d1/d11", true, "d1/d11/");

    // At d2.
    validateListFileInfo(tempTestBucket, "d2/", true, "d2/o21", "d2/o22");

    // TODO(user) : bug in GCS? fails only when running gcsfs tests?
    // validateListFileInfo(bucketName, "d2", true, "d2/");

    // At non-existent path.
    validateListFileInfo(tempTestBucket, dirDoesNotExist, false);
    validateListFileInfo(tempTestBucket, objDoesNotExist, false);
    validateListFileInfo("gcsio-test-bucket-" + objDoesNotExist, objDoesNotExist, false);

    validateListFileInfo(null, null, true, sharedBucketName1, sharedBucketName2, tempTestBucket);
  }

  @Test @SuppressWarnings("EqualsIncompatibleType")
  public void testGoogleCloudStorageItemInfoNegativeEquality() {
    // Assert that .equals with an incorrect type returns false and does not throw.
    assertThat(!GoogleCloudStorageItemInfo.ROOT_INFO.equals("non-item-info")).isTrue();
  }

  /**
   * Validates simple write and read operations.
   */
  @Test
  public void testWriteAndReadObject()
      throws IOException {
    String bucketName = sharedBucketName1;
    String message = "Hello world!\n";

    // Write an object.
    int numBytesWritten = gcsiHelper.writeTextFile(
        bucketName, objectName, message);

    // Read the whole object.
    String message2 = gcsiHelper.readTextFile(
        bucketName, objectName, 0, numBytesWritten, true);

    // Verify we read what we wrote.
    assertThat(message2).isEqualTo(message);
  }

  /**
   * Validates partial reads.
   */
  @Test
  public void testReadPartialObject()
      throws IOException {
    String bucketName = sharedBucketName1;
    String message = "Hello world!\n";

    // Write an object.
    gcsiHelper.writeTextFile(bucketName, objectName, message);

    // Read the whole object in 2 parts.
    int offset = 6;  // chosen arbitrarily.
    String message1 = gcsiHelper.readTextFile(
        bucketName, objectName, 0, offset, false);
    String message2 = gcsiHelper.readTextFile(
        bucketName, objectName, offset, message.length() - offset, true);

    // Verify we read what we wrote.
    assertWithMessage("partial read mismatch")
        .that(message1)
        .isEqualTo(message.substring(0, offset));
    assertWithMessage("partial read mismatch").that(message2).isEqualTo(message.substring(offset));
  }

  @Test
  public void read_failure_ifObjectWasModifiedDuringRead() throws IOException {
    URI testObject = gcsiHelper.getUniqueObjectUri("generation-strict");
    String message1 = "Hello world!\n";
    String message2 = "Sayonara world!\n";

    gcsiHelper.writeTextFile(testObject, message1);
    int offset = 5;
    // These read options force the readChannel to open stream again on second read.
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder()
            .setFadvise(Fadvise.RANDOM)
            .setMinRangeRequestSize(0)
            .build();
    try (SeekableByteChannel readChannel = gcsiHelper.open(testObject, readOptions)) {
      String read1 = gcsiHelper.readText(readChannel, 0, offset, false);
      assertWithMessage("partial read mismatch")
          .that(read1)
          .isEqualTo(message1.substring(0, offset));
      gcsiHelper.writeTextFileOverwriting(testObject, message2);
      FileNotFoundException expected =
          assertThrows(
              FileNotFoundException.class,
              () -> gcsiHelper.readText(readChannel, offset, message1.length() - offset, true));
      assertThat(expected)
          .hasMessageThat()
          .contains(
              "Note, it is possible that the live version is still available"
                  + " but the requested generation is deleted.");
    }
  }

  /** Validates that we cannot open a non-existent object. */
  @Test
  public void testOpenNonExistentObject() throws IOException {
    String bucketName = sharedBucketName1;
    assertThrows(
        FileNotFoundException.class,
        () -> gcsiHelper.readTextFile(bucketName, objectName + "_open-non-existent", 0, 100, true));
  }

  /** Validates that we cannot open an object in non-existent bucket. */
  @Test
  public void testOpenInNonExistentBucket() throws IOException {
    String bucketName = gcsiHelper.getUniqueBucketName("open-non-existent");
    assertThrows(
        FileNotFoundException.class,
        () -> gcsiHelper.readTextFile(bucketName, objectName, 0, 100, true));
  }

  /** Validates delete(). */
  public void deleteHelper(DeletionBehavior behavior) throws Exception {
    String bucketName = sharedBucketName1;

    // Objects created for this test.
    String[] objectNames = {
      "f1",
      "d0/",
      "d1/f1",
      "d1/d0/",
      "d1/d11/f1",
    };

    // -------------------------------------------------------
    // Create test objects.
    gcsiHelper.clearBucket(bucketName);
    gcsiHelper.createObjectsWithSubdirs(bucketName, objectNames);

    // The same set of objects are also created under a bucket that
    // we will delete as a part of the test.
    String tempBucket = gcsiHelper.createUniqueBucket("delete");
    gcsiHelper.createObjectsWithSubdirs(tempBucket, objectNames);

    // -------------------------------------------------------
    // Initialize test data.
    List<DeleteData> deleteData = new ArrayList<>();
    String doesNotExist = "does-not-exist";
    String dirDoesNotExist = "does-not-exist";

    // Delete an item that does not exist.
    deleteData.add(new DeleteData(
        "Delete an object that does not exist: file",
        bucketName, doesNotExist, false,
        behavior.nonExistentDeleteOutcome(),  // expected outcome
        null,  // expected to exist
        null));  // expected to be deleted
    deleteData.add(new DeleteData(
        "Delete an object that does not exist: dir",
        bucketName, dirDoesNotExist, false,
        behavior.nonExistentDeleteOutcome(),  // expected outcome
        null,  // expected to exist
        null));  // expected to be deleted
    deleteData.add(new DeleteData(
        "Delete a bucket that does not exist",
        doesNotExist, doesNotExist, false,
        behavior.nonExistentDeleteOutcome(),  // expected outcome
        null,  // expected to exist
        null));  // expected to be deleted

    // Delete an empty directory.
    deleteData.add(new DeleteData(
        "Delete an empty directory",
        bucketName, "d0/", true,
        new MethodOutcome(MethodOutcome.Type.RETURNS_TRUE),  // expected outcome
        null,  // expected to exist
        Lists.newArrayList("d0/")));  // expected to be deleted

    // Delete a non-empty directory (recursive == false).
    deleteData.add(new DeleteData(
        "Delete a non-empty directory (recursive == false)",
        bucketName, "d1/", false,
        behavior.nonEmptyDeleteOutcome(),  // expected outcome
        Lists.newArrayList("d1/", "d1/f1", "d1/d0/", "d1/d11/f1"),  // expected to exist
        null));  // expected to be deleted

    // Delete a non-empty directory (recursive == true).
    deleteData.add(new DeleteData(
        "Delete a non-empty directory (recursive == true)",
        bucketName, "d1/", true,
        new MethodOutcome(MethodOutcome.Type.RETURNS_TRUE),  // expected outcome
        null,  // expected to exist
        Lists.newArrayList("d1/", "d1/f1", "d1/d0/", "d1/d11/f1")));  // expected to be deleted

    // Delete a non-empty bucket (recursive == false).
    deleteData.add(new DeleteData(
        "Delete a non-empty bucket (recursive == false)",
        tempBucket, null, false,
        behavior.nonEmptyDeleteOutcome(),  // expected outcome
        Lists.newArrayList(
            // expected to exist
            "f1", "d0/", "d1/", "d1/f1", "d1/d0/", "d1/d11/f1"),
        null));  // expected to be deleted

    // Delete a non-empty bucket (recursive == true).
    deleteData.add(new DeleteData(
        "Delete a non-empty bucket (recursive == true)",
        tempBucket, null, true,
        new MethodOutcome(MethodOutcome.Type.RETURNS_TRUE),  // expected outcome
        null,  // expected to exist
        Lists.newArrayList(
            // expected to be deleted
            "f1", "d0/", "d1/", "d1/f1", "d1/d0/", "d1/d11/f1")));

    // -------------------------------------------------------
    // Call delete() for each path and verify the expected behavior.
    for (DeleteData dd : deleteData) {

      // Verify that items that we expect to delete are present before the operation.
      assertPathsExist(dd.description, dd.bucketName, dd.objectsExpectedToBeDeleted, true);

      URI path = gcsiHelper.getPath(dd.bucketName, dd.objectName);
      try {

        // Perform the delete operation.
        boolean result = gcsiHelper.delete(path, dd.recursive);

        if (result) {
          assertWithMessage(
                  "Unexpected result for '%s' path: %s :: expected %s, actually returned true.",
                  path, dd.description, dd.expectedOutcome)
              .that(dd.expectedOutcome.getType())
              .isEqualTo(MethodOutcome.Type.RETURNS_TRUE);
        } else {
          assertWithMessage(
                  "Unexpected result for '%s' path: %s :: expected %s, actually returned false.",
                  path, dd.description, dd.expectedOutcome)
              .that(dd.expectedOutcome.getType())
              .isEqualTo(MethodOutcome.Type.RETURNS_FALSE);
        }
      } catch (Exception e) {
        assertWithMessage(
                "Unexpected result for '%s' path: %s :: expected %s, actually threw exception %s",
                path, dd.description, dd.expectedOutcome, Throwables.getStackTraceAsString(e))
            .that(dd.expectedOutcome.getType())
            .isEqualTo(MethodOutcome.Type.THROWS_EXCEPTION);
      }

      // Verify that items that we expect to exist are present.
      assertPathsExist(dd.description, dd.bucketName, dd.objectsExpectedToExist, true);

      // Verify that items that we expect to be deleted are not present.
      assertPathsExist(dd.description, dd.bucketName, dd.objectsExpectedToBeDeleted, false);
    }
  }

  /**
   * Call mkdir then create a file with the same name, not including the trailing slash for the
   * param to mkdir. The create should fail.
   */
  @Test
  public void testMkdirAndCreateFileOfSameName() throws Exception {
    String bucketName = sharedBucketName1;
    String uniqueDirName = "dir-" + UUID.randomUUID();
    gcsiHelper.mkdir(
        bucketName, uniqueDirName + GoogleCloudStorage.PATH_DELIMITER);
    IOException ioe =
        assertThrows(
            IOException.class,
            () -> gcsiHelper.writeTextFile(bucketName, uniqueDirName, "hello world"));
    assertWithMessage("unexpected exception:%n%s", Throwables.getStackTraceAsString(ioe))
        .that(ioe)
        .hasMessageThat()
        .matches(".*(A directory with that name exists|Is a directory|already exists).*");

    gcsiHelper.delete(bucketName, uniqueDirName);
  }

  /** Validates mkdirs(). */
  @Test
  public void testMkdirs() throws Exception {
    mkdirsHelper(new MkdirsBehavior() {
      @Override
      public MethodOutcome mkdirsRootOutcome() {
        return new MethodOutcome(MethodOutcome.Type.RETURNS_TRUE);
      }

      @Override
      public MethodOutcome fileAlreadyExistsOutcome() {
        return new MethodOutcome(MethodOutcome.Type.THROWS_EXCEPTION, IOException.class);
      }
    });
  }

  /** Validates mkdirs(). */
  public void mkdirsHelper(MkdirsBehavior behavior) throws Exception {
    String bucketName = sharedBucketName1;

    // Objects created for this test.
    String[] objectNames = {
      "f1",
      "d0/",
      "d1/f11",
    };

    // -------------------------------------------------------
    // Create test objects.
    gcsiHelper.clearBucket(bucketName);
    gcsiHelper.createObjectsWithSubdirs(bucketName, objectNames);

    // -------------------------------------------------------
    // Initialize test data.
    // key == directory path to pass to mkdirs()
    // val == Expected outcome
    Map<URI, MethodOutcome> dirData = new HashMap<>();

    // Verify that attempt to create root dir does not throw (no-op).
    dirData.put(GoogleCloudStorageFileSystem.GCS_ROOT, behavior.mkdirsRootOutcome());

    // Verify that no exception is thrown when directory already exists.
    dirData.put(gcsiHelper.getPath(bucketName, "d0/"),
        new MethodOutcome(MethodOutcome.Type.RETURNS_TRUE));
    dirData.put(gcsiHelper.getPath(bucketName, "d0"),
        new MethodOutcome(MethodOutcome.Type.RETURNS_TRUE));

    // Expect IOException if a file with the given name already exists.
    dirData.put(gcsiHelper.getPath(bucketName, "f1/"),
        behavior.fileAlreadyExistsOutcome());
    dirData.put(gcsiHelper.getPath(bucketName, "d1/f11/d3/"),
        behavior.fileAlreadyExistsOutcome());

    // Some intermediate directories exist (but not all).
    dirData.put(gcsiHelper.getPath(bucketName, "d1/d2/d3/"),
                new MethodOutcome(MethodOutcome.Type.RETURNS_TRUE));

    // No intermediate directories exist.
    dirData.put(gcsiHelper.getPath(bucketName, "dA/dB/dC/"),
                new MethodOutcome(MethodOutcome.Type.RETURNS_TRUE));

    // Trying to create the same dirs again is a no-op.
    dirData.put(gcsiHelper.getPath(bucketName, "dA/dB/dC/"),
                new MethodOutcome(MethodOutcome.Type.RETURNS_TRUE));

    // Make paths that include making a top-level directory (bucket).
    String uniqueBucketName = gcsiHelper.getUniqueBucketName("mkdir-1");
    dirData.put(gcsiHelper.getPath(uniqueBucketName, null),
                new MethodOutcome(MethodOutcome.Type.RETURNS_TRUE));

    // Create the same bucket again, should be no-op.
    dirData.put(gcsiHelper.getPath(uniqueBucketName, null),
                new MethodOutcome(MethodOutcome.Type.RETURNS_TRUE));

    // Make a path where the bucket is a non-existent parent directory.
    String uniqueBucketName2 = gcsiHelper.getUniqueBucketName("mkdir-2");
    dirData.put(
        gcsiHelper.getPath(uniqueBucketName2, "foo/bar"),
        new MethodOutcome(
            gcsiHelper.getClass().getSimpleName().equals("HadoopFileSystemIntegrationHelper")
                ? MethodOutcome.Type.RETURNS_TRUE
                : MethodOutcome.Type.THROWS_EXCEPTION));

    // Call mkdirs() for each path and verify the expected behavior.
    for (URI path : dirData.keySet()) {
      MethodOutcome expectedOutcome = dirData.get(path);
      try {
        boolean result = gcsiHelper.mkdirs(path);
        if (result) {
          assertWithMessage(
                  "Unexpected result for path: '%s' : expected %s, actually returned true.",
                  path, expectedOutcome)
              .that(expectedOutcome.getType())
              .isEqualTo(MethodOutcome.Type.RETURNS_TRUE);

          // Assert that all of the sub-dirs have been created.
          List<URI> subDirPaths = getSubDirPaths(path);
          for (URI subDirPath : subDirPaths) {
            assertWithMessage("Sub-path '%s' of path '%s' not found or not a dir", subDirPath, path)
                .that(gcsiHelper.exists(subDirPath) && gcsiHelper.isDirectory(subDirPath))
                .isTrue();
          }
        } else {
          assertWithMessage(
                  "Unexpected result for path: '%s' : expected '%s', actually returned false.",
                  path, expectedOutcome)
              .that(expectedOutcome.getType())
              .isEqualTo(MethodOutcome.Type.RETURNS_FALSE);
        }
      } catch (Exception e) {
        assertWithMessage(
                "Unexpected result for path: '%s' : expected '%s', actually threw exception %s.",
                path, expectedOutcome, Throwables.getStackTraceAsString(e))
            .that(expectedOutcome.getType())
            .isEqualTo(MethodOutcome.Type.THROWS_EXCEPTION);
      }
    }
  }

  /** Validates getFileInfos(). */
  @Test
  public void testGetFileInfos() throws Exception {
    String bucketName = sharedBucketName1;
    // Objects created for this test.
    String[] objectNames = {
      "f1",
      "d0/",
    };

    // -------------------------------------------------------
    // Create test objects.
    gcsiHelper.clearBucket(bucketName);
    gcsiHelper.createObjectsWithSubdirs(bucketName, objectNames);

    List<URI> pathsToGet = new ArrayList<>();
    // Mix up the types of the paths to ensure the method will return the values in the same order
    // as their respective input parameters regardless of whether some are ROOT, directories, etc.
    pathsToGet.add(gcsiHelper.getPath(bucketName, "nonexistent"));
    pathsToGet.add(gcsiHelper.getPath(bucketName, "f1"));
    pathsToGet.add(gcsiHelper.getPath(null, null));
    pathsToGet.add(gcsiHelper.getPath(bucketName, "d0"));
    pathsToGet.add(gcsiHelper.getPath(bucketName, null));

    List<FileInfo> fileInfos = gcsfs.getFileInfos(pathsToGet);

    // First one doesn't exist.
    assertThat(fileInfos.get(0).exists()).isFalse();
    assertThat(fileInfos.get(0).getItemInfo().getResourceId())
        .isEqualTo(new StorageResourceId(bucketName, "nonexistent"));

    // Second one exists and is a StorageObject.
    assertThat(fileInfos.get(1).exists()).isTrue();
    assertThat(fileInfos.get(1).getItemInfo().getResourceId().isStorageObject()).isTrue();
    assertThat(fileInfos.get(1).getItemInfo().getResourceId())
        .isEqualTo(new StorageResourceId(bucketName, "f1"));

    // Third one exists and is root.
    assertThat(fileInfos.get(2).exists()).isTrue();
    assertThat(fileInfos.get(2).isGlobalRoot()).isTrue();

    // Fourth one exists, but had to be auto-converted into a directory path.
    assertThat(fileInfos.get(3).exists()).isTrue();
    assertThat(fileInfos.get(3).isDirectory()).isTrue();
    assertThat(fileInfos.get(3).getItemInfo().getResourceId().isStorageObject()).isTrue();
    assertThat(fileInfos.get(3).getItemInfo().getResourceId())
        .isEqualTo(new StorageResourceId(bucketName, "d0/"));

    // Fifth one is a bucket.
    assertThat(fileInfos.get(4).exists()).isTrue();
    assertThat(fileInfos.get(4).isDirectory()).isTrue();
    assertThat(fileInfos.get(4).getItemInfo().getResourceId().isBucket()).isTrue();
    assertThat(fileInfos.get(4).getItemInfo().getResourceId())
        .isEqualTo(new StorageResourceId(bucketName));
  }

  /**
   * Contains data needed for testing the rename() operation.
   */
  private static class RenameData {

    // Description of test case.
    String description;

    // Bucket component of the src path.
    String srcBucketName;

    // Object component of the src path.
    String srcObjectName;

    // Bucket component of the dst path.
    String dstBucketName;

    // Object component of the dst path.
    String dstObjectName;

    // Expected outcome; can return true, return false, or return exception of a certain type.
    MethodOutcome expectedOutcome;

    // Objects expected to exist in src bucket after the operation.
    List<String> objectsExpectedToExistSrc;

    // Objects expected to exist in dst bucket after the operation.
    List<String> objectsExpectedToExistDst;

    // Objects expected to be deleted after the operation.
    List<String> objectsExpectedToBeDeleted;

    /**
     * Constructs an instance of the RenameData class.
     */
    RenameData(String description,
        String srcBucketName, String srcObjectName,
        String dstBucketName, String dstObjectName,
        MethodOutcome expectedOutcome,
        List<String> objectsExpectedToExistSrc,
        List<String> objectsExpectedToExistDst,
        List<String> objectsExpectedToBeDeleted) {

      this.description = description;
      this.srcBucketName = srcBucketName;
      this.srcObjectName = srcObjectName;
      this.dstBucketName = dstBucketName;
      this.dstObjectName = dstObjectName;
      this.expectedOutcome = expectedOutcome;
      this.objectsExpectedToExistSrc = objectsExpectedToExistSrc;
      this.objectsExpectedToExistDst = objectsExpectedToExistDst;
      this.objectsExpectedToBeDeleted = objectsExpectedToBeDeleted;
    }
  }

  /** Validates rename(). */
  @Test
  public void testRename() throws Exception {
    renameHelper(new RenameBehavior() {
      @Override
      public MethodOutcome renameFileIntoRootOutcome() {
        // GCSFS throws IOException on rename into root.
        return new MethodOutcome(
            MethodOutcome.Type.THROWS_EXCEPTION, IOException.class);
      }

      @Override
      public MethodOutcome renameRootOutcome() {
        // GCSFS throws IllegalArgumentException on rename of root.
        return new MethodOutcome(
            MethodOutcome.Type.THROWS_EXCEPTION, IllegalArgumentException.class);
      }

      @Override
      public MethodOutcome nonExistentSourceOutcome() {
        // GCSFS throws FileNotFoundException on nonexistent src.
        return new MethodOutcome(
            MethodOutcome.Type.THROWS_EXCEPTION, FileNotFoundException.class);
      }

      @Override
      public MethodOutcome destinationFileExistsSrcIsFileOutcome() {
        // GCSFS throws IOException if dst already exists, is a file, and src is also a file.
        return new MethodOutcome(
            MethodOutcome.Type.THROWS_EXCEPTION, IOException.class);
      }

      @Override
      public MethodOutcome destinationFileExistsSrcIsDirectoryOutcome() {
        // GCSFS throws IOException if dst already exists, is a file, and src is a directory.
        return new MethodOutcome(
            MethodOutcome.Type.THROWS_EXCEPTION, IOException.class);
      }

      @Override
      public MethodOutcome nonExistentDestinationFileParentOutcome() {
        // GCSFS throws FileNotFoundException if a parent of file dst doesn't exist.
        return new MethodOutcome(
            MethodOutcome.Type.THROWS_EXCEPTION, FileNotFoundException.class);
      }

      @Override
      public MethodOutcome nonExistentDestinationDirectoryParentOutcome() {
        // GCSFS throws FileNotFoundException if a parent of directory dst doesn't exist.
        return new MethodOutcome(
            MethodOutcome.Type.THROWS_EXCEPTION, FileNotFoundException.class);
      }
    });
  }

  /** Validates rename(). */
  protected void renameHelper(RenameBehavior behavior) throws Exception {
    String bucketName = sharedBucketName1;
    String otherBucketName = sharedBucketName2;

    String uniqueDir = "dir-" + UUID.randomUUID() + GoogleCloudStorage.PATH_DELIMITER;
    String uniqueFile = uniqueDir + "f1";

    // Objects created for this test.
    String[] objectNames = {
      "f1",
      "f2",
      "d0/",
      "d0-a/",
      "d0-b/",
      "d1/f1",
      "d1/d0/",
      "d1/d11/f1",
      "d1-a/f1",
      "d1-b/f1",
      "d1-c/f1",
      "d1-d/f1",
      "d1-e/f1",
      "d1-f/f1",
      "d1-g/f1",
      "d1-h/f1",
      "d1-i/f1",
      uniqueFile,
      "td0-a/",
      "td0-b/",
      "n1-src/d1/f1",
      "n1-dst/",
      "n2-src/d0/",
      "n2-src/f1",
      "n2-src/d1/f1",
      "n2-src/d2/d21/d211/f1",
      "n2-dst/",
      "n2-dst/f1"
    };

    // Objects created in other bucket for this test.
    String[] otherObjectNames = {
      "td0/"
    };

    // -------------------------------------------------------
    // Create test objects.
    gcsiHelper.clearBucket(bucketName);
    gcsiHelper.clearBucket(otherBucketName);
    gcsiHelper.createObjectsWithSubdirs(bucketName, objectNames);
    gcsiHelper.createObjectsWithSubdirs(otherBucketName, otherObjectNames);

    // -------------------------------------------------------
    // Initialize test data.
    List<RenameData> renameData = new ArrayList<>();
    String doesNotExist = "does-not-exist";
    String dirDoesNotExist = "does-not-exist";

    // TODO(user) : add test case for dst under src (not allowed)

    // src == root.
    renameData.add(new RenameData(
        "src == root",
        null, null,
        otherBucketName, doesNotExist,
        behavior.renameRootOutcome(),  // expected outcome
        null,  // expected to exist in src
        null,  // expected to exist in dst
        null));  // expected to be deleted

    // src does not exist.
    renameData.add(new RenameData(
        "src does not exist: 1",
        bucketName, doesNotExist,
        otherBucketName, doesNotExist,
        behavior.nonExistentSourceOutcome(),  // expected outcome
        null,  // expected to exist in src
        null,  // expected to exist in dst
        null));  // expected to be deleted
    renameData.add(new RenameData(
        "src does not exist: 2",
        bucketName, dirDoesNotExist,
        otherBucketName, dirDoesNotExist,
        behavior.nonExistentSourceOutcome(),  // expected outcome
        null,  // expected to exist in src
        null,  // expected to exist in dst
        null));  // expected to be deleted
    renameData.add(new RenameData(
        "src does not exist: 3",
        doesNotExist, doesNotExist,
        otherBucketName, doesNotExist,
        behavior.nonExistentSourceOutcome(),  // expected outcome
        null,  // expected to exist in src
        null,  // expected to exist in dst
        null));  // expected to be deleted

    // dst is a file that already exists.
    if (behavior.destinationFileExistsSrcIsFileOutcome().getType()
        == MethodOutcome.Type.RETURNS_TRUE) {
      renameData.add(new RenameData(
          "dst is a file that already exists: 1",
          bucketName, "f1",
          bucketName, "f2",
          behavior.destinationFileExistsSrcIsFileOutcome(),  // expected outcome
          null,  // expected to exist in src
          Lists.newArrayList("f2"),  // expected to exist in dst
          Lists.newArrayList("f1")));  // expected to be deleted
    } else {
      renameData.add(new RenameData(
          "dst is a file that already exists: 1",
          bucketName, "f1",
          bucketName, "f2",
          behavior.destinationFileExistsSrcIsFileOutcome(),  // expected outcome
          Lists.newArrayList("f1"),  // expected to exist in src
          Lists.newArrayList("f2"),  // expected to exist in dst
          null));  // expected to be deleted
    }

    renameData.add(new RenameData(
        "dst is a file that already exists: 2",
        bucketName, "d0/",
        bucketName, "f2",
        behavior.destinationFileExistsSrcIsDirectoryOutcome(),  // expected outcome
        Lists.newArrayList("d0/"),  // expected to exist in src
        Lists.newArrayList("f2"),  // expected to exist in dst
        null));  // expected to be deleted

    // Parent of destination does not exist.
    renameData.add(new RenameData(
        "Parent of destination does not exist: 1",
        bucketName, "f1",
        bucketName, "does-not-exist/f1",
        behavior.nonExistentDestinationFileParentOutcome(),  // expected outcome
        null,  // expected to exist in src
        null,  // expected to exist in dst
        null));  // expected to be deleted

    if (behavior.nonExistentDestinationDirectoryParentOutcome().getType()
        == MethodOutcome.Type.RETURNS_TRUE) {
      renameData.add(new RenameData(
          "Parent of destination does not exist: 2",
          bucketName, "d0-b/",
          bucketName, "does-not-exist2/d0-b/",
          behavior.nonExistentDestinationDirectoryParentOutcome(),  // expected outcome
          null,  // expected to exist in src
          Lists.newArrayList("does-not-exist2/d0-b/"),  // expected to exist in dst
          Lists.newArrayList("d0-b/")));  // expected to be deleted
    } else {
      renameData.add(new RenameData(
          "Parent of destination does not exist: 2",
          bucketName, "d0-b/",
          bucketName, "does-not-exist2/d0-b/",
          behavior.nonExistentDestinationDirectoryParentOutcome(),  // expected outcome
          Lists.newArrayList("d0-b/"),  // expected to exist in src
          null,  // expected to exist in dst
          null));  // expected to be deleted
    }


    // This test case fails for LocalFileSystem; it clobbers the destination instead.
    // TODO(user): Make the MethodOutcome able to encompass high-level behaviors.
    renameData.add(new RenameData(
        "destination is a dir that exists and non-empty: 2",
        bucketName, "d1-h/",
        bucketName, "td0-a",
        new MethodOutcome(MethodOutcome.Type.RETURNS_TRUE),  // expected outcome
        Lists.newArrayList("td0-a/", "td0-a/d1-h/", "td0-a/d1-h/f1"),  // expected to exist in src
        null,  // expected to exist in dst
        Lists.newArrayList("d1-h/", "d1-h/f1")));  // expected to be deleted

    // Rename a dir: destination is a dir that does not exist
    renameData.add(new RenameData(
        "destination is a dir that does not exist",
        bucketName, "d1-b/",
        bucketName, "td0-x/",
        new MethodOutcome(MethodOutcome.Type.RETURNS_TRUE),  // expected outcome
        Lists.newArrayList("td0-x/", "td0-x/f1"),  // expected to exist in src
        null,  // expected to exist in dst
        Lists.newArrayList("d1-b/", "d1-b/f1")));  // expected to be deleted

    // Rename a dir: destination is a file that does not exist
    renameData.add(new RenameData(
        "destination is a file that does not exist",
        bucketName, "d1-c/",
        bucketName, "td0-a/df",
        new MethodOutcome(MethodOutcome.Type.RETURNS_TRUE),  // expected outcome
        Lists.newArrayList("td0-a/", "td0-a/df/", "td0-a/df/f1"),  // expected to exist in src
        null,  // expected to exist in dst
        Lists.newArrayList("d1-c/", "d1-c/f1")));  // expected to be deleted

    // Rename a file: destination is a file that does not exist
    renameData.add(new RenameData(
        "destination is a file that does not exist",
        bucketName, "d1-d/f1",
        bucketName, "td0-a/f1-x",
        new MethodOutcome(MethodOutcome.Type.RETURNS_TRUE),  // expected outcome
        Lists.newArrayList("d1-d/", "td0-a/", "td0-a/f1-x"),  // expected to exist in src
        null,  // expected to exist in dst
        Lists.newArrayList("d1-d/f1")));  // expected to be deleted

    // Rename a file: destination is root.
    if (behavior.renameFileIntoRootOutcome().getType() == MethodOutcome.Type.RETURNS_TRUE) {
      // TODO(user): Refactor the way assertPathsExist so that it can check for existence in
      // root as well.
      renameData.add(new RenameData(
          "file : destination is root",
          bucketName, "d1-i/f1",
          null, null,
          behavior.renameFileIntoRootOutcome(),  // expected outcome
          Lists.newArrayList("d1-i/"),  // expected to exist in src
          null,  // expected to exist in dst
          Lists.newArrayList("d1-i/f1")));  // expected to be deleted
    } else {
      renameData.add(new RenameData(
          "file : destination is root",
          bucketName, "d1-i/f1",
          null, null,
          behavior.renameFileIntoRootOutcome(),  // expected outcome
          Lists.newArrayList("d1-i/", "d1-i/f1"),  // expected to exist in src
          null,  // expected to exist in dst
          null));  // expected to be deleted
    }


    // Rename a file: src is a directory with a multi-level sub-directory.
    renameData.add(new RenameData(
        "src is a directory with a multi-level subdirectory; dst is a directory which exists.",
        bucketName, "n1-src/",
        bucketName, "n1-dst/",
        new MethodOutcome(MethodOutcome.Type.RETURNS_TRUE),
        Lists.newArrayList("n1-dst/", "n1-dst/n1-src/d1/", "n1-dst/n1-src/d1/f1"),
        null,
        Lists.newArrayList("n1-src/", "n1-src/d1/", "n1-src/d1/f1")));

    // Rename a file: src is a directory with a multi-level sub-directory.
    // Similar to the previous case but with more levels involved.
    renameData.add(new RenameData(
        "src is a directory with a multi-level subdirectory; dst is a directory which exists - 2",
        bucketName, "n2-src/",
        bucketName, "n2-dst/",
        new MethodOutcome(MethodOutcome.Type.RETURNS_TRUE),
        Lists.newArrayList("n2-dst/", "n2-dst/f1",
            "n2-dst/n2-src/d0/", "n2-dst/n2-src/f1",
            "n2-dst/n2-src/d1/f1", "n2-dst/n2-src/d2/d21/d211/f1"),
        null,
        Lists.newArrayList("n2-src/", "n2-src/d0/", "n2-src/f1",
            "n2-src/d1/f1", "n2-src/d2/d21/d211/f1")));

    // -------------------------------------------------------
    // Call rename() for each path and verify the expected behavior.
    final ExecutorService threadPool = Executors.newCachedThreadPool();

    try {
      // First do a run-through to check existence of starting files.
      final List<Throwable> errorList = new ArrayList<>();
      final CountDownLatch checkStartCounter = new CountDownLatch(renameData.size());
      for (final RenameData rd : renameData) {
        @SuppressWarnings("unused") // go/futurereturn-lsc
        Future<?> possiblyIgnoredError =
            threadPool.submit(
                () -> {
                  try {
                    // Verify that items that we expect to rename are present before the operation
                    assertPathsExist(
                        rd.description, rd.srcBucketName, rd.objectsExpectedToBeDeleted, true);
                  } catch (Throwable t) {
                    synchronized (errorList) {
                      errorList.add(t);
                    }
                  } finally {
                    checkStartCounter.countDown();
                  }
                });
      }

      checkStartCounter.await();

      if (!errorList.isEmpty()) {
        AssertionError error = new AssertionError();
        for (Throwable t : errorList) {
          error.addSuppressed(t);
        }
        throw error;
      }

      // Do a loop to do all the renames.
      final CountDownLatch renameCounter = new CountDownLatch(renameData.size());
      for (final RenameData rd : renameData) {
        @SuppressWarnings("unused") // go/futurereturn-lsc
        Future<?> possiblyIgnoredError =
            threadPool.submit(
                () -> {
                  try {
                    URI src = gcsiHelper.getPath(rd.srcBucketName, rd.srcObjectName);
                    URI dst = gcsiHelper.getPath(rd.dstBucketName, rd.dstObjectName);
                    String desc = src + " -> " + dst;

                    try {
                      // Perform the rename operation.
                      if (gcsiHelper.rename(src, dst)) {
                        assertWithMessage(
                                "Unexpected result for '%s': %s :: expected %s,"
                                    + " actually returned true.",
                                desc, rd.description, rd.expectedOutcome)
                            .that(rd.expectedOutcome.getType())
                            .isEqualTo(MethodOutcome.Type.RETURNS_TRUE);
                      } else {
                        assertWithMessage(
                                "Unexpected result for '%s': %s :: expected %s,"
                                    + " actually returned false.",
                                desc, rd.description, rd.expectedOutcome)
                            .that(rd.expectedOutcome.getType())
                            .isEqualTo(MethodOutcome.Type.RETURNS_FALSE);
                      }
                    } catch (Exception e) {
                      assertWithMessage(
                              "Unexpected result for '%s': %s :: expected %s, actually threw %s.",
                              desc,
                              rd.description,
                              rd.expectedOutcome,
                              Throwables.getStackTraceAsString(e))
                          .that(rd.expectedOutcome.getType())
                          .isEqualTo(MethodOutcome.Type.THROWS_EXCEPTION);
                    }
                  } catch (Throwable t) {
                    synchronized (errorList) {
                      errorList.add(t);
                    }
                  } finally {
                    renameCounter.countDown();
                  }
                });
      }

      renameCounter.await();

      if (!errorList.isEmpty()) {
        AssertionError error = new AssertionError();
        for (Throwable t : errorList) {
          error.addSuppressed(t);
        }
        throw error;
      }

      // Finally, check the existence of final destination files.
      final CountDownLatch checkDestCounter = new CountDownLatch(renameData.size());
      for (final RenameData rd : renameData) {
        @SuppressWarnings("unused") // go/futurereturn-lsc
        Future<?> possiblyIgnoredError =
            threadPool.submit(
                () -> {
                  try {
                    URI src = gcsiHelper.getPath(rd.srcBucketName, rd.srcObjectName);

                    // Verify that items that we expect to exist are present.
                    assertPathsExist(
                        rd.description, rd.srcBucketName, rd.objectsExpectedToExistSrc, true);
                    String dstBucketName;
                    if ((rd.dstBucketName == null) && (rd.dstObjectName == null)) {
                      // If both bucket and object names are null that means the destination
                      // of the rename is root path. In that case, the leaf directory
                      // of the source path becomes the destination bucket.
                      String srcDirName = gcsiHelper.getItemName(src);
                      dstBucketName = srcDirName;
                    } else {
                      dstBucketName = rd.dstBucketName;
                    }
                    assertPathsExist(
                        rd.description, dstBucketName, rd.objectsExpectedToExistDst, true);

                    // Verify that items that we expect to be deleted are not present.
                    assertPathsExist(
                        rd.description, rd.srcBucketName, rd.objectsExpectedToBeDeleted, false);
                  } catch (Throwable t) {
                    synchronized (errorList) {
                      errorList.add(t);
                    }
                  } finally {
                    checkDestCounter.countDown();
                  }
                });
      }

        checkDestCounter.await();

      if (!errorList.isEmpty()) {
        AssertionError error = new AssertionError();
        for (Throwable t : errorList) {
          error.addSuppressed(t);
        }
        throw error;
      }
    } finally {
      threadPool.shutdown();
      if (!threadPool.awaitTermination(10L, TimeUnit.SECONDS)) {
        logger.atSevere().log("Failed to awaitTermination! Forcing executor shutdown.");
        threadPool.shutdownNow();
      }
    }
  }

  @Test
  public void testRenameWithContentChecking() throws Exception {
    String bucketName = sharedBucketName1;
    // TODO(user): Split out separate test cases, extract a suitable variant of RenameData to
    // follow same pattern of iterating over subcases.
    String[] fileNames = {
        "test-recursive/oldA/B/file2",
        "test-recursive/oldA/file1",
        "test-flat/oldA/aaa",
        "test-flat/oldA/b"
    };

    // Create the objects; their contents will be their own object names as an ASCII string.
    gcsiHelper.clearBucket(bucketName);
    gcsiHelper.createObjectsWithSubdirs(bucketName, fileNames);

    // Check original file existence.
    String testDescRecursive = "Rename of directory with file1 and subdirectory with file2";
    List<String> originalObjects = ImmutableList.of(
        "test-recursive/",
        "test-recursive/oldA/",
        "test-recursive/oldA/B/",
        "test-recursive/oldA/B/file2",
        "test-recursive/oldA/file1",
        "test-flat/oldA/aaa",
        "test-flat/oldA/b");
    assertPathsExist(testDescRecursive, bucketName, originalObjects, true);

    // Check original file content.
    for (String originalName : fileNames) {
      assertThat(gcsiHelper.readTextFile(bucketName, originalName)).isEqualTo(originalName);
    }

    // Do rename oldA -> newA in test-recursive.
    {
      URI src = gcsiHelper.getPath(bucketName, "test-recursive/oldA");
      URI dst = gcsiHelper.getPath(bucketName, "test-recursive/newA");
      assertThat(gcsiHelper.rename(src, dst)).isTrue();
    }

    // Do rename oldA -> newA in test-flat.
    {
      URI src = gcsiHelper.getPath(bucketName, "test-flat/oldA");
      URI dst = gcsiHelper.getPath(bucketName, "test-flat/newA");
      assertThat(gcsiHelper.rename(src, dst)).isTrue();
    }

    // Check resulting file existence.
    List<String> resultingObjects = ImmutableList.of(
        "test-recursive/",
        "test-recursive/newA/",
        "test-recursive/newA/B/",
        "test-recursive/newA/B/file2",
        "test-recursive/newA/file1",
        "test-flat/newA/aaa",
        "test-flat/newA/b");
    assertPathsExist(testDescRecursive, bucketName, resultingObjects, true);

    // Check resulting file content.
    for (String originalName : fileNames) {
      String resultingName = originalName.replaceFirst("oldA", "newA");
      assertThat(gcsiHelper.readTextFile(bucketName, resultingName)).isEqualTo(originalName);
    }

    // Things which mustn't exist anymore.
    List<String> deletedObjects = ImmutableList.of(
        "test-recursive/oldA/",
        "test-recursive/oldA/B/",
        "test-recursive/oldA/B/file2",
        "test-recursive/oldA/file1",
        "test-flat/oldA/aaa",
        "test-flat/oldA/b");
    assertPathsExist(testDescRecursive, bucketName, deletedObjects, false);
  }

  @Test
  public void testFileCreationSetsAttributes() throws IOException {
    CreateFileOptions createFileOptions =
        CreateFileOptions.builder()
            .setAttributes(ImmutableMap.of("key1", "value1".getBytes(StandardCharsets.UTF_8)))
            .build();

    URI testFilePath = gcsiHelper.getPath(sharedBucketName1, "test-file-creation-attributes.txt");
    try (WritableByteChannel channel =
        gcsfs.create(testFilePath, createFileOptions)) {
      assertThat(channel).isNotNull();
    }

    FileInfo info = gcsfs.getFileInfo(testFilePath);

    assertThat(info.getAttributes()).hasSize(1);
    assertThat(info.getAttributes()).containsKey("key1");
    assertThat(info.getAttributes().get("key1"))
        .isEqualTo("value1".getBytes(StandardCharsets.UTF_8));
  }

  @Test
  public void renameDirectoryShouldCopyMarkerFilesLast() throws Exception {
    URI dir = gcsiHelper.getPath(sharedBucketName1, "test-marker-files/rename-dir/");

    String subDir = "subdirectory/";

    List<String> files = ImmutableList.of("file", subDir + "file");
    List<String> markerFiles = ImmutableList.of("_SUCCESS", subDir + "_FAILURE");

    URI srcDir = dir.resolve("src-directory/");
    // gcsfs.mkdirs(srcDir.resolve(subDir));
    gcsfs.mkdirs(srcDir);
    // Create a test objects in our source directory:
    for (String file : Iterables.concat(markerFiles, files)) {
      try (WritableByteChannel channel = gcsfs.create(srcDir.resolve(file))) {
        assertThat(channel).isNotNull();
      }
    }

    Thread.sleep(100);

    URI dstDir = dir.resolve("dst-directory/");

    gcsfs.rename(srcDir, dstDir);

    List<FileInfo> fileInfos =
        gcsfs.getFileInfos(files.stream().map(dstDir::resolve).collect(toList()));
    List<FileInfo> markerFileInfos =
        gcsfs.getFileInfos(markerFiles.stream().map(dstDir::resolve).collect(toList()));

    // assert that marker files were copied last, e.g. marker files modification timestamp
    // should be less than regular files modification timestamp
    for (FileInfo mf : markerFileInfos) {
      fileInfos.forEach(
          f -> assertThat(f.getModificationTime()).isLessThan(mf.getModificationTime()));
    }
  }

  @Test
  public void testComposeSuccess() throws IOException {
    String bucketName = sharedBucketName1;
    URI directory = gcsiHelper.getPath(bucketName, "test-compose/");
    URI object1 = directory.resolve("object1");
    URI object2 = directory.resolve("object2");
    URI destination = directory.resolve("destination");
    gcsfs.mkdirs(directory);

    // Create the source objects
    try (WritableByteChannel channel1 = gcsfs.create(object1)) {
      assertThat(channel1).isNotNull();
      channel1.write(ByteBuffer.wrap("content1".getBytes(UTF_8)));
    }
    try (WritableByteChannel channel2 = gcsfs.create(object2)) {
      assertThat(channel2).isNotNull();
      channel2.write(ByteBuffer.wrap("content2".getBytes(UTF_8)));
    }
    assertThat(gcsfs.exists(object1) && gcsfs.exists(object2)).isTrue();

    gcsfs.compose(
        ImmutableList.of(object1, object2), destination, CreateObjectOptions.CONTENT_TYPE_DEFAULT);

    byte[] expectedOutput = "content1content2".getBytes(UTF_8);
    ByteBuffer actualOutput = ByteBuffer.allocate(expectedOutput.length);
    try (SeekableByteChannel destinationChannel =
            gcsiHelper.open(bucketName, "test-compose/destination")) {
      destinationChannel.read(actualOutput);
    }
    assertThat(actualOutput.array()).isEqualTo(expectedOutput);
  }

  /**
   * Gets a unique path of a non-existent file.
   */
  public static URI getTempFilePath() {
    return gcsiHelper.getPath(sharedBucketName1, "file-" + UUID.randomUUID());
  }

  /**
   * Returns intermediate sub-paths for the given path.
   *
   * <p>For example, getSubDirPaths(gs://foo/bar/zoo) returns: (gs://foo/, gs://foo/bar/)
   *
   * @param path Path to get sub-paths of.
   * @return List of sub-directory paths.
   */
  private List<URI> getSubDirPaths(URI path) {
    StorageResourceId resourceId = StorageResourceId.fromUriPath(path, true);

    List<String> subdirs = GoogleCloudStorageFileSystem.getDirs(resourceId.getObjectName());
    List<URI> subDirPaths = new ArrayList<>(subdirs.size());
    for (String subdir : subdirs) {
      subDirPaths.add(gcsiHelper.getPath(resourceId.getBucketName(), subdir));
    }

    return subDirPaths;
  }

  /**
   * If the given paths are expected to exist then asserts that they do,
   * otherwise asserts that they do not exist.
   */
  private void assertPathsExist(
      String testCaseDescription, String bucketName,
      List<String> objectNames, boolean expectedToExist)
      throws IOException {
    if (objectNames != null) {
      for (String object : objectNames) {
        URI path = gcsiHelper.getPath(bucketName, object);
        String msg = String.format("test-case: %s :: %s: %s",
            testCaseDescription,
            (expectedToExist
                ? "Path expected to exist but not found"
                : "Path expected to not exist but found"),
            path.toString());
        assertWithMessage(msg).that(gcsiHelper.exists(path)).isEqualTo(expectedToExist);
      }
    }
  }
}
