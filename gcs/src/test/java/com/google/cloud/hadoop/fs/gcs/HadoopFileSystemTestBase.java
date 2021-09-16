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

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertThrows;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemIntegrationTest;
import com.google.cloud.hadoop.gcsio.StringPaths;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Abstract base class with extended tests against the base Hadoop FileSystem interface but with
 * coverage of tricky edge cases relevant to GoogleHadoopFileSystem. Tests here are intended to be
 * useful for all different FileSystem implementations.
 *
 * <p>We reuse test code from GoogleCloudStorageIntegrationTest and
 * GoogleCloudStorageFileSystemIntegrationTest.
 */
public abstract class HadoopFileSystemTestBase extends GoogleCloudStorageFileSystemIntegrationTest {

  // GHFS access instance.
  static FileSystem ghfs;
  static FileSystemDescriptor ghfsFileSystemDescriptor;

  protected static HadoopFileSystemIntegrationHelper ghfsHelper;

  public static void postCreateInit() throws IOException {
    postCreateInit(
        new HadoopFileSystemIntegrationHelper(ghfs, ghfsFileSystemDescriptor));
  }

  /**
   * Perform initialization after creating test instances.
   */
  public static void postCreateInit(HadoopFileSystemIntegrationHelper helper)
      throws IOException {
    ghfsHelper = helper;
    ghfsHelper.ghfs.mkdirs(
        new Path(ghfsHelper.ghfsFileSystemDescriptor.getFileSystemRoot().toUri()));
    GoogleCloudStorageFileSystemIntegrationTest.postCreateInit(ghfsHelper);

    // Ensures that we do not accidentally end up testing wrong functionality.
    gcsfs = null;
  }

  @ClassRule
  public static NotInheritableExternalResource storageResource =
      new NotInheritableExternalResource(HadoopFileSystemTestBase.class) {
        @Override
        public void after() {
          if (ghfs != null) {
            if (ghfs instanceof GoogleHadoopFileSystemBase) {
              gcs = ((GoogleHadoopFileSystemBase) ghfs).getGcsFs().getGcs();
            }
            GoogleCloudStorageFileSystemIntegrationTest.storageResource.after();
            try {
              ghfs.close();
            } catch (IOException e) {
              throw new RuntimeException("Unexpected exception", e);
            }
            ghfs = null;
            ghfsFileSystemDescriptor = null;
          }
        }
      };

  // -----------------------------------------------------------------
  // Overridden methods from GCS test.
  // -----------------------------------------------------------------

  /**
   * Actual logic for validating the GoogleHadoopFileSystemBase-specific FileStatus returned
   * by getFileStatus() or listStatus().
   */
  private void validateFileStatusInternal(
      String bucketName, String objectName, boolean expectedToExist, FileStatus fileStatus)
      throws IOException {
    assertWithMessage("Existence of bucketName '%s', objectName '%s'", bucketName, objectName)
        .that(fileStatus != null)
        .isEqualTo(expectedToExist);

    if (fileStatus != null) {
      // File/dir exists, check its attributes.
      long expectedSize =
          ghfsHelper.getExpectedObjectSize(objectName, expectedToExist);
      if (expectedSize != Long.MIN_VALUE) {
        assertWithMessage("%s", fileStatus.getPath())
            .that(fileStatus.getLen())
            .isEqualTo(expectedSize);
      }

      boolean expectedToBeDir =
          Strings.isNullOrEmpty(objectName) || StringPaths.isDirectoryPath(objectName);
      assertWithMessage("%s", fileStatus.getPath())
          .that(fileStatus.isDir())
          .isEqualTo(expectedToBeDir);

      Instant currentTime = Instant.now();
      Instant modificationTime = Instant.ofEpochMilli(fileStatus.getModificationTime());
      // Ignore modification time for inferred directories that always set to 0
      if (!expectedToBeDir || fileStatus.getModificationTime() != 0) {
        // We must subtract 1000, because some FileSystems, like LocalFileSystem, have only
        // second granularity, so we might have something like testStartTime == 1234123
        // and modificationTime == 1234000. Unfortunately, "Instant" doesn't support easy
        // conversions between units to clip to the "second" precision.
        // Alternatively, we should just use TimeUnit and formally convert "toSeconds".
        assertWithMessage(
                "Stale file? testStartTime: %s modificationTime: %s bucket: '%s' object: '%s'",
                testStartTime, modificationTime, bucketName, objectName)
            .that(modificationTime)
            .isAtLeast(testStartTime.minusMillis(1000));
      }
      assertWithMessage(
              "Clock skew? currentTime: %s modificationTime: %s bucket: '%s' object: '%s'",
              currentTime, modificationTime, bucketName, objectName)
          .that(modificationTime)
          .isAtMost(currentTime);
    }
  }

  /**
   * Validates FileStatus for the given item.
   *
   * <p>See {@link #testGetAndListFileInfo()} for more info.
   */
  @Override
  protected void validateGetFileInfo(String bucketName, String objectName, boolean expectedToExist)
      throws IOException {
    URI path = ghfsHelper.getPath(bucketName, objectName, true);
    Path hadoopPath = ghfsHelper.castAsHadoopPath(path);
    FileStatus fileStatus = null;

    try {
      fileStatus = ghfs.getFileStatus(hadoopPath);
    } catch (FileNotFoundException e) {
      // Leaves fileStatus == null on FileNotFoundException.
    }

    if (fileStatus != null) {
      assertWithMessage("Hadoop paths for URI: %s", path)
          .that(hadoopPath)
          .isEqualTo(fileStatus.getPath());
    }
    validateFileStatusInternal(bucketName, objectName, expectedToExist, fileStatus);
  }

  /**
   * Validates FileInfo returned by listFileInfo().
   *
   * <p>See {@link #testGetAndListFileInfo()} for more info.
   */
  @Override
  protected void validateListFileInfo(
      String bucketName,
      String objectNamePrefix,
      boolean expectedToExist,
      String... expectedListedNames)
      throws IOException {
    boolean childPathsExpectedToExist = expectedToExist && (expectedListedNames != null);
    boolean listRoot = bucketName == null;

    // Prepare list of expected paths.
    List<Path> expectedPaths = new ArrayList<>();
    // Also maintain a backwards mapping to keep track of which of "expectedListedNames" and
    // "bucketName" is associated with each path, so that we can supply validateFileStatusInternal
    // with the objectName and thus enable it to lookup the internally stored expected size,
    // directory status, etc., of the associated FileStatus.
    Map<Path, String[]> pathToComponents = new HashMap<>();
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
        Path expectedPath = ghfsHelper.castAsHadoopPath(
            ghfsHelper.getPath(pathComponents[0], pathComponents[1], true));
        expectedPaths.add(expectedPath);
        pathToComponents.put(expectedPath, pathComponents);
      }
    }

    // Get list of actual paths.
    URI path = ghfsHelper.getPath(bucketName, objectNamePrefix, true);
    Path hadoopPath = ghfsHelper.castAsHadoopPath(path);
    FileStatus[] fileStatus;
    try {
      fileStatus = ghfsHelper.listStatus(hadoopPath);
    } catch (FileNotFoundException fnfe) {
      fileStatus = null;
      assertWithMessage("Hadoop path %s expected to exist", hadoopPath)
          .that(expectedToExist)
          .isFalse();
    }

    if (!ghfsFileSystemDescriptor.getScheme().equals("file")) {
      assertWithMessage("Hadoop path %s", hadoopPath)
          .that(fileStatus != null)
          .isEqualTo(expectedToExist);
    } else {
      // LocalFileSystem -> ChecksumFileSystem will return an empty array instead of null for
      // nonexistent paths.
      if (!expectedToExist && fileStatus != null) {
        assertThat(fileStatus).isEmpty();
      }
    }

    if (fileStatus != null) {
      Set<Path> actualPaths = new HashSet<>();
      for (FileStatus status : fileStatus) {
        Path actualPath = status.getPath();
        if (status.isDir()) {
          assertThat(status.getPath().getName()).isNotEmpty();
        }
        actualPaths.add(actualPath);
        String[] uriComponents = pathToComponents.get(actualPath);
        if (uriComponents != null) {
          // Only do fine-grained validation for the explicitly expected paths.
          validateFileStatusInternal(uriComponents[0], uriComponents[1], true, status);
        }
      }

      if (listRoot) {
        assertThat(actualPaths).containsAtLeastElementsIn(expectedPaths);
      } else {
        assertThat(actualPaths).containsExactlyElementsIn(expectedPaths);
      }
    }
  }

  // -----------------------------------------------------------------
  // Tests that test behavior at GHFS layer.

  /**
   * Tests read() when invalid arguments are passed.
   */
  @Test
  public void testReadInvalidArgs()
      throws IOException {
    URI path = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path hadoopPath = ghfsHelper.castAsHadoopPath(path);
    ghfsHelper.writeFile(hadoopPath, "file text", 1, /* overwrite= */ true);
    FSDataInputStream readStream = ghfs.open(hadoopPath);
    byte[] buffer = new byte[1];

    // Verify that normal read works.
    int numBytesRead = readStream.read(buffer, 0, buffer.length);
    assertWithMessage("Expected exactly 1 byte to be read").that(numBytesRead).isEqualTo(1);

    // Null buffer.
    testReadInvalidArgsHelper(readStream, null, 0, 1, NullPointerException.class);

    // offset < 0
    testReadInvalidArgsHelper(readStream, buffer, -1, 1, IndexOutOfBoundsException.class);

    // length < 0
    testReadInvalidArgsHelper(readStream, buffer, 0, -1, IndexOutOfBoundsException.class);

    // length > buffer.length - offset
    testReadInvalidArgsHelper(readStream, buffer, 0, 2, IndexOutOfBoundsException.class);
  }

  private void testReadInvalidArgsHelper(
      FSDataInputStream readStream, byte[] buffer, int offset, int length,
      Class<? extends Exception> exceptionClass) {
    Exception e = assertThrows(Exception.class, () -> readStream.read(buffer, offset, length));
    assertThat(e).isInstanceOf(exceptionClass);
  }

  /**
   * Writes a file one byte at a time (exercises write(byte b)).
   */
  @Test
  public void testWrite1Byte()
      throws IOException {
    String text = "Hello World!";
    byte[] textBytes = text.getBytes(UTF_8);
    URI path = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path hadoopPath = ghfsHelper.castAsHadoopPath(path);
    FSDataOutputStream writeStream = ghfs.create(hadoopPath);

    // Write one byte at a time.
    for (byte b : textBytes) {
      writeStream.write(b);
    }
    writeStream.close();

    // Read the file back and verify contents.
    String readText =
        ghfsHelper.readTextFile(hadoopPath, 0, textBytes.length, true);
    assertWithMessage("testWrite1Byte: write-read mismatch").that(readText).isEqualTo(text);
  }

  /** Validates delete(). */
  @Test
  @Override
  public void testDelete() throws Exception {
    deleteHelper(new HdfsBehavior());
  }

  /** Validates mkdirs(). */
  @Test
  @Override
  public void testMkdirs() throws Exception {
    mkdirsHelper(new HdfsBehavior());
  }

  /** Validates rename(). */
  @Test
  @Override
  public void testRename() throws Exception {
    renameHelper(new HdfsBehavior());
  }

  /** Validates that we can / cannot overwrite a file. */
  @Test
  public void testOverwrite() throws IOException {
    // Get a temp path and ensure that it does not already exist.
    URI path = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path hadoopPath = ghfsHelper.castAsHadoopPath(path);
    assertThrows(FileNotFoundException.class, () -> ghfs.getFileStatus(hadoopPath));

    // Create a file.
    String text = "Hello World!";
    int numBytesWritten =
        ghfsHelper.writeFile(hadoopPath, text, /* numWrites= */ 1, /* overwrite= */ false);
    assertThat(numBytesWritten).isEqualTo(text.getBytes(UTF_8).length);

    // Try to create the same file again with overwrite == false.
    assertThrows(
        IOException.class,
        () -> ghfsHelper.writeFile(hadoopPath, text, /* numWrites= */ 1, /* overwrite= */ false));

    // Try to create the same file again with overwrite == true.
    String textToOverwrite = "World Hello!";
    ghfsHelper.writeFile(hadoopPath, textToOverwrite, /* numWrites= */ 1, /* overwrite= */ true);

    String readText = ghfsHelper.readTextFile(hadoopPath);
    assertThat(readText).isEqualTo(textToOverwrite);
  }

  @Test
  public void testConcurrentCreationWithoutOverwrite_onlyOneSucceeds() throws Exception {
    // Get a temp path and ensure that it does not already exist.
    URI path = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path hadoopPath = ghfsHelper.castAsHadoopPath(path);
    assertThrows(FileNotFoundException.class, () -> ghfs.getFileStatus(hadoopPath));

    List<String> texts = ImmutableList.of("Hello World!", "World Hello! Long");

    ExecutorService executorService = Executors.newFixedThreadPool(2);
    List<Future<Integer>> futures =
        executorService.invokeAll(
            ImmutableList.of(
                () ->
                    ghfsHelper.writeFile(
                        hadoopPath, texts.get(0), /* numWrites= */ 1, /* overwrite= */ false),
                () ->
                    ghfsHelper.writeFile(
                        hadoopPath, texts.get(1), /* numWrites= */ 1, /* overwrite= */ false)));
    executorService.shutdown();

    assertThat(executorService.awaitTermination(1, MINUTES)).isTrue();

    // Verify at least one creation request succeeded.
    String readText = ghfsHelper.readTextFile(hadoopPath);
    assertThat(ImmutableList.of(readText)).containsAnyIn(texts);

    // One future should fail and one succeed
    for (int i = 0; i < futures.size(); i++) {
      Future<Integer> future = futures.get(i);
      String text = texts.get(i);
      if (readText.equals(text)) {
        assertThat(future.get()).isEqualTo(text.length());
      } else {
        assertThrows(ExecutionException.class, future::get);
      }
    }

    // Verify it can be overwritten by another creation.
    String text = "World!";
    ghfsHelper.writeFile(hadoopPath, text, /* numWrites= */ 1, /* overwrite= */ true);
    assertThat(ghfsHelper.readTextFile(hadoopPath)).isEqualTo("World!");
  }

  @Test
  public void testAppend() throws IOException {
    URI path = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path hadoopPath = ghfsHelper.castAsHadoopPath(path);

    ghfsHelper.writeTextFile(path.getAuthority(), path.getPath(), "content");

    try (FSDataOutputStream fsos = ghfs.append(hadoopPath)) {
      fsos.write("_appended".getBytes(UTF_8));
    }

    assertThat(ghfsHelper.readTextFile(hadoopPath)).isEqualTo("content_appended");
  }

  /**
   * Validates getDefaultReplication().
   */
  @Test
  public void testGetDefaultReplication()
      throws IOException {
    assertThat(ghfs.getDefaultReplication())
        .isEqualTo(GoogleHadoopFileSystemBase.REPLICATION_FACTOR_DEFAULT);
  }

  /**
   * Validates functionality related to getting/setting current position.
   */
  @Test
  public void testFilePosition()
      throws IOException {

    // Write an object.
    URI path = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path hadoopPath = ghfsHelper.castAsHadoopPath(path);
    String text = "Hello World!";
    int numBytesWritten = ghfsHelper.writeFile(hadoopPath, text, 1, /* overwrite= */ false);

    // Verify that position is at 0 for a newly opened stream.
    try (FSDataInputStream readStream = ghfs.open(hadoopPath)) {
      assertThat(readStream.getPos()).isEqualTo(0);

      // Verify that position advances by 2 after reading 2 bytes.
      assertThat(readStream.read()).isEqualTo('H');
      assertThat(readStream.read()).isEqualTo('e');
      assertThat(readStream.getPos()).isEqualTo(2);

      // Verify that setting position to the same value is a no-op.
      readStream.seek(2);
      assertThat(readStream.getPos()).isEqualTo(2);
      readStream.seek(2);
      assertThat(readStream.getPos()).isEqualTo(2);

      // Verify that position can be set to a valid position.
      readStream.seek(6);
      assertThat(readStream.getPos()).isEqualTo(6);
      assertThat(readStream.read()).isEqualTo('W');

      // Verify that position can be set to end of file.
      long posEOF = numBytesWritten - 1L;
      int val;
      readStream.seek(posEOF);
      assertThat(readStream.getPos()).isEqualTo(posEOF);
      val = readStream.read();
      assertThat(val).isNotEqualTo(-1);
      val = readStream.read();
      assertThat(val).isEqualTo(-1);
      readStream.seek(0);
      assertThat(readStream.getPos()).isEqualTo(0);

      // Verify that position cannot be set to a negative position.
      // Note:
      // HDFS implementation allows seek(-1) to succeed.
      // It even sets the position to -1!
      // We cannot enable the following test till HDFS bug is fixed.
      // try {
      //   readStream.seek(-1);
      //   Assert.fail("Expected IOException");
      // } catch (IOException expected) {
      //   // Expected.
      // }

      // Verify that position cannot be set beyond end of file.
      assertThrows(IOException.class, () -> readStream.seek(numBytesWritten + 1));

      // Perform some misc checks.
      // TODO(user): Make it no longer necessary to do instanceof.
      if (ghfs instanceof GoogleHadoopFileSystemBase) {
        long someValidPosition = 2;
        assertThat(readStream.seekToNewSource(someValidPosition)).isFalse();
        assertThat(readStream.markSupported()).isFalse();
      }
    }
  }

  /**
   * More comprehensive testing of various "seek" calls backwards and forwards and around
   * the edge cases related to buffer sizes.
   */
  @Test
  public void testFilePositionInDepthSeeks()
      throws IOException {
    // Write an object.
    URI path = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path hadoopPath = ghfsHelper.castAsHadoopPath(path);

    int bufferSize = 8 * 1024 * 1024;

    byte[] testBytes = new byte[bufferSize * 3];
    // The value of each byte should be each to its integer index squared, cast as a byte.
    for (int i = 0; i < testBytes.length; ++i) {
      testBytes[i] = (byte) (i * i);
    }
    int numBytesWritten = ghfsHelper.writeFile(hadoopPath, testBytes, 1, /* overwrite= */ false);
    assertThat(numBytesWritten).isEqualTo(testBytes.length);

    try (FSDataInputStream readStream = ghfs.open(hadoopPath, bufferSize)) {
      assertThat(readStream.getPos()).isEqualTo(0);
      assertThat((byte) readStream.read()).isEqualTo(testBytes[0]);
      assertThat(readStream.getPos()).isEqualTo(1);
      assertThat((byte) readStream.read()).isEqualTo(testBytes[1]);
      assertThat(readStream.getPos()).isEqualTo(2);

      // Seek backwards after reads got us to the current position.
      readStream.seek(0);
      assertThat(readStream.getPos()).isEqualTo(0);
      assertThat((byte) readStream.read()).isEqualTo(testBytes[0]);
      assertThat(readStream.getPos()).isEqualTo(1);
      assertThat((byte) readStream.read()).isEqualTo(testBytes[1]);
      assertThat(readStream.getPos()).isEqualTo(2);

      // Seek to same position, should be no-op, data should still be right.
      readStream.seek(2);
      assertThat(readStream.getPos()).isEqualTo(2);
      assertThat((byte) readStream.read()).isEqualTo(testBytes[2]);
      assertThat(readStream.getPos()).isEqualTo(3);

      // Seek farther, but within the read buffersize.
      int midPos = bufferSize / 2;
      readStream.seek(midPos);
      assertThat(readStream.getPos()).isEqualTo(midPos);
      assertThat((byte) readStream.read()).isEqualTo(testBytes[midPos]);

      // Seek backwards after we got here from some seeking, seek close to but not equal to
      // the beginning.
      readStream.seek(42);
      assertThat(readStream.getPos()).isEqualTo(42);
      assertThat((byte) readStream.read()).isEqualTo(testBytes[42]);

      // Seek to right before the end of the internal buffer.
      int edgePos = bufferSize - 1;
      readStream.seek(edgePos);
      assertThat(readStream.getPos()).isEqualTo(edgePos);
      assertThat((byte) readStream.read()).isEqualTo(testBytes[edgePos]);

      // This read should put us over the buffer's limit and require a new read from the underlying
      // stream.
      assertThat(readStream.getPos()).isEqualTo(edgePos + 1);
      assertThat((byte) readStream.read()).isEqualTo(testBytes[edgePos + 1]);

      // Seek back to the edge and this time seek forward.
      readStream.seek(edgePos);
      assertThat(readStream.getPos()).isEqualTo(edgePos);
      readStream.seek(edgePos + 1);
      assertThat(readStream.getPos()).isEqualTo(edgePos + 1);
      assertThat((byte) readStream.read()).isEqualTo(testBytes[edgePos + 1]);

      // Seek into buffer 2, then seek a bit further into it.
      int bufferTwoStart = bufferSize * 2;
      readStream.seek(bufferTwoStart);
      assertThat(readStream.getPos()).isEqualTo(bufferTwoStart);
      assertThat((byte) readStream.read()).isEqualTo(testBytes[bufferTwoStart]);
      readStream.seek(bufferTwoStart + 42);
      assertThat(readStream.getPos()).isEqualTo(bufferTwoStart + 42);
      assertThat((byte) readStream.read()).isEqualTo(testBytes[bufferTwoStart + 42]);

      // Seek backwards in-place inside buffer 2.
      readStream.seek(bufferTwoStart);
      assertThat(readStream.getPos()).isEqualTo(bufferTwoStart);
      assertThat((byte) readStream.read()).isEqualTo(testBytes[bufferTwoStart]);

      // Seek backwards by one buffer, but not all the way back to buffer 0.
      int bufferOneInternal = bufferSize + 42;
      readStream.seek(bufferOneInternal);
      assertThat(readStream.getPos()).isEqualTo(bufferOneInternal);
      assertThat((byte) readStream.read()).isEqualTo(testBytes[bufferOneInternal]);

      // Seek to the very beginning again and then seek to the very end.
      readStream.seek(0);
      assertThat(readStream.getPos()).isEqualTo(0);
      assertThat((byte) readStream.read()).isEqualTo(testBytes[0]);
      assertThat(readStream.getPos()).isEqualTo(1);
      readStream.seek(testBytes.length - 1);
      assertThat((byte) readStream.read()).isEqualTo(testBytes[testBytes.length - 1]);
      assertThat(readStream.getPos()).isEqualTo(testBytes.length);
    }
  }

  /**
   * Validates when paths already contain a pre-escaped substring, e.g. file:///foo%3Abar/baz,
   * that the FileSystem doesn't accidentally unescape it along the way, e.g. translating into
   * file:///foo:bar/baz.
   */
  @Test
  public void testPreemptivelyEscapedPaths()
      throws IOException {
    URI parentUri = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path parentPath = ghfsHelper.castAsHadoopPath(parentUri);
    Path escapedPath = new Path(parentPath, new Path("foo%3Abar"));

    ghfsHelper.writeFile(escapedPath, "foo", 1, /* overwrite= */ true);
    assertThat(ghfs.exists(escapedPath)).isTrue();

    FileStatus status = ghfs.getFileStatus(escapedPath);
    assertThat(status.getPath()).isEqualTo(escapedPath);

    // Cleanup.
    assertThat(ghfs.delete(parentPath, true)).isTrue();
  }

  /**
   * Contains data needed for testing the setWorkingDirectory() operation.
   */
  static class WorkingDirData {

    // Path passed to setWorkingDirectory().
    Path path;

    // Expected working directory after calling setWorkingDirectory().
    // null == no change to working directory expected.
    Path expectedPath;

    /**
     * Constructs an instance of WorkingDirData.
     *
     * The given path and expectedPath are used without further modification.
     * The caller decides whether to pass absolute or relative paths.
     */
    private WorkingDirData(Path path, Path expectedPath) {
      this.path = path;
      this.expectedPath = expectedPath;
    }

    /**
     * Constructs an instance of WorkingDirData.
     *
     * The given objectName is converted to an absolute path by combining it with
     * the default test bucket. The resulting path is passed to setWorkingDirectory().
     * Similarly the expectedObjectName is also converted to an absolute path.
     */
    static WorkingDirData absolute(
        HadoopFileSystemIntegrationHelper ghfsHelper,
        String objectName, String expectedObjectName) {
      return new WorkingDirData(
          ghfsHelper.createSchemeCompatibleHadoopPath(sharedBucketName1, objectName),
          ghfsHelper.createSchemeCompatibleHadoopPath(sharedBucketName1, expectedObjectName));
    }

    /**
     * Constructs an instance of WorkingDirData.
     *
     * The given objectName is converted to an absolute path by combining it with
     * the default test bucket. The resulting path is passed to setWorkingDirectory().
     */
    static WorkingDirData absolute(
        HadoopFileSystemIntegrationHelper ghfsHelper, String objectName) {
      return new WorkingDirData(
          ghfsHelper.createSchemeCompatibleHadoopPath(sharedBucketName1, objectName), null);
    }

    /**
     * Constructs an instance of WorkingDirData.
     *
     * The given path and expectedPath are used without further modification.
     * The caller decides whether to pass absolute or relative paths.
     */
    static WorkingDirData any(Path path, Path expectedPath) {
      return new WorkingDirData(path, expectedPath);
    }
  }

  /**
   * Helper for creating the necessary objects for testing working directory settings, returns a
   * list of WorkingDirData where each element represents a different test condition.
   */
  protected List<WorkingDirData> setUpWorkingDirectoryTest() throws Exception {
    // Objects created for this test.
    String[] objectNames = {
      "f1",
      "d0/",
      "d1/f1",
      "d1/d11/f1",
    };

    // -------------------------------------------------------
    // Create test objects.
    ghfsHelper.clearBucket(sharedBucketName1);
    ghfsHelper.createObjectsWithSubdirs(sharedBucketName1, objectNames);

    // -------------------------------------------------------
    // Initialize test data.
    List<WorkingDirData> wddList = new ArrayList<>();

    // Set working directory to root.
    Path rootPath = ghfsFileSystemDescriptor.getFileSystemRoot();
    wddList.add(WorkingDirData.any(rootPath, rootPath));

    // Set working directory to an existing directory (empty).
    wddList.add(WorkingDirData.absolute(ghfsHelper, "d0/", "d0/"));

    // Set working directory to an existing directory (non-empty).
    wddList.add(WorkingDirData.absolute(ghfsHelper, "d1/", "d1/"));


    // Set working directory to an existing directory (multi-level).
    wddList.add(WorkingDirData.absolute(ghfsHelper, "d1/d11/", "d1/d11/"));

    // Set working directory to an existing directory (bucket).
    wddList.add(WorkingDirData.absolute(ghfsHelper, null, null));

    return wddList;
  }

  /** Validates setWorkingDirectory() and getWorkingDirectory(). */
  @Test
  public void testWorkingDirectory() throws Exception {
    List<WorkingDirData> wddList = setUpWorkingDirectoryTest();

    // -------------------------------------------------------
    // Call setWorkingDirectory() for each path and verify the expected behavior.
    for (WorkingDirData wdd : wddList) {
      Path path = wdd.path;
      Path expectedWorkingDir = wdd.expectedPath;
      Path currentWorkingDir = ghfs.getWorkingDirectory();
      ghfs.setWorkingDirectory(path);
      Path newWorkingDir = ghfs.getWorkingDirectory();
      if (expectedWorkingDir != null) {
        assertThat(newWorkingDir).isEqualTo(expectedWorkingDir);
      } else {
        assertThat(newWorkingDir).isEqualTo(currentWorkingDir);
      }
    }
  }

  @Test
  public void testReadToEOFAndRewind() throws IOException {
    URI path = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();

    Path hadoopPath = ghfsHelper.castAsHadoopPath(path);
    byte[] byteBuffer = new byte[1024];
    for (int i = 0; i < byteBuffer.length; i++) {
      byteBuffer[i] = (byte) (i % 255);
    }
    ghfsHelper.writeFile(hadoopPath, byteBuffer, 1, /* overwrite= */ false);
    try (FSDataInputStream input = ghfs.open(hadoopPath)) {
      byte[] readBuffer1 = new byte[512];
      input.seek(511);
      assertThat(input.read(readBuffer1, 0, 512)).isEqualTo(512);
      input.seek(0);

      input.seek(511);
      assertThat(input.read(readBuffer1, 0, 512)).isEqualTo(512);
    } finally {
      ghfs.delete(hadoopPath);
    }
  }

  // -----------------------------------------------------------------
  // Inherited tests that we suppress because they do not make sense
  // in the context of this layer.
  // -----------------------------------------------------------------

  @Override
  public void testGetFileInfos() {}

  @Override
  public void testFileCreationSetsAttributes() {}

  @Override
  public void renameDirectoryShouldCopyMarkerFilesLast() {}

  @Override
  public void testComposeSuccess() {}

  @Override
  public void read_failure_ifObjectWasModifiedDuringRead() {}
}
