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

import static com.google.cloud.hadoop.gcsio.testing.InMemoryGoogleCloudStorage.getInMemoryGoogleCloudStorageOptions;
import static com.google.common.truth.Truth.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertThrows;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.cloud.hadoop.gcsio.testing.InMemoryGoogleCloudStorage;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.cloud.hadoop.util.RequesterPaysOptions;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.Before;
import org.junit.Test;

/**
 * The unittest version of {@code GoogleCloudStorageFileSystemIntegrationTest}; the external
 * GoogleCloudStorage dependency is replaced by an in-memory version which mimics the same
 * bucket/object semantics.
 */
public abstract class GoogleCloudStorageFileSystemTestBase
    extends GoogleCloudStorageFileSystemIntegrationTest {

  @Before
  public void before() throws Exception {
    // Disable logging.
    // Normally you would need to keep a strong reference to any logger used for
    // configuration, but the "root" logger is always present.
    Logger.getLogger("").setLevel(Level.OFF);
    if (gcsfs == null) {
      gcsfs =
          new GoogleCloudStorageFileSystem(
              InMemoryGoogleCloudStorage::new,
              GoogleCloudStorageFileSystemOptions.builder()
                  .setCloudStorageOptions(getInMemoryGoogleCloudStorageOptions())
                  .setMarkerFilePattern("_(FAILURE|SUCCESS)")
                  .build());
      gcs = gcsfs.getGcs();
      postCreateInit();
    }
  }

  /**
   * Helper to fill out some default valid options after which the caller may want to reset a few
   * invalid options for individual items for particular tests.
   */
  private static void setDefaultValidOptions(
      GoogleCloudStorageFileSystemOptions.Builder optionsBuilder) {
    optionsBuilder.setCloudStorageOptions(
        GoogleCloudStorageOptions.builder()
            .setAppName("appName")
            .setProjectId("projectId")
            .setWriteChannelOptions(
                AsyncWriteChannelOptions.builder()
                    .setUploadChunkSize(UPLOAD_CHUNK_SIZE_DEFAULT)
                    .build())
            .build());
  }

  private GoogleCloudStorageFileSystemOptions.Builder getDefaultFileSystemOptions() {
    return GoogleCloudStorageFileSystemOptions.builder().setClientType(storageClientType);
  }

  private Class getGoogleCloudStorageImplClass() {
    switch (storageClientType) {
      case STORAGE_CLIENT:
        return GoogleCloudStorageClientImpl.class;
      default:
        return GoogleCloudStorageImpl.class;
    }
  }

  @Test
  public void testClientType() throws IOException {
    GoogleCloudStorageFileSystemOptions.Builder optionsBuilder = getDefaultFileSystemOptions();

    setDefaultValidOptions(optionsBuilder);

    GoogleCloudStorageFileSystemOptions options = optionsBuilder.build();

    optionsBuilder.setCloudStorageOptions(
        options.getCloudStorageOptions().toBuilder().setProjectId(null).build());

    GoogleCloudStorageFileSystem gcsfs =
        new GoogleCloudStorageFileSystem((Credential) null, optionsBuilder.build());
    assertThat(gcsfs.getGcs()).isInstanceOf(getGoogleCloudStorageImplClass());
  }

  /** Validates constructor. */
  @Test
  public void testConstructor() throws IOException {
    GoogleCredential cred = new GoogleCredential();
    GoogleCloudStorageFileSystemOptions.Builder optionsBuilder = getDefaultFileSystemOptions();

    setDefaultValidOptions(optionsBuilder);

    GoogleCloudStorageFileSystemOptions options = optionsBuilder.build();

    // Verify that projectId == null or empty does not throw.
    optionsBuilder.setCloudStorageOptions(
        options.getCloudStorageOptions().toBuilder().setProjectId(null).build());
    new GoogleCloudStorageFileSystem(cred, optionsBuilder.build());

    optionsBuilder.setCloudStorageOptions(
        options.getCloudStorageOptions().toBuilder().setProjectId("").build());
    new GoogleCloudStorageFileSystem(cred, optionsBuilder.build());

    optionsBuilder.setCloudStorageOptions(
        options
            .getCloudStorageOptions()
            .toBuilder()
            .setProjectId("projectId")
            .setRequesterPaysOptions(RequesterPaysOptions.DEFAULT)
            .build());

    // Verify that appName == null or empty throws IllegalArgumentException.

    optionsBuilder.setCloudStorageOptions(
        options.getCloudStorageOptions().toBuilder().setAppName(null).build());
    assertThrows(
        IllegalArgumentException.class,
        () -> new GoogleCloudStorageFileSystem(cred, optionsBuilder.build()));

    optionsBuilder.setCloudStorageOptions(
        options.getCloudStorageOptions().toBuilder().setAppName("").build());
    assertThrows(
        IllegalArgumentException.class,
        () -> new GoogleCloudStorageFileSystem(cred, optionsBuilder.build()));

    optionsBuilder.setCloudStorageOptions(
        options.getCloudStorageOptions().toBuilder().setAppName("appName").build());

    // Verify that credential == null works - this is required for unauthenticated access.
    new GoogleCloudStorageFileSystem((Credential) null, optionsBuilder.build());

    // Verify that fake projectId/appName and empty cred does not throw.
    setDefaultValidOptions(optionsBuilder);

    GoogleCloudStorageFileSystem tmpGcsFs =
        new GoogleCloudStorageFileSystem(cred, optionsBuilder.build());

    // White-box testing; check a few internal outcomes of our options.
    assertThat(tmpGcsFs.getGcs()).isInstanceOf(getGoogleCloudStorageImplClass());
    assertThat(gcsfs.getOptions().getCloudStorageOptions().getRequesterPaysOptions())
        .isEqualTo(RequesterPaysOptions.DEFAULT);
  }

  /** Verify that PATH_COMPARATOR produces correct sorting order. */
  @Test
  public void testPathComparator() throws URISyntaxException {
    String[] paths = {
      "gs://aa",
      "gs://abcdefghij",
      "gs://aaa",
      "gs:/",
      "gs://aa/f",
      "gs://aaa/f",
      "gs://aa/bb/f",
      "gs://ab",
      "gs://aa/bb/",
      "gs://aa",
    };

    String[] expectedAfterSort = {
      "gs:/",
      "gs://aa",
      "gs://aa",
      "gs://ab",
      "gs://aaa",
      "gs://aa/f",
      "gs://aaa/f",
      "gs://aa/bb/",
      "gs://aa/bb/f",
      "gs://abcdefghij",
    };

    // Prepare URI lists from their string equivalents.
    List<URI> pathUris = new ArrayList<>();
    List<URI> expectedUris = new ArrayList<>();

    for (String path : paths) {
      pathUris.add(new URI(path));
    }

    for (String path : expectedAfterSort) {
      expectedUris.add(new URI(path));
    }

    // Sanity check for input data using "natural-ordering" sorting.
    List<URI> pathUrisNaturalSorted = new ArrayList<>(pathUris);
    Collections.sort(pathUrisNaturalSorted);
    List<URI> expectedUrisNaturalSorted = new ArrayList<>(expectedUris);
    Collections.sort(expectedUrisNaturalSorted);
    assertThat(pathUrisNaturalSorted.toArray()).isEqualTo(expectedUrisNaturalSorted.toArray());

    // Sort the paths with the GCSFS-supplied PATH_COMPARATOR and verify.
    Collections.sort(pathUris, GoogleCloudStorageFileSystem.PATH_COMPARATOR);
    assertThat(pathUris.toArray()).isEqualTo(expectedUris.toArray());
  }

  /** Verify that we cannot pass invalid path to GoogleCloudStorageFileSystem. */
  @Test
  public void testInvalidPath() throws IOException, URISyntaxException {
    String[] invalidPaths = {

      // Path with a scheme other than gs.
      "foo://bucket/object",

      // Path with empty object name.
      "gs://bucket/",
      "gs://bucket",

      // Path with consecutive / chars in the path component.
      "gs://bucket//obj",
      "gs://bucket/obj//foo/bar",
    };

    for (String invalidPath : invalidPaths) {
      assertThrows(
          IllegalArgumentException.class,
          () -> StorageResourceId.fromUriPath(new URI(invalidPath), false));
    }

    String[] validPaths = {
      "gs:/", "gs://bucket/obj", "gs://bucket/obj/", "gs://bucket/obj/bar",
    };

    for (String validPath : validPaths) {
      StorageResourceId.fromUriPath(new URI(validPath), false);
    }

    String invalidBucketName = "bucket-name-has-invalid-char^";
    assertThrows(
        IllegalArgumentException.class,
        () -> UriPaths.fromStringPathComponents(invalidBucketName, null, true));
  }

  /** Verify getItemName(). */
  @Test
  public void testGetItemName() throws URISyntaxException {
    // Trailing slashes are effectively stripped for returned bucket names, but not for object
    // names.
    String[] inputPaths = {
      "gs:/",
      "gs://my-bucket",
      "gs://my-bucket/",
      "gs://my-bucket/foo",
      "gs://my-bucket/foo/",
      "gs://my-bucket/foo/bar",
      "gs://my-bucket/foo/bar/",
    };

    String[] expectedNames = {
      null, "my-bucket", "my-bucket", "foo", "foo/", "bar", "bar/",
    };

    List<String> actualNames = new ArrayList<>();
    for (String inputPath : inputPaths) {
      actualNames.add(gcsfs.getItemName(new URI(inputPath)));
    }
    assertThat(actualNames.toArray(new String[0])).isEqualTo(expectedNames);
  }

  /** Verify getParentPath(). */
  @Test
  public void testGetParentPathEdgeCases() throws URISyntaxException {
    URI[] inputPaths = {
      new URI("gs:/"),
      new URI("gs://my-bucket"),
      new URI("gs://my-bucket/"),
      new URI("gs://my-bucket/foo"),
      new URI("gs://my-bucket/foo/"),
      new URI("gs://my-bucket/foo/bar"),
      new URI("gs://my-bucket/foo/bar/"),
    };

    URI[] expectedPaths = {
      null,
      new URI("gs:/"),
      new URI("gs:/"),
      new URI("gs://my-bucket/"),
      new URI("gs://my-bucket/"),
      new URI("gs://my-bucket/foo/"),
      new URI("gs://my-bucket/foo/"),
    };

    List<URI> actualPaths = new ArrayList<>();
    for (URI inputPath : inputPaths) {
      actualPaths.add(UriPaths.getParentPath(inputPath));
    }
    assertThat(actualPaths.toArray(new URI[0])).isEqualTo(expectedPaths);
  }

  /** Verify validateBucketName(). */
  @Test
  public void testValidateBucketName() {
    String[] invalidBucketNames = {

      // Empty or null.
      null,
      "",

      // With a '/' character in it.
      "foo/bar",
      "/bar",
    };

    for (String bucketName : invalidBucketNames) {
      assertThrows(
          IllegalArgumentException.class, () -> StringPaths.validateBucketName(bucketName));
    }

    String[] validBucketNames = {
      "foo", "foo/",
    };

    for (String bucketName : validBucketNames) {
      StringPaths.validateBucketName(bucketName);
    }
  }

  /** Verify validateObjectName(). */
  @Test
  public void testValidateObjectName() {
    String[] invalidObjectNames = {

      // Empty or null.
      null,
      "",

      // With consecutive '/' characters in it.
      "//",
      "///",
      "foo//bar",
      "foo/bar//",
      "//foo/bar",
      "foo////bar",

      // other cases
      "/",
    };

    for (String objectName : invalidObjectNames) {
      assertThrows(
          IllegalArgumentException.class, () -> StringPaths.validateObjectName(objectName, false));
    }

    // Verify that an empty object name is allowed when explicitly allowed.
    StringPaths.validateObjectName(null, true);
    StringPaths.validateObjectName("", true);

    String[] validObjectNames = {
      "foo", "foo/bar", "foo/bar/",
    };

    for (String objectName : validObjectNames) {
      StringPaths.validateObjectName(objectName, false);
    }
  }

  /** Verify misc cases for FileInfo. */
  @Test
  public void testFileInfo() throws IOException {
    assertThat(gcsfs.getFileInfo(GoogleCloudStorageFileSystem.GCS_ROOT).getPath())
        .isEqualTo(GoogleCloudStorageFileSystem.GCS_ROOT);
    assertThat(gcsfs.getFileInfo(GoogleCloudStorageFileSystem.GCS_ROOT).getItemInfo())
        .isEqualTo(GoogleCloudStorageItemInfo.ROOT_INFO);
  }

  /** Verify misc cases for create/open. */
  @Test
  public void testMiscCreateAndOpen() throws URISyntaxException, IOException {
    URI dirPath = new URI("gs://foo/bar/");
    assertThrows(IOException.class, () -> gcsfs.create(dirPath));

    assertThrows(IllegalArgumentException.class, () -> gcsfs.open(dirPath));
  }

  @Test
  public void testCreateNoParentDirectories() throws URISyntaxException, IOException {
    String bucketName = sharedBucketName1;
    String testDir = "no/parent/dirs";

    gcsfs.create(new URI(String.format("gs://%s/%s/exist/a.txt", bucketName, testDir))).close();

    GoogleCloudStorage gcs = gcsfs.getGcs();
    assertThat(
            gcs.getItemInfo(new StorageResourceId(bucketName, testDir + "/exist/a.txt")).exists())
        .isTrue();
    assertThat(gcs.getItemInfo(new StorageResourceId(bucketName, testDir + "/exist/")).exists())
        .isFalse();
    assertThat(gcs.getItemInfo(new StorageResourceId(bucketName, testDir)).exists()).isFalse();
  }

  @Test
  public void testCreateAllowConflictWithExistingDirectory()
      throws URISyntaxException, IOException {
    String bucketName = sharedBucketName1;
    gcsfs.mkdirs(new URI("gs://" + bucketName + "/conflicting-dirname"));
    gcsfs
        .create(
            new URI("gs://" + bucketName + "/conflicting-dirname"),
            CreateFileOptions.builder().setEnsureNoDirectoryConflict(false).build())
        .close();

    // This is a "shoot yourself in the foot" use case, but working as intended if
    // checkNoDirectoryConflict is disabled; object and directory have same basename.
    assertThat(
            gcsfs
                .getGcs()
                .getItemInfo(new StorageResourceId(bucketName, "conflicting-dirname"))
                .exists())
        .isTrue();
    assertThat(
            gcsfs
                .getGcs()
                .getItemInfo(new StorageResourceId(bucketName, "conflicting-dirname/"))
                .exists())
        .isTrue();
  }

  @Test
  public void testRenameDirectory_rejectsPathTraversalOutsideDestination() throws Exception {
    for (boolean moveEnabled : new boolean[] {true, false}) {
      GoogleCloudStorageOptions gcsOptions =
          getInMemoryGoogleCloudStorageOptions()
              .toBuilder()
              .setMoveOperationEnabled(moveEnabled)
              .build();
      GoogleCloudStorageFileSystem localGcsFs =
          new GoogleCloudStorageFileSystem(
              InMemoryGoogleCloudStorage::new,
              GoogleCloudStorageFileSystemOptions.builder()
                  .setCloudStorageOptions(gcsOptions)
                  .setMarkerFilePattern("_(FAILURE|SUCCESS)")
                  .build());
      try {
        String bucket = "traversal-bucket-" + moveEnabled;
        localGcsFs.getGcs().createBucket(bucket);
        writeObject(localGcsFs.getGcs(), bucket, "staging/legit.txt", "legit");
        writeObject(localGcsFs.getGcs(), bucket, "staging/../victim/pwn", "ATTACKER_PAYLOAD");
        writeObject(localGcsFs.getGcs(), bucket, "victim/existing.txt", "ORIGINAL");
        writeObject(localGcsFs.getGcs(), bucket, "staging/../victim/existing.txt", "OVERWRITTEN");

        IOException exception =
            assertThrows(
                IOException.class,
                () ->
                    localGcsFs.rename(
                        new URI("gs://" + bucket + "/staging/"),
                        new URI("gs://" + bucket + "/final/")));
        assertThat(exception).hasMessageThat().contains("Refusing to rename");

        // No escaped object created, existing object untouched, and no partial copy/move.
        assertThat(
                localGcsFs
                    .getGcs()
                    .getItemInfo(new StorageResourceId(bucket, "victim/pwn"))
                    .exists())
            .isFalse();
        assertThat(readObject(localGcsFs.getGcs(), bucket, "victim/existing.txt"))
            .isEqualTo("ORIGINAL");
        assertThat(
                localGcsFs
                    .getGcs()
                    .getItemInfo(new StorageResourceId(bucket, "final/legit.txt"))
                    .exists())
            .isFalse();
        assertThat(
                localGcsFs
                    .getGcs()
                    .getItemInfo(new StorageResourceId(bucket, "staging/legit.txt"))
                    .exists())
            .isTrue();
      } finally {
        localGcsFs.close();
      }
    }
  }

  @Test
  public void testHasDotSegment_boundaryInputs() {
    assertThat(StringPaths.hasDotSegment(null)).isFalse();
    assertThat(StringPaths.hasDotSegment("")).isFalse();
    assertThat(StringPaths.hasDotSegment("/")).isFalse();
    assertThat(StringPaths.hasDotSegment("...")).isFalse();
    assertThat(StringPaths.hasDotSegment(".hidden")).isFalse();
    assertThat(StringPaths.hasDotSegment("..leading")).isFalse();
    assertThat(StringPaths.hasDotSegment("trailing..")).isFalse();
    assertThat(StringPaths.hasDotSegment("a/.b/c..d")).isFalse();

    assertThat(StringPaths.hasDotSegment(".")).isTrue();
    assertThat(StringPaths.hasDotSegment("..")).isTrue();
    assertThat(StringPaths.hasDotSegment("/.")).isTrue();
    assertThat(StringPaths.hasDotSegment("/..")).isTrue();
    assertThat(StringPaths.hasDotSegment("./")).isTrue();
    assertThat(StringPaths.hasDotSegment("../")).isTrue();
    assertThat(StringPaths.hasDotSegment("a/./b")).isTrue();
    assertThat(StringPaths.hasDotSegment("a/../b")).isTrue();
    assertThat(StringPaths.hasDotSegment("a/.")).isTrue();
    assertThat(StringPaths.hasDotSegment("a/..")).isTrue();
    assertThat(StringPaths.indexOfDotSegment("globroot/bad/../evil.txt")).isEqualTo(13);
  }

  @Test
  public void testRenameDirectory_rejectsIntraDestinationTraversalAndDotSegments()
      throws Exception {
    String[] maliciousObjects = {
      "staging/untrusted/../trusted/data.parquet",
      "staging/.",
      "staging/a/..",
      "staging/./file.txt",
      "staging/a/./b.txt",
      "staging/..",
      "staging/../_SUCCESS",
    };

    for (boolean moveEnabled : new boolean[] {true, false}) {
      for (String maliciousObject : maliciousObjects) {
        GoogleCloudStorageOptions gcsOptions =
            getInMemoryGoogleCloudStorageOptions()
                .toBuilder()
                .setMoveOperationEnabled(moveEnabled)
                .build();
        GoogleCloudStorageFileSystem localGcsFs =
            new GoogleCloudStorageFileSystem(
                InMemoryGoogleCloudStorage::new,
                GoogleCloudStorageFileSystemOptions.builder()
                    .setCloudStorageOptions(gcsOptions)
                    .setMarkerFilePattern("_(FAILURE|SUCCESS)")
                    .build());
        try {
          String bucket = "dot-segment-bucket-" + moveEnabled;
          localGcsFs.getGcs().createBucket(bucket);
          writeObject(localGcsFs.getGcs(), bucket, "staging/trusted/data.parquet", "TRUSTED");
          writeObject(localGcsFs.getGcs(), bucket, maliciousObject, "ATTACKER");
          URI srcUri = new URI("gs://" + bucket + "/staging/");
          URI dstUri = new URI("gs://" + bucket + "/final/");

          IOException exception =
              assertThrows(IOException.class, () -> localGcsFs.rename(srcUri, dstUri));
          assertThat(exception).hasMessageThat().contains("Refusing to rename");
          assertThat(
                  localGcsFs
                      .getGcs()
                      .getItemInfo(new StorageResourceId(bucket, "final/trusted/data.parquet"))
                      .exists())
              .isFalse();
          assertThat(
                  localGcsFs.getGcs().getItemInfo(new StorageResourceId(bucket, "final/")).exists())
              .isFalse();
        } finally {
          localGcsFs.close();
        }
      }
    }
  }

  @Test
  public void testRenameDestinationAndHnsFolder_rejectDotSegmentsAndAllowPlaceholder()
      throws Exception {
    GoogleCloudStorageOptions gcsOptions = getInMemoryGoogleCloudStorageOptions();
    GoogleCloudStorageFileSystem localGcsFs =
        new GoogleCloudStorageFileSystem(
            InMemoryGoogleCloudStorage::new,
            GoogleCloudStorageFileSystemOptions.builder()
                .setCloudStorageOptions(gcsOptions)
                .build());
    try {
      String bucket = "dst-dot-bucket";
      localGcsFs.getGcs().createBucket(bucket);
      localGcsFs.mkdirs(new URI("gs://" + bucket + "/staging/"));
      writeObject(localGcsFs.getGcs(), bucket, "src_dir/payload.txt", "DATA");
      writeObject(localGcsFs.getGcs(), bucket, "src_file.txt", "FILE_DATA");

      URI srcDir = new URI("gs://" + bucket + "/src_dir/");
      URI srcFile = new URI("gs://" + bucket + "/src_file.txt");
      URI dotDotDst = new URI("gs", bucket, "/staging/..", null, null);
      URI dotDst = new URI("gs", bucket, "/staging/.", null, null);

      assertThat(assertThrows(IOException.class, () -> localGcsFs.rename(srcDir, dotDotDst)))
          .hasMessageThat()
          .contains("Refusing to rename");
      assertThat(assertThrows(IOException.class, () -> localGcsFs.rename(srcFile, dotDst)))
          .hasMessageThat()
          .contains("Refusing to rename");
    } finally {
      localGcsFs.close();
    }

    // Verify HNS bucket rejects dot segments in source before calling renameHnFolder.
    GoogleCloudStorageOptions hnsOptions =
        getInMemoryGoogleCloudStorageOptions().toBuilder().setHnBucketRenameEnabled(true).build();
    GoogleCloudStorage hnsGcs =
        new ForwardingGoogleCloudStorage(new InMemoryGoogleCloudStorage(hnsOptions)) {
          @Override
          public boolean isHnBucket(URI src) {
            return true;
          }

          @Override
          public void renameHnFolder(URI src, URI dst) {
            throw new AssertionError("renameHnFolder should not be called for dot-segment paths");
          }
        };
    GoogleCloudStorageFileSystem hnsFs =
        new GoogleCloudStorageFileSystem(
            ignoredOptions -> hnsGcs,
            GoogleCloudStorageFileSystemOptions.builder()
                .setCloudStorageOptions(hnsOptions)
                .build());
    try {
      hnsGcs.createBucket("hns-bkt");
      writeObject(hnsGcs, "hns-bkt", "staging/../child.txt", "DATA");
      URI badHnsSrc = new URI("gs", "hns-bkt", "/staging/../", null, null);
      URI cleanHnsDst = new URI("gs://hns-bkt/final/");
      assertThat(assertThrows(IOException.class, () -> hnsFs.rename(badHnsSrc, cleanHnsDst)))
          .hasMessageThat()
          .contains("Refusing to rename");
    } finally {
      hnsFs.close();
    }

    // Verify directory rename succeeds when listObjectInfo includes the source directory
    // placeholder object itself (srcObjectName.equals(srcPrefix)).
    GoogleCloudStorageOptions placeholderOpts =
        getInMemoryGoogleCloudStorageOptions().toBuilder().setMoveOperationEnabled(false).build();
    GoogleCloudStorage placeholderGcs =
        new ForwardingGoogleCloudStorage(new InMemoryGoogleCloudStorage(placeholderOpts)) {
          @Override
          public List<GoogleCloudStorageItemInfo> listObjectInfo(
              String bucketName, String objectNamePrefix, ListObjectOptions listOptions)
              throws IOException {
            return super.listObjectInfo(
                bucketName,
                objectNamePrefix,
                listOptions.toBuilder().setIncludePrefix(true).build());
          }
        };
    GoogleCloudStorageFileSystem placeholderFs =
        new GoogleCloudStorageFileSystem(
            ignoredOptions -> placeholderGcs,
            GoogleCloudStorageFileSystemOptions.builder()
                .setCloudStorageOptions(placeholderOpts)
                .build());
    try {
      placeholderGcs.createBucket("placeholder-bkt");
      placeholderFs.mkdirs(new URI("gs://placeholder-bkt/staging/"));
      writeObject(placeholderGcs, "placeholder-bkt", "staging/file.txt", "HELLO");
      placeholderFs.rename(
          new URI("gs://placeholder-bkt/staging/"), new URI("gs://placeholder-bkt/final/"));
      assertThat(readObject(placeholderGcs, "placeholder-bkt", "final/file.txt"))
          .isEqualTo("HELLO");
    } finally {
      placeholderFs.close();
    }
  }

  @Test
  public void testRenameBucket_rejectsPathTraversal() throws Exception {
    GoogleCloudStorageOptions gcsOptions = getInMemoryGoogleCloudStorageOptions();
    GoogleCloudStorageFileSystem localGcsFs =
        new GoogleCloudStorageFileSystem(
            InMemoryGoogleCloudStorage::new,
            GoogleCloudStorageFileSystemOptions.builder()
                .setCloudStorageOptions(gcsOptions)
                .build());
    try {
      localGcsFs.getGcs().createBucket("src-traversal-bkt");
      localGcsFs.getGcs().createBucket("dst-traversal-bkt");
      writeObject(localGcsFs.getGcs(), "src-traversal-bkt", "../victim/pwn", "ATTACKER");

      IOException exception =
          assertThrows(
              IOException.class,
              () ->
                  localGcsFs.rename(
                      new URI("gs://src-traversal-bkt/"), new URI("gs://dst-traversal-bkt/")));
      assertThat(exception).hasMessageThat().contains("Refusing to rename");
      assertThat(
              localGcsFs
                  .getGcs()
                  .listObjectInfo("dst-traversal-bkt", null, ListObjectOptions.DEFAULT_FLAT_LIST))
          .isEmpty();
    } finally {
      localGcsFs.close();
    }
  }

  @Test
  public void testRenameSingleItemIntoExistingDirectory_rejectsDotSegments() throws Exception {
    GoogleCloudStorageOptions gcsOptions = getInMemoryGoogleCloudStorageOptions();
    GoogleCloudStorageFileSystem localGcsFs =
        new GoogleCloudStorageFileSystem(
            InMemoryGoogleCloudStorage::new,
            GoogleCloudStorageFileSystemOptions.builder()
                .setCloudStorageOptions(gcsOptions)
                .build());
    try {
      String bucket = "single-item-dot-bucket";
      localGcsFs.getGcs().createBucket(bucket);
      localGcsFs.mkdirs(new URI("gs://" + bucket + "/final/sub/"));
      writeObject(localGcsFs.getGcs(), bucket, "staging/..", "ATTACKER_FILE");
      writeObject(localGcsFs.getGcs(), bucket, "staging/.", "ATTACKER_DOT");
      writeObject(localGcsFs.getGcs(), bucket, "staging/../child.txt", "ATTACKER_DIR_CHILD");
      writeObject(localGcsFs.getGcs(), bucket, "a/staging/../child.txt", "ATTACKER_NESTED_CHILD");

      assertThat(
              assertThrows(
                  IOException.class,
                  () ->
                      localGcsFs.rename(
                          new URI("gs", bucket, "/staging/..", null, null),
                          new URI("gs://" + bucket + "/final/sub/"))))
          .hasMessageThat()
          .contains("Refusing to rename");

      assertThat(
              assertThrows(
                  IOException.class,
                  () ->
                      localGcsFs.rename(
                          new URI("gs", bucket, "/staging/.", null, null),
                          new URI("gs://" + bucket + "/final/sub/"))))
          .hasMessageThat()
          .contains("Refusing to rename");

      assertThat(
              assertThrows(
                  IOException.class,
                  () ->
                      localGcsFs.rename(
                          new URI("gs", bucket, "/staging/../", null, null),
                          new URI("gs://" + bucket + "/final/sub/"))))
          .hasMessageThat()
          .contains("Refusing to rename");

      assertThat(
              assertThrows(
                  IOException.class,
                  () ->
                      localGcsFs.rename(
                          new URI("gs", bucket, "/a/staging/../", null, null),
                          new URI("gs://" + bucket + "/final/sub/"))))
          .hasMessageThat()
          .contains("Refusing to rename");

      // Also verify GCS_ROOT destination rejects "../" instead of creating a bucket named "..".
      assertThat(
              assertThrows(
                  IOException.class,
                  () ->
                      localGcsFs.rename(
                          new URI("gs", bucket, "/staging/../", null, null),
                          GoogleCloudStorageFileSystem.GCS_ROOT)))
          .hasMessageThat()
          .contains("Refusing to rename");

      assertThat(
              localGcsFs
                  .getGcs()
                  .getItemInfo(new StorageResourceId(bucket, "final/child.txt"))
                  .exists())
          .isFalse();
    } finally {
      localGcsFs.close();
    }
  }

  @Test
  public void testRenameDirectoryToSubdirectory_forbidden() throws Exception {
    GoogleCloudStorageOptions gcsOptions = getInMemoryGoogleCloudStorageOptions();
    GoogleCloudStorageFileSystem localGcsFs =
        new GoogleCloudStorageFileSystem(
            InMemoryGoogleCloudStorage::new,
            GoogleCloudStorageFileSystemOptions.builder()
                .setCloudStorageOptions(gcsOptions)
                .build());
    try {
      String bucket = "subdir-check-bucket";
      localGcsFs.getGcs().createBucket(bucket);
      writeObject(localGcsFs.getGcs(), bucket, "a/file.txt", "data");
      localGcsFs.mkdirs(new URI("gs://" + bucket + "/a/sub/"));

      assertThat(
              assertThrows(
                  IOException.class,
                  () ->
                      localGcsFs.rename(
                          new URI("gs://" + bucket + "/"), new URI("gs://" + bucket + "/a/"))))
          .hasMessageThat()
          .contains("Rename to subdir is forbidden");

      assertThat(
              assertThrows(
                  IOException.class,
                  () ->
                      localGcsFs.rename(
                          new URI("gs://" + bucket + "/a/"),
                          new URI("gs://" + bucket + "/a/sub/"))))
          .hasMessageThat()
          .contains("Rename to subdir is forbidden");

      // Sibling prefix starting with same characters ("ab/") is not a subdirectory of "a/".
      localGcsFs.rename(new URI("gs://" + bucket + "/a/"), new URI("gs://" + bucket + "/ab/"));
      assertThat(readObject(localGcsFs.getGcs(), bucket, "ab/file.txt")).isEqualTo("data");
    } finally {
      localGcsFs.close();
    }
  }

  @Test
  public void testRenameDirectory_allowsValidDotAndSpecialCharNames() throws Exception {
    String[] validRelativeNames = {
      ".hidden",
      "_SUCCESS",
      "..leading",
      "trailing..",
      "foo..bar",
      "a/.b/c..d.parquet",
      "part:001.parquet",
      "2026-11-20T04:00:00Z.parquet",
      "my file.txt",
      "query?a=1#frag",
      "%2e%2e/literal.txt",
    };

    for (boolean moveEnabled : new boolean[] {true, false}) {
      GoogleCloudStorageOptions gcsOptions =
          getInMemoryGoogleCloudStorageOptions()
              .toBuilder()
              .setMoveOperationEnabled(moveEnabled)
              .build();
      GoogleCloudStorageFileSystem localGcsFs =
          new GoogleCloudStorageFileSystem(
              InMemoryGoogleCloudStorage::new,
              GoogleCloudStorageFileSystemOptions.builder()
                  .setCloudStorageOptions(gcsOptions)
                  .setMarkerFilePattern("_(FAILURE|SUCCESS)")
                  .build());
      try {
        String bucket = "valid-names-bucket-" + moveEnabled;
        localGcsFs.getGcs().createBucket(bucket);
        for (String name : validRelativeNames) {
          writeObject(localGcsFs.getGcs(), bucket, "staging/" + name, "content:" + name);
        }

        localGcsFs.rename(
            new URI("gs://" + bucket + "/staging/"), new URI("gs://" + bucket + "/final/"));

        for (String name : validRelativeNames) {
          assertThat(
                  localGcsFs
                      .getGcs()
                      .getItemInfo(new StorageResourceId(bucket, "staging/" + name))
                      .exists())
              .isFalse();
          assertThat(readObject(localGcsFs.getGcs(), bucket, "final/" + name))
              .isEqualTo("content:" + name);
        }

        // Also verify single-file rename with special characters into an existing directory.
        String specialFile = "part:001?a=1#b file.parquet";
        writeObject(localGcsFs.getGcs(), bucket, "single/" + specialFile, "single-content");
        localGcsFs.rename(
            new URI("gs", bucket, "/single/" + specialFile, null, null),
            new URI("gs://" + bucket + "/final/"));
        assertThat(readObject(localGcsFs.getGcs(), bucket, "final/" + specialFile))
            .isEqualTo("single-content");
      } finally {
        localGcsFs.close();
      }
    }
  }

  private static void writeObject(
      GoogleCloudStorage gcs, String bucket, String objectName, String content) throws IOException {
    try (WritableByteChannel channel =
        gcs.create(
            new StorageResourceId(bucket, objectName), CreateObjectOptions.DEFAULT_OVERWRITE)) {
      channel.write(ByteBuffer.wrap(content.getBytes(UTF_8)));
    }
  }

  private static String readObject(GoogleCloudStorage gcs, String bucket, String objectName)
      throws IOException {
    try (SeekableByteChannel channel =
        gcs.open(
            new StorageResourceId(bucket, objectName), GoogleCloudStorageReadOptions.DEFAULT)) {
      ByteBuffer buffer = ByteBuffer.allocate((int) channel.size());
      while (buffer.hasRemaining()) {
        if (channel.read(buffer) == -1) {
          break;
        }
      }
      return new String(buffer.array(), UTF_8);
    }
  }

  /*
   * TODO(user): add support of generations in InMemoryGoogleCloudStorage so
   * we can run the following tests in this class.
   */
  @Override
  public void read_failure_ifObjectWasModifiedDuringRead() {}
}
