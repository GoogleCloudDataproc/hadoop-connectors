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

package com.google.cloud.hadoop.gcsio;

import static com.google.cloud.hadoop.gcsio.testing.InMemoryGoogleCloudStorage.getInMemoryGoogleCloudStorageOptions;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.hadoop.gcsio.testing.InMemoryGoogleCloudStorage;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.cloud.hadoop.util.RequesterPaysOptions;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
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
      GoogleCloudStorageOptions gcsOptions =
          getInMemoryGoogleCloudStorageOptions().toBuilder().setBidiEnabled(bidiEnabled).build();
      gcsfs =
          new GoogleCloudStorageFileSystemImpl(
              InMemoryGoogleCloudStorage::new,
              GoogleCloudStorageFileSystemOptions.builder()
                  .setCloudStorageOptions(gcsOptions)
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
                AsyncWriteChannelOptions.builder().setUploadChunkSize(24 * 1024 * 1024).build())
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

  private void verifyUnauthorizedAccess(GoogleCloudStorageFileSystemOptions options)
      throws IOException {
    switch (storageClientType) {
      case STORAGE_CLIENT:
        new GoogleCloudStorageFileSystemImpl(NoCredentials.getInstance(), options);
        break;
      default:
        new GoogleCloudStorageFileSystemImpl((Credentials) null, options);
    }
  }

  @Test
  public void testClientType() throws IOException {
    GoogleCredentials cred = GoogleCredentials.create(/* accessToken= */ null);
    GoogleCloudStorageFileSystemOptions.Builder optionsBuilder = getDefaultFileSystemOptions();

    setDefaultValidOptions(optionsBuilder);

    GoogleCloudStorageFileSystemOptions options = optionsBuilder.build();

    optionsBuilder.setCloudStorageOptions(
        options.getCloudStorageOptions().toBuilder().setProjectId(null).build());

    GoogleCloudStorageFileSystemImpl gcsfs =
        new GoogleCloudStorageFileSystemImpl(cred, optionsBuilder.build());
    assertThat(gcsfs.getGcs()).isInstanceOf(getGoogleCloudStorageImplClass());
  }

  /** Validates constructor. */
  @Test
  @SuppressWarnings("CheckReturnValue")
  public void testConstructor() throws IOException {
    GoogleCredentials cred = GoogleCredentials.create(/* accessToken= */ null);
    GoogleCloudStorageFileSystemOptions.Builder optionsBuilder = getDefaultFileSystemOptions();

    setDefaultValidOptions(optionsBuilder);

    GoogleCloudStorageFileSystemOptions options = optionsBuilder.build();

    // Verify that projectId == null or empty does not throw.
    optionsBuilder.setCloudStorageOptions(
        options.getCloudStorageOptions().toBuilder().setProjectId(null).build());
    new GoogleCloudStorageFileSystemImpl(cred, optionsBuilder.build());

    optionsBuilder.setCloudStorageOptions(
        options.getCloudStorageOptions().toBuilder().setProjectId("").build());
    new GoogleCloudStorageFileSystemImpl(cred, optionsBuilder.build());

    optionsBuilder.setCloudStorageOptions(
        options.getCloudStorageOptions().toBuilder()
            .setProjectId("projectId")
            .setRequesterPaysOptions(RequesterPaysOptions.DEFAULT)
            .build());

    // Verify that appName == null or empty throws IllegalArgumentException.

    optionsBuilder.setCloudStorageOptions(
        options.getCloudStorageOptions().toBuilder().setAppName(null).build());
    assertThrows(
        IllegalArgumentException.class,
        () -> new GoogleCloudStorageFileSystemImpl(cred, optionsBuilder.build()));

    optionsBuilder.setCloudStorageOptions(
        options.getCloudStorageOptions().toBuilder().setAppName("").build());
    assertThrows(
        IllegalArgumentException.class,
        () -> new GoogleCloudStorageFileSystemImpl(cred, optionsBuilder.build()));

    optionsBuilder.setCloudStorageOptions(
        options.getCloudStorageOptions().toBuilder().setAppName("appName").build());

    // Verify that credentials == null works - this is required for unauthenticated access.
    verifyUnauthorizedAccess(optionsBuilder.build());

    // Verify that fake projectId/appName and empty cred does not throw.
    setDefaultValidOptions(optionsBuilder);

    GoogleCloudStorageFileSystem tmpGcsFs =
        new GoogleCloudStorageFileSystemImpl(cred, optionsBuilder.build());

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
    pathUris.sort(GoogleCloudStorageFileSystemImpl.PATH_COMPARATOR);
    assertThat(pathUris.toArray()).isEqualTo(expectedUris.toArray());
  }

  /** Verify that we cannot pass invalid path to GoogleCloudStorageFileSystem. */
  @Test
  public void testInvalidPath() throws URISyntaxException {
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
      actualNames.add(GoogleCloudStorageFileSystemImpl.getItemName(new URI(inputPath)));
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
  public void testMiscCreateAndOpen() throws URISyntaxException {
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

  /*
   * TODO(user): add support of generations in InMemoryGoogleCloudStorage so
   * we can run the following tests in this class.
   */
  @Override
  public void read_failure_ifObjectWasModifiedDuringRead() {}
}
