/*
 * Copyright 2026 Google LLC
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

import com.google.cloud.hadoop.gcsio.testing.InMemoryGoogleCloudStorage;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests {@link GoogleCloudStorageFileSystemImpl#listFileInfo} with and without {@link
 * GoogleCloudStorageFileSystemOptions#isListDirectoryFirstEnabled()}.
 */
@RunWith(JUnit4.class)
public class GoogleCloudStorageFileSystemListDirectoryFirstTest {

  private static final String BUCKET = "list-directory-first";

  /** Counts the object lookups and directory listings {@code listFileInfo} sends. */
  private static class CountingGoogleCloudStorage extends ForwardingGoogleCloudStorage {
    final AtomicInteger getItemInfoCalls = new AtomicInteger();
    final AtomicInteger listObjectInfoCalls = new AtomicInteger();

    CountingGoogleCloudStorage(GoogleCloudStorage delegate) {
      super(delegate);
    }

    @Override
    public GoogleCloudStorageItemInfo getItemInfo(StorageResourceId resourceId)
        throws IOException {
      getItemInfoCalls.incrementAndGet();
      return super.getItemInfo(resourceId);
    }

    @Override
    public List<GoogleCloudStorageItemInfo> listObjectInfo(
        String bucketName, String objectNamePrefix, ListObjectOptions listOptions)
        throws IOException {
      listObjectInfoCalls.incrementAndGet();
      return super.listObjectInfo(bucketName, objectNamePrefix, listOptions);
    }

    void resetCounts() {
      getItemInfoCalls.set(0);
      listObjectInfoCalls.set(0);
    }
  }

  private CountingGoogleCloudStorage gcs;
  private GoogleCloudStorageFileSystemImpl gcsFs;

  @After
  public void tearDown() {
    if (gcsFs != null) {
      gcsFs.close();
    }
  }

  /** Creates the objects in a fresh bucket and returns a file system over it. */
  private GoogleCloudStorageFileSystemImpl newGcsFs(boolean directoryFirst, String... objects)
      throws IOException {
    GoogleCloudStorageOptions gcsOptions = getInMemoryGoogleCloudStorageOptions();
    gcs = new CountingGoogleCloudStorage(new InMemoryGoogleCloudStorage(gcsOptions));
    gcs.createBucket(BUCKET);
    for (String object : objects) {
      gcs.createEmptyObject(new StorageResourceId(BUCKET, object));
    }
    gcs.resetCounts();
    gcsFs =
        new GoogleCloudStorageFileSystemImpl(
            gcs,
            GoogleCloudStorageFileSystemOptions.builder()
                .setCloudStorageOptions(gcsOptions)
                // Sequential, so the request counts below are deterministic.
                .setStatusParallelEnabled(false)
                .setListDirectoryFirstEnabled(directoryFirst)
                .build());
    return gcsFs;
  }

  private static URI uri(String objectName) {
    return URI.create("gs://" + BUCKET + "/" + objectName);
  }

  private static List<String> list(GoogleCloudStorageFileSystem fs, String objectName)
      throws IOException {
    return fs.listFileInfo(uri(objectName)).stream()
        .map(info -> info.getPath().toString().substring(("gs://" + BUCKET + "/").length()))
        .collect(Collectors.toList());
  }

  @Test
  public void defaultsToObjectFirst() {
    assertThat(GoogleCloudStorageFileSystemOptions.DEFAULT.isListDirectoryFirstEnabled())
        .isFalse();
  }

  @Test
  public void directory_directoryFirst_skipsObjectLookup() throws IOException {
    newGcsFs(/* directoryFirst= */ true, "d/o1", "d/o2", "d/sub/o3");

    assertThat(list(gcsFs, "d")).containsExactly("d/o1", "d/o2", "d/sub/").inOrder();
    assertThat(gcs.getItemInfoCalls.get()).isEqualTo(0);
    assertThat(gcs.listObjectInfoCalls.get()).isEqualTo(1);
  }

  @Test
  public void directory_objectFirst_looksUpObject() throws IOException {
    newGcsFs(/* directoryFirst= */ false, "d/o1", "d/o2", "d/sub/o3");

    assertThat(list(gcsFs, "d")).containsExactly("d/o1", "d/o2", "d/sub/").inOrder();
    assertThat(gcs.getItemInfoCalls.get()).isEqualTo(1);
    assertThat(gcs.listObjectInfoCalls.get()).isEqualTo(1);
  }

  @Test
  public void emptyDirectoryMarker_directoryFirst_isEmptyWithoutObjectLookup()
      throws IOException {
    newGcsFs(/* directoryFirst= */ true, "e/");

    assertThat(list(gcsFs, "e")).isEmpty();
    assertThat(gcs.getItemInfoCalls.get()).isEqualTo(0);
  }

  @Test
  public void file_directoryFirst_returnsObjectAfterEmptyListing() throws IOException {
    newGcsFs(/* directoryFirst= */ true, "f", "f-sibling/x");

    assertThat(list(gcsFs, "f")).containsExactly("f");
    assertThat(gcs.listObjectInfoCalls.get()).isEqualTo(1);
    assertThat(gcs.getItemInfoCalls.get()).isEqualTo(1);
  }

  @Test
  public void file_objectFirst_returnsObject() throws IOException {
    newGcsFs(/* directoryFirst= */ false, "f");

    assertThat(list(gcsFs, "f")).containsExactly("f");
  }

  @Test
  public void missingPath_bothModes_throwFileNotFound() throws IOException {
    for (boolean directoryFirst : new boolean[] {true, false}) {
      newGcsFs(directoryFirst, "other");

      assertThrows(FileNotFoundException.class, () -> gcsFs.listFileInfo(uri("missing")));
      gcsFs.close();
      gcsFs = null;
    }
  }

  @Test
  public void collisionWithChildren_directoryFirst_returnsDirectoryContents()
      throws IOException {
    newGcsFs(/* directoryFirst= */ true, "a", "a/x", "a/y");

    assertThat(list(gcsFs, "a")).containsExactly("a/x", "a/y").inOrder();
    assertThat(gcs.getItemInfoCalls.get()).isEqualTo(0);
  }

  @Test
  public void collisionWithChildren_objectFirst_returnsObject() throws IOException {
    newGcsFs(/* directoryFirst= */ false, "a", "a/x", "a/y");

    assertThat(list(gcsFs, "a")).containsExactly("a");
  }

  @Test
  public void collisionWithMarkerOnly_directoryFirst_returnsEmptyDirectory() throws IOException {
    newGcsFs(/* directoryFirst= */ true, "a", "a/");

    assertThat(list(gcsFs, "a")).isEmpty();
    assertThat(gcs.getItemInfoCalls.get()).isEqualTo(0);
  }

  @Test
  public void collisionWithMarkerOnly_objectFirst_returnsObject() throws IOException {
    newGcsFs(/* directoryFirst= */ false, "a", "a/");

    assertThat(list(gcsFs, "a")).containsExactly("a");
  }

  @Test
  public void collisionBelowListedDirectory_directoryFirst_listsBothEntries() throws IOException {
    newGcsFs(/* directoryFirst= */ true, "t/a", "t/a/x");

    // Listing the parent returns both the object and the inferred directory, as before.
    assertThat(list(gcsFs, "t")).containsExactly("t/a", "t/a/").inOrder();
    // Recursing into the directory now reaches its contents instead of the object again.
    assertThat(list(gcsFs, "t/a")).containsExactly("t/a/x");
  }

  @Test
  public void trailingSlash_bothModes_listsDirectoryWithoutObjectLookup() throws IOException {
    for (boolean directoryFirst : new boolean[] {true, false}) {
      newGcsFs(directoryFirst, "a", "a/x");

      assertThat(list(gcsFs, "a/")).containsExactly("a/x");
      assertThat(gcs.getItemInfoCalls.get()).isEqualTo(0);
      gcsFs.close();
      gcsFs = null;
    }
  }

  @Test
  public void getFileInfo_directoryFirst_unchangedOnCollision() throws IOException {
    newGcsFs(/* directoryFirst= */ true, "a", "a/x");

    FileInfo info = gcsFs.getFileInfo(uri("a"));
    assertThat(info.exists()).isTrue();
    assertThat(info.isDirectory()).isFalse();
  }

  @Test
  public void listFileInfoStartingFrom_trailingSlash_startsAfterObject() throws IOException {
    newGcsFs(/* directoryFirst= */ true, "t/a", "t/a-1/x", "t/a/x", "t/b");

    List<String> listed =
        gcsFs.listFileInfoStartingFrom(uri("t/a/"), ListFileOptions.DEFAULT).stream()
            .map(info -> info.getPath().toString().substring(("gs://" + BUCKET + "/").length()))
            .collect(Collectors.toList());

    // startOffset "t/a/" is passed through as is, so neither the object "t/a" nor the sibling
    // "t/a-1/x" (which sorts before "t/a/") is listed.
    assertThat(listed).containsExactly("t/a/x", "t/b").inOrder();
  }
}
