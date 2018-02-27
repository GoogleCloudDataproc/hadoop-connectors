/**
 * Copyright 2014 Google Inc. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *    
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.gcsio;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.expectThrows;

import com.google.common.base.Function;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * UnitTests for FileSystemBackedDirectoryListCache class. Since FileSystemBackedDirectoryListCache
 * is intended to be used in a large multi-process environment, we mix in a lot of race-condition
 * test cases that may arise from multi-process interactions.
 */
@RunWith(JUnit4.class)
public class FileSystemBackedDirectoryListCacheTest extends DirectoryListCacheTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(FileSystemBackedDirectoryListCacheTest.class);

  @Rule
  public TemporaryFolder tempDirectoryProvider = new TemporaryFolder();

  // The File corresponding to the temporary basePath of the testInstance.
  private File basePathFile;

  // Get a reference to the cache impl so we can do some gray-box testing with access to
  // its internal helpers.
  private FileSystemBackedDirectoryListCache fileBackedCache;

  @Override
  protected DirectoryListCache getTestInstance() throws IOException {
    basePathFile = tempDirectoryProvider.newFolder("gcs_metadata");
    // Re-root to a subdir which doesn't exist yet just to make sure it's handled properly.
    basePathFile = basePathFile.toPath().resolve("subdir").toFile();
    fileBackedCache = new FileSystemBackedDirectoryListCache(basePathFile.toString());
    DirectoryListCache cache = fileBackedCache;
    cache.getMutableConfig()
        .setMaxEntryAgeMillis(MAX_ENTRY_AGE)
        .setMaxInfoAgeMillis(MAX_INFO_AGE);
    return cache;
  }

  /**
   * Whereas basic key-value based DirectoryListCache implementations may not care if the user
   * simultaneously creates a directory and file of the same name, since we're actually using
   * a filesystem under the hood we enforce having no collisions between directories/files
   * of the same name.
   */
  @Test
  public void testObjectExistsButDirectoryStatusDoesntMatchGetCacheEntry() throws IOException {
    StorageResourceId fileToCreate = new StorageResourceId("foo-bucket", "foo/my-file");
    StorageResourceId fileCollision = new StorageResourceId("foo-bucket", "foo/my-file/");

    StorageResourceId dirToCreate = new StorageResourceId("foo-bucket", "foo/my-dir/");
    StorageResourceId dirCollision = new StorageResourceId("foo-bucket", "foo/my-dir");

    cache.putResourceId(fileToCreate);
    cache.putResourceId(dirToCreate);

    // Calling getCacheEntry with the colliding names just results in not-found even though
    // under the hood a directory/file was indeed found, just with a mismatch.
    assertThat(cache.getCacheEntry(fileCollision)).isNull();
    assertThat(cache.getCacheEntry(dirCollision)).isNull();
  }

  @Test
  public void testObjectExistsButDirectoryStatusDoesntMatchPutDirAfterFile() throws IOException {
    StorageResourceId fileToCreate = new StorageResourceId("foo-bucket", "foo/my-file");
    StorageResourceId fileCollision = new StorageResourceId("foo-bucket", "foo/my-file/");

    cache.putResourceId(fileToCreate);

    IOException thrown = expectThrows(IOException.class, () -> cache.putResourceId(fileCollision));
    assertThat(thrown).hasMessageThat().contains("isDirectory");
  }

  @Test
  public void testObjectExistsButDirectoryStatusDoesntMatchPutFileAfterDir() throws IOException {
    StorageResourceId dirToCreate = new StorageResourceId("foo-bucket", "foo/my-dir/");
    StorageResourceId dirCollision = new StorageResourceId("foo-bucket", "foo/my-dir");

    cache.putResourceId(dirToCreate);

    IOException thrown = expectThrows(IOException.class, () -> cache.putResourceId(dirCollision));
    assertThat(thrown).hasMessageThat().contains("isDirectory");
  }

  @Test
  public void testCreateNestedObjectParentExistsAsConflictingFile() throws IOException {
    StorageResourceId dirCollision = new StorageResourceId("foo-bucket", "foo/my-dir");
    StorageResourceId fileToCreate = new StorageResourceId("foo-bucket", "foo/my-dir/baz.txt");

    cache.putResourceId(dirCollision);

    IOException thrown = expectThrows(IOException.class, () -> cache.putResourceId(fileToCreate));
    assertThat(thrown).hasMessageThat().contains("isn't a directory");
  }

  @Test
  public void testRaceConditionDeleteParentDirectory() throws IOException {
    // Test retries on file-creation.
    final StorageResourceId fileToCreate = new StorageResourceId("foo-bucket", "foo/bar/baz.txt");

    // Test retries on directory-creation.
    final StorageResourceId dirToCreate = new StorageResourceId("foo-bucket", "foo/bar/bat/baf/");

    // Keep a count of how many times we intercept a create call.
    final int[] count = new int[1];

    // Number of times we clobbered the parent directory in our interceptor.
    final int[] clobberCountForFile = new int[1];
    final int[] clobberCountForDirectory = new int[1];

    fileBackedCache.setCreateMirrorFileListener(
        new Function<StorageResourceId, Void>() {
          // TODO(b/37774152): implement hashCode() (go/equals-hashcode-lsc)
          @SuppressWarnings("EqualsHashCode")
          @Override
          public boolean equals(Object obj) {
            return false;
          }

          @Override
          public Void apply(StorageResourceId resourceId) {
            LOG.info(
                "Intercepting creation of '{}', count is {}, clobberCountForFile is {}, "
                    + "clobberCountForDirectory is {}",
                resourceId,
                count[0],
                clobberCountForFile[0],
                clobberCountForDirectory[0]);
            ++count[0];
            Path mirrorPath = fileBackedCache.getMirrorPath(resourceId);
            File parentFile = mirrorPath.toFile().getParentFile();
            assertThat(parentFile.exists()).isTrue();
            if (resourceId.equals(fileToCreate) && clobberCountForFile[0] < 2) {
              // Only on the final object creation, clobber the parentFile just after the cache
              // thinks
              // it successfully checked for its existence and before it creates the child object.
              assertThat(parentFile.delete()).isTrue();
              ++clobberCountForFile[0];
            } else if (resourceId.equals(dirToCreate) && clobberCountForDirectory[0] < 2) {
              // Only on the final object creation, clobber the parentFile just after the cache
              // thinks
              // it successfully checked for its existence and before it creates the child object.
              assertThat(parentFile.delete()).isTrue();
              ++clobberCountForDirectory[0];
            }
            return null;
          }
        });

    // Put the file.
    cache.putResourceId(fileToCreate);
    assertThat(clobberCountForFile[0]).isEqualTo(2);
    assertThat(clobberCountForDirectory[0]).isEqualTo(0);

    // 4 for the initial 3 parents and 1 child file.
    // +2 for re-creating foo-bucket/foo/bar/, then retrying baz.txt the first time.
    // +2 for re-creating foo-bucket/foo/bar/, then retrying baz.txt the second time.
    assertThat(count[0]).isEqualTo(8);
    assertThat(cache.getCacheEntry(fileToCreate)).isNotNull();

    // Reset the count.
    count[0] = 0;

    // Put the directory.
    cache.putResourceId(dirToCreate);
    assertThat(clobberCountForFile[0]).isEqualTo(2);
    assertThat(clobberCountForDirectory[0]).isEqualTo(2);

    // 2 for initial directory bat/ and bat/baf/.
    // +2 for creating parent, then baf/ the first time.
    // +2 for creating parent, then baf/ the second time.
    assertThat(count[0]).isEqualTo(6);
    assertThat(cache.getCacheEntry(dirToCreate)).isNotNull();
  }

  @Test
  public void testRaceConditionCreateSameFile() throws IOException {
    final FileSystemBackedDirectoryListCache fileBackedCache =
        (FileSystemBackedDirectoryListCache) cache;

    // Test already-exists race on file-creation.
    final StorageResourceId fileToCreate = new StorageResourceId("foo-bucket", "foo/bar/baz.txt");

    // Test already-exists race on directory-creation.
    final StorageResourceId dirToCreate = new StorageResourceId("foo-bucket", "foo/bar/bat/baf/");

    // Keep a count of how many times we intercept a create call.
    final int[] count = new int[1];

    fileBackedCache.setCreateMirrorFileListener(
        new Function<StorageResourceId, Void>() {
          // TODO(b/37774152): implement hashCode() (go/equals-hashcode-lsc)
          @SuppressWarnings("EqualsHashCode")
          @Override
          public boolean equals(Object obj) {
            return false;
          }

          @Override
          public Void apply(StorageResourceId resourceId) {
            LOG.info("Intercepting creation of '{}', count is {}", resourceId, count[0]);
            ++count[0];
            Path mirrorPath = fileBackedCache.getMirrorPath(resourceId);
            if (resourceId.equals(fileToCreate) || resourceId.equals(dirToCreate)) {
              try {
                if (resourceId.isDirectory()) {
                  LOG.info(
                      "Pre-emptively creating dir '{}' for resourceId '{}'",
                      mirrorPath,
                      resourceId);
                  Files.createDirectory(mirrorPath);
                } else {
                  LOG.info(
                      "Pre-emptively creating file '{}' for resourceId '{}'",
                      mirrorPath,
                      resourceId);
                  Files.createFile(mirrorPath);
                }
              } catch (IOException ioe) {
                throw new RuntimeException(ioe);
              }
            }
            return null;
          }
        });

    cache.putResourceId(fileToCreate);
    cache.putResourceId(dirToCreate);

    // No extraneous retries or creates; FileAlreadyExistsExceptions should've been caught and
    // treated as 'success'.
    assertThat(count[0]).isEqualTo(6);
    assertThat(cache.getCacheEntry(fileToCreate)).isNotNull();
    assertThat(cache.getCacheEntry(dirToCreate)).isNotNull();
  }

  @Test
  public void testRaceConditionCreateSameFileAsConflictingDirectoryOrFile() throws IOException {
    final FileSystemBackedDirectoryListCache fileBackedCache =
        (FileSystemBackedDirectoryListCache) cache;

    // Test already-exists collision race on file-creation.
    final StorageResourceId fileToCreate = new StorageResourceId("foo-bucket", "foo/bar/baz.txt");

    // Test already-exists collision race on directory-creation.
    final StorageResourceId dirToCreate = new StorageResourceId("foo-bucket", "foo/bar/bat/baf/");

    // Keep a count of how many times we intercept a create call.
    final int[] count = new int[1];

    fileBackedCache.setCreateMirrorFileListener(
        new Function<StorageResourceId, Void>() {
          // TODO(b/37774152): implement hashCode() (go/equals-hashcode-lsc)
          @SuppressWarnings("EqualsHashCode")
          @Override
          public boolean equals(Object obj) {
            return false;
          }

          @Override
          public Void apply(StorageResourceId resourceId) {
            LOG.info("Intercepting creation of '{}', count is {}", resourceId, count[0]);
            ++count[0];
            Path mirrorPath = fileBackedCache.getMirrorPath(resourceId);
            if (resourceId.equals(fileToCreate) || resourceId.equals(dirToCreate)) {
              try {
                if (resourceId.isDirectory()) {
                  LOG.info(
                      "Pre-emptively creating colliding file '{}' for resourceId '{}'",
                      mirrorPath,
                      resourceId);
                  Files.createFile(mirrorPath);
                } else {
                  LOG.info(
                      "Pre-emptively creating colliding dir '{}' for resourceId '{}'",
                      mirrorPath,
                      resourceId);
                  Files.createDirectory(mirrorPath);
                }
              } catch (IOException ioe) {
                throw new RuntimeException(ioe);
              }
            }
            return null;
          }
        });

    assertThrows(IOException.class, () -> cache.putResourceId(fileToCreate));

    assertThrows(IOException.class, () -> cache.putResourceId(dirToCreate));

    // No extraneous retries or creates; exceptions should have caused immediate giving up.
    assertThat(count[0]).isEqualTo(6);
    assertThat(cache.getCacheEntry(fileToCreate)).isNull();
    assertThat(cache.getCacheEntry(dirToCreate)).isNull();
  }

  @Test
  public void testRaceConditionDeleteParentDirectoryExhaustAllRetries() throws IOException {
    // Test exhaust all retries on file-creation.
    final StorageResourceId fileToCreate = new StorageResourceId("foo-bucket", "foo/bar/baz.txt");

    // Number of times we clobbered the parent directory in our interceptor.
    final int[] clobberCountForFile = new int[1];

    fileBackedCache.setCreateMirrorFileListener(
        new Function<StorageResourceId, Void>() {
          // TODO(b/37774152): implement hashCode() (go/equals-hashcode-lsc)
          @SuppressWarnings("EqualsHashCode")
          @Override
          public boolean equals(Object obj) {
            return false;
          }

          @Override
          public Void apply(StorageResourceId resourceId) {
            LOG.info(
                "Intercepting creation of '{}', clobberCountForFile is {}",
                resourceId,
                clobberCountForFile[0]);
            Path mirrorPath = fileBackedCache.getMirrorPath(resourceId);
            File parentFile = mirrorPath.toFile().getParentFile();
            assertThat(parentFile.exists()).isTrue();
            int maxRetries = FileSystemBackedDirectoryListCache.MAX_RETRIES_FOR_CREATE;
            if (resourceId.equals(fileToCreate) && clobberCountForFile[0] <= maxRetries) {
              // Only on the final object creation, clobber the parentFile just after the cache
              // thinks
              // it successfully checked for its existence and before it creates the child object.
              assertThat(parentFile.delete()).isTrue();
              ++clobberCountForFile[0];
            }
            return null;
          }
        });

    // Put the file.
    IOException thrown = expectThrows(IOException.class, () -> cache.putResourceId(fileToCreate));
    assertThat(thrown).hasMessageThat().contains("Exhausted all retries");
  }
}
