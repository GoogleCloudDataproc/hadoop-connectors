/*
 * Copyright 2015 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the * License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.gcsio;

import static com.google.cloud.hadoop.gcsio.testing.InMemoryGoogleCloudStorage.getInMemoryGoogleCloudStorageOptions;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

import com.google.cloud.hadoop.gcsio.testing.InMemoryGoogleCloudStorage;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link GoogleCloudStorageFileSystemOptions} . */
@RunWith(JUnit4.class)
public class GoogleCloudStorageFileSystemOptionsTest {

  @BeforeClass
  public static void beforeAllTests() throws IOException {
    // Disable logging.
    // Normally you would need to keep a strong reference to any logger used for
    // configuration, but the "root" logger is always present.
    Logger.getLogger("").setLevel(Level.OFF);
  }

  @Test
  public void testGcsFsInheritsGcsOptions() throws Exception {
    GoogleCloudStorageOptions gcsOptions =
        GoogleCloudStorageOptions.builder()
            .setProjectId("foo-project")
            .setAppName("foo-app")
            .build();
    GoogleCloudStorageFileSystem gcsfs =
        new GoogleCloudStorageFileSystem(
            InMemoryGoogleCloudStorage::new,
            GoogleCloudStorageFileSystemOptions.builder()
                .setCloudStorageOptions(gcsOptions)
                .build());
    assertThat(gcsfs.getOptions().getCloudStorageOptions().getProjectId()).isEqualTo("foo-project");
    assertThat(gcsfs.getOptions().getCloudStorageOptions().getAppName()).isEqualTo("foo-app");
  }

  /** With inferred directories, the directories should appear to be there. */
  @Test
  public void testInferredDirectories() throws IOException, URISyntaxException {
    // The test bucket name.
    String testBucketName = "bucket1";

    // The test files we create.
    ImmutableList<String> inputFiles =
        ImmutableList.of(
            "gs://" + testBucketName + "/a/b/f1.txt",
            "gs://" + testBucketName + "/a/c/f2.txt",
            "gs://" + testBucketName + "/e/f");

    // The implied directories that directly contain input files.
    ImmutableList<String> impliedDirs0 =
        ImmutableList.of(
            "gs://" + testBucketName + "/a/b",
            "gs://" + testBucketName + "/a/c",
            "gs://" + testBucketName + "/e");

    String impliedDirA = "gs://" + testBucketName + "/a";

    // The implied directories that only contain implied directories.
    ImmutableList<String> impliedDirs1 = ImmutableList.of(impliedDirA);

    // All implied directories
    ImmutableList<String> impliedDirs =
        ImmutableList.<String>builder().addAll(impliedDirs0).addAll(impliedDirs1).build();

    // We need different GCSFS options for our test.
    GoogleCloudStorageFileSystem gcsfs =
        new GoogleCloudStorageFileSystem(
            InMemoryGoogleCloudStorage::new,
            GoogleCloudStorageFileSystemOptions.builder()
                .setCloudStorageOptions(getInMemoryGoogleCloudStorageOptions())
                .build());
    GoogleCloudStorage gcs = gcsfs.getGcs();
    gcs.createBucket(testBucketName);
    for (String inputFile : inputFiles) {
      gcs.createEmptyObject(StorageResourceId.fromStringPath(inputFile));
    }

    // Make sure the files we just created exist
    for (String inputFile : inputFiles) {
      FileInfo fileInfo = gcsfs.getFileInfo(new URI(inputFile));
      assertThat(fileInfo.exists()).isTrue();
    }

    // The directory objects should exist (as inferred directories).
    for (String dir : impliedDirs) {
      FileInfo dirInfo = gcsfs.getFileInfo(new URI(dir));
      assertWithMessage("Directory %s should exist (inferred)", dir)
          .that(dirInfo.exists())
          .isTrue();
      assertWithMessage("Creation time on inferred directory %s should be zero.", dir)
          .that(dirInfo.getCreationTime())
          .isEqualTo(0);
    }

    List<FileInfo> subInfo = gcsfs.listFileInfo(new URI(impliedDirA));
    assertWithMessage("Implied directory %s should have 2 children", impliedDirA)
        .that(subInfo.size())
        .isEqualTo(2);
  }
}
