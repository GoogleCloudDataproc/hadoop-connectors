/*
 * Copyright 2019 Google Inc. All Rights Reserved.
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

import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemTestBase.loadConfig;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemIntegrationHelper;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import java.io.FileNotFoundException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration tests for {@link InMemoryGlobberFileSystem} class. */
@RunWith(JUnit4.class)
public class InMemoryGlobberFileSystemTest {
  private static GoogleCloudStorageFileSystemIntegrationHelper gcsFsIHelper;

  @BeforeClass
  public static void beforeClass() throws Exception {
    gcsFsIHelper =
        GoogleCloudStorageFileSystemIntegrationHelper.create(
            GoogleHadoopFileSystemIntegrationHelper.APP_NAME);
    gcsFsIHelper.beforeAllTests();
  }

  @AfterClass
  public static void afterClass() {
    gcsFsIHelper.afterAllTests();
  }

  @Test
  public void testGetFileStatus_shouldReturnExceptionForNotExistingFile() throws Exception {
    StorageResourceId testFile =
        new StorageResourceId(
            gcsFsIHelper.sharedBucketName1, "InMemoryGlobberFileSystem_testGetNullFileStatus");
    GoogleHadoopFileSystem ghfs =
        GoogleHadoopFileSystemIntegrationHelper.createGhfs(
            testFile.toString(), GoogleHadoopFileSystemIntegrationHelper.getTestConfig());
    String testContent = "test content";
    gcsFsIHelper.writeTextFile(testFile.getBucketName(), testFile.getObjectName(), testContent);
    URI fileUri =
        new URI(String.format("gs://%s/%s", testFile.getBucketName(), testFile.getObjectName()));

    Path filePath = new Path(fileUri);
    FileStatus status = ghfs.getFileStatus(filePath);
    List<FileStatus> fileStatuses = Arrays.asList(status);
    FileSystem helperFileSystem =
        InMemoryGlobberFileSystem.createInstance(
            loadConfig(), ghfs.getWorkingDirectory(), fileStatuses);
    ghfs.delete(filePath, true);

    FileNotFoundException e =
        assertThrows(
            FileNotFoundException.class,
            () -> helperFileSystem.getFileStatus(ghfs.getWorkingDirectory()));

    assertThat(e.getLocalizedMessage())
        .startsWith(
            String.format(
                "Path '%s' (qualified: '%s') does not exist.",
                ghfs.getWorkingDirectory(), ghfs.getWorkingDirectory()));
  }
}
