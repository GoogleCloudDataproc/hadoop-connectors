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

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemIntegrationTest;
import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemTestBase.loadConfig;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemTestHelper.createInMemoryGoogleHadoopFileSystem;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.Assert.assertThrows;

/**
 * Integration tests for {@link InMemoryGlobberFileSystem} class.
 */
@RunWith(JUnit4.class)
public class InMemoryGlobberFileSystemTest {
  private static HadoopFileSystemIntegrationHelper ghfsHelper;
  private static FileSystem ghfs;

  @BeforeClass
  public static void setup() throws Throwable {
    GoogleHadoopFileSystemIntegrationTest.storageResource.before();
    ghfsHelper = GoogleHadoopFileSystemIntegrationTest.ghfsHelper;
    ghfs = GoogleHadoopFileSystemIntegrationTest.ghfs;
  }

  @AfterClass
  public static void cleanup() {
    ghfs = null;
    ghfsHelper = null;
    GoogleHadoopFileSystemIntegrationTest.storageResource.after();
  }

  @Test
  public void testGetNullFileStatus() throws IOException {
    URI fileUri = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path filePath = ghfsHelper.castAsHadoopPath(fileUri);
    ghfsHelper.writeFile(filePath, "foo", 1, /* overwrite= */ true);

    FileStatus status = ghfs.getFileStatus(filePath);
    List<FileStatus> fileStatuses = Arrays.asList(status);

    FileSystem helperFileSystem =
            InMemoryGlobberFileSystem.createInstance(loadConfig(), ghfs.getWorkingDirectory(),fileStatuses);

    ghfsHelper.delete(filePath.toUri(),true);
    FileNotFoundException e =
            assertThrows(FileNotFoundException.class, () -> helperFileSystem.getFileStatus(ghfs.getWorkingDirectory()));
    assertThat(e.getLocalizedMessage()).startsWith(String.format("Path '%s' (qualified: '%s') does not exist.",ghfs.getWorkingDirectory(),ghfs.getWorkingDirectory()));
  }
}
