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

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemIntegrationTest;
import com.google.common.base.Throwables;
import com.google.common.flogger.LoggerConfig;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;

import static com.google.common.truth.Truth.assertWithMessage;

/** Unittests for {@link InMemoryGlobberFileSystem} class. */
@RunWith(JUnit4.class)
public class InMemoryGlobberFileSystemTest extends GoogleHadoopFileSystemIntegrationTest {

  @ClassRule
  public static NotInheritableExternalResource storageResource =
      new NotInheritableExternalResource(InMemoryGlobberFileSystemTest.class) {
        @Override
        public void before() throws Throwable {
          // Disable logging.
          LoggerConfig.getConfig("").setLevel(Level.OFF);

          ghfs = GoogleHadoopFileSystemTestHelper.createInMemoryGoogleHadoopFileSystem();
          ghfsFileSystemDescriptor = (FileSystemDescriptor) ghfs;

          GoogleHadoopFileSystemIntegrationTest.postCreateInit();
        }

        @Override
        public void after() {
          GoogleHadoopFileSystemIntegrationTest.storageResource.after();
        }
      };

  @Test
  public void testGetNullFileStatus() throws IOException, URISyntaxException {
    URI fileUri = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path filePath = ghfsHelper.castAsHadoopPath(fileUri);
    ghfsHelper.writeFile(filePath, "foo", 1, /* overwrite= */ true);

    FileStatus status = ghfs.getFileStatus(filePath);
    List<FileStatus> fileStatuses = Arrays.asList(status);

    FileSystem helperFileSystem =
            InMemoryGlobberFileSystem.createInstance(loadConfig(), ghfs.getWorkingDirectory(), fileStatuses);

    try {
      helperFileSystem.getFileStatus(filePath);
    } catch (FileNotFoundException e) {
      assertWithMessage("Path should not exist", Throwables.getStackTraceAsString(e))
              .that(e)
              .hasMessageThat()
              .startsWith(String.format("Path '%s' (qualified: '%s') does not exist.", filePath.toString(), filePath.toString()));
    }
  }
}
