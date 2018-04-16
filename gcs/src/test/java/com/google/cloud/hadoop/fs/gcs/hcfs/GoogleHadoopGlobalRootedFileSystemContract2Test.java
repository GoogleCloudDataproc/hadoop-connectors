/**
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

package com.google.cloud.hadoop.fs.gcs.hcfs;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemTestHelper;
import java.io.IOException;
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.apache.hadoop.fs.Path;
import org.junit.Before;

/**
 * Runs the Hadoop tests in FileSystemContractBaseTest over the GoogleHadoopGlobalRootedFileSystem.
 * Tests that the GoogleHadoopGlobalRootedFileSystem obeys the file system contract
 * specified for Hadoop.
 */
public class GoogleHadoopGlobalRootedFileSystemContract2Test
  extends FileSystemContractBaseTest {

  @Before
  @SuppressWarnings("MissingOverride")
  public void setUp() throws IOException {
    fs = GoogleHadoopFileSystemTestHelper.createInMemoryGoogleHadoopGlobalRootedFileSystem();
  }

  /**
   * Gets the default working directory for GHFS.
   */
  @Override
  protected String getDefaultWorkingDirectory() {
    return "gsg:/fake-test-system-bucket/some-dir";
  }


  /**
   * Tests get/setWorkingDirectory().
   */
  @Override
  public void testWorkingDirectory()
      throws Exception {
    // Set the pseudo default working directory before the test begins.
    fs.setWorkingDirectory(new Path(getDefaultWorkingDirectory()));
    super.testWorkingDirectory();
  }

  @Override
  public void testMkdirsWithUmask() {}

  @Override
  public void testListStatusThrowsExceptionForNonExistentFile() {}
}
