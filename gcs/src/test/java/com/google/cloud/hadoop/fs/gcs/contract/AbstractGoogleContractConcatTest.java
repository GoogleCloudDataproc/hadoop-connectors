/*
 * Copyright 2019 Google LLC
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

package com.google.cloud.hadoop.fs.gcs.contract;

import static org.apache.hadoop.fs.contract.ContractTestUtils.assertFileHasLength;
import static org.apache.hadoop.fs.contract.ContractTestUtils.createFile;
import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorage;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractContractConcatTest;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.junit.Test;

public abstract class AbstractGoogleContractConcatTest extends AbstractContractConcatTest {

  @Test
  public void testConcatMultiple() throws Throwable {
    int numFiles = GoogleCloudStorage.MAX_COMPOSE_OBJECTS * 3 / 2;
    Path testPath = path("test");

    byte[][] blocks = new byte[numFiles][0];
    Path[] srcs = new Path[numFiles];
    for (int i = 0; i < numFiles; i++) {
      Path srcFile = new Path(testPath, "" + i);
      blocks[i] = dataset(TEST_FILE_LEN, i, 255);
      createFile(getFileSystem(), srcFile, true, blocks[i]);
      srcs[i] = srcFile;
    }
    Path target = new Path(testPath, "target");

    createFile(getFileSystem(), target, false, new byte[0]);
    getFileSystem().concat(target, srcs);
    assertFileHasLength(getFileSystem(), target, TEST_FILE_LEN * numFiles);
    ContractTestUtils.validateFileContent(
        ContractTestUtils.readDataset(getFileSystem(), target, TEST_FILE_LEN * numFiles), blocks);
  }
}
