/*
 * Copyright 2024 Google Inc. All Rights Reserved.
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

import static com.google.common.truth.Truth.assertThat;

import com.google.api.services.storage.model.Folder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class FolderInfoTest {

  private final String BUCKET_NAME = "folder-info-test-bucket";
  private final String FOLDER_NAME = "test-parent-folder/test-folder-name/";

  @Test
  public void checkForRootFolder() throws Exception {
    FolderInfo rootFolderInfo = new FolderInfo(new Folder().setBucket(null).setName(null));

    assertThat(rootFolderInfo.getBucket()).isEqualTo(null);
    assertThat(rootFolderInfo.getFolderName()).isEqualTo(null);
    assertThat(rootFolderInfo.isBucket()).isFalse();
    assertThat(rootFolderInfo.getParentFolderName()).isEqualTo("");
  }

  @Test
  public void checkForBucket() throws Exception {
    FolderInfo bucketFolderInfo = new FolderInfo(new Folder().setBucket(BUCKET_NAME).setName(null));

    assertThat(bucketFolderInfo.getBucket()).isEqualTo(BUCKET_NAME);
    assertThat(bucketFolderInfo.getFolderName()).isEqualTo(null);
    assertThat(bucketFolderInfo.isBucket()).isTrue();
    assertThat(bucketFolderInfo.getParentFolderName()).isEqualTo("");
  }

  @Test
  public void checkForFolder() throws Exception {
    FolderInfo testFolderInfo =
        new FolderInfo(new Folder().setBucket(BUCKET_NAME).setName(FOLDER_NAME));
    assertThat(testFolderInfo.getBucket()).isEqualTo(BUCKET_NAME);
    assertThat(testFolderInfo.getFolderName()).isEqualTo(FOLDER_NAME);
    assertThat(testFolderInfo.isBucket()).isFalse();
    assertThat(testFolderInfo.getParentFolderName()).isEqualTo("test-parent-folder/");
  }
}
