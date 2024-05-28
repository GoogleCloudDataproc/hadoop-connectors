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

import static com.google.cloud.hadoop.gcsio.FolderInfo.PATH;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class FolderInfoTest {

  private final String BUCKET_NAME = "folder-info-test-bucket";
  private final String FOLDER_NAME = "test-parent-folder/test-folder-name";

  @Test
  public void checkForRootFolderWithNull() {
    assertThrows(
        "Folder resource has invalid bucket name",
        IllegalStateException.class,
        () -> new FolderInfo(FolderInfo.createFolderInfoObject(null, null)));
  }

  @Test
  public void checkForRootFolderWithEmptyString() {
    assertThrows(
        "Folder resource has invalid bucket name",
        IllegalStateException.class,
        () -> new FolderInfo(FolderInfo.createFolderInfoObject("", "")));
  }

  @Test
  public void checkForBucketWithNullFolder() {
    assertThrows(
        "Folder resource has invalid folder name",
        IllegalStateException.class,
        () -> new FolderInfo(FolderInfo.createFolderInfoObject(BUCKET_NAME, null)));
  }

  @Test
  public void checkForBucketWithEmptyFolder() {
    FolderInfo bucketFolderInfo =
        new FolderInfo(FolderInfo.createFolderInfoObject(BUCKET_NAME, ""));
    assertThat(bucketFolderInfo.getBucket()).isEqualTo(BUCKET_NAME);
    assertThat(bucketFolderInfo.getFolderName()).isEqualTo("");
    assertThat(bucketFolderInfo.isBucket()).isTrue();
    assertThat(bucketFolderInfo.getParentFolderName()).isEqualTo("");
  }

  @Test
  public void checkForNullBucket() {
    assertThrows(
        "Folder resource has invalid bucket name",
        IllegalStateException.class,
        () -> new FolderInfo(FolderInfo.createFolderInfoObject(null, FOLDER_NAME)));
  }

  @Test
  public void checkForEmptyBucket() {
    assertThrows(
        "Folder resource has invalid bucket name",
        IllegalStateException.class,
        () -> new FolderInfo(FolderInfo.createFolderInfoObject("", FOLDER_NAME)));
  }

  @Test
  public void checkForFolder() {
    FolderInfo testFolderInfo =
        new FolderInfo(FolderInfo.createFolderInfoObject(BUCKET_NAME, FOLDER_NAME));
    assertThat(testFolderInfo.getBucket()).isEqualTo(BUCKET_NAME);
    assertThat(testFolderInfo.getFolderName()).isEqualTo(FOLDER_NAME + PATH);
    assertThat(testFolderInfo.isBucket()).isFalse();
    assertThat(testFolderInfo.getParentFolderName()).isEqualTo("test-parent-folder/");
  }

  @Test
  public void checkForFolderParent() {
    FolderInfo testFolderInfo =
        new FolderInfo(FolderInfo.createFolderInfoObject(BUCKET_NAME, "test-parent-folder"));
    assertThat(testFolderInfo.getBucket()).isEqualTo(BUCKET_NAME);
    assertThat(testFolderInfo.getFolderName()).isEqualTo("test-parent-folder" + PATH);
    assertThat(testFolderInfo.isBucket()).isFalse();
    assertThat(testFolderInfo.getParentFolderName()).isEqualTo("");
  }
}
