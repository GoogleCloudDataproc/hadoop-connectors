/*
 * Copyright 2020 Google LLC
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

import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.ImmutableMap;
import com.google.common.testing.EqualsTester;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link GoogleCloudStorageItemInfo} class */
@RunWith(JUnit4.class)
public class GoogleCloudStorageItemInfoTest {

  @Test
  public void testEquals() throws Exception {
    StorageResourceId testBucket = StorageResourceId.fromStringPath("gs://test-bucket");
    StorageResourceId testObject = StorageResourceId.fromStringPath("gs://test-bucket/dir/object");
    StorageResourceId testDirectory =
        StorageResourceId.fromStringPath("gs://test-bucket/dir/subdir/");

    new EqualsTester()
        .addEqualityGroup(
            GoogleCloudStorageItemInfo.ROOT_INFO, GoogleCloudStorageItemInfo.ROOT_INFO)
        .addEqualityGroup(
            GoogleCloudStorageItemInfo.createBucket(
                testBucket,
                /* creationTime= */ 1000,
                /* modificationTime= */ 1200,
                /* location= */ "US",
                /* storageClass= */ "standard"),
            GoogleCloudStorageItemInfo.createBucket(
                testBucket,
                /* creationTime= */ 1000,
                /* modificationTime= */ 1200,
                /* location= */ "US",
                /* storageClass= */ "standard"))
        .addEqualityGroup(
            GoogleCloudStorageItemInfo.createBucket(
                testBucket,
                /* creationTime= */ 100,
                /* modificationTime= */ 1200,
                /* location= */ "US",
                /* storageClass= */ "standard"))
        .addEqualityGroup(
            GoogleCloudStorageItemInfo.createBucket(
                StorageResourceId.fromStringPath("gs://test-bucket-other"),
                /* creationTime= */ 100,
                /* modificationTime= */ 1200,
                /* location= */ "US",
                /* storageClass= */ "standard"))
        .addEqualityGroup(
            GoogleCloudStorageItemInfo.createObject(
                testObject,
                /* creationTime= */ 100,
                /* modificationTime= */ 1000,
                /* size= */ 324,
                /* contentType= */ "text",
                /* contentEncoding */ "gzip",
                /* metadata */ ImmutableMap.of("mkey", new byte[] {1, 6}),
                /* contentGeneration= */ 122,
                /* metaGeneration= */ 3,
                /* verificationAttributes= */ null))
        .addEqualityGroup(
            GoogleCloudStorageItemInfo.createObject(
                testObject,
                /* creationTime= */ 100,
                /* modificationTime= */ 1000,
                /* size= */ 324,
                /* contentType= */ "text",
                /* contentEncoding */ "gzip",
                /* metadata */ ImmutableMap.of("mkey", new byte[] {1, 6}),
                /* contentGeneration= */ 122,
                /* metaGeneration= */ 3,
                new VerificationAttributes(/* md5hash= */ null, /* crc32c= */ null)))
        .addEqualityGroup(
            GoogleCloudStorageItemInfo.createObject(
                testObject,
                /* creationTime= */ 100,
                /* modificationTime= */ 1000,
                /* size= */ 324,
                /* contentType= */ "text",
                /* contentEncoding */ "gzip",
                /* metadata */ ImmutableMap.of("mkey", new byte[] {1, 6}),
                /* contentGeneration= */ 122,
                /* metaGeneration= */ 3,
                new VerificationAttributes(
                    /* md5hash= */ new byte[] {1, 6}, /* crc32c= */ new byte[] {10})))
        .addEqualityGroup(
            GoogleCloudStorageItemInfo.createObject(
                testObject,
                /* creationTime= */ 100,
                /* modificationTime= */ 1000,
                /* size= */ 324,
                /* contentType= */ "text",
                /* contentEncoding */ "gzip",
                /* metadata */ ImmutableMap.of("mkey", new byte[] {1, 6}, "akey", new byte[] {9}),
                /* contentGeneration= */ 122,
                /* metaGeneration= */ 3,
                new VerificationAttributes(
                    /* md5hash= */ new byte[] {1, 6}, /* crc32c= */ new byte[] {11})),
            GoogleCloudStorageItemInfo.createObject(
                testObject,
                /* creationTime= */ 100,
                /* modificationTime= */ 1000,
                /* size= */ 324,
                /* contentType= */ "text",
                /* contentEncoding */ "gzip",
                /* metadata */ ImmutableMap.of("akey", new byte[] {9}, "mkey", new byte[] {1, 6}),
                /* contentGeneration= */ 122,
                /* metaGeneration= */ 3,
                new VerificationAttributes(
                    /* md5hash= */ new byte[] {1, 6}, /* crc32c= */ new byte[] {11})))
        .addEqualityGroup(GoogleCloudStorageItemInfo.createInferredDirectory(testDirectory))
        .addEqualityGroup(GoogleCloudStorageItemInfo.createNotFound(testBucket))
        .addEqualityGroup(GoogleCloudStorageItemInfo.createNotFound(testObject))
        .addEqualityGroup(GoogleCloudStorageItemInfo.createNotFound(testDirectory))
        .testEquals();
  }

  @Test
  public void testCreateFolderEquals() {
    StorageResourceId testFolder =
        StorageResourceId.fromStringPath("gs://test-bucket/test-folder/");
    long createTimeSeconds = 1672531200L; // Jan 01 2023 00:00:00 GMT
    long updateTimeSeconds = 1672617600L; // Jan 02 2023 00:00:00 GMT

    long metaGeneration = 5L;

    com.google.protobuf.Timestamp createTime =
        com.google.protobuf.Timestamp.newBuilder().setSeconds(createTimeSeconds).build();
    com.google.protobuf.Timestamp updateTime =
        com.google.protobuf.Timestamp.newBuilder().setSeconds(updateTimeSeconds).build();

    com.google.storage.control.v2.Folder folder =
        com.google.storage.control.v2.Folder.newBuilder()
            .setName("projects/_/buckets/test-bucket/folders/test-folder/")
            .setCreateTime(createTime)
            .setUpdateTime(updateTime)
            .setMetageneration(metaGeneration)
            .build();

    // Create variations for inequality checks
    StorageResourceId differentFolderId =
        StorageResourceId.fromStringPath("gs://test-bucket/different-folder/");
    com.google.protobuf.Timestamp differentCreateTime =
        com.google.protobuf.Timestamp.newBuilder().setSeconds((createTimeSeconds) + 1).build();
    com.google.protobuf.Timestamp differentUpdateTime =
        com.google.protobuf.Timestamp.newBuilder().setSeconds((updateTimeSeconds) + 1).build();
    long differentMetaGeneration = metaGeneration + 1;

    new EqualsTester()
        .addEqualityGroup(
            GoogleCloudStorageItemInfo.createFolder(testFolder, folder),
            GoogleCloudStorageItemInfo.createFolder(testFolder, folder))
        .addEqualityGroup(
            GoogleCloudStorageItemInfo.createFolder(
                differentFolderId,
                folder.toBuilder()
                    .setName("projects/_/buckets/test-bucket/folders/different-folder/")
                    .build()))
        .addEqualityGroup(
            GoogleCloudStorageItemInfo.createFolder(
                testFolder, folder.toBuilder().setCreateTime(differentCreateTime).build()))
        .addEqualityGroup(
            GoogleCloudStorageItemInfo.createFolder(
                testFolder, folder.toBuilder().setUpdateTime(differentUpdateTime).build()))
        .addEqualityGroup(
            GoogleCloudStorageItemInfo.createFolder(
                testFolder, folder.toBuilder().setMetageneration(differentMetaGeneration).build()))
        .testEquals();
  }

  @Test
  public void testCreateFolder_setsCorrectDefaultAttributes() {
    StorageResourceId resourceId =
        StorageResourceId.fromStringPath("gs://test-bucket/test-folder/");
    long createTimeMillis = 1672531200000L;
    long updateTimeMillis = 1672617600000L;

    com.google.protobuf.Timestamp createTime =
        com.google.protobuf.Timestamp.newBuilder().setSeconds(createTimeMillis / 1000).build();
    com.google.protobuf.Timestamp updateTime =
        com.google.protobuf.Timestamp.newBuilder().setSeconds(updateTimeMillis / 1000).build();

    com.google.storage.control.v2.Folder folder =
        com.google.storage.control.v2.Folder.newBuilder()
            .setName("projects/_/buckets/test-bucket/folders/test-folder/")
            .setCreateTime(createTime)
            .setUpdateTime(updateTime)
            .setMetageneration(5L)
            .build();

    GoogleCloudStorageItemInfo itemInfo =
        GoogleCloudStorageItemInfo.createFolder(resourceId, folder);

    // Verify variable attributes
    assertThat(itemInfo.getResourceId()).isEqualTo(resourceId);
    assertThat(itemInfo.getCreationTime()).isEqualTo(createTimeMillis);
    assertThat(itemInfo.getModificationTime()).isEqualTo(updateTimeMillis);
    assertThat(itemInfo.getMetaGeneration()).isEqualTo(5L);

    // Verify default attributes for a folder
    assertThat(itemInfo.exists()).isTrue();
    assertThat(itemInfo.isNativeHNSFolder()).isTrue();
    assertThat(itemInfo.isDirectory()).isTrue();
    assertThat(itemInfo.getSize()).isEqualTo(0);
    assertThat(itemInfo.getContentGeneration()).isEqualTo(0);
    assertThat(itemInfo.getLocation()).isNull();
    assertThat(itemInfo.getStorageClass()).isNull();
    assertThat(itemInfo.getContentType()).isNull();
    assertThat(itemInfo.getContentEncoding()).isNull();
    assertThat(itemInfo.getMetadata()).isEmpty();
    assertThat(itemInfo.getVerificationAttributes()).isNull();
  }
}
