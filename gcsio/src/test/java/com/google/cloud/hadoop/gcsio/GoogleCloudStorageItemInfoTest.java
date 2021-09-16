/*
 * Copyright 2020 Google LLC. All Rights Reserved.
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
}
