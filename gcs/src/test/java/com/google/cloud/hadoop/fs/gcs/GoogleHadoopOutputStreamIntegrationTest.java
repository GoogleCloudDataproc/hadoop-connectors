/*
 * Copyright 2019 Google LLC. All Rights Reserved.
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

import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemIntegrationHelper.getTestConfig;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.hadoop.gcsio.CreateFileOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemIntegrationHelper;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GoogleHadoopOutputStreamIntegrationTest {
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
  public void createGoogleHadoopOutputStream_WhenBufferSizeAsZero() throws Exception {
    StorageResourceId testFile =
        new StorageResourceId(gcsFsIHelper.sharedBucketName1, "GHFSInputStream_testAvailable");
    GoogleHadoopFileSystem ghfs =
        GoogleHadoopFileSystemIntegrationHelper.createGhfs(
            testFile.toString(), GoogleHadoopFileSystemIntegrationHelper.getTestConfig());
    Configuration config = getTestConfig();

    config.setInt(GoogleHadoopFileSystemConfiguration.GCS_OUTPUT_STREAM_BUFFER_SIZE.getKey(), 0);
    GoogleHadoopFileSystem myGhfs = new GoogleHadoopFileSystem();
    myGhfs.initialize(ghfs.initUri, config);

    String testContent = "test content";
    gcsFsIHelper.writeTextFile(testFile.getBucketName(), testFile.getObjectName(), testContent);

    GoogleHadoopOutputStream out =
        new GoogleHadoopOutputStream(
            myGhfs,
            new URI(testFile.toString()),
            new FileSystem.Statistics(ghfs.getScheme()),
            CreateFileOptions.DEFAULT);
    assertThat(out).isInstanceOf(GoogleHadoopOutputStream.class);
  }
}
