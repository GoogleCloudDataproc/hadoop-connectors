/*
 * Copyright 2019 Google LLC. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.gcsio;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.api.client.auth.oauth2.Credential;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper;
import com.google.cloud.hadoop.gcsio.testing.TestConfiguration;
import com.google.cloud.hadoop.util.RetryHttpInitializer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GoogleCloudStorageItemInfoIntegrationTest {

  private static GoogleCloudStorageFileSystemIntegrationHelper gcsfsIHelper;
  private static GoogleCloudStorageFileSystem gcsfs;

  @Rule public TestName name = new TestName();

  @BeforeClass
  public static void before() throws Throwable {
    String projectId =
        checkNotNull(TestConfiguration.getInstance().getProjectId(), "projectId can not be null");
    String appName = GoogleCloudStorageIntegrationHelper.APP_NAME;
    Credential credential =
        checkNotNull(GoogleCloudStorageTestHelper.getCredential(), "credential must not be null");

    GoogleCloudStorageOptions gcsOptions =
        GoogleCloudStorageOptions.newBuilder().setAppName(appName).setProjectId(projectId).build();

    gcsfs =
        new GoogleCloudStorageFileSystem(
            credential,
            GoogleCloudStorageFileSystemOptions.newBuilder()
                .setEnableBucketDelete(true)
                .setCloudStorageOptionsBuilder(gcsOptions.toBuilder())
                .build());

    gcsfsIHelper = new GoogleCloudStorageFileSystemIntegrationHelper(gcsfs);
    gcsfsIHelper.beforeAllTests();
  }

  @AfterClass
  public static void afterClass() throws Throwable {
    gcsfsIHelper.afterAllTests();
    GoogleCloudStorageFileSystem gcsfs = gcsfsIHelper.gcsfs;
    assertThat(gcsfs.exists(new URI("gs://" + gcsfsIHelper.sharedBucketName1))).isFalse();
    assertThat(gcsfs.exists(new URI("gs://" + gcsfsIHelper.sharedBucketName2))).isFalse();
  }

  @Test
  public void getModificationTime_changeForDirectoryWenMetadataAddedToFile() throws Exception {
    String directoryObjectName = "test-modification-timestamps/create-dir/";
    String childFilename = "file.txt";
    URI directory = gcsfsIHelper.getPath(gcsfsIHelper.sharedBucketName1, directoryObjectName);

    gcsfs.mkdir(directory);

    FileInfo directoryInfo = gcsfs.getFileInfo(directory);

    gcsfsIHelper.writeFile(
        gcsfsIHelper.sharedBucketName1, childFilename, "foo".getBytes(UTF_8), 1, true);

    StorageResourceId resourceId =
        new StorageResourceId(gcsfsIHelper.sharedBucketName1, directoryObjectName);
    Map<String, byte[]> updatedMetadata = ImmutableMap.of("test-metadata", "test-value".getBytes());

    List<GoogleCloudStorageItemInfo> updatedObjects =
        gcsfs
            .getGcs()
            .updateItems(ImmutableList.of(new UpdatableItemInfo(resourceId, updatedMetadata)));

    FileInfo newDirectoryInfo = gcsfs.getFileInfo(directory);

    assertThat(updatedObjects.get(0).getMetadata().keySet()).isEqualTo(updatedMetadata.keySet());

    assertWithMessage("Modification times should not be equal")
        .that(newDirectoryInfo.getModificationTime())
        .isNotEqualTo(directoryInfo.getModificationTime());

    long timeDelta = directoryInfo.getCreationTime() - newDirectoryInfo.getModificationTime();
    assertThat(Math.abs(timeDelta)).isLessThan(TimeUnit.MINUTES.toMillis(10));
  }
}
