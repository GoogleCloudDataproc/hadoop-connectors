/*
 * Copyright 2019 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.gcsio;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.api.client.auth.oauth2.Credential;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper;
import com.google.cloud.hadoop.gcsio.testing.TestConfiguration;
import java.net.URI;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ForwardingGoogleCloudStorageIntegrationTest {

  private static GoogleCloudStorageFileSystemIntegrationHelper gcsfsIHelper;

  @BeforeClass
  public static void beforeClass() throws Throwable {
    String projectId =
        checkNotNull(TestConfiguration.getInstance().getProjectId(), "projectId can not be null");
    String appName = GoogleCloudStorageIntegrationHelper.APP_NAME;
    Credential credential =
        checkNotNull(GoogleCloudStorageTestHelper.getCredential(), "credential must not be null");

    GoogleCloudStorageOptions gcsOptions =
        GoogleCloudStorageOptions.newBuilder().setAppName(appName).setProjectId(projectId).build();

    GoogleCloudStorageFileSystem gcsfs =
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
  public void delegate_throwsExceptionWhenNull() {
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> new ForwardingGoogleCloudStorage(null));
    assertThat(exception).hasMessageThat().startsWith("delegate must not be null");
  }
}
