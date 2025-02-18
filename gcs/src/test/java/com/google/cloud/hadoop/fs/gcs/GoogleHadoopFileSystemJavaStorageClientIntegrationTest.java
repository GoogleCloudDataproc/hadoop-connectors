package com.google.cloud.hadoop.fs.gcs;
/*
 * Copyright 2023 Google LLC
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

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions.ClientType;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

// TODO: Ignored test for gRPC-directpath
// https://github.com/GoogleCloudDataproc/hadoop-connectors/issues/998
@RunWith(JUnit4.class)
public class GoogleHadoopFileSystemJavaStorageClientIntegrationTest
    extends GoogleHadoopFileSystemIntegrationTest {

  @Before
  public void before() throws Exception {
    storageClientType = ClientType.STORAGE_CLIENT;
    super.before();
  }

  @Ignore
  @Test
  public void testImpersonationGroupNameIdentifierUsed() {}

  @Ignore
  @Test
  public void testImpersonationServiceAccountAndUserAndGroupNameIdentifierUsed() {}

  @Ignore
  @Test
  public void testImpersonationServiceAccountUsed() {}

  @Ignore
  @Test
  public void testImpersonationUserAndGroupNameIdentifiersUsed() {}

  @Ignore
  @Test
  public void testImpersonationUserNameIdentifierUsed() {}

  @Ignore
  @Test
  public void unauthenticatedAccessToPublicBuckets_fsGsProperties() {}

  @Ignore
  @Test
  public void unauthenticatedAccessToPublicBuckets_googleCloudProperties() {}

  @Ignore
  @Test
  public void testGcsJsonAPIMetrics() {
    // TODO: Update this will once gRPC API metrics are added
  }

  @Ignore
  @Test
  public void testGetFileStatusWithHint() throws Exception {
    // TODO: Update this will once gRPC API metrics are added
  }

  @Ignore
  @Test
  public void testGcsThreadLocalMetrics() {
    // TODO: Update this will once gRPC API metrics are added
  }

  @Ignore
  @Test
  public void multiThreadTest() {
    // TODO: Update this will once gRPC API metrics are added
  }
}
