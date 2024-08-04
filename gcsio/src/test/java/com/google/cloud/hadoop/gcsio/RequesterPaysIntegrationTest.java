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

import static com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.writeObject;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.api.client.auth.oauth2.Credential;
import com.google.auth.Credentials;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.TestBucketHelper;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.TrackingStorageWrapper;
import com.google.cloud.hadoop.gcsio.testing.TestConfiguration;
import com.google.cloud.hadoop.util.RequesterPaysOptions;
import com.google.cloud.hadoop.util.RequesterPaysOptions.RequesterPaysMode;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class RequesterPaysIntegrationTest {

  private final boolean testStorageClientImpl;
  private TestBucketHelper bucketHelper;
  private String testBucket;
  private Storage bucketStorageClient;

  public RequesterPaysIntegrationTest(boolean tesStorageClientImpl) {
    this.testStorageClientImpl = tesStorageClientImpl;
  }

  @Parameters
  // We want to test this entire class with both javaClientImpl and gcsImpl
  // Some of our internal endpoints only work with TD
  public static Iterable<Boolean> javaClientEnabled() {
    return ImmutableList.of(false, true);
  }

  @Rule
  public TestName name =
      new TestName() {
        // With parametrization method name will get [index] appended in their name.
        @Override
        public String getMethodName() {
          return super.getMethodName().replaceAll("[\\[,\\]]", "");
        }
      };

  @Before
  public void before() throws IOException {
    bucketHelper = new TestBucketHelper("dataproc-requesterpays");
    testBucket = bucketHelper.getUniqueBucketPrefix();
    // Initalizing a storage client as gcsio abstraction don't offer a way to ingested
    // "requesterPays" filed while creating bucket.
    // Doesn't matter which client library was enabled it always uses java-storage client to create
    // bucket
    bucketStorageClient =
        StorageOptions.grpc()
            .setCredentials(GoogleCloudStorageTestHelper.getCredentials())
            .build()
            .getService();
    bucketStorageClient.create(BucketInfo.newBuilder(testBucket).setRequesterPays(true).build());
  }

  @After
  public void after() throws Exception {
    GoogleCloudStorage helperGcs =
        GoogleCloudStorageClientImpl.builder()
            .setOptions(getStandardOptionBuilder().build())
            .setCredentials(GoogleCloudStorageTestHelper.getCredentials())
            .setCredential(GoogleCloudStorageTestHelper.getCredential())
            .build();
    try {
      bucketHelper.cleanup(helperGcs);
    } finally {
      bucketStorageClient.close();
      helperGcs.close();
    }
  }

  @Test
  public void requesterPays_autoMode() throws Exception {
    TrackingStorageWrapper<GoogleCloudStorage> trackingGcs =
        newTrackingGoogleCloudStorage(
            GoogleCloudStorageTestHelper.getStandardOptionBuilder()
                .setRequesterPaysOptions(
                    RequesterPaysOptions.DEFAULT
                        .toBuilder()
                        .setMode(RequesterPaysMode.AUTO)
                        .setProjectId(TestConfiguration.getInstance().getProjectId())
                        .build())
                .build());
    int expectedSize = 5 * 1024 * 1024;
    StorageResourceId resourceId = new StorageResourceId(testBucket, name.getMethodName());
    writeObject(
        trackingGcs.delegate,
        resourceId,
        /* partitionSize= */ expectedSize,
        /* partitionsCount= */ 1);

    assertThat(trackingGcs.requestsTracker.getAllRequestInvocationIds().size())
        .isEqualTo(trackingGcs.requestsTracker.getAllRequests().size());

    assertThat(trackingGcs.getAllRequestStrings())
        .contains(testIamPermissionRequestString(testBucket, "storage.buckets.get"));

    trackingGcs.requestsTracker.reset();
    // any other request will not result in call to testIamPermissions API
    // Also every other request will have "userProject" header
    trackingGcs.delegate.getItemInfo(resourceId);
    verifyUserProject(
        trackingGcs.requestsTracker.getAllRawRequestStrings(),
        TestConfiguration.getInstance().getProjectId());
  }

  private void verifyUserProject(List<String> rawRequestString, String projectId) {
    String userProjectFiled = "userProject=" + projectId;
    for (String reqString : rawRequestString) {
      assertTrue(reqString.contains(userProjectFiled));
    }
  }

  private TrackingStorageWrapper<GoogleCloudStorage> newTrackingGoogleCloudStorage(
      GoogleCloudStorageOptions options) throws IOException {
    Credential credential = GoogleCloudStorageTestHelper.getCredential();
    Credentials credentials = GoogleCloudStorageTestHelper.getCredentials();
    return new TrackingStorageWrapper<>(
        options,
        (httpRequestInitializer, grpcRequestInterceptors) ->
            testStorageClientImpl
                ? GoogleCloudStorageClientImpl.builder()
                    .setOptions(options)
                    .setCredentials(credentials)
                    .setCredential(credential)
                    .setHttpRequestInitializer(httpRequestInitializer)
                    .setGRPCInterceptors(grpcRequestInterceptors)
                    .build()
                : new GoogleCloudStorageImpl(options, httpRequestInitializer),
        credential);
  }

  public static GoogleCloudStorageOptions.Builder getStandardOptionBuilder() {
    return GoogleCloudStorageOptions.builder()
        .setAppName(GoogleCloudStorageTestHelper.APP_NAME)
        .setDirectPathPreferred(TestConfiguration.getInstance().isDirectPathPreferred())
        .setGrpcWriteEnabled(true)
        .setRequesterPaysOptions(
            RequesterPaysOptions.DEFAULT
                .toBuilder()
                .setMode(RequesterPaysMode.ENABLED)
                .setProjectId(checkNotNull(TestConfiguration.getInstance().getProjectId()))
                .build())
        .setProjectId(checkNotNull(TestConfiguration.getInstance().getProjectId()));
  }

  private String testIamPermissionRequestString(String bucketName, String permissions) {
    if (this.testStorageClientImpl) {
      return TrackingGrpcRequestInterceptor.getTestIamPermissionRequestFormat();
    }
    return TrackingHttpRequestInitializer.getTestIamPermissionRequestFormat(
        bucketName, permissions);
  }
}
