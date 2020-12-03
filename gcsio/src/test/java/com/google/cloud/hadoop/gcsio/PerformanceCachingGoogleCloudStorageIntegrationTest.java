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

import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.listRequestWithTrailingDelimiter;
import static com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.getStandardOptionBuilder;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.TestBucketHelper;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.TrackingStorageWrapper;
import java.io.IOException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration tests for {@link PerformanceCachingGoogleCloudStorage}. */
@RunWith(JUnit4.class)
public class PerformanceCachingGoogleCloudStorageIntegrationTest {

  private static final TestBucketHelper BUCKET_HELPER = new TestBucketHelper("gcs-perf-cache");
  private static final String TEST_BUCKET = BUCKET_HELPER.getUniqueBucketPrefix();

  private static final GoogleCloudStorageOptions GCS_OPTIONS = getStandardOptionBuilder().build();

  private static final PerformanceCachingGoogleCloudStorageOptions PERF_CACHE_GCS_OPTIONS =
      PerformanceCachingGoogleCloudStorageOptions.DEFAULT.toBuilder()
          .setMaxEntryAgeMillis(10_000)
          .build();

  private static GoogleCloudStorage helperGcs;

  @Rule public TestName name = new TestName();

  @BeforeClass
  public static void beforeAll() throws IOException {
    helperGcs = GoogleCloudStorageTestHelper.createGoogleCloudStorage();
    helperGcs.createBucket(TEST_BUCKET);
  }

  @AfterClass
  public static void afterAll() throws IOException {
    try {
      BUCKET_HELPER.cleanup(helperGcs);
    } finally {
      helperGcs.close();
    }
  }

  @Test
  public void getItemInfo_multipleCalls_oneGcsListRequest() throws Exception {
    String parentDirName = name.getMethodName();
    StorageResourceId resourceId = new StorageResourceId(TEST_BUCKET, parentDirName + "/object");

    TrackingStorageWrapper<PerformanceCachingGoogleCloudStorage> trackingGcs =
        newTrackingGoogleCloudStorage(GCS_OPTIONS);

    helperGcs.createEmptyObject(resourceId);

    GoogleCloudStorageItemInfo object1 = trackingGcs.delegate.getItemInfo(resourceId);
    GoogleCloudStorageItemInfo object2 = trackingGcs.delegate.getItemInfo(resourceId);
    GoogleCloudStorageItemInfo object3 = trackingGcs.delegate.getItemInfo(resourceId);

    assertThat(object1.getResourceId()).isEqualTo(resourceId);
    assertThat(object1).isSameInstanceAs(object2);
    assertThat(object1).isSameInstanceAs(object3);

    assertThat(trackingGcs.requestsTracker.getAllRequestStrings())
        .containsExactly(
            listRequestWithTrailingDelimiter(
                TEST_BUCKET, parentDirName + "/", /* maxResults= */ 1024, /* pageToken= */ null));
  }

  private static TrackingStorageWrapper<PerformanceCachingGoogleCloudStorage>
      newTrackingGoogleCloudStorage(GoogleCloudStorageOptions options) throws IOException {
    return new TrackingStorageWrapper<>(
        options,
        httpRequestInitializer ->
            new PerformanceCachingGoogleCloudStorage(
                new GoogleCloudStorageImpl(options, httpRequestInitializer),
                PERF_CACHE_GCS_OPTIONS));
  }
}
