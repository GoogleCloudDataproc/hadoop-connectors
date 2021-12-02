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

import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.getRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.listRequestWithTrailingDelimiter;
import static com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.getStandardOptionBuilder;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.TestBucketHelper;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.TrackingStorageWrapper;
import java.io.IOException;
import java.net.URI;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration tests for {@link GoogleCloudStorageFileSystem} with enabled performance cache. */
@RunWith(JUnit4.class)
public class GoogleCloudStorageFileSystemPerformanceCacheIntegrationTest {

  private static final TestBucketHelper BUCKET_HELPER = new TestBucketHelper("gcs-fs-perf-cache");
  private static final String TEST_BUCKET = BUCKET_HELPER.getUniqueBucketPrefix();

  private static final GoogleCloudStorageFileSystemOptions GCSFS_OPTIONS =
      GoogleCloudStorageFileSystemOptions.builder()
          .setCloudStorageOptions(getStandardOptionBuilder().build())
          .build();

  private static final PerformanceCachingGoogleCloudStorageOptions PERF_CACHE_GCS_OPTIONS =
      PerformanceCachingGoogleCloudStorageOptions.DEFAULT
          .toBuilder()
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
  public void getFileInfo_parallel_notFound() throws Exception {
    String parentDir = name.getMethodName();
    String file = parentDir + "/file";
    URI fileUri = UriPaths.fromStringPathComponents(TEST_BUCKET, file, false);

    TrackingStorageWrapper<GoogleCloudStorageFileSystem> trackingGcsfs =
        newTrackingGoogleCloudStorageFileSystem(GCSFS_OPTIONS);

    FileInfo dirInfo1 = trackingGcsfs.delegate.getFileInfo(fileUri);
    FileInfo dirInfo2 = trackingGcsfs.delegate.getFileInfo(fileUri);
    FileInfo dirInfo3 = trackingGcsfs.delegate.getFileInfo(fileUri);

    assertThat(dirInfo1.exists()).isFalse();
    assertThat(dirInfo1.getPath()).isEqualTo(fileUri);
    assertThat(dirInfo1).isEqualTo(dirInfo2);
    assertThat(dirInfo1).isEqualTo(dirInfo3);

    assertThat(trackingGcsfs.requestsTracker.getAllRequestStrings())
        .containsExactly(
            getRequestString(TEST_BUCKET, file),
            getRequestString(TEST_BUCKET, file),
            getRequestString(TEST_BUCKET, file),
            listRequestWithTrailingDelimiter(
                TEST_BUCKET, file + "/", /* maxResults= */ 1, /* pageToken= */ null),
            listRequestWithTrailingDelimiter(
                TEST_BUCKET, file + "/", /* maxResults= */ 1, /* pageToken= */ null),
            listRequestWithTrailingDelimiter(
                TEST_BUCKET, file + "/", /* maxResults= */ 1, /* pageToken= */ null));
  }

  @Test
  public void getFileInfo_parallel_notFound_parentExists() throws Exception {
    String parentDir = name.getMethodName();
    String file = parentDir + "/file";
    URI fileUri = UriPaths.fromStringPathComponents(TEST_BUCKET, file, false);

    TrackingStorageWrapper<GoogleCloudStorageFileSystem> trackingGcsfs =
        newTrackingGoogleCloudStorageFileSystem(GCSFS_OPTIONS);

    helperGcs.createEmptyObject(new StorageResourceId(TEST_BUCKET, parentDir + "/"));

    FileInfo dirInfo1 = trackingGcsfs.delegate.getFileInfo(fileUri);
    FileInfo dirInfo2 = trackingGcsfs.delegate.getFileInfo(fileUri);
    FileInfo dirInfo3 = trackingGcsfs.delegate.getFileInfo(fileUri);

    assertThat(dirInfo1.exists()).isFalse();
    assertThat(dirInfo1.getPath()).isEqualTo(fileUri);
    assertThat(dirInfo1).isEqualTo(dirInfo2);
    assertThat(dirInfo1).isEqualTo(dirInfo3);

    assertThat(trackingGcsfs.requestsTracker.getAllRequestStrings())
        .containsExactly(
            getRequestString(TEST_BUCKET, file),
            getRequestString(TEST_BUCKET, file),
            getRequestString(TEST_BUCKET, file),
            listRequestWithTrailingDelimiter(
                TEST_BUCKET, file + "/", /* maxResults= */ 1, /* pageToken= */ null),
            listRequestWithTrailingDelimiter(
                TEST_BUCKET, file + "/", /* maxResults= */ 1, /* pageToken= */ null),
            listRequestWithTrailingDelimiter(
                TEST_BUCKET, file + "/", /* maxResults= */ 1, /* pageToken= */ null));
  }

  @Test
  public void getFileInfo_parallel_notFound_siblingExists() throws Exception {
    String parentDir = name.getMethodName();
    String file = parentDir + "/file";
    URI fileUri = UriPaths.fromStringPathComponents(TEST_BUCKET, file, false);

    TrackingStorageWrapper<GoogleCloudStorageFileSystem> trackingGcsfs =
        newTrackingGoogleCloudStorageFileSystem(GCSFS_OPTIONS);

    helperGcs.createEmptyObject(new StorageResourceId(TEST_BUCKET, file + "_sibling"));

    FileInfo dirInfo1 = trackingGcsfs.delegate.getFileInfo(fileUri);
    FileInfo dirInfo2 = trackingGcsfs.delegate.getFileInfo(fileUri);
    FileInfo dirInfo3 = trackingGcsfs.delegate.getFileInfo(fileUri);

    assertThat(dirInfo1.exists()).isFalse();
    assertThat(dirInfo1.getPath()).isEqualTo(fileUri);
    assertThat(dirInfo1).isEqualTo(dirInfo2);
    assertThat(dirInfo1).isEqualTo(dirInfo3);

    assertThat(trackingGcsfs.requestsTracker.getAllRequestStrings())
        .containsExactly(
            getRequestString(TEST_BUCKET, file),
            getRequestString(TEST_BUCKET, file),
            getRequestString(TEST_BUCKET, file),
            listRequestWithTrailingDelimiter(
                TEST_BUCKET, file + "/", /* maxResults= */ 1, /* pageToken= */ null),
            listRequestWithTrailingDelimiter(
                TEST_BUCKET, file + "/", /* maxResults= */ 1, /* pageToken= */ null),
            listRequestWithTrailingDelimiter(
                TEST_BUCKET, file + "/", /* maxResults= */ 1, /* pageToken= */ null));
  }

  @Test
  public void getFileInfo_parallel_multipleRequests() throws Exception {
    String parentDir = name.getMethodName();
    String file = parentDir + "/file";
    URI fileUri = UriPaths.fromStringPathComponents(TEST_BUCKET, file, false);

    TrackingStorageWrapper<GoogleCloudStorageFileSystem> trackingGcsfs =
        newTrackingGoogleCloudStorageFileSystem(GCSFS_OPTIONS);

    helperGcs.createEmptyObject(new StorageResourceId(TEST_BUCKET, file));

    FileInfo fileInfo1 = trackingGcsfs.delegate.getFileInfo(fileUri);
    FileInfo fileInfo2 = trackingGcsfs.delegate.getFileInfo(fileUri);
    FileInfo fileInfo3 = trackingGcsfs.delegate.getFileInfo(fileUri);

    assertThat(fileInfo1.exists()).isTrue();
    assertThat(fileInfo1.getPath()).isEqualTo(fileUri);
    assertThat(fileInfo1).isEqualTo(fileInfo2);
    assertThat(fileInfo1.getItemInfo()).isSameInstanceAs(fileInfo2.getItemInfo());
    assertThat(fileInfo1).isEqualTo(fileInfo3);
    assertThat(fileInfo1.getItemInfo()).isSameInstanceAs(fileInfo3.getItemInfo());

    assertThat(trackingGcsfs.requestsTracker.getAllRequestStrings())
        .containsExactly(
            getRequestString(TEST_BUCKET, file),
            listRequestWithTrailingDelimiter(
                TEST_BUCKET, file + "/", /* maxResults= */ 1, /* pageToken= */ null));
  }

  @Test
  public void getDirInfo_parallel_notFound() throws Exception {
    String parentDir = name.getMethodName();
    String dir = parentDir + "/dir/";
    URI dirUri = UriPaths.fromStringPathComponents(TEST_BUCKET, dir, false);

    TrackingStorageWrapper<GoogleCloudStorageFileSystem> trackingGcsfs =
        newTrackingGoogleCloudStorageFileSystem(GCSFS_OPTIONS);

    FileInfo dirInfo1 = trackingGcsfs.delegate.getFileInfo(dirUri);
    FileInfo dirInfo2 = trackingGcsfs.delegate.getFileInfo(dirUri);
    FileInfo dirInfo3 = trackingGcsfs.delegate.getFileInfo(dirUri);

    assertThat(dirInfo1.exists()).isFalse();
    assertThat(dirInfo1.getPath()).isEqualTo(dirUri);
    assertThat(dirInfo1).isEqualTo(dirInfo2);
    assertThat(dirInfo1).isEqualTo(dirInfo3);

    assertThat(trackingGcsfs.requestsTracker.getAllRequestStrings())
        .containsExactly(
            listRequestWithTrailingDelimiter(
                TEST_BUCKET, dir, /* maxResults= */ 1, /* pageToken= */ null),
            listRequestWithTrailingDelimiter(
                TEST_BUCKET, dir, /* maxResults= */ 1, /* pageToken= */ null),
            listRequestWithTrailingDelimiter(
                TEST_BUCKET, dir, /* maxResults= */ 1, /* pageToken= */ null));
  }

  @Test
  public void getDirInfo_parallel_multipleRequests() throws Exception {
    getDirInfo_parallel_multipleRequests(/* testImplicitDirectories= */ false);
  }

  @Test
  public void getDirInfo_implicit_parallel_multipleRequests() throws Exception {
    getDirInfo_parallel_multipleRequests(/* testImplicitDirectories= */ true);
  }

  private void getDirInfo_parallel_multipleRequests(boolean testImplicitDirectories)
      throws Exception {
    String parentDir = name.getMethodName();
    String dir = parentDir + "/dir";
    URI dirUri = UriPaths.fromStringPathComponents(TEST_BUCKET, dir, false);

    TrackingStorageWrapper<GoogleCloudStorageFileSystem> trackingGcsfs =
        newTrackingGoogleCloudStorageFileSystem(GCSFS_OPTIONS);

    if (!testImplicitDirectories) {
      helperGcs.createEmptyObject(new StorageResourceId(TEST_BUCKET, dir + "/"));
    }
    helperGcs.createEmptyObject(new StorageResourceId(TEST_BUCKET, dir + "/file1"));
    helperGcs.createEmptyObject(new StorageResourceId(TEST_BUCKET, dir + "/file2"));

    FileInfo dirInfo1 = trackingGcsfs.delegate.getFileInfo(dirUri);
    FileInfo dirInfo2 = trackingGcsfs.delegate.getFileInfo(dirUri);
    FileInfo dirInfo3 = trackingGcsfs.delegate.getFileInfo(dirUri);

    assertThat(dirInfo1.exists()).isTrue();
    assertThat(dirInfo1.getPath().toString()).isEqualTo(dirUri + "/");
    assertThat(dirInfo1).isEqualTo(dirInfo2);
    assertThat(dirInfo1).isEqualTo(dirInfo3);
    assertThat(dirInfo2.getItemInfo()).isSameInstanceAs(dirInfo3.getItemInfo());

    assertThat(trackingGcsfs.requestsTracker.getAllRequestStrings())
        .containsExactly(
            getRequestString(TEST_BUCKET, dir),
            listRequestWithTrailingDelimiter(
                TEST_BUCKET, dir + "/", /* maxResults= */ 1, /* pageToken= */ null));
  }

  private static TrackingStorageWrapper<GoogleCloudStorageFileSystem>
      newTrackingGoogleCloudStorageFileSystem(GoogleCloudStorageFileSystemOptions gcsfsOptions)
          throws IOException {
    return new TrackingStorageWrapper<>(
        gcsfsOptions.getCloudStorageOptions(),
        httpRequestInitializer ->
            new GoogleCloudStorageFileSystem(
                gcsOptions ->
                    new PerformanceCachingGoogleCloudStorage(
                        new GoogleCloudStorageImpl(gcsOptions, httpRequestInitializer),
                        PERF_CACHE_GCS_OPTIONS),
                gcsfsOptions));
  }
}
