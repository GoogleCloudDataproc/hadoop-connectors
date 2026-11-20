/*
 * Copyright 2026 Google LLC
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

package com.google.cloud.hadoop.gcsio.integration;

import static com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.getStandardOptionBuilder;
import static com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.writeObject;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.Assert.assertThrows;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemImpl;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.ListObjectOptions;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.TestBucketHelper;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.cloud.hadoop.util.RequesterPaysOptions;
import com.google.cloud.hadoop.util.RequesterPaysOptions.RequesterPaysMode;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.cache.Cache;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.UUID;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Integration tests for Requester Pays against a real Requester Pays bucket.
 *
 * <p>By default the test creates its own bucket in {@code GCS_TEST_PROJECT_ID}, turns on Requester
 * Pays, bills requests to {@code GCS_TEST_PROJECT_ID} and deletes the bucket afterwards. The test
 * identity needs permission to create buckets and {@code serviceusage.services.use} on that
 * project, both of which {@code roles/editor} has.
 *
 * <p>Findings that shaped this test:
 *
 * <ul>
 *   <li>A Requester Pays bucket in the caller's own project still enforces Requester Pays. A
 *       service account with {@code roles/editor} on the project that owns the bucket got {@code
 *       400 "Bucket is a requester pays bucket but no user project provided."} on {@code
 *       buckets.getStorageLayout} and {@code objects.list} without a billing project. So the CI
 *       identity reproduces the failure on a bucket it creates; no bucket in another project is
 *       needed.
 *   <li>Some principals are still exempt: a user account got {@code 200} on the same request
 *       without a billing project. With such an identity these tests would pass without exercising
 *       Requester Pays, so {@link #requesterPaysDisabled_requestsAreRejected()} fails if requests
 *       without a billing project are not rejected.
 * </ul>
 *
 * <p>Set {@value #REQUESTER_PAYS_BUCKET_ENV} to test an existing Requester Pays bucket instead (for
 * example one in another project). The test identity then needs {@code roles/storage.objectAdmin}
 * on it.
 */
@RunWith(JUnit4.class)
public class RequesterPaysIntegrationTest {

  static final String REQUESTER_PAYS_BUCKET_ENV = "GCS_TEST_REQUESTER_PAYS_BUCKET";

  private static TestBucketHelper bucketHelper;
  private static String bucket;

  @Rule public TestName name = new TestName();

  private String prefix;
  private GoogleCloudStorage gcs;
  private GoogleCloudStorageFileSystem gcsfs;

  @BeforeClass
  public static void beforeAll() throws Exception {
    String existingBucket = System.getenv(REQUESTER_PAYS_BUCKET_ENV);
    if (existingBucket != null && !existingBucket.isEmpty()) {
      bucket = existingBucket;
      return;
    }
    bucketHelper = new TestBucketHelper("dataproc-requester-pays");
    bucket = bucketHelper.getUniqueBucketName("rp");
    Storage storage =
        StorageOptions.newBuilder()
            .setProjectId(billingProjectId())
            .setCredentials(GoogleCloudStorageTestHelper.getCredentials())
            .build()
            .getService();
    try {
      storage.create(BucketInfo.newBuilder(bucket).setRequesterPays(true).build());
    } finally {
      storage.close();
    }
  }

  @AfterClass
  public static void afterAll() throws Exception {
    if (bucketHelper == null) {
      return;
    }
    // Deleting objects and buckets in a Requester Pays bucket also needs a billing project.
    GoogleCloudStorage cleanupGcs = createGcs(requesterPaysOptions(RequesterPaysMode.ENABLED));
    try {
      bucketHelper.cleanup(cleanupGcs);
    } finally {
      bucketHelper = null;
      cleanupGcs.close();
    }
  }

  @Before
  public void before() throws Exception {
    // Nested under a parent directory, like a task attempt directory under the job output path.
    // Deleting a top-level directory would also look up the bucket (storage.buckets.get).
    prefix = "requester-pays-it/" + name.getMethodName() + "-" + UUID.randomUUID() + "/";
    // isHnBucket results are cached JVM-wide per bucket; clear them so each test sends its own
    // getStorageLayout request.
    clearHnBucketCache();
  }

  @After
  public void after() throws IOException {
    if (gcsfs != null) {
      try {
        gcsfs.delete(dirUri(), /* recursive= */ true);
      } catch (IOException ignored) {
        // Best effort, the test may already have deleted it.
      }
      gcsfs.close();
    }
    if (gcs != null) {
      gcs.close();
    }
  }

  /** Confirms the bucket enforces Requester Pays for the test identity. */
  @Test
  public void requesterPaysDisabled_requestsAreRejected() throws Exception {
    gcs = createGcs(RequesterPaysOptions.DEFAULT);

    IOException e =
        assertThrows(
            IOException.class,
            () -> gcs.listObjectInfo(bucket, prefix, ListObjectOptions.DEFAULT_FLAT_LIST));

    assertWithMessage(
            "The test identity must not be exempt from Requester Pays on %s; got: %s", bucket, e)
        .that(ApiErrorExtractor.INSTANCE.userProjectMissing(e))
        .isTrue();
  }

  /** buckets.getStorageLayout has no userProject field, so the project must be sent as a header. */
  @Test
  public void isHnBucket_requesterPaysEnabled_succeeds() throws Exception {
    gcs = createGcs(requesterPaysOptions(RequesterPaysMode.ENABLED));

    assertThat(gcs.isHnBucket(new URI("gs://" + bucket))).isFalse();
  }

  @Test
  public void isHnBucket_requesterPaysCustom_succeeds() throws Exception {
    gcs =
        createGcs(
            requesterPaysOptions(RequesterPaysMode.CUSTOM).toBuilder().setBuckets(bucket).build());

    assertThat(gcs.isHnBucket(new URI("gs://" + bucket))).isFalse();
  }

  @Test
  public void isHnBucket_requesterPaysAuto_succeeds() throws Exception {
    gcs = createGcs(requesterPaysOptions(RequesterPaysMode.AUTO));

    assertThat(gcs.isHnBucket(new URI("gs://" + bucket))).isFalse();
  }

  /**
   * Same calls Hadoop's FileOutputCommitter makes when it cleans up a task attempt directory:
   * {@code delete} runs {@code isHnBucket}, which used to fail on Requester Pays buckets.
   */
  @Test
  public void recursiveDelete_requesterPaysEnabled_succeeds() throws Exception {
    GoogleCloudStorageOptions gcsOptions =
        gcsOptions(requesterPaysOptions(RequesterPaysMode.ENABLED));
    gcs =
        GoogleCloudStorageImpl.builder()
            .setOptions(gcsOptions)
            .setCredentials(GoogleCloudStorageTestHelper.getCredentials())
            .build();
    gcsfs =
        new GoogleCloudStorageFileSystemImpl(
            GoogleCloudStorageTestHelper.getCredentials(),
            GoogleCloudStorageFileSystemOptions.builder()
                .setCloudStorageOptions(gcsOptions)
                .build());
    writeObject(gcs, new StorageResourceId(bucket, prefix + "part-00000"), /* objectSize= */ 1024);
    writeObject(gcs, new StorageResourceId(bucket, prefix + "part-00001"), /* objectSize= */ 1024);

    gcsfs.delete(dirUri(), /* recursive= */ true);

    assertThat(gcsfs.exists(dirUri())).isFalse();
  }

  private URI dirUri() {
    return URI.create("gs://" + bucket + "/" + prefix);
  }

  private static GoogleCloudStorage createGcs(RequesterPaysOptions requesterPaysOptions)
      throws IOException {
    return GoogleCloudStorageImpl.builder()
        .setOptions(gcsOptions(requesterPaysOptions))
        .setCredentials(GoogleCloudStorageTestHelper.getCredentials())
        .build();
  }

  private static GoogleCloudStorageOptions gcsOptions(RequesterPaysOptions requesterPaysOptions) {
    // JSON API writes only, to keep the test focused on the JSON request path.
    return getStandardOptionBuilder()
        .setGrpcWriteEnabled(false)
        .setRequesterPaysOptions(requesterPaysOptions)
        .build();
  }

  private static RequesterPaysOptions requesterPaysOptions(RequesterPaysMode mode) {
    return RequesterPaysOptions.builder().setMode(mode).setProjectId(billingProjectId()).build();
  }

  private static String billingProjectId() {
    return getStandardOptionBuilder().build().getProjectId();
  }

  private static void clearHnBucketCache() throws ReflectiveOperationException {
    Field cacheField = GoogleCloudStorageImpl.class.getDeclaredField("cache");
    cacheField.setAccessible(true);
    ((Cache<?, ?>) cacheField.get(null)).invalidateAll();
  }
}
