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

package com.google.cloud.hadoop.gcsio;

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl.encodeMetadata;
import static com.google.cloud.hadoop.gcsio.MockGoogleCloudStorageImplFactory.mockedGcsClientImpl;
import static com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTest.BYTE_ARRAY_EQUIVALENCE;
import static com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTest.assertMapsEqual;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.mockTransport;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.google.api.client.testing.http.MockHttpTransport;
import com.google.cloud.hadoop.gcsio.testing.FakeServer;
import com.google.cloud.hadoop.gcsio.testing.MockStorage;
import com.google.cloud.storage.StorageException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.protobuf.Empty;
import com.google.protobuf.Timestamp;
import com.google.storage.v2.Bucket;
import com.google.storage.v2.Bucket.Lifecycle;
import com.google.storage.v2.Bucket.Lifecycle.Rule;
import com.google.storage.v2.Bucket.Lifecycle.Rule.Action;
import com.google.storage.v2.Bucket.Lifecycle.Rule.Condition;
import com.google.storage.v2.BucketName;
import com.google.storage.v2.CreateBucketRequest;
import com.google.storage.v2.DeleteBucketRequest;
import com.google.storage.v2.DeleteObjectRequest;
import com.google.storage.v2.ListBucketsResponse;
import com.google.storage.v2.Object;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link GoogleCloudStorageClientImpl} */
@RunWith(JUnit4.class)
public class GoogleCloudStorageClientTest {

  private static final String TEST_BUCKET_NAME = "foo-bucket";

  private static final String TEST_OBJECT_NAME = "foo-object";

  private static final Timestamp CREATE_TIME = Timestamp.newBuilder().build();

  private static final Timestamp UPDATE_TIME = CREATE_TIME;

  private static final String BUCKET_STORAGE_CLASS = "STANDARD";

  private static final String BUCKET_LOCATION = "some-location";

  private static final int TTL_DAYS = 10;

  private static final int GENERATION = 123456;

  private static final Bucket TEST_BUCKET =
      Bucket.newBuilder()
          .setName(TEST_BUCKET_NAME)
          .setCreateTime(CREATE_TIME)
          .setUpdateTime(UPDATE_TIME)
          .build();

  private static final Bucket TEST_BUCKET_WITH_OPTIONS =
      Bucket.newBuilder()
          .setName("foo-bar-bucket")
          .setStorageClass(BUCKET_STORAGE_CLASS)
          .setLocation(BUCKET_LOCATION)
          .setCreateTime(CREATE_TIME)
          .setUpdateTime(UPDATE_TIME)
          .setLifecycle(
              Lifecycle.newBuilder()
                  .addRule(
                      Rule.newBuilder()
                          .setAction(Action.newBuilder().setType("Delete"))
                          .setCondition(Condition.newBuilder().setAgeDays(TTL_DAYS).build())
                          .build())
                  .build())
          .build();

  private static final Object TEST_OBJECT =
      Object.newBuilder()
          .setName(TEST_OBJECT_NAME)
          .setBucket(BucketName.of("", TEST_BUCKET_NAME).toString())
          .setGeneration(GENERATION)
          .setMetageneration(123L)
          .setCreateTime(CREATE_TIME)
          .setUpdateTime(UPDATE_TIME)
          .setSize(1234)
          .build();

  private static final MockHttpTransport transport = mockTransport();

  private static final MockStorage mockStorage = new MockStorage();

  @Before
  public void setUp() {
    mockStorage.reset();
  }

  @Test
  public void createBucket_succeeds() throws Exception {
    mockStorage.addResponse(TEST_BUCKET);

    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());
      gcs.createBucket(TEST_BUCKET_NAME);
    }

    assertEquals(mockStorage.getRequests().size(), 1);

    CreateBucketRequest bucketRequest = (CreateBucketRequest) mockStorage.getRequests().get(0);
    assertEquals(bucketRequest.getBucketId(), TEST_BUCKET_NAME);
  }

  @Test
  public void createBucket_withOptions_succeeds() throws Exception {
    mockStorage.addResponse(TEST_BUCKET_WITH_OPTIONS);

    CreateBucketOptions bucketOptions =
        CreateBucketOptions.builder()
            .setLocation(BUCKET_LOCATION)
            .setStorageClass(BUCKET_STORAGE_CLASS)
            .setTtl(Duration.ofDays(TTL_DAYS))
            .build();

    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());
      gcs.createBucket(TEST_BUCKET_NAME, bucketOptions);
    }

    assertEquals(mockStorage.getRequests().size(), 1);

    CreateBucketRequest bucketRequest = (CreateBucketRequest) mockStorage.getRequests().get(0);
    // Assert correct fields were set in request.
    assertEquals(bucketRequest.getBucketId(), TEST_BUCKET_NAME);
    assertEquals(bucketRequest.getBucket().getLocation(), BUCKET_LOCATION);
    assertEquals(bucketRequest.getBucket().getStorageClass(), BUCKET_STORAGE_CLASS);
    assertEquals(
        bucketRequest.getBucket().getLifecycle().getRule(0).getAction().getType(), "Delete");
    assertEquals(
        bucketRequest.getBucket().getLifecycle().getRule(0).getCondition().getAgeDays(), TTL_DAYS);
  }

  @Test
  public void createBucket_throwsFileAlreadyExistsException() throws Exception {
    mockStorage.addException(new StatusRuntimeException(Status.ALREADY_EXISTS));
    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());
      assertThrows(FileAlreadyExistsException.class, () -> gcs.createBucket(TEST_BUCKET_NAME));
    }
  }

  @Test
  public void createBucket_throwsIOException() throws Exception {
    mockStorage.addException(
        new StorageException(0, "Some exception", new StatusException(Status.INVALID_ARGUMENT)));

    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());
      assertThrows(IOException.class, () -> gcs.createBucket(TEST_BUCKET_NAME));
    }
  }

  @Test
  public void createBucket_illegalArguments() throws Exception {
    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());

      assertThrows(IllegalArgumentException.class, () -> gcs.createBucket(null));
      assertThrows(IllegalArgumentException.class, () -> gcs.createBucket(""));
    }
  }

  @Test
  public void deleteBuckets_illegalArguments() throws Exception {
    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());

      assertThrows(
          IllegalArgumentException.class,
          () -> gcs.deleteBuckets(Lists.newArrayList((String) null)));
      assertThrows(IllegalArgumentException.class, () -> gcs.deleteBuckets(Lists.newArrayList("")));
    }
  }

  @Test
  public void deleteBuckets_succeeds() throws Exception {
    mockStorage.addResponse(Empty.newBuilder().build());

    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());

      gcs.deleteBuckets(ImmutableList.of(TEST_BUCKET_NAME));
      assertEquals(mockStorage.getRequests().size(), 1);

      DeleteBucketRequest deleteBucketRequest =
          (DeleteBucketRequest) mockStorage.getRequests().get(0);
      assertThat(deleteBucketRequest.getName()).contains(TEST_BUCKET_NAME);
    }
  }

  @Test
  public void deleteBuckets_throwsException() throws Exception {
    mockStorage.addException(new StatusRuntimeException(Status.INVALID_ARGUMENT));

    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());

      assertThrows(IOException.class, () -> gcs.deleteBuckets(ImmutableList.of(TEST_BUCKET_NAME)));
    }
  }

  @Test
  public void deleteBuckets_throwsFileNotFoundException() throws Exception {
    mockStorage.addException(new StatusRuntimeException(Status.NOT_FOUND));

    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());

      assertThrows(
          FileNotFoundException.class, () -> gcs.deleteBuckets(ImmutableList.of(TEST_BUCKET_NAME)));
    }
  }

  @Test
  public void deleteObjects_illegalArguments() throws Exception {
    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());

      ImmutableList.of(StorageResourceId.ROOT, new StorageResourceId(TEST_BUCKET_NAME))
          .forEach(
              resourceId ->
                  assertThrows(
                      IllegalArgumentException.class,
                      () -> gcs.deleteObjects(ImmutableList.of(resourceId))));
    }
  }

  @Test
  public void deleteObjects_withoutGeneration_succeeds() throws Exception {
    // Mock response for get object call for fetching object metadata.
    mockStorage.addResponse(TEST_OBJECT);
    mockStorage.addResponse(Empty.newBuilder().build());

    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());

      gcs.deleteObjects(
          ImmutableList.of(new StorageResourceId(TEST_BUCKET_NAME, TEST_OBJECT_NAME)));

      assertEquals(mockStorage.getRequests().size(), 2);
      DeleteObjectRequest actualRequest = (DeleteObjectRequest) mockStorage.getRequests().get(1);
      assertThat(actualRequest.getBucket()).contains(TEST_BUCKET_NAME);
      assertEquals(actualRequest.getObject(), TEST_OBJECT_NAME);
    }
  }

  @Test
  public void deleteObjects_withoutGeneration_failsToGetGeneration() throws Exception {
    // Mock response for get object call for fetching object metadata.
    mockStorage.addException(new StatusException(Status.INVALID_ARGUMENT));
    mockStorage.addResponse(Empty.newBuilder().build());

    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());

      assertThrows(
          IOException.class,
          () ->
              gcs.deleteObjects(
                  ImmutableList.of(new StorageResourceId(TEST_BUCKET_NAME, TEST_OBJECT_NAME))));
    }
  }

  @Test
  public void deleteObjects_withoutGeneration_objectDoesntExist() throws Exception {
    // Mock response for get object call for fetching object metadata.
    mockStorage.addException(new StatusException(Status.NOT_FOUND));
    mockStorage.addResponse(Empty.newBuilder().build());

    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());

      gcs.deleteObjects(
          ImmutableList.of(new StorageResourceId(TEST_BUCKET_NAME, TEST_OBJECT_NAME)));

      // Delete is not issues if object is not found.
      assertEquals(mockStorage.getRequests().size(), 0);
    }
  }

  @Test
  public void deleteObjects_withGeneration_succeeds() throws Exception {
    mockStorage.addResponse(Empty.newBuilder().build());

    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());

      gcs.deleteObjects(
          ImmutableList.of(new StorageResourceId(TEST_BUCKET_NAME, TEST_OBJECT_NAME, GENERATION)));

      assertEquals(mockStorage.getRequests().size(), 1);
      DeleteObjectRequest actualRequest = (DeleteObjectRequest) mockStorage.getRequests().get(0);
      assertThat(actualRequest.getBucket()).contains(TEST_BUCKET_NAME);
      assertEquals(actualRequest.getObject(), TEST_OBJECT_NAME);
    }
  }

  @Test
  public void deleteObjects_throwsException() throws Exception {
    mockStorage.addException(new StatusRuntimeException(Status.INVALID_ARGUMENT));

    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());

      assertThrows(
          IOException.class,
          () ->
              gcs.deleteObjects(
                  ImmutableList.of(
                      new StorageResourceId(TEST_BUCKET_NAME, TEST_OBJECT_NAME, GENERATION))));
    }
  }

  @Test
  public void listBucketNames_succeeds() throws Exception {
    mockStorage.addResponse(
        ListBucketsResponse.newBuilder()
            .addBuckets(TEST_BUCKET)
            .addBuckets(TEST_BUCKET_WITH_OPTIONS)
            .build());

    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());
      List<String> listedBuckets = gcs.listBucketNames();
      assertThat(listedBuckets)
          .containsExactly(TEST_BUCKET.getName(), TEST_BUCKET_WITH_OPTIONS.getName());
    }
  }

  @Test
  public void listBucketInfo_succeeds() throws Exception {
    mockStorage.addResponse(
        ListBucketsResponse.newBuilder()
            .addBuckets(TEST_BUCKET)
            .addBuckets(TEST_BUCKET_WITH_OPTIONS)
            .build());
    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());

      List<GoogleCloudStorageItemInfo> listedBuckets = gcs.listBucketInfo();

      assertThat(listedBuckets)
          .containsExactly(
              GoogleCloudStorageItemInfo.createBucket(
                  new StorageResourceId(TEST_BUCKET.getName()), 0, 0, "", null),
              GoogleCloudStorageItemInfo.createBucket(
                  new StorageResourceId(TEST_BUCKET_WITH_OPTIONS.getName()),
                  0,
                  0,
                  BUCKET_LOCATION,
                  BUCKET_STORAGE_CLASS));
    }
  }

  @Test
  public void updateItems_succeeds() throws Exception {
    Map<String, byte[]> metadata =
        ImmutableMap.of(
            "key1", "value1".getBytes(StandardCharsets.UTF_8),
            "key2", "value2".getBytes(StandardCharsets.UTF_8));
    mockStorage.addResponse(
        TEST_OBJECT.toBuilder().putAllMetadata(encodeMetadata(metadata)).build());

    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());

      List<GoogleCloudStorageItemInfo> results =
          gcs.updateItems(
              ImmutableList.of(
                  new UpdatableItemInfo(
                      new StorageResourceId(TEST_BUCKET_NAME, TEST_OBJECT_NAME), metadata)));

      assertEquals(mockStorage.getRequests().size(), 1);
      assertEquals(results.size(), 1);
      assertMapsEqual(metadata, results.get(0).getMetadata(), BYTE_ARRAY_EQUIVALENCE);
    }
  }

  @Test
  public void updateItems_throwsException() throws Exception {
    mockStorage.addException(new StatusRuntimeException(Status.INVALID_ARGUMENT));
    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());

      assertThrows(
          IOException.class,
          () ->
              gcs.updateItems(
                  ImmutableList.of(
                      new UpdatableItemInfo(
                          new StorageResourceId(TEST_BUCKET_NAME, TEST_OBJECT_NAME),
                          ImmutableMap.of()))));
    }
  }

  @Test
  public void updateItems_returnsNotFound() throws Exception {
    mockStorage.addException(new StatusRuntimeException(Status.NOT_FOUND));
    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());

      List<GoogleCloudStorageItemInfo> results =
          gcs.updateItems(
              ImmutableList.of(
                  new UpdatableItemInfo(
                      new StorageResourceId(TEST_BUCKET_NAME, TEST_OBJECT_NAME),
                      ImmutableMap.of())));

      assertEquals(results.size(), 1);
      assertThat(results.get(0).exists()).isFalse();
    }
  }
}
