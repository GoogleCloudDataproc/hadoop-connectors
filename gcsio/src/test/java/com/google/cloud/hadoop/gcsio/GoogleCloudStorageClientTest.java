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
import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.hadoop.util.ErrorTypeExtractor.ErrorType;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.Empty;
import com.google.protobuf.Timestamp;
import com.google.storage.v2.Bucket;
import com.google.storage.v2.Bucket.HierarchicalNamespace;
import com.google.storage.v2.Bucket.Lifecycle;
import com.google.storage.v2.Bucket.Lifecycle.Rule;
import com.google.storage.v2.Bucket.Lifecycle.Rule.Action;
import com.google.storage.v2.Bucket.Lifecycle.Rule.Condition;
import com.google.storage.v2.BucketName;
import com.google.storage.v2.ComposeObjectRequest;
import com.google.storage.v2.CreateBucketRequest;
import com.google.storage.v2.DeleteBucketRequest;
import com.google.storage.v2.DeleteObjectRequest;
import com.google.storage.v2.ListBucketsResponse;
import com.google.storage.v2.MoveObjectRequest;
import com.google.storage.v2.Object;
import com.google.storage.v2.RewriteObjectRequest;
import com.google.storage.v2.RewriteResponse;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.time.Duration;
import java.util.Comparator;
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

  private static final String TEST_OBJECT_NAME_2 = "foo-object-2";

  private static final String OTHER_BUCKET_NAME = "other-bucket";

  private static final StorageResourceId TEST_RESOURCE_ID =
      new StorageResourceId(TEST_BUCKET_NAME, TEST_OBJECT_NAME);

  private static final StorageResourceId TEST_RESOURCE_ID_2 =
      new StorageResourceId(TEST_BUCKET_NAME, TEST_OBJECT_NAME_2);

  private static final Timestamp CREATE_TIME = Timestamp.newBuilder().build();

  private static final Timestamp UPDATE_TIME = CREATE_TIME;

  private static final String BUCKET_STORAGE_CLASS = "STANDARD";

  private static final String BUCKET_LOCATION = "some-location";

  private static final int TTL_DAYS = 10;

  private static final int GENERATION = 123456;

  private static final String ZONAL_PLACEMENT = "us-central1-a";

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

  private static final Bucket TEST_BUCKET_WITH_ZONAL_OPTIONS =
      Bucket.newBuilder()
          .setName(TEST_BUCKET_NAME)
          .setLocation(BUCKET_LOCATION)
          .setCustomPlacementConfig(
              Bucket.CustomPlacementConfig.newBuilder().addDataLocations(ZONAL_PLACEMENT).build())
          .setStorageClass("RAPID")
          .setIamConfig(
              Bucket.IamConfig.newBuilder()
                  .setUniformBucketLevelAccess(
                      Bucket.IamConfig.UniformBucketLevelAccess.newBuilder()
                          .setEnabled(true)
                          .build())
                  .build())
          .setHierarchicalNamespace(HierarchicalNamespace.newBuilder().setEnabled(true).build())
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

  private static MockHttpTransport transport = mockTransport();

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
  public void createBucket_withZonalOptions_succeeds() throws Exception {
    mockStorage.addResponse(TEST_BUCKET_WITH_ZONAL_OPTIONS);

    CreateBucketOptions bucketOptions =
        CreateBucketOptions.builder()
            .setLocation(BUCKET_LOCATION)
            .setZonalPlacement(ZONAL_PLACEMENT)
            .setStorageClass("RAPID")
            .setHierarchicalNamespaceEnabled(true)
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
    assertEquals(
        bucketRequest.getBucket().getCustomPlacementConfig().getDataLocations(0), ZONAL_PLACEMENT);
    assertEquals(bucketRequest.getBucket().getStorageClass(), "RAPID");
    assertThat(bucketRequest.getBucket().getHierarchicalNamespace().getEnabled()).isTrue();
    assertThat(bucketRequest.getBucket().getIamConfig().getUniformBucketLevelAccess().getEnabled())
        .isTrue();
    // Assert TTL was not set for zonal bucket
    assertThat(bucketRequest.getBucket().getLifecycle().getRuleList()).isEmpty();
  }

  @Test
  public void createBucket_withZonalUnsetStorageClass_defaultsToRapid() throws Exception {
    mockStorage.addResponse(TEST_BUCKET_WITH_ZONAL_OPTIONS);

    CreateBucketOptions bucketOptions =
        CreateBucketOptions.builder()
            .setLocation(BUCKET_LOCATION)
            .setZonalPlacement(ZONAL_PLACEMENT)
            .setHierarchicalNamespaceEnabled(true)
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
    assertEquals(
        bucketRequest.getBucket().getCustomPlacementConfig().getDataLocations(0), ZONAL_PLACEMENT);
    assertEquals(bucketRequest.getBucket().getStorageClass(), "RAPID");
    assertThat(bucketRequest.getBucket().getHierarchicalNamespace().getEnabled()).isTrue();
    assertThat(bucketRequest.getBucket().getIamConfig().getUniformBucketLevelAccess().getEnabled())
        .isTrue();
    // Assert TTL was not set for zonal bucket
    assertThat(bucketRequest.getBucket().getLifecycle().getRuleList()).isEmpty();
  }

  @Test
  public void createBucket_zonalButHnsDisabled_throwsException() throws Exception {
    CreateBucketOptions bucketOptions =
        CreateBucketOptions.builder()
            .setZonalPlacement(ZONAL_PLACEMENT)
            .setHierarchicalNamespaceEnabled(false)
            .build();

    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());

      UnsupportedOperationException thrown =
          assertThrows(
              UnsupportedOperationException.class,
              () -> gcs.createBucket(TEST_BUCKET_NAME, bucketOptions));
      assertThat(thrown).hasMessageThat().isEqualTo("Zonal buckets must have HNS Enabled.");
    }
  }

  @Test
  public void createBucket_withZonalOptionsAndTtl_throwsException() throws Exception {
    CreateBucketOptions bucketOptions =
        CreateBucketOptions.builder()
            .setLocation(BUCKET_LOCATION)
            .setZonalPlacement(ZONAL_PLACEMENT)
            .setHierarchicalNamespaceEnabled(true)
            .setTtl(Duration.ofDays(TTL_DAYS))
            .build();

    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());

      UnsupportedOperationException thrown =
          assertThrows(
              UnsupportedOperationException.class,
              () -> gcs.createBucket(TEST_BUCKET_NAME, bucketOptions));
      assertThat(thrown).hasMessageThat().isEqualTo("Zonal buckets do not support TTL");
    }
  }

  @Test
  public void createBucket_zonalWithInvalidStorageClass_throwsException() throws Exception {
    CreateBucketOptions bucketOptions =
        CreateBucketOptions.builder()
            .setZonalPlacement(ZONAL_PLACEMENT)
            .setHierarchicalNamespaceEnabled(true)
            .setStorageClass("STANDARD") // Invalid for zonal
            .build();

    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());

      UnsupportedOperationException thrown =
          assertThrows(
              UnsupportedOperationException.class,
              () -> gcs.createBucket(TEST_BUCKET_NAME, bucketOptions));
      assertThat(thrown).hasMessageThat().isEqualTo("Zonal bucket storage class must be RAPID");
    }
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
  public void createEmptyObject_ignoresException() throws Exception {
    Bucket mockBucket = mockBucket("STANDARD");
    mockStorage.addResponse(mockBucket);
    mockStorage.addException(new StatusRuntimeException(Status.RESOURCE_EXHAUSTED));
    // Mock for getItemInfo in case the object already exists.
    mockStorage.addResponse(
        TEST_OBJECT.toBuilder().setSize(0L).putAllMetadata(ImmutableMap.of()).build());

    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());

      gcs.createEmptyObject(TEST_RESOURCE_ID);
    }
    assertThat(mockStorage.getRequests().size()).isEqualTo(3);
  }

  @Test
  public void createEmptyObject_mismatchedMetadata_doesNotIgnoreException() throws Exception {
    Bucket mockBucket = mockBucket("STANDARD");
    mockStorage.addResponse(mockBucket);
    mockStorage.addException(new StatusRuntimeException(Status.RESOURCE_EXHAUSTED));
    // Mock for getItemInfo in case the object already exists.
    mockStorage.addResponse(
        TEST_OBJECT.toBuilder().setSize(0L).putAllMetadata(ImmutableMap.of()).build());

    CreateObjectOptions createOptions =
        CreateObjectOptions.DEFAULT_OVERWRITE.toBuilder()
            .setMetadata(ImmutableMap.of("foo", new byte[0]))
            .build();

    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());

      IOException thrown =
          assertThrows(
              IOException.class, () -> gcs.createEmptyObject(TEST_RESOURCE_ID, createOptions));
      assertThat(thrown).hasMessageThat().contains(ErrorType.RESOURCE_EXHAUSTED.toString());
    }
  }

  @Test
  public void createEmptyObject_mismatchedMetadata_ignoreException() throws Exception {
    Bucket mockBucket = mockBucket("STANDARD");
    mockStorage.addResponse(mockBucket);
    mockStorage.addException(new StatusRuntimeException(Status.RESOURCE_EXHAUSTED));
    // Mock for getItemInfo in case the object already exists.
    mockStorage.addResponse(
        TEST_OBJECT.toBuilder().setSize(0L).putAllMetadata(ImmutableMap.of("foo", "bar")).build());

    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());

      // The fetch will "mismatch" with more metadata than our default EMPTY_METADATA used in the
      // default CreateObjectOptions, but we won't care because the metadata-check requirement
      // will be false, so the call will complete successfully.
      gcs.createEmptyObject(TEST_RESOURCE_ID);
    }
    assertThat(mockStorage.getRequests().size()).isEqualTo(3);
  }

  @Test
  public void createEmptyObject_doesNotIgnoreExceptions() throws Exception {
    Bucket mockBucket = mockBucket("STANDARD");
    mockStorage.addResponse(mockBucket);
    // Non-Ignorable exception.
    mockStorage.addException(new StatusRuntimeException(Status.UNAVAILABLE));
    // Mock for getItemInfo in case the object already exists.
    mockStorage.addResponse(
        TEST_OBJECT.toBuilder().setSize(0L).putAllMetadata(ImmutableMap.of()).build());

    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());

      IOException thrown =
          assertThrows(IOException.class, () -> gcs.createEmptyObject(TEST_RESOURCE_ID));
      assertThat(thrown).hasMessageThat().contains(ErrorType.UNAVAILABLE.toString());
    }
  }

  @Test
  public void createEmptyObject_alreadyExists_throwsException() throws Exception {
    Bucket mockBucket = mockBucket("STANDARD");
    mockStorage.addResponse(mockBucket);
    mockStorage.addException(new StatusRuntimeException(Status.ALREADY_EXISTS));

    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());

      IOException thrown =
          assertThrows(
              FileAlreadyExistsException.class, () -> gcs.createEmptyObject(TEST_RESOURCE_ID));
      assertThat(thrown)
          .hasMessageThat()
          .isEqualTo(
              String.format(
                  "Object 'gs://%s/%s' already exists.", TEST_BUCKET_NAME, TEST_OBJECT_NAME));
    }
  }

  @Test
  public void createEmptyObjects_ignoresExceptions() throws Exception {
    Bucket mockBucket = mockBucket("STANDARD");
    mockStorage.addResponse(mockBucket);
    mockStorage.addResponse(mockBucket);
    mockStorage.addException(new StatusRuntimeException(Status.RESOURCE_EXHAUSTED));
    mockStorage.addException(new StatusRuntimeException(Status.RESOURCE_EXHAUSTED));
    // Mock for getItemInfo in case the object already exists.
    mockStorage.addResponse(
        TEST_OBJECT.toBuilder().setSize(0L).putAllMetadata(ImmutableMap.of()).build());
    mockStorage.addResponse(
        TEST_OBJECT.toBuilder().setSize(0L).putAllMetadata(ImmutableMap.of()).build());

    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());

      gcs.createEmptyObjects(ImmutableList.of(TEST_RESOURCE_ID, TEST_RESOURCE_ID));
    }
    assertThat(mockStorage.getRequests().size()).isEqualTo(6);
  }

  @Test
  public void createEmptyObjects_doesNotIgnoreExceptions() throws Exception {
    Bucket mockBucket = mockBucket("STANDARD");
    mockStorage.addResponse(mockBucket);
    mockStorage.addResponse(mockBucket);
    mockStorage.addException(new StatusRuntimeException(Status.UNAVAILABLE));
    mockStorage.addException(new StatusRuntimeException(Status.UNAVAILABLE));

    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());

      IOException thrown =
          assertThrows(
              IOException.class,
              () -> gcs.createEmptyObjects(ImmutableList.of(TEST_RESOURCE_ID, TEST_RESOURCE_ID)));
      assertThat(thrown).hasMessageThat().isEqualTo("Multiple IOExceptions.");
    }
  }

  @Test
  public void createEmptyObject_rapidStorage_ignoresException() throws Exception {
    Bucket mockBucket = mockBucket("RAPID");
    mockStorage.addResponse(mockBucket);

    mockStorage.addException(new StatusRuntimeException(Status.RESOURCE_EXHAUSTED));
    mockStorage.addException(new StatusRuntimeException(Status.RESOURCE_EXHAUSTED));

    mockStorage.addResponse(
        TEST_OBJECT.toBuilder()
            .setSize(0L)
            .putAllMetadata(ImmutableMap.of())
            .setStorageClass("RAPID")
            .build());

    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      StorageOptions originalOptions = fakeServer.getGrpcStorageOptions();

      // Customize RetrySettings
      RetrySettings customRetrySettings =
          originalOptions.getRetrySettings().toBuilder()
              .setMaxAttempts(2) // Set max attempts to 2 (1 initial + 1 retry)
              .build();

      StorageOptions customStorageOptions =
          originalOptions.toBuilder().setRetrySettings(customRetrySettings).build();

      Storage customStorage = customStorageOptions.getService();

      GoogleCloudStorage gcs = mockedGcsClientImpl(transport, customStorage);

      gcs.createEmptyObject(TEST_RESOURCE_ID);
    }
    assertThat(mockStorage.getRequests().size()).isEqualTo(4);
  }

  @Test
  public void createEmptyObject_rapidStorage_doesNotIgnoreExceptions() throws Exception {
    Bucket mockBucket = mockBucket("RAPID");
    mockStorage.addResponse(mockBucket);
    // Non-Ignorable exception.
    mockStorage.addException(new StatusRuntimeException(Status.UNAVAILABLE));

    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      StorageOptions originalOptions = fakeServer.getGrpcStorageOptions();

      RetrySettings customRetrySettings =
          originalOptions.getRetrySettings().toBuilder()
              .setMaxAttempts(1) // No retries
              .build();

      StorageOptions customStorageOptions =
          originalOptions.toBuilder().setRetrySettings(customRetrySettings).build();

      Storage customStorage = customStorageOptions.getService();

      GoogleCloudStorage gcs = mockedGcsClientImpl(transport, customStorage);

      IOException thrown =
          assertThrows(IOException.class, () -> gcs.createEmptyObject(TEST_RESOURCE_ID));
      assertThat(thrown).hasMessageThat().contains(Status.UNAVAILABLE.getCode().toString());
    }
  }

  @Test
  public void createEmptyObject_rapidStorage_alreadyExists_throwsException() throws Exception {
    Bucket mockBucket = mockBucket("RAPID");
    mockStorage.addResponse(mockBucket);
    mockStorage.addException(new StatusRuntimeException(Status.FAILED_PRECONDITION));

    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());

      IOException thrown =
          assertThrows(
              FileAlreadyExistsException.class, () -> gcs.createEmptyObject(TEST_RESOURCE_ID));
      assertThat(thrown)
          .hasMessageThat()
          .isEqualTo(
              String.format(
                  "Object gs://%s/%s already exists.", TEST_BUCKET_NAME, TEST_OBJECT_NAME));
    }
  }

  @Test
  public void compose_succeeds() throws Exception {
    mockStorage.addResponse(TEST_OBJECT);
    mockStorage.addResponse(TEST_OBJECT);
    List<String> sources = ImmutableList.of("object1", "object2");

    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());

      gcs.compose(TEST_BUCKET_NAME, sources, TEST_OBJECT_NAME, "application/octet-stream");
    }

    assertEquals(mockStorage.getRequests().size(), 2);

    ComposeObjectRequest actualRequest = (ComposeObjectRequest) mockStorage.getRequests().get(1);
    assertThat(actualRequest.getDestination().getName()).contains(TEST_OBJECT_NAME);
    assertThat(actualRequest.getSourceObjects(0).getName()).isEqualTo("object1");
    assertThat(actualRequest.getSourceObjects(1).getName()).isEqualTo("object2");
  }

  @Test
  public void compose_throwsException() throws Exception {
    mockStorage.addResponse(TEST_OBJECT);
    mockStorage.addException(new StatusRuntimeException(Status.INVALID_ARGUMENT));
    List<String> sources = ImmutableList.of("object1", "object2");

    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());

      assertThrows(
          IOException.class,
          () ->
              gcs.compose(TEST_BUCKET_NAME, sources, TEST_OBJECT_NAME, "application/octet-stream"));
    }
  }

  @Test
  public void copyObjects_sameBucket_succeeds() throws Exception {
    RewriteResponse expectedResponse =
        RewriteResponse.newBuilder()
            .setTotalBytesRewritten(-1109205579)
            .setObjectSize(-1277221631)
            .setDone(true)
            .setRewriteToken("rewriteToken80654285")
            .setResource(Object.newBuilder().build())
            .build();
    mockStorage.addResponse(expectedResponse);

    String destinationObject = TEST_OBJECT_NAME + "-copy";

    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());

      gcs.copy(
          TEST_BUCKET_NAME,
          ImmutableList.of(TEST_OBJECT_NAME),
          TEST_BUCKET_NAME,
          ImmutableList.of(destinationObject));
    }
    assertEquals(mockStorage.getRequests().size(), 1);
    RewriteObjectRequest actualResponse = (RewriteObjectRequest) mockStorage.getRequests().get(0);

    assertThat(actualResponse.getDestinationName()).isEqualTo(destinationObject);
    assertThat(actualResponse.getDestinationBucket()).contains(TEST_BUCKET_NAME);
    assertThat(actualResponse.getSourceBucket()).contains(TEST_BUCKET_NAME);
    assertThat(actualResponse.getSourceObject()).isEqualTo(TEST_OBJECT_NAME);
  }

  @Test
  public void copyObjects_differentBuckets_succeeds() throws Exception {
    String destinationBucket = TEST_BUCKET_NAME + "-copy";
    String destinationObject = TEST_OBJECT_NAME + "-copy";

    RewriteResponse expectedRewriteResponse =
        RewriteResponse.newBuilder()
            .setTotalBytesRewritten(-1109205579)
            .setObjectSize(-1277221631)
            .setDone(true)
            .setRewriteToken("rewriteToken80654285")
            .setResource(Object.newBuilder().build())
            .build();
    mockStorage.addResponse(TEST_BUCKET);
    mockStorage.addResponse(
        Bucket.newBuilder()
            .setName(destinationBucket)
            .setCreateTime(CREATE_TIME)
            .setUpdateTime(UPDATE_TIME)
            .build());
    mockStorage.addResponse(expectedRewriteResponse);

    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());

      gcs.copy(
          TEST_BUCKET_NAME,
          ImmutableList.of(TEST_OBJECT_NAME),
          destinationBucket,
          ImmutableList.of(destinationObject));
    }
    assertEquals(mockStorage.getRequests().size(), 3);
    RewriteObjectRequest actualResponse = (RewriteObjectRequest) mockStorage.getRequests().get(2);

    assertThat(actualResponse.getDestinationName()).isEqualTo(destinationObject);
    assertThat(actualResponse.getDestinationBucket()).contains(destinationBucket);
    assertThat(actualResponse.getSourceBucket()).contains(TEST_BUCKET_NAME);
    assertThat(actualResponse.getSourceObject()).isEqualTo(TEST_OBJECT_NAME);
  }

  @Test
  public void move_succeedsSingleObject() throws Exception {

    String destinationObjectName = TEST_OBJECT_NAME + "-move";

    // Add Mock response for the move operation.
    mockStorage.addResponse(
        TEST_OBJECT.toBuilder()
            .setName(destinationObjectName)
            .setGeneration(GENERATION + 1)
            .setMetageneration(1L)
            .build());

    Map<StorageResourceId, StorageResourceId> moveMap =
        ImmutableMap.of(
            TEST_RESOURCE_ID, new StorageResourceId(TEST_BUCKET_NAME, destinationObjectName));

    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());

      gcs.move(moveMap);
    }

    assertEquals(mockStorage.getRequests().size(), 1);
    MoveObjectRequest actualRequest = (MoveObjectRequest) mockStorage.getRequests().get(0);

    assertThat(actualRequest.getDestinationObject()).isEqualTo(destinationObjectName);
    assertThat(actualRequest.getBucket()).contains(TEST_BUCKET_NAME);
    assertThat(actualRequest.getSourceObject()).isEqualTo(TEST_OBJECT_NAME);
    // Assert no generation condition was set by default.
    assertThat(actualRequest.getIfGenerationMatch()).isEqualTo(0);
  }

  @Test
  public void move_succeedsMultipleObjects() throws Exception {

    String destinationObjectName1 = TEST_OBJECT_NAME + "-move1";
    String destinationObjectName2 = TEST_OBJECT_NAME + "-move2";

    // Add Mock responses for two move operations.
    mockStorage.addResponse(
        TEST_OBJECT.toBuilder()
            .setName(destinationObjectName1)
            .setGeneration(GENERATION + 1)
            .setMetageneration(1L)
            .build());
    mockStorage.addResponse(
        TEST_OBJECT.toBuilder()
            .setName(destinationObjectName2)
            .setGeneration(GENERATION + 1)
            .setMetageneration(1L)
            .build());

    Map<StorageResourceId, StorageResourceId> moveMap =
        ImmutableMap.of(
            TEST_RESOURCE_ID,
            new StorageResourceId(TEST_BUCKET_NAME, destinationObjectName1),
            new StorageResourceId(TEST_BUCKET_NAME, TEST_OBJECT_NAME + "-2"),
            new StorageResourceId(TEST_BUCKET_NAME, destinationObjectName2));

    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());

      gcs.move(moveMap);
    }

    assertEquals(mockStorage.getRequests().size(), 2);

    // sort the requests since the tasks are being performed asynchronously.
    List<AbstractMessage> requestList = mockStorage.getRequests();
    requestList.sort(Comparator.comparing(java.lang.Object::toString));

    // Verify first request parameters.
    MoveObjectRequest actualRequest1 = (MoveObjectRequest) requestList.get(0);
    assertThat(actualRequest1.getSourceObject()).isEqualTo(TEST_OBJECT_NAME);
    assertThat(actualRequest1.getDestinationObject()).isEqualTo(destinationObjectName1);

    // Verify second request parameters.
    MoveObjectRequest actualRequest2 = (MoveObjectRequest) requestList.get(1);
    assertThat(actualRequest2.getSourceObject()).isEqualTo(TEST_OBJECT_NAME_2);
    assertThat(actualRequest2.getDestinationObject()).isEqualTo(destinationObjectName2);
  }

  @Test
  public void move_throwsFileNotFoundExceptionOnSourceMissing() throws Exception {
    // Mock the move operation to fail with FILE_NOT_FOUND exception.
    mockStorage.addException(new StatusRuntimeException(Status.NOT_FOUND));

    String destinationObjectName = TEST_OBJECT_NAME + "-move";

    Map<StorageResourceId, StorageResourceId> moveMap =
        ImmutableMap.of(
            TEST_RESOURCE_ID, new StorageResourceId(TEST_BUCKET_NAME, destinationObjectName));

    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());

      FileNotFoundException thrown =
          assertThrows(FileNotFoundException.class, () -> gcs.move(moveMap));
      assertThat(thrown)
          .hasMessageThat()
          .contains(
              String.format(
                  "Item not found: '%s'. Note, it is possible that the live version"
                      + " is still available but the requested generation is deleted.",
                  TEST_RESOURCE_ID.toString()));
    }
    // Verify no requests should is sent.
    assertEquals(mockStorage.getRequests().size(), 0);
  }

  @Test
  public void move_throwsGeneralIOExceptionOnError() throws Exception {
    // Mock the move operation to fail with a different error.
    mockStorage.addException(new StatusRuntimeException(Status.INVALID_ARGUMENT));

    String destinationObjectName = TEST_OBJECT_NAME + "-move";

    Map<StorageResourceId, StorageResourceId> moveMap =
        ImmutableMap.of(
            TEST_RESOURCE_ID, new StorageResourceId(TEST_BUCKET_NAME, destinationObjectName));

    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());

      IOException thrown = assertThrows(IOException.class, () -> gcs.move(moveMap));
      assertThat(thrown)
          .hasMessageThat()
          .contains(
              String.format(
                  "Error moving '%s'",
                  StringPaths.fromComponents(TEST_BUCKET_NAME, TEST_OBJECT_NAME)));
    }
    assertEquals(mockStorage.getRequests().size(), 0);
  }

  @Test
  public void move_emptyMap_doesNothing() throws Exception {
    Map<StorageResourceId, StorageResourceId> moveMap = ImmutableMap.of();

    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());

      gcs.move(moveMap);
    }
    // Verify no requests should is sent.
    assertEquals(mockStorage.getRequests().size(), 0);
  }

  @Test
  public void move_differentBuckets_throwsIllegalArgumentException() throws Exception {
    // Define source and destination with different buckets.
    String destinationObjectName = TEST_OBJECT_NAME + "-move";

    Map<StorageResourceId, StorageResourceId> moveMap =
        ImmutableMap.of(
            TEST_RESOURCE_ID, new StorageResourceId(OTHER_BUCKET_NAME, destinationObjectName));

    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());

      UnsupportedOperationException thrown =
          assertThrows(UnsupportedOperationException.class, () -> gcs.move(moveMap));
      assertThat(thrown)
          .hasMessageThat()
          .contains("This operation is not supported across two different buckets");
    }
    assertEquals(mockStorage.getRequests().size(), 0);
  }

  @Test
  public void move_withDestinationGeneration_succeeds() throws Exception {
    String destinationObjectName = TEST_OBJECT_NAME + "-move";

    // Add Mock response for the move operation.
    mockStorage.addResponse(
        TEST_OBJECT.toBuilder()
            .setName(destinationObjectName)
            .setGeneration(9876L)
            .setMetageneration(1L)
            .build());

    StorageResourceId dstResourceIDWithGenID =
        new StorageResourceId(TEST_BUCKET_NAME, destinationObjectName, 9876L);

    Map<StorageResourceId, StorageResourceId> moveMap =
        ImmutableMap.of(TEST_RESOURCE_ID, dstResourceIDWithGenID);

    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());

      gcs.move(moveMap);
    }

    assertEquals(mockStorage.getRequests().size(), 1);
    MoveObjectRequest actualRequest = (MoveObjectRequest) mockStorage.getRequests().get(0);

    assertThat(actualRequest.getDestinationObject()).isEqualTo(destinationObjectName);
    assertThat(actualRequest.getSourceObject()).contains(TEST_OBJECT_NAME);
    assertThat(actualRequest.getBucket()).contains(TEST_BUCKET_NAME);
    // Assert that generation condition was set in the destination object.
    assertThat(actualRequest.getIfGenerationMatch())
        .isEqualTo(dstResourceIDWithGenID.getGenerationId());
  }

  @Test
  public void move_withSourceGeneration_succeeds() throws Exception {
    String destinationObjectName = TEST_OBJECT_NAME + "-move";
    long sourceGeneration = 999L;

    // Add Mock response for the move operation.
    mockStorage.addResponse(
        TEST_OBJECT.toBuilder()
            .setName(destinationObjectName)
            .setGeneration(GENERATION + 1)
            .setMetageneration(1L)
            .build());

    StorageResourceId srcResourceIdWithGen =
        new StorageResourceId(TEST_BUCKET_NAME, TEST_OBJECT_NAME, sourceGeneration);
    StorageResourceId dstResourceId =
        new StorageResourceId(TEST_BUCKET_NAME, destinationObjectName);

    Map<StorageResourceId, StorageResourceId> moveMap =
        ImmutableMap.of(srcResourceIdWithGen, dstResourceId);

    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());

      gcs.move(moveMap);
    }

    assertEquals(mockStorage.getRequests().size(), 1);
    MoveObjectRequest actualRequest = (MoveObjectRequest) mockStorage.getRequests().get(0);

    assertThat(actualRequest.getDestinationObject()).isEqualTo(destinationObjectName);
    assertThat(actualRequest.getBucket()).contains(TEST_BUCKET_NAME);
    assertThat(actualRequest.getSourceObject()).isEqualTo(TEST_OBJECT_NAME);
    // Assert that source generation condition was set.
    assertThat(actualRequest.getIfSourceGenerationMatch()).isEqualTo(sourceGeneration);
    // Assert that destination generation condition was NOT set.
    assertThat(actualRequest.getIfGenerationMatch()).isEqualTo(0);
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
    }

    assertEquals(mockStorage.getRequests().size(), 1);

    DeleteBucketRequest deleteBucketRequest =
        (DeleteBucketRequest) mockStorage.getRequests().get(0);
    assertThat(deleteBucketRequest.getName()).contains(TEST_BUCKET_NAME);
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
    }

    assertEquals(mockStorage.getRequests().size(), 2);
    DeleteObjectRequest actualRequest = (DeleteObjectRequest) mockStorage.getRequests().get(1);
    assertThat(actualRequest.getBucket()).contains(TEST_BUCKET_NAME);
    assertEquals(actualRequest.getObject(), TEST_OBJECT_NAME);
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
  public void getItemInfo_bucket_succeeds() throws Exception {
    mockStorage.addResponse(TEST_BUCKET);

    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());

      StorageResourceId bucketId = new StorageResourceId(TEST_BUCKET_NAME);

      GoogleCloudStorageItemInfo info = gcs.getItemInfo(bucketId);
      assertThat(info.getBucketName()).isEqualTo(TEST_BUCKET_NAME);
    }
  }

  @Test
  public void getItemInfo_root_succeeds() throws Exception {
    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());

      GoogleCloudStorageItemInfo info = gcs.getItemInfo(StorageResourceId.ROOT);
      assertThat(info).isEqualTo(GoogleCloudStorageItemInfo.ROOT_INFO);
    }
  }

  @Test
  public void getItemInfo_object_succeeds() throws Exception {
    mockStorage.addResponse(TEST_OBJECT);

    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());

      StorageResourceId objectId = new StorageResourceId(TEST_BUCKET_NAME, TEST_OBJECT_NAME);

      GoogleCloudStorageItemInfo info = gcs.getItemInfo(objectId);
      assertThat(info.getBucketName()).isEqualTo(TEST_BUCKET_NAME);
      assertThat(info.getObjectName()).isEqualTo(TEST_OBJECT_NAME);
    }
  }

  @Test
  public void getItemInfo_throwsException() throws Exception {
    mockStorage.addException(new StatusRuntimeException(Status.INVALID_ARGUMENT));

    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());

      StorageResourceId objectId = new StorageResourceId(TEST_BUCKET_NAME, TEST_OBJECT_NAME);

      assertThrows(IOException.class, () -> gcs.getItemInfo(objectId));
    }
  }

  @Test
  public void getItemInfos_returnsNotFound() throws Exception {
    mockStorage.addException(new StatusRuntimeException(Status.NOT_FOUND));
    mockStorage.addException(new StatusRuntimeException(Status.NOT_FOUND));

    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());

      StorageResourceId objectId = new StorageResourceId(TEST_BUCKET_NAME, TEST_OBJECT_NAME);

      List<GoogleCloudStorageItemInfo> itemInfos =
          gcs.getItemInfos(
              ImmutableList.of(
                  objectId, StorageResourceId.ROOT, new StorageResourceId(TEST_BUCKET_NAME)));

      assertThat(itemInfos.size()).isEqualTo(3);

      GoogleCloudStorageItemInfo expectedObject =
          GoogleCloudStorageItemInfo.createNotFound(objectId);
      GoogleCloudStorageItemInfo expectedRoot = GoogleCloudStorageItemInfo.ROOT_INFO;
      GoogleCloudStorageItemInfo expectedBucket =
          GoogleCloudStorageItemInfo.createNotFound(new StorageResourceId(TEST_BUCKET_NAME));

      assertThat(itemInfos).containsExactly(expectedObject, expectedRoot, expectedBucket).inOrder();
    }
  }

  @Test
  public void getItemInfos_succeeds() throws Exception {
    mockStorage.addResponse(TEST_OBJECT);

    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());

      List<GoogleCloudStorageItemInfo> itemInfos =
          gcs.getItemInfos(
              ImmutableList.of(
                  new StorageResourceId(TEST_BUCKET_NAME, TEST_OBJECT_NAME),
                  StorageResourceId.ROOT));

      assertEquals(itemInfos.size(), 2);
      GoogleCloudStorageItemInfo objectInfo = itemInfos.get(0);
      GoogleCloudStorageItemInfo rootInfo = itemInfos.get(1);

      assertThat(objectInfo.getBucketName()).isEqualTo(TEST_BUCKET_NAME);
      assertThat(objectInfo.getObjectName()).isEqualTo(TEST_OBJECT_NAME);
      assertThat(rootInfo.isRoot()).isEqualTo(true);
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

  @Test
  @SuppressWarnings("unchecked")
  public void getUpdatedHeaders_includesFeatureUsageAndUserAgent() throws Exception {
    GoogleCloudStorageFileSystemOptions gcsfsOptions =
        GoogleCloudStorageFileSystemOptions.DEFAULT.toBuilder()
            .setPerformanceCacheEnabled(true)
            .build();
    FeatureHeaderGenerator featureHeaderGenerator = new FeatureHeaderGenerator(gcsfsOptions);

    // Prepare storage options
    GoogleCloudStorageOptions storageOptions =
        GoogleCloudStorageOptions.DEFAULT.toBuilder()
            .setAppName("test-app")
            .setHttpRequestHeaders(ImmutableMap.of())
            .build();
    // Use reflection to invoke the private static method under test.
    Method getUpdatedHeadersMethod =
        GoogleCloudStorageClientImpl.class.getDeclaredMethod(
            "getUpdatedHeaders", GoogleCloudStorageOptions.class, FeatureHeaderGenerator.class);
    getUpdatedHeadersMethod.setAccessible(true);
    ImmutableMap<String, String> headers =
        (ImmutableMap<String, String>)
            getUpdatedHeadersMethod.invoke(null, storageOptions, featureHeaderGenerator);

    String expectedHeaderValue = featureHeaderGenerator.getValue();
    // Verify all expected headers are present.
    assertThat(headers.get("user-agent")).contains("test-app");
    assertThat(headers).containsEntry(FeatureHeaderGenerator.HEADER_NAME, expectedHeaderValue);
  }

  private Bucket mockBucket(String storageClass) {
    return Bucket.newBuilder()
        .setName("projects/_/buckets/foo-bucket")
        .setStorageClass(storageClass)
        .build();
  }

  @Test
  public void open_fastFailDisabled_doesNotInvokeGet() throws Exception {
    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());

      GoogleCloudStorageReadOptions readOptions =
          GoogleCloudStorageReadOptions.builder().setFastFailOnNotFoundEnabled(false).build();

      gcs.open(TEST_RESOURCE_ID, readOptions);
    }
    assertEquals(0, mockStorage.getRequests().size());
  }

  @Test
  public void open_fastFailEnabled_invokesGet() throws Exception {
    mockStorage.addResponse(TEST_OBJECT);
    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());

      GoogleCloudStorageReadOptions readOptions =
          GoogleCloudStorageReadOptions.builder().setFastFailOnNotFoundEnabled(true).build();

      gcs.open(TEST_RESOURCE_ID, readOptions);
    }
    assertEquals(1, mockStorage.getRequests().size());
  }
}
