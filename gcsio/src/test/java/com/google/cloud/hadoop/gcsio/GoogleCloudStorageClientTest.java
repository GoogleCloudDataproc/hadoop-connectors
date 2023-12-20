package com.google.cloud.hadoop.gcsio;

import static com.google.cloud.hadoop.gcsio.MockGoogleCloudStorageImplFactory.mockedGcsClientImpl;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.mockTransport;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.google.api.client.testing.http.MockHttpTransport;
import com.google.cloud.hadoop.gcsio.testing.FakeServer;
import com.google.cloud.hadoop.gcsio.testing.MockStorage;
import com.google.cloud.storage.StorageException;
import com.google.storage.v2.Bucket;
import com.google.storage.v2.Bucket.Lifecycle;
import com.google.storage.v2.Bucket.Lifecycle.Rule;
import com.google.storage.v2.Bucket.Lifecycle.Rule.Action;
import com.google.storage.v2.Bucket.Lifecycle.Rule.Condition;
import com.google.storage.v2.CreateBucketRequest;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.time.Duration;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link GoogleCloudStorageClientImpl} */
@RunWith(JUnit4.class)
public class GoogleCloudStorageClientTest {

  private static final String BUCKET_NAME = "foo-bucket";

  private static final MockHttpTransport transport = mockTransport();

  private static final MockStorage mockStorage = new MockStorage();

  @Before
  public void setUp() {
    mockStorage.reset();
  }

  @Test
  public void createBucket_succeeds() throws Exception {
    mockStorage.addResponse(Bucket.newBuilder().setName(BUCKET_NAME).build());

    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());
      gcs.createBucket(BUCKET_NAME);
    }

    assertEquals(mockStorage.getRequests().size(), 1);

    CreateBucketRequest bucketRequest = (CreateBucketRequest) mockStorage.getRequests().get(0);
    assertEquals(bucketRequest.getBucketId(), BUCKET_NAME);
  }

  @Test
  public void createBucket_withOptions_succeeds() throws Exception {
    String location = "some-location";
    String storageClass = "STANDARD";
    int ttlDays = 10;

    mockStorage.addResponse(
        Bucket.newBuilder()
            .setName(BUCKET_NAME)
            .setStorageClass(storageClass)
            .setLocation(location)
            .setLifecycle(
                Lifecycle.newBuilder()
                    .addRule(
                        Rule.newBuilder()
                            .setAction(Action.newBuilder().setType("Delete"))
                            .setCondition(Condition.newBuilder().setAgeDays(ttlDays).build())
                            .build())
                    .build())
            .build());

    CreateBucketOptions bucketOptions =
        CreateBucketOptions.builder()
            .setLocation(location)
            .setStorageClass(storageClass)
            .setTtl(Duration.ofDays(ttlDays))
            .build();

    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());
      gcs.createBucket(BUCKET_NAME, bucketOptions);
    }

    assertEquals(mockStorage.getRequests().size(), 1);

    CreateBucketRequest bucketRequest = (CreateBucketRequest) mockStorage.getRequests().get(0);
    // Assert correct fields were set in request.
    assertEquals(bucketRequest.getBucketId(), BUCKET_NAME);
    assertEquals(bucketRequest.getBucket().getLocation(), location);
    assertEquals(bucketRequest.getBucket().getStorageClass(), storageClass);
    assertEquals(
        bucketRequest.getBucket().getLifecycle().getRule(0).getAction().getType(), "Delete");
    assertEquals(
        bucketRequest.getBucket().getLifecycle().getRule(0).getCondition().getAgeDays(), ttlDays);
  }

  @Test
  public void createBucket_throwsFileAlreadyExistsException() throws Exception {
    mockStorage.addException(new StatusRuntimeException(Status.ALREADY_EXISTS));
    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());
      assertThrows(FileAlreadyExistsException.class, () -> gcs.createBucket(BUCKET_NAME));
    }
  }

  @Test
  public void createBucket_throwsIOException() throws Exception {
    mockStorage.addException(
        new StorageException(0, "Some exception", new StatusException(Status.INVALID_ARGUMENT)));

    try (FakeServer fakeServer = FakeServer.of(mockStorage)) {
      GoogleCloudStorage gcs =
          mockedGcsClientImpl(transport, fakeServer.getGrpcStorageOptions().getService());
      assertThrows(IOException.class, () -> gcs.createBucket(BUCKET_NAME));
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
}
