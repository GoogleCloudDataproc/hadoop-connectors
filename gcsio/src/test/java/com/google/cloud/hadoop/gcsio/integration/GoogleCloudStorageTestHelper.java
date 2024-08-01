/*
 * Copyright 2014 Google Inc.
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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.truth.Truth.assertWithMessage;
import static java.lang.Math.min;
import static org.junit.Assert.fail;

import com.google.api.client.util.DateTime;
import com.google.api.services.storage.StorageScopes;
import com.google.api.services.storage.model.StorageObject;
import com.google.auth.Credentials;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageClientImpl;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageItemInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions;
import com.google.cloud.hadoop.gcsio.ListObjectOptions;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.cloud.hadoop.gcsio.TrackingGrpcRequestInterceptor;
import com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer;
import com.google.cloud.hadoop.gcsio.testing.TestConfiguration;
import com.google.cloud.hadoop.util.RetryHttpInitializer;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.flogger.GoogleLogger;
import com.google.common.io.BaseEncoding;
import com.google.protobuf.ByteString;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;

/** Helper methods for GCS integration tests. */
public class GoogleCloudStorageTestHelper {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  // Application name for OAuth.
  public static final String APP_NAME = "GHFS/test";

  private static final int BUFFER_SIZE_MAX_BYTES = 32 * 1024 * 1024;

  public static GoogleCloudStorage createGoogleCloudStorage() {
    try {
      return GoogleCloudStorageImpl.builder()
          .setOptions(getStandardOptionBuilder().build())
          .setCredentials(getCredentials())
          .build();
    } catch (IOException e) {
      throw new RuntimeException("Failed to create GoogleCloudStorage instance", e);
    }
  }

  public static GoogleCloudStorage createGcsClientImpl() {
    try {
      return GoogleCloudStorageClientImpl.builder()
          .setOptions(getStandardOptionBuilder().build())
          .setCredentials(getCredentials())
          .build();
    } catch (IOException e) {
      throw new RuntimeException("Failed to create GoogleCloudStorage instance", e);
    }
  }

  public static Credentials getCredentials() throws IOException {
    String serviceAccountJsonKeyFile =
        TestConfiguration.getInstance().getServiceAccountJsonKeyFile();
    if (serviceAccountJsonKeyFile == null) {
      return ComputeEngineCredentials.create().createScoped(StorageScopes.CLOUD_PLATFORM);
    }
    try (FileInputStream fis = new FileInputStream(serviceAccountJsonKeyFile)) {
      return ServiceAccountCredentials.fromStream(fis).createScoped(StorageScopes.CLOUD_PLATFORM);
    }
  }

  public static GoogleCloudStorageOptions.Builder getStandardOptionBuilder() {
    return GoogleCloudStorageOptions.builder()
        .setAppName(GoogleCloudStorageTestHelper.APP_NAME)
        .setDirectPathPreferred(TestConfiguration.getInstance().isDirectPathPreferred())
        .setGrpcWriteEnabled(true)
        .setProjectId(checkNotNull(TestConfiguration.getInstance().getProjectId()));
  }

  /** More efficient version of checking byte arrays than using Assert.assertArrayEquals. */
  @SuppressWarnings("ArrayToString")
  public static void assertByteArrayEquals(byte[] expected, byte[] actual) {
    if ((expected == null) ^ (actual == null)) {
      // Don't use a fancy toString(), just display which is null,
      // which isn't, since the arrays may be very large.
      fail(String.format("Expected was '%s', actual was '%s'", expected, actual));
    } else if (expected == null && actual == null) {
      return;
    }

    if (expected.length != actual.length) {
      fail(
          String.format(
              "Length mismatch: expected: %d, actual: %d", expected.length, actual.length));
    }

    for (int i = 0; i < expected.length; ++i) {
      if (expected[i] != actual[i]) {
        fail(
            String.format(
                "Mismatch at index %d. expected: 0x%02x, actual: 0x%02x",
                i, expected[i], actual[i]));
      }
    }
  }

  public static void assertObjectContent(
      GoogleCloudStorage gcs, StorageResourceId resourceId, byte[] expectedBytes)
      throws IOException {
    assertObjectContent(gcs, resourceId, expectedBytes, /* expectedBytesCount= */ 1);
  }

  public static void assertObjectContent(
      GoogleCloudStorage gcs,
      StorageResourceId resourceId,
      GoogleCloudStorageReadOptions readOptions,
      byte[] expectedBytes)
      throws IOException {
    assertObjectContent(gcs, resourceId, readOptions, expectedBytes, /* expectedBytesCount= */ 1);
  }

  public static void assertObjectContent(
      GoogleCloudStorage gcs,
      StorageResourceId id,
      GoogleCloudStorageReadOptions readOptions,
      byte[] expectedBytes,
      int expectedBytesCount,
      int offset)
      throws IOException {
    checkArgument(expectedBytesCount > 0, "expectedBytesCount should be greater than 0");

    int expectedBytesLength = expectedBytes.length;
    long expectedBytesTotalLength = (long) expectedBytesLength * expectedBytesCount;
    ByteBuffer buffer = ByteBuffer.allocate(min(BUFFER_SIZE_MAX_BYTES, expectedBytesLength));
    long totalRead = 0;
    try (SeekableByteChannel channel = gcs.open(id, readOptions)) {
      if (offset > 0) {
        channel.position(offset);
      }
      int read = channel.read(buffer);
      while (read > 0) {
        totalRead += read;
        assertWithMessage("Bytes read mismatch").that(totalRead).isAtMost(expectedBytesTotalLength);

        buffer.flip();
        byte[] bytesRead = Arrays.copyOf(buffer.array(), buffer.limit());
        byte[] expectedBytesRead = getExpectedBytesRead(expectedBytes, totalRead, read);
        assertByteArrayEquals(expectedBytesRead, bytesRead);

        buffer.clear();
        read = channel.read(buffer);
      }
    }

    assertWithMessage("Bytes read mismatch").that(totalRead).isEqualTo(expectedBytesTotalLength);
  }

  public static void assertObjectContent(
      GoogleCloudStorage gcs,
      StorageResourceId id,
      GoogleCloudStorageReadOptions readOptions,
      byte[] expectedBytes,
      int expectedBytesCount)
      throws IOException {
    assertObjectContent(gcs, id, readOptions, expectedBytes, expectedBytesCount, /* offset= */ 0);
  }

  public static void assertObjectContent(
      GoogleCloudStorage gcs, StorageResourceId id, byte[] expectedBytes, int expectedBytesCount)
      throws IOException {
    assertObjectContent(
        gcs, id, GoogleCloudStorageReadOptions.DEFAULT, expectedBytes, expectedBytesCount);
  }

  private static byte[] getExpectedBytesRead(byte[] expectedBytes, long totalRead, int read) {
    int expectedBytesLength = expectedBytes.length;
    int expectedBytesStart = (int) ((totalRead - read) % expectedBytesLength);
    int expectedBytesEnd = (int) (totalRead % expectedBytesLength);
    if (expectedBytesStart < expectedBytesEnd) {
      return Arrays.copyOfRange(expectedBytes, expectedBytesStart, expectedBytesEnd);
    }
    // expectedBytesRead is not continuous in expectedBytes partition
    // and need to be copied with 2 method calls
    byte[] expectedBytesRead = new byte[read];
    int firstPartSize = expectedBytesLength - expectedBytesStart;
    System.arraycopy(expectedBytes, expectedBytesStart, expectedBytesRead, 0, firstPartSize);
    System.arraycopy(expectedBytes, 0, expectedBytesRead, firstPartSize, expectedBytesEnd);
    return expectedBytesRead;
  }

  public static void fillBytes(byte[] bytes) {
    new Random().nextBytes(bytes);
  }

  public static byte[] writeObject(
      GoogleCloudStorage gcs, StorageResourceId resourceId, int objectSize) throws IOException {
    return writeObject(gcs, resourceId, objectSize, /* partitionsCount= */ 1);
  }

  public static byte[] writeObject(
      GoogleCloudStorage gcs, StorageResourceId resourceId, int partitionSize, int partitionsCount)
      throws IOException {
    return writeObject(gcs.create(resourceId), partitionSize, partitionsCount);
  }

  public static byte[] writeObject(
      WritableByteChannel channel, int partitionSize, int partitionsCount) throws IOException {
    checkArgument(partitionsCount > 0, "partitionsCount should be greater than 0");

    byte[] partition = new byte[partitionSize];
    fillBytes(partition);

    long startTime = System.currentTimeMillis();
    try (WritableByteChannel ignore = channel) {
      for (int i = 0; i < partitionsCount; i++) {
        channel.write(ByteBuffer.wrap(partition));
      }
    }
    long endTime = System.currentTimeMillis();
    logger.atFine().log(
        "Took %sms to write %sB", (endTime - startTime), (long) partitionsCount * partitionSize);
    return partition;
  }

  public static Map<String, byte[]> getDecodedMetadata(Map<String, String> metadata) {
    return metadata.entrySet().stream()
        .collect(
            Collectors.toMap(
                entity -> entity.getKey(), entity -> decodeMetadataValues(entity.getValue())));
  }

  public static byte[] decodeMetadataValues(String value) {
    return BaseEncoding.base64().decode(value);
  }

  public static ByteString createTestData(int numBytes) {
    byte[] result = new byte[numBytes];
    for (int i = 0; i < numBytes; ++i) {
      // Sequential data makes it easier to compare expected vs. actual in
      // case of error. Since chunk sizes are multiples of 256, the modulo
      // ensures chunks have different data.
      result[i] = (byte) (i % 257);
    }
    return ByteString.copyFrom(result);
  }

  public static StorageObject newStorageObject(String bucketName, String objectName) {
    Random r = new Random();
    return new StorageObject()
        .setBucket(bucketName)
        .setName(objectName)
        .setSize(BigInteger.valueOf(r.nextInt(Integer.MAX_VALUE)))
        .setStorageClass("standard")
        .setGeneration((long) r.nextInt(Integer.MAX_VALUE))
        .setMetageneration((long) r.nextInt(Integer.MAX_VALUE))
        .setTimeCreated(new DateTime(new Date()))
        .setUpdated(new DateTime(new Date()));
  }

  /** Helper for dealing with buckets in GCS integration tests. */
  public static class TestBucketHelper {
    private static final int MAX_CLEANUP_BUCKETS = 250;

    private static final String DELIMITER = "_";

    // If bucket created before this time, it considered leaked
    private static final long LEAKED_BUCKETS_CUTOFF_TIME =
        Instant.now().minus(Duration.ofHours(6)).toEpochMilli();

    private final String bucketPrefix;
    private final String uniqueBucketPrefix;

    /**
     * Create a Helper to handle bucket with specified prefix. <br>
     * A Unique string of 13 chars will be generated as below.<br>
     * [Max 8 chars of username]_[min 4 chars of random UUID]. Unique prefix will be created by
     * appending this unique string to provided bucketPrefix.
     *
     * @param bucketPrefix Prefix for bucket name. Cumulative Max length of bucketPrefix and suffix
     *     provided in getUniqueBucketName(suffix) will be 48 chars.
     */
    public TestBucketHelper(String bucketPrefix) {
      this.bucketPrefix = bucketPrefix + DELIMITER;
      this.uniqueBucketPrefix = makeUniqueBucketNamePrefix(bucketPrefix);
      checkState(
          this.uniqueBucketPrefix.startsWith(this.bucketPrefix),
          "uniqueBucketPrefix should start with bucketPrefix");
    }

    private static String makeUniqueBucketNamePrefix(String prefix) {
      String username = System.getProperty("user.name", "unknown").replaceAll("[-.]", "");
      // Total 13 characters of unique string will be created. Maximum only first 8 characters of
      // username is considered. Remaining characters will be taken from UUID. Leaving room of 48
      // characters for bucketPrefix and suffix (excluding delimiters)
      int usernamePrefixLen = min(username.length(), 8);
      username = username.substring(0, usernamePrefixLen);
      String uuidSuffix =
          UUID.randomUUID().toString().replaceAll("-", "").substring(0, 12 - usernamePrefixLen);
      return prefix + DELIMITER + username + DELIMITER + uuidSuffix;
    }

    /**
     * @param suffix Suffix to be added to the bucket name. Max cumulative length of bucketPrefix
     *     and suffix is 48 chars.
     * @return An unique bucket name with specified prefix and suffix.
     */
    public String getUniqueBucketName(String suffix) {
      checkArgument(
          bucketPrefix.length() + suffix.length() <= 48,
          "bucketPrefix and suffix can have cumulative length upto 48 chars to limit bucket name to 63 chars");
      return uniqueBucketPrefix + DELIMITER + suffix;
    }

    public String getUniqueBucketPrefix() {
      return uniqueBucketPrefix;
    }

    public void cleanup(GoogleCloudStorage storage) throws IOException {
      Stopwatch storageStopwatch = Stopwatch.createStarted();
      logger.atInfo().log(
          "Cleaning up GCS buckets that start with %s prefix or leaked", uniqueBucketPrefix);

      List<String> bucketsToDelete = new ArrayList<>();
      for (GoogleCloudStorageItemInfo bucketInfo : storage.listBucketInfo()) {
        String bucketName = bucketInfo.getBucketName();
        if (bucketName.startsWith(bucketPrefix)
            && (bucketName.startsWith(uniqueBucketPrefix)
                || bucketInfo.getCreationTime() < LEAKED_BUCKETS_CUTOFF_TIME)) {
          bucketsToDelete.add(bucketName);
        }
      }
      // randomize buckets order in case concurrent cleanups are running
      Collections.shuffle(bucketsToDelete);
      if (bucketsToDelete.size() > MAX_CLEANUP_BUCKETS) {
        logger.atInfo().log(
            "GCS has %s buckets to cleanup. It's too many, will cleanup only %s buckets: %s",
            bucketsToDelete.size(), MAX_CLEANUP_BUCKETS, bucketsToDelete);
        bucketsToDelete = bucketsToDelete.subList(0, MAX_CLEANUP_BUCKETS);
      } else if (bucketsToDelete.size() > 0) {
        logger.atInfo().log(
            "GCS has %s buckets to cleanup: %s", bucketsToDelete.size(), bucketsToDelete);
      }

      List<GoogleCloudStorageItemInfo> objectsToDelete =
          bucketsToDelete.parallelStream()
              .flatMap(
                  bucket -> {
                    try {
                      return storage
                          .listObjectInfo(
                              bucket,
                              /* objectNamePrefix= */ null,
                              ListObjectOptions.DEFAULT_FLAT_LIST)
                          .stream();
                    } catch (IOException e) {
                      throw new RuntimeException(e);
                    }
                  })
              .collect(toImmutableList());
      logger.atInfo().log(
          "GCS has %s objects to cleanup: %s", objectsToDelete.size(), objectsToDelete);

      try {
        storage.deleteObjects(
            Lists.transform(objectsToDelete, GoogleCloudStorageItemInfo::getResourceId));
        storage.deleteBuckets(bucketsToDelete);
      } catch (IOException ioe) {
        logger.atWarning().withCause(ioe).log(
            "Caught exception during GCS (%s) buckets cleanup", storage);
      }

      logger.atInfo().log("GCS cleaned up in %s seconds", storageStopwatch.elapsed().getSeconds());
    }
  }

  @FunctionalInterface
  public interface CheckedFunction2<T, T2, R, E extends Throwable> {

    R apply(T t, T2 t2) throws E;
  }

  public static class TrackingStorageWrapper<T> {

    public final TrackingGrpcRequestInterceptor grpcRequestInterceptor;
    public final TrackingHttpRequestInitializer requestsTracker;
    public final T delegate;

    public TrackingStorageWrapper(
        GoogleCloudStorageOptions options,
        CheckedFunction2<TrackingHttpRequestInitializer, ImmutableList, T, IOException>
            delegateStorageFn,
        Credentials credentials)
        throws IOException {
      this.requestsTracker =
          new TrackingHttpRequestInitializer(
              new RetryHttpInitializer(credentials, options.toRetryHttpInitializerOptions()));
      this.grpcRequestInterceptor = new TrackingGrpcRequestInterceptor();
      this.delegate =
          delegateStorageFn.apply(this.requestsTracker, ImmutableList.of(grpcRequestInterceptor));
    }

    public ImmutableList getAllRequestStrings() {
      return ImmutableList.builder()
          .addAll(requestsTracker.getAllRequestStrings())
          .addAll(grpcRequestInterceptor.getAllRequestStrings())
          .build();
    }
  }
}
