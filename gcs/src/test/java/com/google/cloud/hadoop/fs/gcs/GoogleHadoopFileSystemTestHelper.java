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

package com.google.cloud.hadoop.fs.gcs;

import static com.google.cloud.hadoop.gcsio.testing.InMemoryGoogleCloudStorage.getInMemoryGoogleCloudStorageOptions;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.Assert.fail;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemImpl;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.testing.InMemoryGoogleCloudStorage;
import com.google.cloud.hadoop.util.CheckedFunction;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * GoogleHadoopFileSystemTestHelper contains helper methods and factory methods for setting up the
 * test instances used for various unit and integration tests.
 */
public class GoogleHadoopFileSystemTestHelper {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();
  public static final String IN_MEMORY_TEST_BUCKET = "gs://fake-in-memory-test-bucket";

  /**
   * Creates an instance of a bucket-rooted GoogleHadoopFileSystem using an in-memory underlying
   * store.
   */
  public static GoogleHadoopFileSystem createInMemoryGoogleHadoopFileSystem() throws IOException {
    return createInMemoryGoogleHadoopFileSystem(InMemoryGoogleCloudStorage::new);
  }

  public static GoogleHadoopFileSystem createInMemoryGoogleHadoopFileSystem(
      CheckedFunction<GoogleCloudStorageOptions, GoogleCloudStorage, IOException> gcsFn)
      throws IOException {
    GoogleCloudStorageFileSystem memoryGcsFs =
        new GoogleCloudStorageFileSystemImpl(
            gcsFn,
            GoogleCloudStorageFileSystemOptions.builder()
                .setCloudStorageOptions(getInMemoryGoogleCloudStorageOptions())
                .build());
    GoogleHadoopFileSystem ghfs = new GoogleHadoopFileSystem(memoryGcsFs);
    initializeInMemoryFileSystem(ghfs, IN_MEMORY_TEST_BUCKET);
    return ghfs;
  }

  /**
   * Helper for plumbing through an initUri and creating the proper Configuration object. Calls
   * FileSystem.initialize on {@code ghfs}.
   */
  private static void initializeInMemoryFileSystem(FileSystem ghfs, String initUriString)
      throws IOException {
    URI initUri;
    try {
      initUri = new URI(initUriString);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
    Configuration config = new Configuration();
    ghfs.initialize(initUri, config);
    // Create test bucket
    ghfs.mkdirs(new Path(initUriString));
  }

  public static void fillBytes(byte[] bytes) {
    new Random().nextBytes(bytes);
  }

  public static byte[] writeObject(
      FSDataOutputStream outputStream, int partitionSize, int partitionsCount) throws IOException {
    checkArgument(partitionsCount > 0, "partitionsCount should be greater than 0");

    byte[] partition = new byte[partitionSize];
    fillBytes(partition);

    long startTime = System.currentTimeMillis();
    try (OutputStream ignore = outputStream) {
      for (int i = 0; i < partitionsCount; i++) {
        ignore.write(partition);
      }
    }
    long endTime = System.currentTimeMillis();
    logger.atInfo().log(
        "Took %sms to write %sB", (endTime - startTime), (long) partitionsCount * partitionSize);
    return partition;
  }

  public static void assertObjectContent(
      FileSystem ghfs, Path gcsPath, ByteBuffer expectedContent, long position) throws IOException {

    int expectedBytesLength = expectedContent.limit();
    byte[] bytesRead = new byte[expectedBytesLength];
    try (FSDataInputStream stream = ghfs.open(gcsPath)) {
      int read = stream.read(position, bytesRead, 0, expectedBytesLength);
      ByteBuffer readContent = ByteBuffer.wrap(bytesRead);
      assertByteBuffers(readContent, expectedContent.duplicate());
      assertWithMessage("Bytes read mismatch").that(read).isAtMost(expectedBytesLength);
    }
  }

  public static void assertByteBuffers(ByteBuffer expected, ByteBuffer actual) {
    if ((expected == null) ^ (actual == null)) {
      // Don't use a fancy toString(), just display which is null,
      // which isn't, since the arrays may be very large.
      fail(String.format("Expected was '%s', actual was '%s'", expected, actual));
    } else if (expected == null && actual == null) {
      return;
    }

    if (expected.remaining() != actual.remaining()) {
      fail(
          String.format(
              "Length mismatch: expected: %d, actual: %d",
              expected.remaining(), actual.remaining()));
    }

    for (int i = 0; i < expected.remaining(); ++i) {
      byte expectedByte = expected.get();
      byte actualByte = actual.get();
      if (expectedByte != actualByte) {
        fail(
            String.format(
                "Mismatch at index %d. expected: 0x%02x, actual: 0x%02x",
                i, expectedByte, actualByte));
      }
    }
  }
}
