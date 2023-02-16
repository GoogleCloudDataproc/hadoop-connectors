/*
 * Copyright 2021 Google LLC
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

import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.getMediaRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.getRequestString;
import static com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.getStandardOptionBuilder;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.truth.Truth.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.auth.Credentials;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemImpl;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageItemInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer;
import com.google.cloud.hadoop.gcsio.UriPaths;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper;
import com.google.cloud.hadoop.util.RetryHttpInitializer;
import com.google.common.io.CharStreams;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/** Integration tests for GoogleHadoopFileSystem class. */
@RunWith(Parameterized.class)
public class GoogleHadoopFileSystemNewIntegrationTest {

  private static GoogleCloudStorageOptions gcsOptions;
  private static RetryHttpInitializer httpRequestsInitializer;
  private static HadoopFileSystemIntegrationHelper ghfsIHelper;
  private static String testBucketName;

  @Rule public TestName name = new TestName();
  private TrackingHttpRequestInitializer gcsRequestsTracker;

  @Parameterized.Parameter public boolean testStorageClientImpl;

  @Parameters
  public static Iterable<Boolean> getTesStorageClientImplParameter() {
    return List.of(false, true);
  }

  @Before
  public void before() throws Throwable {
    Credentials credentials =
        checkNotNull(
            GoogleCloudStorageTestHelper.getCredentials(), "credentials should not be null");

    gcsOptions = getStandardOptionBuilder().build();
    httpRequestsInitializer =
        new RetryHttpInitializer(credentials, gcsOptions.toRetryHttpInitializerOptions());

    GoogleHadoopFileSystem ghfs = new GoogleHadoopFileSystem();
    ghfsIHelper = new HadoopFileSystemIntegrationHelper(ghfs);

    testBucketName = ghfsIHelper.getUniqueBucketName("new-it");
    URI testBucketUri = new URI("gs://" + testBucketName);

    ghfs.initialize(
        testBucketUri, GoogleHadoopFileSystemTestBase.loadConfig(testStorageClientImpl));

    ghfs.getGcsFs().mkdir(testBucketUri);
    gcsRequestsTracker = new TrackingHttpRequestInitializer(httpRequestsInitializer);
  }

  @After
  public void after() {
    ghfsIHelper.afterAllTests();
  }

  @Test
  public void openFile() throws Exception {
    String expectedContent = "test-file-content: " + name.getMethodName();

    GoogleHadoopFileSystem ghfs = createGhfs();

    StorageResourceId fileId = new StorageResourceId(testBucketName, name.getMethodName());
    Path filePath = new Path(fileId.toString());

    try (FSDataOutputStream os = ghfsIHelper.ghfs.create(filePath)) {
      os.writeBytes(expectedContent);
    }

    GoogleCloudStorageItemInfo itemInfo =
        ((GoogleHadoopFileSystem) ghfsIHelper.ghfs).getGcsFs().getGcs().getItemInfo(fileId);

    CompletableFuture<FSDataInputStream> isFuture = ghfs.openFile(filePath).build();

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            getRequestString(
                testBucketName,
                name.getMethodName(),
                /* fields= */ "contentEncoding,generation,size"));

    String fileContent;
    try (FSDataInputStream is = isFuture.get()) {
      fileContent = CharStreams.toString(new InputStreamReader(is, UTF_8));
    }

    assertThat(fileContent).isEqualTo(expectedContent);
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            getRequestString(
                testBucketName,
                name.getMethodName(),
                /* fields= */ "contentEncoding,generation,size"),
            getMediaRequestString(
                testBucketName, name.getMethodName(), itemInfo.getContentGeneration()))
        .inOrder();
  }

  @Test
  public void openFile_withFileStatus() throws Exception {
    String expectedContent = "test-file-content: " + name.getMethodName();

    GoogleHadoopFileSystem ghfs = createGhfs();

    StorageResourceId fileId = new StorageResourceId(testBucketName, name.getMethodName());
    Path filePath = new Path(fileId.toString());

    try (FSDataOutputStream os = ghfsIHelper.ghfs.create(filePath)) {
      os.writeBytes(expectedContent);
    }

    FileStatus fileStatus = ghfsIHelper.ghfs.getFileStatus(filePath);
    GoogleCloudStorageItemInfo itemInfo =
        ((GoogleHadoopFileSystem) ghfsIHelper.ghfs).getGcsFs().getGcs().getItemInfo(fileId);

    CompletableFuture<FSDataInputStream> isFuture =
        ghfs.openFile(filePath).withFileStatus(fileStatus).build();

    assertThat(gcsRequestsTracker.getAllRequestStrings()).isEmpty();

    String fileContent;
    try (FSDataInputStream is = isFuture.get()) {
      fileContent = CharStreams.toString(new InputStreamReader(is, UTF_8));
    }

    assertThat(fileContent).isEqualTo(expectedContent);
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            getMediaRequestString(
                testBucketName, name.getMethodName(), itemInfo.getContentGeneration()));
  }

  private GoogleHadoopFileSystem createGhfs() throws IOException {
    GoogleHadoopFileSystem ghfs =
        new GoogleHadoopFileSystem(
            new GoogleCloudStorageFileSystemImpl(
                GoogleCloudStorageImpl.builder()
                    .setOptions(gcsOptions)
                    .setCredentials(httpRequestsInitializer.getCredentials())
                    .setHttpRequestInitializer(gcsRequestsTracker)
                    .build(),
                GoogleCloudStorageFileSystemOptions.builder()
                    .setCloudStorageOptions(gcsOptions)
                    .build()));
    ghfs.initialize(
        UriPaths.fromResourceId(
            new StorageResourceId(testBucketName), /* allowEmptyObjectName= */ true),
        GoogleHadoopFileSystemTestBase.loadConfig(testStorageClientImpl));
    return ghfs;
  }
}
