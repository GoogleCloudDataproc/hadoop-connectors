/*
 * Copyright 2019 Google LLC. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.gcsio;

import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.deleteMatchMetaGenerationRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.updateMetadataRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.uploadRequestString;
import static com.google.cloud.hadoop.gcsio.cooplock.CoopLockOperationType.DELETE;
import static com.google.cloud.hadoop.gcsio.cooplock.CoopLockOperationType.RENAME;
import static com.google.cloud.hadoop.gcsio.cooplock.CoopLockRecordsDao.LOCK_DIRECTORY;
import static com.google.cloud.hadoop.gcsio.cooplock.CoopLockRecordsDao.LOCK_PATH;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.http.HttpExecuteInterceptor;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.cloud.hadoop.gcsio.cooplock.CoopLockRecordsDao;
import com.google.cloud.hadoop.gcsio.cooplock.CooperativeLockingOptions;
import com.google.cloud.hadoop.gcsio.cooplock.DeleteOperation;
import com.google.cloud.hadoop.gcsio.cooplock.RenameOperation;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper;
import com.google.cloud.hadoop.gcsio.testing.TestConfiguration;
import com.google.cloud.hadoop.util.RetryHttpInitializer;
import com.google.gson.Gson;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration tests for Cooperative Locking feature. */
@RunWith(JUnit4.class)
public class CoopLockIntegrationTest {

  private static final Gson GSON = CoopLockRecordsDao.createGson();

  private static final String OPERATION_FILENAME_PATTERN_FORMAT =
      "[0-9]{8}T[0-9]{6}\\.[0-9]{3}Z_%s_[a-z0-9\\-]+";

  private static final Duration COOP_LOCK_TIMEOUT = Duration.ofSeconds(30);

  private static GoogleCloudStorageOptions gcsOptions;
  private static RetryHttpInitializer httpRequestInitializer;
  private static GoogleCloudStorageFileSystemIntegrationHelper gcsfsIHelper;

  @BeforeClass
  public static void before() throws Throwable {
    String projectId =
        checkNotNull(TestConfiguration.getInstance().getProjectId(), "projectId can not be null");
    String appName = GoogleCloudStorageIntegrationHelper.APP_NAME;
    Credential credential =
        checkNotNull(GoogleCloudStorageTestHelper.getCredential(), "credential must not be null");

    gcsOptions =
        GoogleCloudStorageOptions.builder().setAppName(appName).setProjectId(projectId).build();
    httpRequestInitializer =
        new RetryHttpInitializer(credential, gcsOptions.toRetryHttpInitializerOptions());

    GoogleCloudStorageFileSystem gcsfs =
        new GoogleCloudStorageFileSystem(
            credential,
            GoogleCloudStorageFileSystemOptions.builder()
                .setBucketDeleteEnabled(true)
                .setCloudStorageOptions(gcsOptions)
                .build());

    gcsfsIHelper = new GoogleCloudStorageFileSystemIntegrationHelper(gcsfs);
    gcsfsIHelper.beforeAllTests();
  }

  @AfterClass
  public static void afterClass() throws Throwable {
    gcsfsIHelper.afterAllTests();
    GoogleCloudStorageFileSystem gcsfs = gcsfsIHelper.gcsfs;
    assertThat(gcsfs.exists(new URI("gs://" + gcsfsIHelper.sharedBucketName1))).isFalse();
    assertThat(gcsfs.exists(new URI("gs://" + gcsfsIHelper.sharedBucketName2))).isFalse();
  }

  @Test
  public void moveDirectory() throws Exception {
    GoogleCloudStorageFileSystemOptions gcsFsOptions = newGcsFsOptions();
    TrackingHttpRequestInitializer trackingRequestInitializer =
        new TrackingHttpRequestInitializer(httpRequestInitializer);
    GoogleCloudStorageFileSystem gcsFs = newGcsFs(gcsFsOptions, trackingRequestInitializer);

    String bucketName = gcsfsIHelper.createUniqueBucket("coop-rename");
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String dirName = "rename_" + UUID.randomUUID();
    String fileName = "file";
    URI srcDirUri = bucketUri.resolve(dirName + "_src/");
    URI dstDirUri = bucketUri.resolve(dirName + "_dst/");

    // create file to rename
    gcsfsIHelper.writeTextFile(bucketName, srcDirUri.resolve(fileName).getPath(), "file_content");

    gcsFs.rename(srcDirUri, dstDirUri);

    assertThat(trackingRequestInitializer.getAllRequestStrings())
        .containsAtLeast(
            uploadRequestString(bucketName, LOCK_PATH, /* generationId= */ 1),
            updateMetadataRequestString(bucketName, LOCK_PATH, /* metaGenerationId= */ 1),
            deleteMatchMetaGenerationRequestString(
                bucketName, LOCK_PATH, /* metaGenerationId= */ 2));

    assertThat(gcsFs.exists(srcDirUri)).isFalse();
    assertThat(gcsFs.exists(srcDirUri.resolve(fileName))).isFalse();
    assertThat(gcsFs.exists(dstDirUri)).isTrue();
    assertThat(gcsFs.exists(dstDirUri.resolve(fileName))).isTrue();

    // Validate lock files
    List<URI> lockFiles =
        gcsFs.listFileInfo(bucketUri.resolve(LOCK_DIRECTORY)).stream()
            .map(FileInfo::getPath)
            .collect(toList());

    assertThat(lockFiles).hasSize(2);
    String fileNamePattern = String.format(OPERATION_FILENAME_PATTERN_FORMAT, RENAME);
    URI lockFileUri = matchFile(lockFiles, fileNamePattern + "\\.lock").get();
    URI logFileUri = matchFile(lockFiles, fileNamePattern + "\\.log").get();

    String lockContent = gcsfsIHelper.readTextFile(bucketName, lockFileUri.getPath());
    assertThat(GSON.fromJson(lockContent, RenameOperation.class).setLockExpiration(null))
        .isEqualTo(
            new RenameOperation()
                .setLockExpiration(null)
                .setSrcResource(srcDirUri.toString())
                .setDstResource(dstDirUri.toString())
                .setCopySucceeded(true));
    assertThat(gcsfsIHelper.readTextFile(bucketName, logFileUri.getPath()))
        .isEqualTo(
            String.format(
                "{\"src\":\"%s\",\"dst\":\"%s\"}\n",
                srcDirUri.resolve(fileName), dstDirUri.resolve(fileName)));
  }

  @Test
  public void deleteDirectory() throws Exception {
    GoogleCloudStorageFileSystemOptions gcsFsOptions = newGcsFsOptions();
    TrackingHttpRequestInitializer trackingRequestInitializer =
        new TrackingHttpRequestInitializer(httpRequestInitializer);
    GoogleCloudStorageFileSystem gcsFs = newGcsFs(gcsFsOptions, trackingRequestInitializer);

    String bucketName = gcsfsIHelper.createUniqueBucket("coop-delete");
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String fileName = "file";
    URI dirUri = bucketUri.resolve("delete_" + UUID.randomUUID() + "/");

    // create file to rename
    gcsfsIHelper.writeTextFile(bucketName, dirUri.resolve(fileName).getPath(), "file_content");

    gcsFs.delete(dirUri, /* recursive= */ true);

    assertThat(trackingRequestInitializer.getAllRequestStrings())
        .containsAtLeast(
            uploadRequestString(bucketName, LOCK_PATH, /* generationId= */ 1),
            updateMetadataRequestString(bucketName, LOCK_PATH, /* metaGenerationId= */ 1),
            deleteMatchMetaGenerationRequestString(
                bucketName, LOCK_PATH, /* metaGenerationId= */ 2));

    assertThat(gcsFs.exists(dirUri)).isFalse();
    assertThat(gcsFs.exists(dirUri.resolve(fileName))).isFalse();

    // Validate lock files
    List<URI> lockFiles =
        gcsFs.listFileInfo(bucketUri.resolve(LOCK_DIRECTORY)).stream()
            .map(FileInfo::getPath)
            .collect(toList());

    assertThat(lockFiles).hasSize(2);
    String fileNamePattern = String.format(OPERATION_FILENAME_PATTERN_FORMAT, DELETE);
    URI lockFileUri = matchFile(lockFiles, fileNamePattern + "\\.lock").get();
    URI logFileUri = matchFile(lockFiles, fileNamePattern + "\\.log").get();
    String lockContent = gcsfsIHelper.readTextFile(bucketName, lockFileUri.getPath());
    assertThat(GSON.fromJson(lockContent, DeleteOperation.class).setLockExpiration(null))
        .isEqualTo(new DeleteOperation().setLockExpiration(null).setResource(dirUri.toString()));
    assertThat(gcsfsIHelper.readTextFile(bucketName, logFileUri.getPath()))
        .isEqualTo(dirUri.resolve(fileName) + "\n" + dirUri + "\n");
  }

  @Test
  public void directoryDelete_lockRenewed() throws Exception {
    String bucketName = gcsfsIHelper.createUniqueBucket("coop-delete-lock-renewed");
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String fileName = "file";
    URI dirUri = bucketUri.resolve("delete_" + UUID.randomUUID() + "/");
    URI fileUri = dirUri.resolve(fileName);

    // create file to delete
    gcsfsIHelper.writeTextFile(bucketName, fileUri.getPath(), "file_content");

    CooperativeLockingOptions coopLockOptions =
        CooperativeLockingOptions.builder()
            .setLockExpirationTimeoutMilli(COOP_LOCK_TIMEOUT.toMillis())
            .build();
    GoogleCloudStorageFileSystemOptions gcsFsOptions =
        newGcsFsOptions(
            gcsOptions.toBuilder().setCooperativeLockingOptions(coopLockOptions).build());

    Duration lockRenewalDelay = COOP_LOCK_TIMEOUT.dividedBy(2);
    Duration lockRenewalTimeout = COOP_LOCK_TIMEOUT.dividedBy(4);

    String encodedFilePath = URLEncoder.encode(fileUri.getPath().substring(1), UTF_8.name());
    HttpRequestInitializer sleepingRequestInitializer =
        interceptingRequestInitializer(
            r -> {
              String reqUrl = "/b/" + bucketName + "/o/" + encodedFilePath;
              if ("DELETE".equals(r.getRequestMethod()) && r.getUrl().toString().contains(reqUrl)) {
                sleepUninterruptibly(lockRenewalDelay.plus(lockRenewalTimeout));
              }
            });
    GoogleCloudStorageFileSystem sleepingGcsFs = newGcsFs(gcsFsOptions, sleepingRequestInitializer);

    Instant operationStart = Instant.now();

    sleepingGcsFs.delete(dirUri, /* recursive= */ true);

    GoogleCloudStorageFileSystem gcsFs = newGcsFs(gcsFsOptions, httpRequestInitializer);

    assertThat(gcsFs.exists(dirUri)).isFalse();
    assertThat(gcsFs.exists(fileUri)).isFalse();

    // Validate lock files
    List<URI> lockFiles =
        gcsFs.listFileInfo(bucketUri.resolve(LOCK_DIRECTORY)).stream()
            .map(FileInfo::getPath)
            .collect(toList());

    assertThat(lockFiles).hasSize(2);
    String filenamePattern = String.format(OPERATION_FILENAME_PATTERN_FORMAT, DELETE);
    URI lockFileUri = matchFile(lockFiles, filenamePattern + "\\.lock").get();
    URI logFileUri = matchFile(lockFiles, filenamePattern + "\\.log").get();
    String lockContent = gcsfsIHelper.readTextFile(bucketName, lockFileUri.getPath());
    assertThat(GSON.fromJson(lockContent, DeleteOperation.class).setLockExpiration(null))
        .isEqualTo(new DeleteOperation().setLockExpiration(null).setResource(dirUri.toString()));
    assertThat(gcsfsIHelper.readTextFile(bucketName, logFileUri.getPath()))
        .isEqualTo(fileUri + "\n" + dirUri + "\n");

    Instant expectedRenewedLockExpiration =
        operationStart.plus(COOP_LOCK_TIMEOUT).plus(lockRenewalDelay);

    Instant renewedLockExpiration =
        GSON.fromJson(lockContent, DeleteOperation.class).getLockExpiration();
    assertThat(renewedLockExpiration).isGreaterThan(expectedRenewedLockExpiration);
    assertThat(renewedLockExpiration)
        .isLessThan(expectedRenewedLockExpiration.plus(lockRenewalTimeout));
  }

  private static Optional<URI> matchFile(List<URI> files, String pattern) {
    return files.stream().filter(f -> f.toString().matches("^gs://.*/" + pattern + "$")).findAny();
  }

  private static GoogleCloudStorageFileSystemOptions newGcsFsOptions() {
    return newGcsFsOptions(CoopLockIntegrationTest.gcsOptions);
  }

  private static GoogleCloudStorageFileSystemOptions newGcsFsOptions(
      GoogleCloudStorageOptions gcsOptions) {
    return GoogleCloudStorageFileSystemOptions.builder()
        .setCloudStorageOptions(gcsOptions)
        .setCooperativeLockingEnabled(true)
        .build();
  }

  private static GoogleCloudStorageFileSystem newGcsFs(
      GoogleCloudStorageFileSystemOptions gcsfsOptions, HttpRequestInitializer requestInitializer)
      throws IOException {
    return new GoogleCloudStorageFileSystem(
        o -> new GoogleCloudStorageImpl(o, requestInitializer), gcsfsOptions);
  }

  private static HttpRequestInitializer interceptingRequestInitializer(
      Consumer<HttpRequest> interceptFn) {
    return request -> {
      httpRequestInitializer.initialize(request);
      HttpExecuteInterceptor executeInterceptor = checkNotNull(request.getInterceptor());
      request.setInterceptor(
          interceptedRequest -> {
            executeInterceptor.intercept(interceptedRequest);
            interceptFn.accept(interceptedRequest);
          });
    };
  }
}
