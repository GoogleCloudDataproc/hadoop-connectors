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

package com.google.cloud.hadoop.fs.gcs;

import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_CONFIG_PREFIX;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_COOPERATIVE_LOCKING_EXPIRATION_TIMEOUT_MS;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_PROJECT_ID;
import static com.google.cloud.hadoop.gcsio.cooplock.CoopLockOperationType.DELETE;
import static com.google.cloud.hadoop.gcsio.cooplock.CoopLockOperationType.RENAME;
import static com.google.cloud.hadoop.gcsio.cooplock.CoopLockRecordsDao.LOCK_DIRECTORY;
import static com.google.cloud.hadoop.gcsio.cooplock.CoopLockRecordsDao.LOCK_PATH;
import static com.google.cloud.hadoop.util.HadoopCredentialConfiguration.ENABLE_SERVICE_ACCOUNTS_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialConfiguration.SERVICE_ACCOUNT_EMAIL_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialConfiguration.SERVICE_ACCOUNT_KEYFILE_SUFFIX;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth8.assertThat;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertThrows;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.http.HttpExecuteInterceptor;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.cloud.hadoop.gcsio.FileInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemIntegrationHelper;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageIntegrationHelper;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.cooplock.CoopLockRecords;
import com.google.cloud.hadoop.gcsio.cooplock.CoopLockRecordsDao;
import com.google.cloud.hadoop.gcsio.cooplock.CooperativeLockingOptions;
import com.google.cloud.hadoop.gcsio.cooplock.DeleteOperation;
import com.google.cloud.hadoop.gcsio.cooplock.RenameOperation;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper;
import com.google.cloud.hadoop.gcsio.testing.TestConfiguration;
import com.google.cloud.hadoop.util.RetryHttpInitializer;
import com.google.common.base.Ascii;
import com.google.common.collect.Iterables;
import com.google.gson.Gson;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Predicate;
import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration tests for Cooperative Locking FSCK tool. */
@RunWith(JUnit4.class)
public class CoopLockRepairIntegrationTest {

  private static final Gson GSON = CoopLockRecordsDao.createGson();

  private static final Duration COOP_LOCK_TIMEOUT = Duration.ofSeconds(30);

  private static final String OPERATION_FILENAME_PATTERN_FORMAT =
      "[0-9]{8}T[0-9]{6}\\.[0-9]{3}Z_%s_[a-z0-9\\-]+";

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
  public void emptyArgs() {
    String[] args = {};
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> CoopLockFsck.main(args));
    assertThat(e).hasMessageThat().isEqualTo("No arguments are specified");
  }

  @Test
  public void helpCommand() throws Exception {
    CoopLockFsck.main(new String[] {"--help"});
  }

  @Test
  public void validRepairCommand_withoutBucketParameter() {
    String[] args = {"--check"};
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> CoopLockFsck.main(args));
    assertThat(e).hasMessageThat().contains("2 arguments should be specified");
  }

  @Test
  public void validRepairCommand_withoutBucketAndOperationIdParameters() {
    String[] args = {"--rollBack"};
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> CoopLockFsck.main(args));
    assertThat(e).hasMessageThat().contains("3 arguments should be specified");
  }

  @Test
  public void validRepairCommand_withoutOperationIdParameter() {
    String[] args = {"--rollForward", "gs://bucket"};
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> CoopLockFsck.main(args));
    assertThat(e).hasMessageThat().contains("3 arguments should be specified");
  }

  @Test
  public void validRepairCommand_withInvalidBucketParameter() {
    String[] args = {"--rollBack", "bucket", "operation-id"};
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> CoopLockFsck.main(args));
    assertThat(e).hasMessageThat().contains("bucket parameter should have 'gs://' scheme");
  }

  @Test
  public void invalidRepairCommand_withValidParameter() {
    String[] args = {"--invalidCommand", "gs://bucket", "operation-id"};
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> CoopLockFsck.main(args));
    assertThat(e).hasMessageThat().contains("Unknown --invalidCommand command");
  }

  @Test
  public void noOperations_checkSucceeds() throws Exception {
    String bucketName = gcsfsIHelper.createUniqueBucket("coop-no-op-check-succeeds");
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String fileName = "file";
    URI dirUri = bucketUri.resolve("delete_" + UUID.randomUUID() + "/");

    // create file to delete
    gcsfsIHelper.writeTextFile(bucketName, dirUri.resolve(fileName).getPath(), "file_content");

    GoogleCloudStorageFileSystemOptions gcsFsOptions = newGcsFsOptions();

    GoogleCloudStorageFileSystem gcsFs = newGcsFs(gcsFsOptions, httpRequestInitializer);

    assertThat(gcsFs.exists(dirUri)).isTrue();
    assertThat(gcsFs.exists(dirUri.resolve(fileName))).isTrue();

    CoopLockFsck fsck = new CoopLockFsck();
    fsck.setConf(getTestConfiguration());

    fsck.run(new String[] {"--check", "gs://" + bucketName});

    assertThat(gcsFs.exists(dirUri)).isTrue();
    assertThat(gcsFs.exists(dirUri.resolve(fileName))).isTrue();

    assertThat(gcsFs.exists(bucketUri.resolve(LOCK_DIRECTORY))).isFalse();
  }

  @Test
  public void failedDirectoryDelete_checkSucceeds() throws Exception {
    String bucketName = gcsfsIHelper.createUniqueBucket("coop-delete-check-failed");
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String fileName = "file";
    URI dirUri = bucketUri.resolve("delete_" + UUID.randomUUID() + "/");

    // create file to delete
    gcsfsIHelper.writeTextFile(bucketName, dirUri.resolve(fileName).getPath(), "file_content");

    GoogleCloudStorageFileSystemOptions gcsFsOptions = newGcsFsOptions();

    failDeleteOperation(gcsFsOptions, bucketName, dirUri);

    GoogleCloudStorageFileSystem gcsFs = newGcsFs(gcsFsOptions, httpRequestInitializer);

    assertThat(gcsFs.exists(dirUri)).isTrue();
    assertThat(gcsFs.exists(dirUri.resolve(fileName))).isTrue();

    CoopLockFsck fsck = new CoopLockFsck();
    fsck.setConf(getTestConfiguration());

    fsck.run(new String[] {"--check", "gs://" + bucketName});

    assertThat(gcsFs.exists(dirUri)).isTrue();
    assertThat(gcsFs.exists(dirUri.resolve(fileName))).isTrue();

    // Validate lock files
    List<URI> lockFiles =
        gcsFs.listFileInfo(bucketUri.resolve(LOCK_DIRECTORY)).stream()
            .map(FileInfo::getPath)
            .collect(toList());

    assertThat(lockFiles).hasSize(3);
    assertThat(matchFile(lockFiles, "all\\.lock")).isNotNull();
    String filenamePattern = String.format(OPERATION_FILENAME_PATTERN_FORMAT, DELETE);
    URI lockFileUri = matchFile(lockFiles, filenamePattern + "\\.lock").get();
    URI logFileUri = matchFile(lockFiles, filenamePattern + "\\.log").get();
    String lockContent = gcsfsIHelper.readTextFile(bucketName, lockFileUri.getPath());
    assertThat(GSON.fromJson(lockContent, DeleteOperation.class).setLockExpiration(null))
        .isEqualTo(new DeleteOperation().setLockExpiration(null).setResource(dirUri.toString()));
    assertThat(gcsfsIHelper.readTextFile(bucketName, logFileUri.getPath()))
        .isEqualTo(dirUri.resolve(fileName) + "\n" + dirUri + "\n");
  }

  @Test
  public void failedDirectoryDelete_noLockFile_checkSucceeds() throws Exception {
    String bucketName = gcsfsIHelper.createUniqueBucket("coop-delete-check-no-lock-failed");
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String fileName = "file";
    URI dirUri = bucketUri.resolve("delete_" + UUID.randomUUID() + "/");

    // create file to delete
    gcsfsIHelper.writeTextFile(bucketName, dirUri.resolve(fileName).getPath(), "file_content");

    GoogleCloudStorageFileSystemOptions gcsFsOptions = newGcsFsOptions();

    failDeleteOperation(gcsFsOptions, bucketName, dirUri);

    GoogleCloudStorageFileSystem gcsFs = newGcsFs(gcsFsOptions, httpRequestInitializer);

    // delete operation lock file
    List<URI> lockFile =
        gcsFs.listFileInfo(bucketUri.resolve(LOCK_DIRECTORY)).stream()
            .map(FileInfo::getPath)
            .filter(p -> !p.toString().endsWith("/all.lock") && p.toString().endsWith(".lock"))
            .collect(toImmutableList());
    gcsFs.delete(Iterables.getOnlyElement(lockFile), /* recursive */ false);

    assertThat(gcsFs.exists(dirUri)).isTrue();
    assertThat(gcsFs.exists(dirUri.resolve(fileName))).isTrue();

    CoopLockFsck fsck = new CoopLockFsck();
    fsck.setConf(getTestConfiguration());

    fsck.run(new String[] {"--check", "gs://" + bucketName});

    assertThat(gcsFs.exists(dirUri)).isTrue();
    assertThat(gcsFs.exists(dirUri.resolve(fileName))).isTrue();

    // Validate lock files
    List<URI> lockFiles =
        gcsFs.listFileInfo(bucketUri.resolve(LOCK_DIRECTORY)).stream()
            .map(FileInfo::getPath)
            .collect(toList());

    assertThat(lockFiles).hasSize(2);
    assertThat(matchFile(lockFiles, "all\\.lock")).isNotNull();
    String filenamePattern = String.format(OPERATION_FILENAME_PATTERN_FORMAT, DELETE);
    URI logFileUri = matchFile(lockFiles, filenamePattern + "\\.log").get();
    assertThat(gcsfsIHelper.readTextFile(bucketName, logFileUri.getPath()))
        .isEqualTo(dirUri.resolve(fileName) + "\n" + dirUri + "\n");
  }

  @Test
  public void failedDirectoryDelete_noLogFile_checkSucceeds() throws Exception {
    String bucketName = gcsfsIHelper.createUniqueBucket("coop-delete-check-no-log-failed");
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String fileName = "file";
    URI dirUri = bucketUri.resolve("delete_" + UUID.randomUUID() + "/");

    // create file to delete
    gcsfsIHelper.writeTextFile(bucketName, dirUri.resolve(fileName).getPath(), "file_content");

    GoogleCloudStorageFileSystemOptions gcsFsOptions = newGcsFsOptions();

    failDeleteOperation(gcsFsOptions, bucketName, dirUri);

    GoogleCloudStorageFileSystem gcsFs = newGcsFs(gcsFsOptions, httpRequestInitializer);

    // delete operation log file
    List<URI> logFile =
        gcsFs.listFileInfo(bucketUri.resolve(LOCK_DIRECTORY)).stream()
            .map(FileInfo::getPath)
            .filter(p -> p.toString().endsWith(".log"))
            .collect(toImmutableList());
    gcsFs.delete(Iterables.getOnlyElement(logFile), /* recursive */ false);

    assertThat(gcsFs.exists(dirUri)).isTrue();
    assertThat(gcsFs.exists(dirUri.resolve(fileName))).isTrue();

    CoopLockFsck fsck = new CoopLockFsck();
    fsck.setConf(getTestConfiguration());

    fsck.run(new String[] {"--check", "gs://" + bucketName});

    assertThat(gcsFs.exists(dirUri)).isTrue();
    assertThat(gcsFs.exists(dirUri.resolve(fileName))).isTrue();

    // Validate lock files
    List<URI> lockFiles =
        gcsFs.listFileInfo(bucketUri.resolve(LOCK_DIRECTORY)).stream()
            .map(FileInfo::getPath)
            .collect(toList());

    assertThat(lockFiles).hasSize(2);
    assertThat(matchFile(lockFiles, "all\\.lock")).isNotNull();
    String filenamePattern = String.format(OPERATION_FILENAME_PATTERN_FORMAT, DELETE);
    assertThat(matchFile(lockFiles, filenamePattern + "\\.log")).isEmpty();
  }

  @Test
  public void failedDirectoryDelete_rollForward_withWrongId_fails() throws Exception {
    String bucketName = gcsfsIHelper.createUniqueBucket("coop-delete-fwd-fail-bad-id");
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String fileName = "file";
    URI dirUri = bucketUri.resolve("delete_" + UUID.randomUUID() + "/");

    // create file to delete
    gcsfsIHelper.writeTextFile(bucketName, dirUri.resolve(fileName).getPath(), "file_content");

    GoogleCloudStorageFileSystemOptions gcsFsOptions = newGcsFsOptions();

    failDeleteOperation(gcsFsOptions, bucketName, dirUri);

    GoogleCloudStorageFileSystem gcsFs = newGcsFs(gcsFsOptions, httpRequestInitializer);

    assertThat(gcsFs.exists(dirUri)).isTrue();
    assertThat(gcsFs.exists(dirUri.resolve(fileName))).isTrue();

    CoopLockFsck fsck = new CoopLockFsck();
    fsck.setConf(getTestConfiguration());

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> fsck.run(new String[] {"--rollForward", "gs://" + bucketName, "wrong-op-id"}));

    assertThat(e).hasMessageThat().isEqualTo("wrong-op-id operation not found");

    assertThat(gcsFs.exists(dirUri)).isTrue();
    assertThat(gcsFs.exists(dirUri.resolve(fileName))).isTrue();

    // Validate lock files
    List<URI> lockFiles =
        gcsFs.listFileInfo(bucketUri.resolve(LOCK_DIRECTORY)).stream()
            .map(FileInfo::getPath)
            .collect(toList());

    assertThat(lockFiles).hasSize(3);
    assertThat(matchFile(lockFiles, "all\\.lock")).isNotNull();
    String filenamePattern = String.format(OPERATION_FILENAME_PATTERN_FORMAT, DELETE);
    URI lockFileUri = matchFile(lockFiles, filenamePattern + "\\.lock").get();
    URI logFileUri = matchFile(lockFiles, filenamePattern + "\\.log").get();
    String lockContent = gcsfsIHelper.readTextFile(bucketName, lockFileUri.getPath());
    assertThat(GSON.fromJson(lockContent, DeleteOperation.class).setLockExpiration(null))
        .isEqualTo(new DeleteOperation().setLockExpiration(null).setResource(dirUri.toString()));
    assertThat(gcsfsIHelper.readTextFile(bucketName, logFileUri.getPath()))
        .isEqualTo(dirUri.resolve(fileName) + "\n" + dirUri + "\n");
  }

  @Test
  public void failedDirectoryDelete_rollForward_withCorrectId_succeeds() throws Exception {
    String bucketName = gcsfsIHelper.createUniqueBucket("coop-delete-fwd-fail-id");
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String fileName = "file";
    URI dirUri = bucketUri.resolve("delete_" + UUID.randomUUID() + "/");

    // create file to delete
    gcsfsIHelper.writeTextFile(bucketName, dirUri.resolve(fileName).getPath(), "file_content");

    GoogleCloudStorageFileSystemOptions gcsFsOptions = newGcsFsOptions();

    failDeleteOperation(gcsFsOptions, bucketName, dirUri);

    GoogleCloudStorageFileSystem gcsFs = newGcsFs(gcsFsOptions, httpRequestInitializer);

    assertThat(gcsFs.exists(dirUri)).isTrue();
    assertThat(gcsFs.exists(dirUri.resolve(fileName))).isTrue();

    FileInfo lockInfo = gcsFs.getFileInfo(bucketUri.resolve(LOCK_PATH));
    String locks = new String(lockInfo.getAttributes().get("lock"), UTF_8);
    CoopLockRecords lockRecords = GSON.fromJson(locks, CoopLockRecords.class);
    String operationId = Iterables.getOnlyElement(lockRecords.getLocks()).getOperationId();

    CoopLockFsck fsck = new CoopLockFsck();
    fsck.setConf(getTestConfiguration());

    // Wait until lock will expire
    sleepUninterruptibly(COOP_LOCK_TIMEOUT);

    fsck.run(new String[] {"--rollForward", "gs://" + bucketName, operationId});

    assertThat(gcsFs.exists(dirUri)).isFalse();
    assertThat(gcsFs.exists(dirUri.resolve(fileName))).isFalse();

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
        .isEqualTo(dirUri.resolve(fileName) + "\n" + dirUri + "\n");
  }

  @Test
  public void successfulDirectoryDelete_rollForward() throws Exception {
    String bucketName = gcsfsIHelper.createUniqueBucket("coop-delete-forward-successful");

    URI bucketUri = new URI("gs://" + bucketName + "/");
    String fileName = "file";
    URI dirUri = bucketUri.resolve("delete_" + UUID.randomUUID() + "/");

    // create file to delete
    gcsfsIHelper.writeTextFile(bucketName, dirUri.resolve(fileName).getPath(), "file_content");

    GoogleCloudStorageFileSystemOptions gcsFsOptions = newGcsFsOptions();

    GoogleCloudStorageFileSystem gcsFs = newGcsFs(gcsFsOptions, httpRequestInitializer);

    assertThat(gcsFs.exists(dirUri)).isTrue();
    assertThat(gcsFs.exists(dirUri.resolve(fileName))).isTrue();

    gcsFs.delete(dirUri, /* recursive= */ true);

    assertThat(gcsFs.exists(dirUri)).isFalse();
    assertThat(gcsFs.exists(dirUri.resolve(fileName))).isFalse();

    CoopLockFsck fsck = new CoopLockFsck();
    fsck.setConf(getTestConfiguration());

    fsck.run(new String[] {"--rollForward", "gs://" + bucketName, "all"});

    assertThat(gcsFs.exists(dirUri)).isFalse();
    assertThat(gcsFs.exists(dirUri.resolve(fileName))).isFalse();

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
        .isEqualTo(dirUri.resolve(fileName) + "\n" + dirUri + "\n");
  }

  @Test
  public void failedDirectoryRename_noLogFile_successfullyRepaired() throws Exception {
    String bucketName = gcsfsIHelper.createUniqueBucket("coop-rename-back-failed-copy-nolog");
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String dirName = "rename_" + UUID.randomUUID();
    String fileName = "file";
    URI srcDirUri = bucketUri.resolve(dirName + "_src/");
    URI dstDirUri = bucketUri.resolve(dirName + "_dst/");

    // create file to rename
    gcsfsIHelper.writeTextFile(bucketName, srcDirUri.resolve(fileName).getPath(), "file_content");

    GoogleCloudStorageFileSystemOptions gcsFsOptions = newGcsFsOptions();

    // fail rename operation during log file creation
    failRenameOperation(
        srcDirUri,
        dstDirUri,
        gcsFsOptions,
        r ->
            "POST".equals(r.getRequestMethod())
                && r.getUrl().toString().contains(".log")
                && !r.getUrl().toString().contains("all.log"));

    GoogleCloudStorageFileSystem gcsFs = newGcsFs(gcsFsOptions, httpRequestInitializer);

    assertThat(gcsFs.exists(srcDirUri)).isTrue();
    assertThat(gcsFs.exists(srcDirUri.resolve(fileName))).isTrue();
    assertThat(gcsFs.exists(dstDirUri)).isFalse();
    assertThat(gcsFs.exists(dstDirUri.resolve(fileName))).isFalse();

    CoopLockFsck fsck = new CoopLockFsck();
    fsck.setConf(getTestConfiguration());

    // Wait until lock will expire
    sleepUninterruptibly(COOP_LOCK_TIMEOUT);

    fsck.run(new String[] {"--rollBack", "gs://" + bucketName, "all"});

    assertThat(gcsFs.exists(dstDirUri)).isFalse();
    assertThat(gcsFs.exists(dstDirUri.resolve(fileName))).isFalse();
    assertThat(gcsFs.exists(srcDirUri)).isTrue();
    assertThat(gcsFs.exists(srcDirUri.resolve(fileName))).isTrue();

    // Validate lock files
    List<URI> lockFiles =
        gcsFs.listFileInfo(bucketUri.resolve(LOCK_DIRECTORY)).stream()
            .map(FileInfo::getPath)
            .collect(toList());

    assertThat(lockFiles).hasSize(1);
    String filenameFormat = String.format(OPERATION_FILENAME_PATTERN_FORMAT, RENAME);
    URI lockFileUri = matchFile(lockFiles, filenameFormat + "\\.lock").get();

    String lockContent = gcsfsIHelper.readTextFile(bucketName, lockFileUri.getPath());
    assertThat(GSON.fromJson(lockContent, RenameOperation.class).setLockExpiration(null))
        .isEqualTo(
            new RenameOperation()
                .setLockExpiration(null)
                .setSrcResource(srcDirUri.toString())
                .setDstResource(dstDirUri.toString())
                .setCopySucceeded(false));
    assertThat(matchFile(lockFiles, filenameFormat + "\\.log")).isEmpty();
  }

  @Test
  public void failedDirectoryRename_successfullyRolledForward_afterFailedCopy() throws Exception {
    failedDirectoryRename_successfullyRepaired("--rollForward", /* failCopy= */ true);
  }

  @Test
  public void failedDirectoryRename_successfullyRolledBack_afterFailedCopy() throws Exception {
    failedDirectoryRename_successfullyRepaired("--rollBack", /* failCopy= */ true);
  }

  @Test
  public void failedDirectoryRename_successfullyRolledForward_afterFailedDelete() throws Exception {
    failedDirectoryRename_successfullyRepaired("--rollForward", /* failCopy= */ false);
  }

  @Test
  public void failedDirectoryRename_successfullyRolledBack_afterFailedDelete() throws Exception {
    failedDirectoryRename_successfullyRepaired("--rollBack", /* failCopy= */ false);
  }

  private static void failedDirectoryRename_successfullyRepaired(String command, boolean failCopy)
      throws Exception {
    String commandSuffix = Ascii.toLowerCase(command).replace("--roll", "");
    String bucketName =
        gcsfsIHelper.createUniqueBucket(
            String.format("coop-rename-%s-failed-%s", commandSuffix, failCopy ? "copy" : "delete"));
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String dirName = "rename_" + UUID.randomUUID();
    String fileName = "file";
    URI srcDirUri = bucketUri.resolve(dirName + "_src/");
    URI dstDirUri = bucketUri.resolve(dirName + "_dst/");

    // create file to rename
    gcsfsIHelper.writeTextFile(bucketName, srcDirUri.resolve(fileName).getPath(), "file_content");

    GoogleCloudStorageFileSystemOptions gcsFsOptions = newGcsFsOptions();

    Predicate<HttpRequest> failPredicate =
        failCopy
            ? r -> "POST".equals(r.getRequestMethod()) && r.getUrl().toString().contains("/copyTo/")
            : r ->
                "DELETE".equals(r.getRequestMethod())
                    && r.getUrl().toString().contains("/b/" + bucketName + "/o/");
    failRenameOperation(srcDirUri, dstDirUri, gcsFsOptions, failPredicate);

    GoogleCloudStorageFileSystem gcsFs = newGcsFs(gcsFsOptions, httpRequestInitializer);

    assertThat(gcsFs.exists(srcDirUri)).isTrue();
    assertThat(gcsFs.exists(srcDirUri.resolve(fileName))).isTrue();
    assertThat(gcsFs.exists(dstDirUri)).isTrue();
    assertThat(gcsFs.exists(dstDirUri.resolve(fileName))).isEqualTo(!failCopy);

    CoopLockFsck fsck = new CoopLockFsck();
    fsck.setConf(getTestConfiguration());

    // Wait until lock will expire
    sleepUninterruptibly(COOP_LOCK_TIMEOUT);

    fsck.run(new String[] {command, "gs://" + bucketName, "all"});

    URI deletedDirUri = "--rollForward".equals(command) ? srcDirUri : dstDirUri;
    URI repairedDirUri = "--rollForward".equals(command) ? dstDirUri : srcDirUri;
    assertThat(gcsFs.exists(deletedDirUri)).isFalse();
    assertThat(gcsFs.exists(deletedDirUri.resolve(fileName))).isFalse();
    assertThat(gcsFs.exists(repairedDirUri)).isTrue();
    assertThat(gcsFs.exists(repairedDirUri.resolve(fileName))).isTrue();

    // Validate lock files
    List<URI> lockFiles =
        gcsFs.listFileInfo(bucketUri.resolve(LOCK_DIRECTORY)).stream()
            .map(FileInfo::getPath)
            .collect(toList());

    assertThat(lockFiles).hasSize(2);
    String filenameFormat = String.format(OPERATION_FILENAME_PATTERN_FORMAT, RENAME);
    URI lockFileUri = matchFile(lockFiles, filenameFormat + "\\.lock").get();
    URI logFileUri = matchFile(lockFiles, filenameFormat + "\\.log").get();

    String lockContent = gcsfsIHelper.readTextFile(bucketName, lockFileUri.getPath());
    assertThat(GSON.fromJson(lockContent, RenameOperation.class).setLockExpiration(null))
        .isEqualTo(
            new RenameOperation()
                .setLockExpiration(null)
                .setSrcResource(srcDirUri.toString())
                .setDstResource(dstDirUri.toString())
                .setCopySucceeded("--rollForward".equals(command)));
    assertThat(gcsfsIHelper.readTextFile(bucketName, logFileUri.getPath()))
        .isEqualTo(
            String.format(
                "{\"src\":\"%s\",\"dst\":\"%s\"}\n",
                srcDirUri.resolve(fileName), dstDirUri.resolve(fileName)));
  }

  private static void failRenameOperation(
      URI srcDirUri,
      URI dstDirUri,
      GoogleCloudStorageFileSystemOptions options,
      Predicate<HttpRequest> failPredicate)
      throws IOException {
    HttpRequestInitializer failingRequestInitializer = newFailingRequestInitializer(failPredicate);
    GoogleCloudStorageFileSystem failingGcsFs = newGcsFs(options, failingRequestInitializer);

    Exception e = assertThrows(Exception.class, () -> failingGcsFs.rename(srcDirUri, dstDirUri));
    assertThat(e).hasCauseThat().hasCauseThat().hasMessageThat().endsWith("Injected failure");
  }

  @Test
  public void failedDirectoryDelete_successfullyRolledForward() throws Exception {
    failedDirectoryDelete_successfullyRepaired("--rollForward");
  }

  @Test
  public void failedDirectoryDelete_successfullyRolledBack() throws Exception {
    failedDirectoryDelete_successfullyRepaired("--rollBack");
  }

  private static void failedDirectoryDelete_successfullyRepaired(String command) throws Exception {
    String bucketName =
        gcsfsIHelper.createUniqueBucket(
            "coop-delete-" + Ascii.toLowerCase(command).replace("--roll", "") + "-failed");
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String fileName = "file";
    URI dirUri = bucketUri.resolve("delete_" + UUID.randomUUID() + "/");

    // create file to delete
    gcsfsIHelper.writeTextFile(bucketName, dirUri.resolve(fileName).getPath(), "file_content");

    GoogleCloudStorageFileSystemOptions gcsFsOptions = newGcsFsOptions();

    failDeleteOperation(gcsFsOptions, bucketName, dirUri);

    GoogleCloudStorageFileSystem gcsFs = newGcsFs(gcsFsOptions, httpRequestInitializer);

    assertThat(gcsFs.exists(dirUri)).isTrue();
    assertThat(gcsFs.exists(dirUri.resolve(fileName))).isTrue();

    CoopLockFsck fsck = new CoopLockFsck();
    fsck.setConf(getTestConfiguration());

    // Wait until lock will expire
    sleepUninterruptibly(COOP_LOCK_TIMEOUT);

    fsck.run(new String[] {command, "gs://" + bucketName, "all"});

    assertThat(gcsFs.exists(dirUri)).isEqualTo(!"--rollForward".equals(command));
    assertThat(gcsFs.exists(dirUri.resolve(fileName))).isEqualTo(!"--rollForward".equals(command));

    // Validate lock files
    List<URI> lockFiles =
        gcsFs.listFileInfo(bucketUri.resolve(LOCK_DIRECTORY)).stream()
            .map(FileInfo::getPath)
            .collect(toList());

    assertThat(lockFiles).hasSize("--rollForward".equals(command) ? 2 : 3);
    String filenamePattern = String.format(OPERATION_FILENAME_PATTERN_FORMAT, DELETE);
    URI lockFileUri = matchFile(lockFiles, filenamePattern + "\\.lock").get();
    URI logFileUri = matchFile(lockFiles, filenamePattern + "\\.log").get();
    String lockContent = gcsfsIHelper.readTextFile(bucketName, lockFileUri.getPath());
    assertThat(GSON.fromJson(lockContent, DeleteOperation.class).setLockExpiration(null))
        .isEqualTo(new DeleteOperation().setLockExpiration(null).setResource(dirUri.toString()));
    assertThat(gcsfsIHelper.readTextFile(bucketName, logFileUri.getPath()))
        .isEqualTo(dirUri.resolve(fileName) + "\n" + dirUri + "\n");
  }

  private static void failDeleteOperation(
      GoogleCloudStorageFileSystemOptions gcsFsOptions, String bucketName, URI dirUri)
      throws Exception {
    HttpRequestInitializer failingRequestInitializer =
        newFailingRequestInitializer(
            request ->
                "DELETE".equals(request.getRequestMethod())
                    && request.getUrl().toString().contains("/b/" + bucketName + "/o/"));
    GoogleCloudStorageFileSystem failingGcsFs = newGcsFs(gcsFsOptions, failingRequestInitializer);

    IOException e =
        assertThrows(IOException.class, () -> failingGcsFs.delete(dirUri, /* recursive= */ true));
    assertThat(e).hasCauseThat().hasCauseThat().hasMessageThat().endsWith("Injected failure");
  }

  private static HttpRequestInitializer newFailingRequestInitializer(
      Predicate<HttpRequest> failurePredicate) {
    return request -> {
      httpRequestInitializer.initialize(request);
      HttpExecuteInterceptor executeInterceptor = checkNotNull(request.getInterceptor());
      request.setInterceptor(
          interceptedRequest -> {
            executeInterceptor.intercept(interceptedRequest);
            if (failurePredicate.test(interceptedRequest)) {
              throw new RuntimeException("Injected failure");
            }
          });
    };
  }

  private static Configuration getTestConfiguration() {
    Configuration conf = new Configuration();
    conf.set("fs.gs.impl", GoogleHadoopFileSystem.class.getName());
    conf.setBoolean(GCS_CONFIG_PREFIX + ENABLE_SERVICE_ACCOUNTS_SUFFIX.getKey(), true);
    conf.setLong(
        GCS_COOPERATIVE_LOCKING_EXPIRATION_TIMEOUT_MS.getKey(), COOP_LOCK_TIMEOUT.toMillis());

    // Configure test authentication
    TestConfiguration testConf = TestConfiguration.getInstance();
    conf.set(GCS_PROJECT_ID.getKey(), testConf.getProjectId());
    if (testConf.getServiceAccount() != null && testConf.getPrivateKeyFile() != null) {
      conf.set(
          GCS_CONFIG_PREFIX + SERVICE_ACCOUNT_EMAIL_SUFFIX.getKey(), testConf.getServiceAccount());
      conf.set(
          GCS_CONFIG_PREFIX + SERVICE_ACCOUNT_KEYFILE_SUFFIX.getKey(),
          testConf.getPrivateKeyFile());
    }
    return conf;
  }

  private static Optional<URI> matchFile(List<URI> files, String pattern) {
    return files.stream().filter(f -> f.toString().matches("^gs://.*/" + pattern + "$")).findAny();
  }

  private static GoogleCloudStorageFileSystemOptions newGcsFsOptions() {
    CooperativeLockingOptions coopLockOptions =
        CooperativeLockingOptions.builder()
            .setLockExpirationTimeoutMilli(COOP_LOCK_TIMEOUT.toMillis())
            .build();
    return GoogleCloudStorageFileSystemOptions.builder()
        .setCloudStorageOptions(
            gcsOptions.toBuilder().setCooperativeLockingOptions(coopLockOptions).build())
        .setCooperativeLockingEnabled(true)
        .build();
  }

  private static GoogleCloudStorageFileSystem newGcsFs(
      GoogleCloudStorageFileSystemOptions gcsFsOptions, HttpRequestInitializer requestInitializer)
      throws IOException {
    return new GoogleCloudStorageFileSystem(
        o -> new GoogleCloudStorageImpl(o, requestInitializer), gcsFsOptions);
  }
}
