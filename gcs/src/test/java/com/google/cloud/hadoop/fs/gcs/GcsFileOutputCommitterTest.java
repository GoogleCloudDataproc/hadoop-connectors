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

package com.google.cloud.hadoop.fs.gcs;

import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_OPERATION_MOVE_ENABLE;
import static com.google.cloud.hadoop.gcsio.testing.InMemoryGoogleCloudStorage.getInMemoryGoogleCloudStorageOptions;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.truth.Truth.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertThrows;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemTest.DownscopedAccessTokenProvider;
import com.google.cloud.hadoop.gcsio.CreateObjectOptions;
import com.google.cloud.hadoop.gcsio.FolderInfo;
import com.google.cloud.hadoop.gcsio.ForwardingGoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorage.ListPage;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemImpl;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageItemInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions;
import com.google.cloud.hadoop.gcsio.ListFolderOptions;
import com.google.cloud.hadoop.gcsio.ListObjectOptions;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.cloud.hadoop.gcsio.testing.InMemoryGoogleCloudStorage;
import com.google.cloud.hadoop.util.AccessTokenProvider;
import com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.AuthenticationType;
import com.google.cloud.hadoop.util.InvocationIdContext;
import com.google.common.collect.ImmutableList;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitterFactory;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link GcsFileOutputCommitter} and {@link GcsFileOutputCommitterFactory}. */
@RunWith(JUnit4.class)
public class GcsFileOutputCommitterTest {

  private static final String TEST_BUCKET = "test-committer-bucket";
  private static final String BUCKET_URI = "gs://" + TEST_BUCKET;

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  private CountingGoogleCloudStorage countingGcs;
  private Configuration baseConf;

  @Before
  public void setUp() throws IOException {
    initStorage(/* hnBucket= */ false, /* hnOptimization= */ false);
  }

  @After
  public void tearDown() throws IOException {
    TestGoogleHadoopFileSystem.setGcsFsSupplier(null);
    FileSystem.closeAll();
  }

  private void initStorage(boolean hnBucket, boolean hnOptimization) throws IOException {
    FileSystem.closeAll();
    GoogleCloudStorageOptions storageOptions =
        getInMemoryGoogleCloudStorageOptions().toBuilder()
            .setMoveOperationEnabled(true)
            .setHnBucketRenameEnabled(true)
            .setHnOptimizationEnabled(hnOptimization)
            .build();
    InMemoryGoogleCloudStorage inMemoryGcs = new InMemoryGoogleCloudStorage(storageOptions);
    inMemoryGcs.createBucket(TEST_BUCKET);
    countingGcs = new CountingGoogleCloudStorage(inMemoryGcs, hnBucket);

    GoogleCloudStorageFileSystemOptions gcsFsOptions =
        GoogleCloudStorageFileSystemOptions.builder()
            .setCloudStorageOptions(storageOptions)
            .build();
    TestGoogleHadoopFileSystem.setGcsFsSupplier(
        () -> {
          try {
            return new GoogleCloudStorageFileSystemImpl(opts -> countingGcs, gcsFsOptions);
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        });

    baseConf = new Configuration(false);
    baseConf.setClass("fs.gs.impl", TestGoogleHadoopFileSystem.class, FileSystem.class);
    baseConf.setInt(FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, 2);
  }

  private static TaskAttemptContext newTaskContext(
      Configuration conf, int jobNum, int taskNum, int attemptNum) {
    JobID jobId = new JobID("202611181200", jobNum);
    TaskID taskId = new TaskID(jobId, TaskType.MAP, taskNum);
    TaskAttemptID attemptId = new TaskAttemptID(taskId, attemptNum);
    return new TaskAttemptContextImpl(conf, attemptId);
  }

  private static TaskAttemptContext newCountingProgressTaskContext(
      Configuration conf, int jobNum, AtomicInteger progressCalls) {
    return new TaskAttemptContextImpl(conf, newTaskContext(conf, jobNum, 0, 0).getTaskAttemptID()) {
      @Override
      public void progress() {
        progressCalls.incrementAndGet();
      }
    };
  }

  private static Configuration newRealGhfsAuthConf(
      Class<? extends AccessTokenProvider> providerClass, boolean moveEnabled) {
    Configuration conf = new Configuration();
    conf.setClass("fs.gs.impl", GoogleHadoopFileSystem.class, FileSystem.class);
    conf.setEnum("fs.gs.auth.type", AuthenticationType.ACCESS_TOKEN_PROVIDER);
    conf.setClass("fs.gs.auth.access.token.provider", providerClass, AccessTokenProvider.class);
    conf.setInt(FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, 2);
    conf.setBoolean(GCS_OPERATION_MOVE_ENABLE.getKey(), moveEnabled);
    return conf;
  }

  private static JobContext newJobContext(Configuration conf, int jobNum) {
    return new JobContextImpl(conf, new JobID("202611181200", jobNum));
  }

  private void writeRawGcsObject(String objectName, String content) throws IOException {
    StorageResourceId id = new StorageResourceId(TEST_BUCKET, objectName);
    try (WritableByteChannel channel =
        countingGcs.create(id, CreateObjectOptions.DEFAULT_OVERWRITE)) {
      channel.write(ByteBuffer.wrap(content.getBytes(UTF_8)));
    }
  }

  private void stageFile(GcsFileOutputCommitter committer, String relativePath, String content)
      throws IOException {
    writeRawGcsObject(toObjectName(new Path(committer.getWorkPath(), relativePath)), content);
  }

  private static String readFile(Path path, Configuration conf) throws IOException {
    try (FSDataInputStream in = path.getFileSystem(conf).open(path)) {
      return new String(in.readAllBytes(), UTF_8);
    }
  }

  private static String toObjectName(Path gcsPath) {
    String path = gcsPath.toUri().getPath();
    return path.startsWith("/") ? path.substring(1) : path;
  }

  private void assertDelegatesToFileOutputCommitter(
      Path outputPath, Configuration conf, int jobNum, @Nullable Boolean expectSuccessMarker)
      throws IOException {
    JobContext jobContext = newJobContext(conf, jobNum);
    TaskAttemptContext taskContext = newTaskContext(conf, jobNum, 0, 0);

    countingGcs.resetCounts();
    GcsFileOutputCommitter jobCommitter = new GcsFileOutputCommitter(outputPath, jobContext);
    assertThat(jobCommitter.useGcsCommitter()).isFalse();
    assertThat(jobCommitter.toString()).contains("GcsFileOutputCommitter{useGcsCommitter=false,");
    assertThat(countingGcs.isHnBucketCalls.get()).isEqualTo(0);
    jobCommitter.setupJob(jobContext);

    FileSystem fs = outputPath.getFileSystem(conf);
    Path tempDir = new Path(outputPath, "_temporary/0");
    assertThat(fs.exists(tempDir)).isTrue();

    countingGcs.resetCounts();
    GcsFileOutputCommitter taskCommitter = new GcsFileOutputCommitter(outputPath, taskContext);
    assertThat(taskCommitter.useGcsCommitter()).isFalse();
    assertThat(taskCommitter.toString()).contains("GcsFileOutputCommitter{useGcsCommitter=false,");
    assertThat(countingGcs.isHnBucketCalls.get()).isEqualTo(0);
    assertThat(taskCommitter.needsTaskCommit(taskContext)).isFalse();
    assertThat(taskCommitter.needsTaskCommit(taskContext, /* taskAttemptPath= */ null)).isFalse();

    Path staged = new Path(taskCommitter.getWorkPath(), "part-00000.parquet");
    try (FSDataOutputStream out = fs.create(staged, true)) {
      out.write("delegated-data".getBytes(UTF_8));
    }
    assertThat(taskCommitter.needsTaskCommit(taskContext)).isTrue();
    taskCommitter.commitTask(taskContext);
    jobCommitter.commitJob(jobContext);

    assertThat(readFile(new Path(outputPath, "part-00000.parquet"), conf))
        .isEqualTo("delegated-data");
    if (expectSuccessMarker != null) {
      assertThat(fs.exists(new Path(outputPath, FileOutputCommitter.SUCCEEDED_FILE_NAME)))
          .isEqualTo(expectSuccessMarker);
    }
  }

  @Test
  public void lifecycle_singleFileFlatBucket_executesMinimalGcsCallsAndPublishes()
      throws IOException {
    Path outputPath = new Path(BUCKET_URI + "/warehouse/table_a");
    JobContext jobContext = newJobContext(baseConf, 1);
    TaskAttemptContext taskContext = newTaskContext(baseConf, 1, 0, 0);

    GcsFileOutputCommitter jobCommitter = new GcsFileOutputCommitter(outputPath, jobContext);
    assertThat(jobCommitter.useGcsCommitter()).isTrue();
    assertThat(countingGcs.isHnBucketCalls.get()).isEqualTo(1);
    assertThat(countingGcs.totalCalls()).isEqualTo(1);
    countingGcs.resetCounts();

    jobCommitter.setupJob(jobContext);
    assertThat(countingGcs.createEmptyCalls.get()).isEqualTo(0);
    assertThat(countingGcs.totalCalls()).isEqualTo(0);

    GcsFileOutputCommitter taskCommitter = new GcsFileOutputCommitter(outputPath, taskContext);
    assertThat(taskCommitter.useGcsCommitter()).isTrue();
    assertThat(taskCommitter.toString()).contains("GcsFileOutputCommitter{useGcsCommitter=true,");
    assertThat(countingGcs.isHnBucketCalls.get()).isEqualTo(1);
    assertThat(countingGcs.totalCalls()).isEqualTo(1);

    countingGcs.resetCounts();
    taskCommitter.setupTask(taskContext);
    assertThat(countingGcs.totalCalls()).isEqualTo(0);

    stageFile(taskCommitter, "part-00000.parquet", "parquet-data-0");
    countingGcs.resetCounts();

    assertThat(taskCommitter.needsTaskCommit(taskContext)).isTrue();
    assertThat(taskCommitter.needsTaskCommit(taskContext, taskCommitter.getWorkPath())).isTrue();
    assertThat(countingGcs.totalCalls()).isEqualTo(0);

    taskCommitter.commitTask(taskContext);
    assertThat(countingGcs.listCalls.get()).isEqualTo(1);
    assertThat(countingGcs.moveCalls.get()).isEqualTo(1);
    assertThat(countingGcs.totalCalls()).isEqualTo(2);
    assertThat(countingGcs.lastListOptions)
        .isEqualTo(
            ListObjectOptions.DEFAULT_FLAT_LIST.toBuilder().setFields("bucket,name").build());
    assertThat(countingGcs.lastMoveMap).hasSize(1);
    Map.Entry<StorageResourceId, StorageResourceId> moveEntry =
        countingGcs.lastMoveMap.entrySet().iterator().next();
    assertThat(moveEntry.getKey().hasGenerationId()).isFalse();
    assertThat(moveEntry.getValue().hasGenerationId()).isFalse();

    assertThat(readFile(new Path(outputPath, "part-00000.parquet"), baseConf))
        .isEqualTo("parquet-data-0");
    assertThat(
            countingGcs.listObjectInfo(
                TEST_BUCKET, "warehouse/table_a/_temporary/", ListObjectOptions.DEFAULT_FLAT_LIST))
        .isEmpty();

    countingGcs.resetCounts();
    jobCommitter.commitJob(jobContext);
    assertThat(countingGcs.deleteCalls.get()).isEqualTo(0);
    assertThat(
            countingGcs
                .getItemInfo(new StorageResourceId(TEST_BUCKET, "warehouse/table_a/_SUCCESS"))
                .exists())
        .isTrue();
  }

  @Test
  public void commitTask_multiFileNestedPartitionsAndSpecialChars_usesSingleListAndBatchedMove()
      throws IOException {
    Path outputPath = new Path(BUCKET_URI + "/Warehouse_Mixed/Table CamelCase");
    JobContext jobContext = newJobContext(baseConf, 2);
    TaskAttemptContext taskContext = newTaskContext(baseConf, 2, 0, 0);

    GcsFileOutputCommitter committer = new GcsFileOutputCommitter(outputPath, taskContext);
    assertThat(committer.useGcsCommitter()).isTrue();
    stageFile(committer, "Case_Part=Val%251/k=a b/part-00000.parquet", "data-0");
    stageFile(committer, "Case_Part=Val%251/k=ünï/UPPER_FILE.parquet", "data-1");
    writeRawGcsObject(
        toObjectName(committer.getWorkPath()) + "/m=a#b/q=a?b:c/part-00002.parquet", "data-2");

    countingGcs.resetCounts();
    committer.commitTask(taskContext);

    assertThat(countingGcs.listCalls.get()).isEqualTo(1);
    assertThat(countingGcs.moveCalls.get()).isEqualTo(1);
    assertThat(countingGcs.movedObjectCount.get()).isEqualTo(3);
    assertThat(countingGcs.totalCalls()).isEqualTo(2);

    assertThat(
            readFile(new Path(outputPath, "Case_Part=Val%251/k=a b/part-00000.parquet"), baseConf))
        .isEqualTo("data-0");
    assertThat(
            readFile(new Path(outputPath, "Case_Part=Val%251/k=ünï/UPPER_FILE.parquet"), baseConf))
        .isEqualTo("data-1");
    assertThat(
            readFile(
                new Path(outputPath, new Path(null, null, "m=a#b/q=a?b:c/part-00002.parquet")),
                baseConf))
        .isEqualTo("data-2");

    new GcsFileOutputCommitter(outputPath, jobContext).commitJob(jobContext);
    assertThat(
            countingGcs
                .getItemInfo(
                    new StorageResourceId(TEST_BUCKET, "Warehouse_Mixed/Table CamelCase/_SUCCESS"))
                .exists())
        .isTrue();
  }

  @Test
  public void commitTask_bucketRootOutputPaths_commitWithAndWithoutTrailingSlash()
      throws IOException {
    Path noSlashRoot = new Path(BUCKET_URI);
    TaskAttemptContext rootTask1 = newTaskContext(baseConf, 3, 0, 0);
    GcsFileOutputCommitter committer1 = new GcsFileOutputCommitter(noSlashRoot, rootTask1);
    assertThat(committer1.useGcsCommitter()).isTrue();
    stageFile(committer1, "part-00000.parquet", "root-no-slash");
    countingGcs.resetCounts();
    committer1.commitTask(rootTask1);
    assertThat(countingGcs.listCalls.get()).isEqualTo(1);
    assertThat(countingGcs.moveCalls.get()).isEqualTo(1);
    assertThat(countingGcs.totalCalls()).isEqualTo(2);
    assertThat(readFile(new Path(BUCKET_URI + "/part-00000.parquet"), baseConf))
        .isEqualTo("root-no-slash");

    Path slashRoot = new Path(BUCKET_URI + "/");
    JobContext jobContext = newJobContext(baseConf, 4);
    TaskAttemptContext rootTask2 = newTaskContext(baseConf, 4, 0, 0);
    GcsFileOutputCommitter committer2 = new GcsFileOutputCommitter(slashRoot, rootTask2);
    assertThat(committer2.useGcsCommitter()).isTrue();
    countingGcs.resetCounts();
    committer2.setupJob(jobContext);
    assertThat(countingGcs.createEmptyCalls.get()).isEqualTo(0);
    assertThat(countingGcs.totalCalls()).isEqualTo(0);

    stageFile(committer2, "part-00001.parquet", "root-slash");
    countingGcs.resetCounts();
    committer2.commitTask(rootTask2);
    assertThat(countingGcs.listCalls.get()).isEqualTo(1);
    assertThat(countingGcs.moveCalls.get()).isEqualTo(1);
    assertThat(countingGcs.totalCalls()).isEqualTo(2);

    countingGcs.resetCounts();
    committer2.commitJob(jobContext);
    assertThat(countingGcs.deleteCalls.get()).isEqualTo(0);

    assertThat(readFile(new Path(slashRoot, "part-00001.parquet"), baseConf))
        .isEqualTo("root-slash");
    assertThat(countingGcs.getItemInfo(new StorageResourceId(TEST_BUCKET, "_SUCCESS")).exists())
        .isTrue();
  }

  @Test
  public void commitTask_explicitAttemptPath_commitsExplicitSubtree() throws IOException {
    Path outputPath = new Path(BUCKET_URI + "/warehouse/explicit_attempt");
    TaskAttemptContext taskContext = newTaskContext(baseConf, 5, 0, 0);
    GcsFileOutputCommitter committer = new GcsFileOutputCommitter(outputPath, taskContext);
    assertThat(committer.useGcsCommitter()).isTrue();
    Path explicitAttemptPath = new Path(outputPath, "_custom_stage/task_0");
    writeRawGcsObject(toObjectName(explicitAttemptPath) + "/p=1/part-00000.parquet", "explicit");
    countingGcs.resetCounts();

    assertThat(committer.needsTaskCommit(taskContext, explicitAttemptPath)).isTrue();
    committer.commitTask(taskContext, explicitAttemptPath);

    assertThat(countingGcs.listCalls.get()).isEqualTo(1);
    assertThat(countingGcs.moveCalls.get()).isEqualTo(1);
    assertThat(countingGcs.totalCalls()).isEqualTo(2);
    assertThat(readFile(new Path(outputPath, "p=1/part-00000.parquet"), baseConf))
        .isEqualTo("explicit");
  }

  @Test
  public void commitTask_emptyAndDirectoryMarkerOnlyStages_skipMoveAndReportProgress()
      throws IOException {
    Path outputPath = new Path(BUCKET_URI + "/warehouse/empty_task");
    AtomicInteger progressCalls = new AtomicInteger();
    TaskAttemptContext taskContext = newCountingProgressTaskContext(baseConf, 6, progressCalls);

    GcsFileOutputCommitter committer = new GcsFileOutputCommitter(outputPath, taskContext);
    assertThat(committer.useGcsCommitter()).isTrue();
    countingGcs.resetCounts();

    committer.commitTask(taskContext);
    assertThat(countingGcs.listCalls.get()).isEqualTo(1);
    assertThat(countingGcs.moveCalls.get()).isEqualTo(0);
    assertThat(progressCalls.get()).isEqualTo(1);

    // Stage only an empty directory marker under workPath (exercises srcToDst.isEmpty()).
    String markerName = toObjectName(new Path(committer.getWorkPath(), "p=empty")) + "/";
    countingGcs.createEmptyObject(new StorageResourceId(TEST_BUCKET, markerName));
    countingGcs.resetCounts();

    committer.commitTask(taskContext);
    assertThat(countingGcs.listCalls.get()).isEqualTo(1);
    assertThat(countingGcs.moveCalls.get()).isEqualTo(0);
    assertThat(progressCalls.get()).isEqualTo(2);
    assertThat(
            countingGcs
                .getItemInfo(new StorageResourceId(TEST_BUCKET, "warehouse/empty_task/p=empty/"))
                .exists())
        .isFalse();
  }

  @Test
  public void commitTask_directoryMarkerWithSiblingFile_skipsMarkerAndCleansOnCommitJob()
      throws IOException {
    Path outputPath = new Path(BUCKET_URI + "/warehouse/with_dir_markers");
    JobContext jobContext = newJobContext(baseConf, 7);
    TaskAttemptContext taskContext = newTaskContext(baseConf, 7, 0, 0);

    GcsFileOutputCommitter jobCommitter = new GcsFileOutputCommitter(outputPath, jobContext);
    GcsFileOutputCommitter taskCommitter = new GcsFileOutputCommitter(outputPath, taskContext);
    assertThat(taskCommitter.useGcsCommitter()).isTrue();

    String emptySubdirMarker =
        toObjectName(new Path(taskCommitter.getWorkPath(), "empty_sub")) + "/";
    countingGcs.createEmptyObject(new StorageResourceId(TEST_BUCKET, emptySubdirMarker));
    stageFile(taskCommitter, "p=1/part-00000.parquet", "data");
    countingGcs.resetCounts();

    taskCommitter.commitTask(taskContext);

    assertThat(countingGcs.listCalls.get()).isEqualTo(1);
    assertThat(countingGcs.moveCalls.get()).isEqualTo(1);
    assertThat(countingGcs.movedObjectCount.get()).isEqualTo(1);
    assertThat(countingGcs.totalCalls()).isEqualTo(2);
    assertThat(readFile(new Path(outputPath, "p=1/part-00000.parquet"), baseConf))
        .isEqualTo("data");
    assertThat(
            countingGcs
                .getItemInfo(
                    new StorageResourceId(TEST_BUCKET, "warehouse/with_dir_markers/empty_sub/"))
                .exists())
        .isFalse();
    assertThat(
            countingGcs.getItemInfo(new StorageResourceId(TEST_BUCKET, emptySubdirMarker)).exists())
        .isTrue();

    jobCommitter.commitJob(jobContext);
    assertThat(
            countingGcs.getItemInfo(new StorageResourceId(TEST_BUCKET, emptySubdirMarker)).exists())
        .isFalse();
    assertThat(
            countingGcs
                .getItemInfo(
                    new StorageResourceId(TEST_BUCKET, "warehouse/with_dir_markers/_SUCCESS"))
                .exists())
        .isTrue();
  }

  @Test
  public void commitTask_multipleObjects_usesSingleBatchedMoveAndReportsProgress()
      throws IOException {
    Path outputPath = new Path(BUCKET_URI + "/warehouse/batched");
    AtomicInteger progressCalls = new AtomicInteger();
    TaskAttemptContext taskContext = newCountingProgressTaskContext(baseConf, 8, progressCalls);

    GcsFileOutputCommitter committer = new GcsFileOutputCommitter(outputPath, taskContext);
    assertThat(committer.useGcsCommitter()).isTrue();
    stageFile(committer, "part-00000.parquet", "d1");
    stageFile(committer, "part-00001.parquet", "d2");
    stageFile(committer, "part-00002.parquet", "d3");
    countingGcs.resetCounts();

    committer.commitTask(taskContext);

    assertThat(countingGcs.listCalls.get()).isEqualTo(1);
    assertThat(countingGcs.moveCalls.get()).isEqualTo(1);
    assertThat(countingGcs.movedObjectCount.get()).isEqualTo(3);
    assertThat(countingGcs.totalCalls()).isEqualTo(2);
    assertThat(progressCalls.get()).isEqualTo(2);
    assertThat(readFile(new Path(outputPath, "part-00000.parquet"), baseConf)).isEqualTo("d1");
    assertThat(readFile(new Path(outputPath, "part-00001.parquet"), baseConf)).isEqualTo("d2");
    assertThat(readFile(new Path(outputPath, "part-00002.parquet"), baseConf)).isEqualTo("d3");
  }

  @Test
  public void commitTask_inferredDirMarker_skipsMarkerAndMovesWithoutGeneration()
      throws IOException {
    Path outputPath = new Path(BUCKET_URI + "/warehouse/zero_gen");
    AtomicInteger progressCalls = new AtomicInteger();
    TaskAttemptContext stubTask = newCountingProgressTaskContext(baseConf, 9, progressCalls);
    GcsFileOutputCommitter stubCommitter = new GcsFileOutputCommitter(outputPath, stubTask);
    assertThat(stubCommitter.useGcsCommitter()).isTrue();
    String attemptPrefix = toObjectName(stubCommitter.getWorkPath()) + "/";
    stageFile(stubCommitter, "part-00003.parquet", "d4");
    countingGcs.stubbedListResult =
        ImmutableList.of(
            GoogleCloudStorageItemInfo.createInferredDirectory(
                new StorageResourceId(TEST_BUCKET, attemptPrefix + "empty_dir/")),
            GoogleCloudStorageItemInfo.createObject(
                new StorageResourceId(TEST_BUCKET, attemptPrefix + "part-00003.parquet"),
                /* creationTime= */ 0L,
                /* modificationTime= */ 0L,
                /* size= */ 0L,
                /* contentType= */ null,
                /* contentEncoding= */ null,
                /* metadata= */ null,
                /* contentGeneration= */ 0L,
                /* metaGeneration= */ 0L,
                /* verificationAttributes= */ null));
    countingGcs.resetCounts();

    stubCommitter.commitTask(stubTask);

    assertThat(countingGcs.listCalls.get()).isEqualTo(1);
    assertThat(countingGcs.moveCalls.get()).isEqualTo(1);
    assertThat(countingGcs.movedObjectCount.get()).isEqualTo(1);
    assertThat(countingGcs.totalCalls()).isEqualTo(2);
    assertThat(progressCalls.get()).isEqualTo(2);
    assertThat(countingGcs.lastMoveMap).hasSize(1);
    Map.Entry<StorageResourceId, StorageResourceId> zeroGenMove =
        countingGcs.lastMoveMap.entrySet().iterator().next();
    assertThat(zeroGenMove.getKey().hasGenerationId()).isFalse();
    assertThat(zeroGenMove.getValue().hasGenerationId()).isFalse();
    assertThat(readFile(new Path(outputPath, "part-00003.parquet"), baseConf)).isEqualTo("d4");
  }

  @Test
  public void commitTask_moveIoException_propagatesAndPreservesStagedObject() throws IOException {
    Path outputPath = new Path(BUCKET_URI + "/warehouse/move_error");
    TaskAttemptContext taskContext = newTaskContext(baseConf, 10, 0, 0);
    GcsFileOutputCommitter committer = new GcsFileOutputCommitter(outputPath, taskContext);
    assertThat(committer.useGcsCommitter()).isTrue();
    String stagedName = toObjectName(new Path(committer.getWorkPath(), "part-00000.parquet"));
    writeRawGcsObject(stagedName, "pending");
    countingGcs.resetCounts();
    countingGcs.moveError = new IOException("injected move failure");

    IOException thrown = assertThrows(IOException.class, () -> committer.commitTask(taskContext));
    assertThat(thrown).hasMessageThat().contains("injected move failure");
    assertThat(InvocationIdContext.getInvocationId()).isEmpty();
    assertThat(countingGcs.listCalls.get()).isEqualTo(1);
    assertThat(countingGcs.moveCalls.get()).isEqualTo(1);
    assertThat(countingGcs.totalCalls()).isEqualTo(2);
    assertThat(countingGcs.getItemInfo(new StorageResourceId(TEST_BUCKET, stagedName)).exists())
        .isTrue();
  }

  @Test
  public void commitTask_siblingUnpaddedAttemptIds_isolatesAttemptPrefix() throws IOException {
    Path outputPath = new Path(BUCKET_URI + "/warehouse/sibling_attempts");
    TaskAttemptContext attempt1 = newTaskContext(baseConf, 11, 3, 1);
    TaskAttemptContext attempt10 = newTaskContext(baseConf, 11, 3, 10);

    GcsFileOutputCommitter committer1 = new GcsFileOutputCommitter(outputPath, attempt1);
    GcsFileOutputCommitter committer10 = new GcsFileOutputCommitter(outputPath, attempt10);
    assertThat(committer1.useGcsCommitter()).isTrue();
    assertThat(committer10.useGcsCommitter()).isTrue();
    assertThat(committer10.getWorkPath().getName()).startsWith(committer1.getWorkPath().getName());

    stageFile(committer1, "part-00003-attempt1.parquet", "attempt-1");
    stageFile(committer10, "part-00003-attempt10.parquet", "attempt-10");
    countingGcs.resetCounts();

    committer1.commitTask(attempt1);

    assertThat(countingGcs.listCalls.get()).isEqualTo(1);
    assertThat(countingGcs.moveCalls.get()).isEqualTo(1);
    assertThat(countingGcs.movedObjectCount.get()).isEqualTo(1);
    assertThat(countingGcs.totalCalls()).isEqualTo(2);
    assertThat(countingGcs.lastMoveMap)
        .containsExactly(
            new StorageResourceId(
                TEST_BUCKET,
                toObjectName(new Path(committer1.getWorkPath(), "part-00003-attempt1.parquet"))),
            new StorageResourceId(
                TEST_BUCKET, "warehouse/sibling_attempts/part-00003-attempt1.parquet"));
    assertThat(readFile(new Path(outputPath, "part-00003-attempt1.parquet"), baseConf))
        .isEqualTo("attempt-1");
    assertThat(
            countingGcs
                .getItemInfo(
                    new StorageResourceId(
                        TEST_BUCKET,
                        toObjectName(
                            new Path(committer10.getWorkPath(), "part-00003-attempt10.parquet"))))
                .exists())
        .isTrue();
    assertThat(
            countingGcs.listObjectInfo(
                TEST_BUCKET, "warehouse/sibling_attempts/0/", ListObjectOptions.DEFAULT_FLAT_LIST))
        .isEmpty();
    assertThat(
            countingGcs
                .getItemInfo(
                    new StorageResourceId(
                        TEST_BUCKET, "warehouse/sibling_attempts/part-00003-attempt10.parquet"))
                .exists())
        .isFalse();
  }

  @Test
  public void commitTask_retryAttempt_overwritesPartialDestinationAndIgnoresStaleOrphan()
      throws IOException {
    Path outputPath = new Path(BUCKET_URI + "/warehouse/retry_table");
    JobContext jobContext = newJobContext(baseConf, 12);
    TaskAttemptContext attempt0 = newTaskContext(baseConf, 12, 0, 0);
    TaskAttemptContext attempt1 = newTaskContext(baseConf, 12, 0, 1);

    GcsFileOutputCommitter jobCommitter = new GcsFileOutputCommitter(outputPath, jobContext);
    assertThat(jobCommitter.useGcsCommitter()).isTrue();
    countingGcs.resetCounts();
    jobCommitter.setupJob(jobContext);
    assertThat(countingGcs.createEmptyCalls.get()).isEqualTo(0);
    assertThat(countingGcs.totalCalls()).isEqualTo(0);

    GcsFileOutputCommitter committer0 = new GcsFileOutputCommitter(outputPath, attempt0);
    assertThat(committer0.useGcsCommitter()).isTrue();
    stageFile(committer0, "part-00000.parquet", "partial-bad");
    stageFile(committer0, "orphan-only-in-attempt0.parquet", "stale");
    writeRawGcsObject("warehouse/retry_table/part-00000.parquet", "partial-bad");

    GcsFileOutputCommitter committer1 = new GcsFileOutputCommitter(outputPath, attempt1);
    assertThat(committer1.useGcsCommitter()).isTrue();
    stageFile(committer1, "part-00000.parquet", "complete-good");
    countingGcs.resetCounts();
    committer1.commitTask(attempt1);
    assertThat(countingGcs.listCalls.get()).isEqualTo(1);
    assertThat(countingGcs.moveCalls.get()).isEqualTo(1);
    assertThat(countingGcs.totalCalls()).isEqualTo(2);

    countingGcs.resetCounts();
    jobCommitter.commitJob(jobContext);
    assertThat(countingGcs.deleteCalls.get()).isEqualTo(1);

    assertThat(readFile(new Path(outputPath, "part-00000.parquet"), baseConf))
        .isEqualTo("complete-good");
    assertThat(
            countingGcs
                .getItemInfo(
                    new StorageResourceId(
                        TEST_BUCKET, "warehouse/retry_table/orphan-only-in-attempt0.parquet"))
                .exists())
        .isFalse();
    assertThat(
            countingGcs.listObjectInfo(
                TEST_BUCKET,
                "warehouse/retry_table/_temporary/",
                ListObjectOptions.DEFAULT_FLAT_LIST))
        .isEmpty();
  }

  @Test
  public void commitTask_invalidAttemptPaths_throwWithoutMovingAndPreserveOtherObjects()
      throws IOException {
    Path outputPath = new Path(BUCKET_URI + "/warehouse/strict_prefix");
    TaskAttemptContext taskContext = newTaskContext(baseConf, 13, 0, 0);
    GcsFileOutputCommitter committer = new GcsFileOutputCommitter(outputPath, taskContext);
    assertThat(committer.useGcsCommitter()).isTrue();
    writeRawGcsObject("warehouse/other_table/part-00000.parquet", "untouched");
    writeRawGcsObject("warehouse/strict_prefix_other/attempt_0/part-00000.parquet", "sibling");
    countingGcs.resetCounts();

    for (Path invalidAttemptPath :
        new Path[] {
          new Path(BUCKET_URI + "/warehouse"),
          new Path(BUCKET_URI + "/"),
          outputPath,
          new Path(BUCKET_URI + "/warehouse/strict_prefix_other/attempt_0")
        }) {
      IllegalStateException ex =
          assertThrows(
              IllegalStateException.class,
              () -> committer.commitTask(taskContext, invalidAttemptPath));
      assertThat(ex).hasMessageThat().contains("must be a child of output prefix");
    }

    IllegalArgumentException wrongBucketEx =
        assertThrows(
            IllegalArgumentException.class,
            () -> committer.commitTask(taskContext, new Path("gs://other-bucket/attempt_0")));
    assertThat(wrongBucketEx).hasMessageThat().contains("Wrong bucket");
    IllegalArgumentException wrongSchemeEx =
        assertThrows(
            IllegalArgumentException.class,
            () -> committer.commitTask(taskContext, new Path("file:///tmp/attempt_0")));
    assertThat(wrongSchemeEx).hasMessageThat().contains("Wrong scheme");

    assertThat(countingGcs.moveCalls.get()).isEqualTo(0);
    assertThat(countingGcs.listCalls.get()).isEqualTo(0);
    assertThat(
            readFile(new Path(BUCKET_URI + "/warehouse/other_table/part-00000.parquet"), baseConf))
        .isEqualTo("untouched");
    assertThat(
            readFile(
                new Path(
                    BUCKET_URI + "/warehouse/strict_prefix_other/attempt_0/part-00000.parquet"),
                baseConf))
        .isEqualTo("sibling");
  }

  @Test
  public void setupJobAndCommitTask_closedCachedFileSystem_refreshesAndCommits()
      throws IOException {
    Path outputPath = new Path(BUCKET_URI + "/warehouse/closed_fs_refresh");
    JobContext jobContext = newJobContext(baseConf, 14);
    TaskAttemptContext taskContext = newTaskContext(baseConf, 14, 0, 0);
    GcsFileOutputCommitter jobCommitter = new GcsFileOutputCommitter(outputPath, jobContext);
    GcsFileOutputCommitter taskCommitter = new GcsFileOutputCommitter(outputPath, taskContext);
    assertThat(jobCommitter.useGcsCommitter()).isTrue();
    assertThat(taskCommitter.useGcsCommitter()).isTrue();
    stageFile(taskCommitter, "part-00000.parquet", "refreshed");

    // Close cached FileSystem before setupJob and again before commitTask to verify CACHE refresh.
    outputPath.getFileSystem(baseConf).close();
    countingGcs.resetCounts();
    jobCommitter.setupJob(jobContext);
    assertThat(countingGcs.createEmptyCalls.get()).isEqualTo(0);
    assertThat(countingGcs.totalCalls()).isEqualTo(0);

    outputPath.getFileSystem(baseConf).close();
    countingGcs.resetCounts();
    taskCommitter.commitTask(taskContext);

    assertThat(countingGcs.listCalls.get()).isEqualTo(1);
    assertThat(countingGcs.moveCalls.get()).isEqualTo(1);
    assertThat(countingGcs.movedObjectCount.get()).isEqualTo(1);
    assertThat(countingGcs.totalCalls()).isEqualTo(2);
    assertThat(readFile(new Path(outputPath, "part-00000.parquet"), baseConf))
        .isEqualTo("refreshed");
  }

  @Test
  public void abortTaskAndAbortJob_inheritedBehavior_deleteTemporaryOutputs() throws IOException {
    Path outputPath = new Path(BUCKET_URI + "/warehouse/abort_table");
    JobContext jobContext = newJobContext(baseConf, 15);
    TaskAttemptContext taskContext = newTaskContext(baseConf, 15, 0, 0);

    GcsFileOutputCommitter jobCommitter = new GcsFileOutputCommitter(outputPath, jobContext);
    GcsFileOutputCommitter taskCommitter = new GcsFileOutputCommitter(outputPath, taskContext);
    stageFile(taskCommitter, "part-00000.parquet", "aborted");

    taskCommitter.abortTask(taskContext);
    assertThat(
            countingGcs.listObjectInfo(
                TEST_BUCKET,
                toObjectName(taskCommitter.getWorkPath()) + "/",
                ListObjectOptions.DEFAULT_FLAT_LIST))
        .isEmpty();

    TaskAttemptContext task1 = newTaskContext(baseConf, 15, 1, 0);
    GcsFileOutputCommitter committer1 = new GcsFileOutputCommitter(outputPath, task1);
    stageFile(committer1, "part-00001.parquet", "uncommitted");

    jobCommitter.abortJob(jobContext, JobStatus.State.FAILED);
    assertThat(
            countingGcs.listObjectInfo(
                TEST_BUCKET, "warehouse/abort_table/", ListObjectOptions.DEFAULT_FLAT_LIST))
        .isEmpty();
  }

  @Test
  public void constructor_hnBucket_fallsBackToFileOutputCommitter() throws IOException {
    for (boolean hnOptimization : new boolean[] {false, true}) {
      initStorage(/* hnBucket= */ true, hnOptimization);
      String tableName = hnOptimization ? "hn_opt_table" : "hn_default_table";
      Path outputPath = new Path(BUCKET_URI + "/warehouse/" + tableName);
      JobContext jobContext = newJobContext(baseConf, hnOptimization ? 17 : 16);
      TaskAttemptContext taskContext = newTaskContext(baseConf, hnOptimization ? 17 : 16, 0, 0);

      GcsFileOutputCommitter jobCommitter = new GcsFileOutputCommitter(outputPath, jobContext);
      assertThat(jobCommitter.useGcsCommitter()).isFalse();
      countingGcs.resetCounts();
      jobCommitter.setupJob(jobContext);
      if (!hnOptimization) {
        assertThat(countingGcs.createEmptyCalls.get()).isEqualTo(1);
      } else {
        assertThat(countingGcs.createFolderCalls.get()).isGreaterThan(0);
      }

      GcsFileOutputCommitter taskCommitter = new GcsFileOutputCommitter(outputPath, taskContext);
      assertThat(taskCommitter.useGcsCommitter()).isFalse();
      assertThat(taskCommitter.needsTaskCommit(taskContext)).isFalse();
      stageFile(taskCommitter, "p=1/part-00000.parquet", "h-" + hnOptimization);
      assertThat(taskCommitter.needsTaskCommit(taskContext)).isTrue();
      countingGcs.resetCounts();
      taskCommitter.commitTask(taskContext);
      jobCommitter.commitJob(jobContext);

      assertThat(countingGcs.foldersUnder("warehouse/" + tableName + "/_temporary/")).isEmpty();
      assertThat(
              countingGcs.listObjectInfo(
                  TEST_BUCKET,
                  "warehouse/" + tableName + "/_temporary/",
                  ListObjectOptions.DEFAULT_FLAT_LIST))
          .isEmpty();
      assertThat(readFile(new Path(outputPath, "p=1/part-00000.parquet"), baseConf))
          .isEqualTo("h-" + hnOptimization);
      assertThat(
              countingGcs
                  .getItemInfo(
                      new StorageResourceId(TEST_BUCKET, "warehouse/" + tableName + "/_SUCCESS"))
                  .exists())
          .isTrue();

      // Verify abortJob also cleans native HNS folders under _temporary/.
      Path abortPath = new Path(BUCKET_URI + "/warehouse/" + tableName + "_abort");
      JobContext abortJob = newJobContext(baseConf, hnOptimization ? 19 : 18);
      TaskAttemptContext abortTask = newTaskContext(baseConf, hnOptimization ? 19 : 18, 0, 0);
      GcsFileOutputCommitter abortJobCommitter = new GcsFileOutputCommitter(abortPath, abortJob);
      abortJobCommitter.setupJob(abortJob);
      stageFile(new GcsFileOutputCommitter(abortPath, abortTask), "p=1/part-0.parquet", "aborted");
      abortJobCommitter.abortJob(abortJob, JobStatus.State.FAILED);
      assertThat(countingGcs.foldersUnder("warehouse/" + tableName + "_abort/_temporary/"))
          .isEmpty();
      assertThat(
              countingGcs.listObjectInfo(
                  TEST_BUCKET,
                  "warehouse/" + tableName + "_abort/_temporary/",
                  ListObjectOptions.DEFAULT_FLAT_LIST))
          .isEmpty();
    }
  }

  @Test
  public void constructor_hnBucketLookupError_fallsBackToFileOutputCommitter() throws IOException {
    Path outputPath = new Path(BUCKET_URI + "/warehouse/hns_lookup_error");
    JobContext jobContext = newJobContext(baseConf, 20);
    TaskAttemptContext taskContext = newTaskContext(baseConf, 20, 0, 0);

    countingGcs.isHnBucketError = new IOException("simulated getStorageLayout error");
    GcsFileOutputCommitter jobCommitter = new GcsFileOutputCommitter(outputPath, jobContext);
    GcsFileOutputCommitter taskCommitter = new GcsFileOutputCommitter(outputPath, taskContext);
    assertThat(jobCommitter.useGcsCommitter()).isFalse();
    assertThat(taskCommitter.useGcsCommitter()).isFalse();
    // Clear transient lookup failure after construction so FileOutputCommitter cleanup can proceed.
    countingGcs.isHnBucketError = null;

    jobCommitter.setupJob(jobContext);
    assertThat(
            countingGcs
                .getItemInfo(
                    new StorageResourceId(TEST_BUCKET, "warehouse/hns_lookup_error/_temporary/0/"))
                .exists())
        .isTrue();

    assertThat(taskCommitter.needsTaskCommit(taskContext)).isFalse();
    stageFile(taskCommitter, "part-00000.parquet", "fallback-after-hns-error");
    assertThat(taskCommitter.needsTaskCommit(taskContext)).isTrue();
    taskCommitter.commitTask(taskContext);
    jobCommitter.commitJob(jobContext);

    assertThat(readFile(new Path(outputPath, "part-00000.parquet"), baseConf))
        .isEqualTo("fallback-after-hns-error");
    assertThat(
            countingGcs
                .getItemInfo(
                    new StorageResourceId(TEST_BUCKET, "warehouse/hns_lookup_error/_SUCCESS"))
                .exists())
        .isTrue();
    assertThat(
            countingGcs.listObjectInfo(
                TEST_BUCKET,
                "warehouse/hns_lookup_error/_temporary/",
                ListObjectOptions.DEFAULT_FLAT_LIST))
        .isEmpty();
  }

  @Test
  public void fallbackConditions_delegateToFileOutputCommitter() throws IOException {
    // 1. marksuccessfuljobs=false
    Configuration noSuccessConf = new Configuration(baseConf);
    noSuccessConf.setBoolean(FileOutputCommitter.SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, false);
    assertDelegatesToFileOutputCommitter(
        new Path(BUCKET_URI + "/warehouse/no_success"),
        noSuccessConf,
        21,
        /* expectSuccessMarker= */ false);

    // 2. Algorithm v1 (both explicit v1 and JobContext ctor)
    Configuration v1Conf = new Configuration(baseConf);
    v1Conf.setInt(FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, 1);
    assertDelegatesToFileOutputCommitter(
        new Path(BUCKET_URI + "/warehouse/algo_v1"), v1Conf, 22, /* expectSuccessMarker= */ true);
    countingGcs.resetCounts();
    assertThat(
            new GcsFileOutputCommitter(
                    new Path(BUCKET_URI + "/warehouse/algo_v1_job"), newJobContext(v1Conf, 23))
                .needsTaskCommit(newTaskContext(v1Conf, 23, 0, 0)))
        .isFalse();
    assertThat(countingGcs.isHnBucketCalls.get()).isEqualTo(0);

    // 3. Dataproc concurrent write enabled
    Configuration concurrentWriteConf = new Configuration(baseConf);
    concurrentWriteConf.setBoolean(
        GcsFileOutputCommitter.FILEOUTPUTCOMMITTER_CONCURRENT_WRITE_ENABLED, true);
    assertDelegatesToFileOutputCommitter(
        new Path(BUCKET_URI + "/warehouse/concurrent_write"),
        concurrentWriteConf,
        26,
        /* expectSuccessMarker= */ null);

    // 4. fs.gs.operation.move.enable=false in Configuration
    Configuration moveDisabledConf = new Configuration(baseConf);
    moveDisabledConf.setBoolean(GCS_OPERATION_MOVE_ENABLE.getKey(), false);
    assertDelegatesToFileOutputCommitter(
        new Path(BUCKET_URI + "/warehouse/move_disabled"),
        moveDisabledConf,
        25,
        /* expectSuccessMarker= */ true);

    // 5. Non-GCS LocalFileSystem
    Path localOutput = new Path(tempFolder.newFolder("local-out").toURI());
    Configuration localConf = new Configuration();
    localConf.setInt(FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, 2);
    assertDelegatesToFileOutputCommitter(
        localOutput, localConf, 27, /* expectSuccessMarker= */ true);
  }

  @Test
  public void lifecycle_unsetAlgorithmVersion_followsHadoopDefault() throws IOException {
    Configuration unsetAlgoConf = new Configuration(baseConf);
    unsetAlgoConf.unset(FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION);
    Path outputPath = new Path(BUCKET_URI + "/warehouse/unset_algo");
    JobContext jobContext = newJobContext(unsetAlgoConf, 24);
    TaskAttemptContext taskContext = newTaskContext(unsetAlgoConf, 24, 0, 0);

    GcsFileOutputCommitter jobCommitter = new GcsFileOutputCommitter(outputPath, jobContext);
    GcsFileOutputCommitter taskCommitter = new GcsFileOutputCommitter(outputPath, taskContext);
    boolean expectActive = FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION_DEFAULT == 2;
    assertThat(jobCommitter.useGcsCommitter()).isEqualTo(expectActive);
    assertThat(taskCommitter.useGcsCommitter()).isEqualTo(expectActive);
    assertThat(taskCommitter.needsTaskCommit(taskContext)).isEqualTo(expectActive);

    jobCommitter.setupJob(jobContext);
    stageFile(taskCommitter, "part-00000.parquet", "default-algo");
    taskCommitter.commitTask(taskContext);
    jobCommitter.commitJob(jobContext);
    assertThat(readFile(new Path(outputPath, "part-00000.parquet"), unsetAlgoConf))
        .isEqualTo("default-algo");
  }

  @Test
  public void constructor_downscopedTokens_propagatesUnlessMoveDisabled() throws IOException {
    FileSystem.closeAll();
    Path outputPath = new Path("gs://downscoped-test-bucket/out");
    Configuration downscopedMoveOnConf =
        newRealGhfsAuthConf(DownscopedAccessTokenProvider.class, /* moveEnabled= */ true);

    InvalidParameterException thrown =
        assertThrows(
            InvalidParameterException.class,
            () ->
                new GcsFileOutputCommitter(
                    outputPath, newTaskContext(downscopedMoveOnConf, 33, 0, 0)));
    assertThat(thrown).hasMessageThat().contains("GetStorageLayout");

    FileSystem.closeAll();
    Configuration downscopedMoveOffConf =
        newRealGhfsAuthConf(DownscopedAccessTokenProvider.class, /* moveEnabled= */ false);
    GcsFileOutputCommitter fallbackCommitter =
        new GcsFileOutputCommitter(outputPath, newTaskContext(downscopedMoveOffConf, 34, 0, 0));
    assertThat(fallbackCommitter.useGcsCommitter()).isFalse();
  }

  @Test
  public void constructor_lazyInitFailure_propagatesWithoutFallback() throws IOException {
    Configuration lazyFailConf = new Configuration(baseConf);
    lazyFailConf.setBoolean("fs.gs.lazy.init.enable", true);
    lazyFailConf.setEnum("fs.gs.auth.type", AuthenticationType.SERVICE_ACCOUNT_JSON_KEYFILE);
    lazyFailConf.set("fs.gs.auth.service.account.json.keyfile", "non-existent.json");
    lazyFailConf.setClass("fs.gs.impl", GoogleHadoopFileSystem.class, FileSystem.class);
    Path lazyPath = new Path("gs://lazy-init-fail-bucket/out");
    RuntimeException lazyEx =
        assertThrows(
            RuntimeException.class,
            () -> new GcsFileOutputCommitter(lazyPath, newTaskContext(lazyFailConf, 40, 0, 0)));
    assertThat(lazyEx).hasMessageThat().isEqualTo("Failed to create GCS FS");
    assertThat(lazyEx).hasCauseThat().isInstanceOf(FileNotFoundException.class);
    assertThat(lazyEx).hasCauseThat().hasMessageThat().contains("non-existent.json");
  }

  @Test
  public void commitTask_listObjectInfoIoException_propagatesWithoutMoving() throws IOException {
    Path outputPath = new Path(BUCKET_URI + "/warehouse/list_error");
    TaskAttemptContext taskContext = newTaskContext(baseConf, 37, 0, 0);
    GcsFileOutputCommitter committer = new GcsFileOutputCommitter(outputPath, taskContext);
    assertThat(committer.useGcsCommitter()).isTrue();
    stageFile(committer, "part-00000.parquet", "pending");
    countingGcs.resetCounts();
    countingGcs.listError = new IOException("injected list failure");

    IOException thrown = assertThrows(IOException.class, () -> committer.commitTask(taskContext));
    assertThat(thrown).hasMessageThat().contains("injected list failure");
    assertThat(countingGcs.moveCalls.get()).isEqualTo(0);
    assertThat(InvocationIdContext.getInvocationId()).isEmpty();
  }

  @Test
  public void lifecycle_zeroFileCommitJob_respectsMarkSuccessfulJobs() throws IOException {
    // 1. marksuccessfuljobs=true: active committer skips mkdirs/delete and writes _SUCCESS.
    Path successOnPath = new Path(BUCKET_URI + "/warehouse/zero_file_success_on");
    JobContext onJob = newJobContext(baseConf, 38);
    TaskAttemptContext onTask = newTaskContext(baseConf, 38, 0, 0);
    GcsFileOutputCommitter onJobCommitter = new GcsFileOutputCommitter(successOnPath, onJob);
    GcsFileOutputCommitter onTaskCommitter = new GcsFileOutputCommitter(successOnPath, onTask);
    assertThat(onJobCommitter.useGcsCommitter()).isTrue();
    assertThat(onTaskCommitter.useGcsCommitter()).isTrue();

    countingGcs.resetCounts();
    onJobCommitter.setupJob(onJob);
    assertThat(countingGcs.createEmptyCalls.get()).isEqualTo(0);
    assertThat(countingGcs.totalCalls()).isEqualTo(0);

    assertThat(onTaskCommitter.needsTaskCommit(onTask)).isTrue();
    onTaskCommitter.commitTask(onTask);
    assertThat(countingGcs.listCalls.get()).isEqualTo(1);
    assertThat(countingGcs.moveCalls.get()).isEqualTo(0);

    countingGcs.resetCounts();
    onJobCommitter.commitJob(onJob);
    assertThat(countingGcs.deleteCalls.get()).isEqualTo(0);
    assertThat(
            countingGcs
                .getItemInfo(
                    new StorageResourceId(TEST_BUCKET, "warehouse/zero_file_success_on/_SUCCESS"))
                .exists())
        .isTrue();

    // 2. marksuccessfuljobs=false: falls back to FileOutputCommitter, no _SUCCESS written.
    Configuration noSuccessConf = new Configuration(baseConf);
    noSuccessConf.setBoolean(FileOutputCommitter.SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, false);
    Path successOffPath = new Path(BUCKET_URI + "/warehouse/zero_file_success_off");
    JobContext offJob = newJobContext(noSuccessConf, 39);
    TaskAttemptContext offTask = newTaskContext(noSuccessConf, 39, 0, 0);
    GcsFileOutputCommitter offJobCommitter = new GcsFileOutputCommitter(successOffPath, offJob);
    GcsFileOutputCommitter offTaskCommitter = new GcsFileOutputCommitter(successOffPath, offTask);
    assertThat(offJobCommitter.useGcsCommitter()).isFalse();
    assertThat(offTaskCommitter.useGcsCommitter()).isFalse();

    offJobCommitter.setupJob(offJob);
    assertThat(
            countingGcs
                .getItemInfo(
                    new StorageResourceId(
                        TEST_BUCKET, "warehouse/zero_file_success_off/_temporary/0/"))
                .exists())
        .isTrue();
    assertThat(offTaskCommitter.needsTaskCommit(offTask)).isFalse();
    offJobCommitter.commitJob(offJob);
    assertThat(
            countingGcs
                .getItemInfo(
                    new StorageResourceId(TEST_BUCKET, "warehouse/zero_file_success_off/_SUCCESS"))
                .exists())
        .isFalse();
    assertThat(
            countingGcs.listObjectInfo(
                TEST_BUCKET,
                "warehouse/zero_file_success_off/_temporary/",
                ListObjectOptions.DEFAULT_FLAT_LIST))
        .isEmpty();
    assertThat(
            successOffPath.getFileSystem(noSuccessConf).getFileStatus(successOffPath).isDirectory())
        .isTrue();
  }

  @Test
  public void commitTask_outputPathSpecialChars_commitsAndReadsBack() throws IOException {
    Path outputPath = new Path("gs", TEST_BUCKET, "/Warehouse_Mixed/Out%#?:+ Dir ü表/tbl");
    JobContext jobContext = newJobContext(baseConf, 40);
    TaskAttemptContext taskContext = newTaskContext(baseConf, 40, 0, 0);
    GcsFileOutputCommitter jobCommitter = new GcsFileOutputCommitter(outputPath, jobContext);
    GcsFileOutputCommitter taskCommitter = new GcsFileOutputCommitter(outputPath, taskContext);
    assertThat(jobCommitter.useGcsCommitter()).isTrue();
    assertThat(taskCommitter.useGcsCommitter()).isTrue();

    countingGcs.resetCounts();
    jobCommitter.setupJob(jobContext);
    assertThat(countingGcs.totalCalls()).isEqualTo(0);

    stageFile(taskCommitter, "part-00000.parquet", "special-output-path");
    countingGcs.resetCounts();
    taskCommitter.commitTask(taskContext);
    assertThat(countingGcs.listCalls.get()).isEqualTo(1);
    assertThat(countingGcs.moveCalls.get()).isEqualTo(1);
    assertThat(countingGcs.movedObjectCount.get()).isEqualTo(1);
    assertThat(countingGcs.totalCalls()).isEqualTo(2);
    assertThat(countingGcs.lastMoveMap.values())
        .containsExactly(
            new StorageResourceId(
                TEST_BUCKET, "Warehouse_Mixed/Out%#?:+ Dir ü表/tbl/part-00000.parquet"));

    countingGcs.resetCounts();
    jobCommitter.commitJob(jobContext);
    assertThat(countingGcs.deleteCalls.get()).isEqualTo(0);

    assertThat(readFile(new Path(outputPath, new Path(null, null, "part-00000.parquet")), baseConf))
        .isEqualTo("special-output-path");
    assertThat(
            countingGcs
                .getItemInfo(
                    new StorageResourceId(
                        TEST_BUCKET, "Warehouse_Mixed/Out%#?:+ Dir ü表/tbl/_SUCCESS"))
                .exists())
        .isTrue();
  }

  @Test
  public void constructors_nullOutputPath_actAsNoOp() throws IOException {
    JobContext jobContext = newJobContext(baseConf, 28);
    TaskAttemptContext taskContext = newTaskContext(baseConf, 28, 0, 0);

    countingGcs.resetCounts();
    GcsFileOutputCommitter taskCommitter =
        new GcsFileOutputCommitter(/* outputPath= */ null, taskContext);
    GcsFileOutputCommitter jobCommitter =
        new GcsFileOutputCommitter(/* outputPath= */ null, jobContext);
    assertThat(countingGcs.isHnBucketCalls.get()).isEqualTo(0);
    assertThat(countingGcs.totalCalls()).isEqualTo(0);

    for (GcsFileOutputCommitter committer :
        new GcsFileOutputCommitter[] {taskCommitter, jobCommitter}) {
      assertThat(committer.useGcsCommitter()).isFalse();
      assertThat(committer.hasOutputPath()).isFalse();
      assertThat(committer.getWorkPath()).isNull();
      committer.setupJob(jobContext);
      committer.setupTask(taskContext);
      assertThat(committer.needsTaskCommit(taskContext)).isFalse();
      committer.commitTask(taskContext);
      committer.abortTask(taskContext);
      committer.commitJob(jobContext);
      committer.abortJob(jobContext, JobStatus.State.FAILED);
    }
    assertThat(countingGcs.totalCalls()).isEqualTo(0);
  }

  @Test
  public void createOutputCommitter_factoryBindingsAndPreconditions_workAndFallBack()
      throws IOException {
    Configuration schemeConf = new Configuration(baseConf);
    schemeConf.set(
        PathOutputCommitterFactory.COMMITTER_FACTORY_SCHEME + ".gs",
        GcsFileOutputCommitterFactory.NAME);

    Path outputPath = new Path(BUCKET_URI + "/warehouse/factory_test");
    TaskAttemptContext taskContext = newTaskContext(schemeConf, 29, 0, 0);
    PathOutputCommitter committer =
        PathOutputCommitterFactory.createCommitter(outputPath, taskContext);
    assertThat(committer).isInstanceOf(GcsFileOutputCommitter.class);

    GcsFileOutputCommitter gcsCommitter = (GcsFileOutputCommitter) committer;
    assertThat(gcsCommitter.useGcsCommitter()).isTrue();
    stageFile(gcsCommitter, "part-00000.parquet", "via-factory");
    countingGcs.resetCounts();
    assertThat(gcsCommitter.needsTaskCommit(taskContext)).isTrue();
    gcsCommitter.commitTask(taskContext);
    assertThat(countingGcs.listCalls.get()).isEqualTo(1);
    assertThat(countingGcs.moveCalls.get()).isEqualTo(1);
    assertThat(countingGcs.totalCalls()).isEqualTo(2);
    assertThat(readFile(new Path(outputPath, "part-00000.parquet"), schemeConf))
        .isEqualTo("via-factory");

    // Global factory key overrides scheme.gs binding.
    Configuration globalConf = new Configuration(schemeConf);
    globalConf.set(
        PathOutputCommitterFactory.COMMITTER_FACTORY_CLASS,
        PathOutputCommitterFactory.COMMITTER_FACTORY_DEFAULT);
    PathOutputCommitter globalCommitter =
        PathOutputCommitterFactory.createCommitter(
            outputPath, newTaskContext(globalConf, 30, 0, 0));
    assertThat(globalCommitter).isInstanceOf(FileOutputCommitter.class);
    assertThat(globalCommitter).isNotInstanceOf(GcsFileOutputCommitter.class);

    // Local FS and null outputPath fall back to standard FileOutputCommitter.
    Configuration factoryConf = new Configuration();
    factoryConf.set(
        PathOutputCommitterFactory.COMMITTER_FACTORY_CLASS, GcsFileOutputCommitterFactory.NAME);
    Path localOutput = new Path(tempFolder.newFolder("factory-local").toURI());
    TaskAttemptContext localContext = newTaskContext(factoryConf, 31, 0, 0);
    PathOutputCommitter localCommitter =
        PathOutputCommitterFactory.createCommitter(localOutput, localContext);
    assertThat(localCommitter).isInstanceOf(FileOutputCommitter.class);
    assertThat(localCommitter).isNotInstanceOf(GcsFileOutputCommitter.class);

    PathOutputCommitter nullCommitter =
        new GcsFileOutputCommitterFactory()
            .createOutputCommitter(/* outputPath= */ null, newTaskContext(schemeConf, 32, 0, 0));
    assertThat(nullCommitter).isInstanceOf(FileOutputCommitter.class);
    assertThat(nullCommitter).isNotInstanceOf(GcsFileOutputCommitter.class);

    NullPointerException npe =
        assertThrows(
            NullPointerException.class,
            () -> new GcsFileOutputCommitterFactory().createOutputCommitter(outputPath, null));
    assertThat(npe).hasMessageThat().contains("context must not be null");
  }

  /**
   * Test {@link GoogleHadoopFileSystem} wired to a per-instance {@link
   * GoogleCloudStorageFileSystem} supplier backed by the test's shared storage.
   */
  public static final class TestGoogleHadoopFileSystem extends GoogleHadoopFileSystem {
    private static volatile Supplier<GoogleCloudStorageFileSystem> gcsFsSupplier;

    static void setGcsFsSupplier(Supplier<GoogleCloudStorageFileSystem> supplier) {
      gcsFsSupplier = supplier;
    }

    public TestGoogleHadoopFileSystem() {
      super(gcsFsSupplier.get());
    }
  }

  /**
   * Counting wrapper around {@link InMemoryGoogleCloudStorage} that tracks GCS calls, simulates
   * sparse field masks and generation preconditions, and models native HNS folders when {@code
   * hnBucket} is {@code true}.
   */
  private static final class CountingGoogleCloudStorage extends ForwardingGoogleCloudStorage {
    final AtomicInteger listCalls = new AtomicInteger();
    final AtomicInteger moveCalls = new AtomicInteger();
    final AtomicInteger movedObjectCount = new AtomicInteger();
    final AtomicInteger deleteCalls = new AtomicInteger();
    final AtomicInteger createEmptyCalls = new AtomicInteger();
    final AtomicInteger isHnBucketCalls = new AtomicInteger();
    final AtomicInteger createFolderCalls = new AtomicInteger();
    final AtomicInteger otherCalls = new AtomicInteger();
    final Set<String> hnFolders = Collections.synchronizedSet(new TreeSet<>());
    @Nullable volatile ListObjectOptions lastListOptions;
    @Nullable volatile Map<StorageResourceId, StorageResourceId> lastMoveMap;
    @Nullable volatile List<GoogleCloudStorageItemInfo> stubbedListResult;
    @Nullable volatile IOException isHnBucketError;
    @Nullable volatile IOException listError;
    @Nullable volatile IOException moveError;
    private final boolean hnBucket;

    CountingGoogleCloudStorage(GoogleCloudStorage delegate, boolean hnBucket) {
      super(delegate);
      this.hnBucket = hnBucket;
    }

    void resetCounts() {
      listCalls.set(0);
      moveCalls.set(0);
      movedObjectCount.set(0);
      deleteCalls.set(0);
      createEmptyCalls.set(0);
      isHnBucketCalls.set(0);
      createFolderCalls.set(0);
      otherCalls.set(0);
      lastListOptions = null;
      lastMoveMap = null;
    }

    int totalCalls() {
      return listCalls.get()
          + moveCalls.get()
          + deleteCalls.get()
          + createEmptyCalls.get()
          + isHnBucketCalls.get()
          + createFolderCalls.get()
          + otherCalls.get();
    }

    List<String> foldersUnder(String prefix) {
      synchronized (hnFolders) {
        List<String> matches = new ArrayList<>();
        for (String folder : hnFolders) {
          if (folder.startsWith(prefix)) {
            matches.add(folder);
          }
        }
        return matches;
      }
    }

    private void recordParentFolders(String objectName) {
      if (!hnBucket || isNullOrEmpty(objectName)) {
        return;
      }
      int idx = 0;
      while ((idx = objectName.indexOf('/', idx)) >= 0) {
        hnFolders.add(objectName.substring(0, idx + 1));
        idx++;
      }
    }

    @Override
    public WritableByteChannel create(StorageResourceId resourceId, CreateObjectOptions options)
        throws IOException {
      otherCalls.incrementAndGet();
      WritableByteChannel channel = super.create(resourceId, options);
      AtomicBoolean closed = new AtomicBoolean();
      return new WritableByteChannel() {
        @Override
        public int write(ByteBuffer src) throws IOException {
          return channel.write(src);
        }

        @Override
        public boolean isOpen() {
          return channel.isOpen();
        }

        @Override
        public void close() throws IOException {
          if (closed.compareAndSet(false, true)) {
            channel.close();
            recordParentFolders(resourceId.getObjectName());
          }
        }
      };
    }

    @Override
    public SeekableByteChannel open(
        StorageResourceId resourceId, GoogleCloudStorageReadOptions readOptions)
        throws IOException {
      otherCalls.incrementAndGet();
      return super.open(resourceId, readOptions);
    }

    @Override
    public SeekableByteChannel open(
        GoogleCloudStorageItemInfo itemInfo, GoogleCloudStorageReadOptions readOptions)
        throws IOException {
      otherCalls.incrementAndGet();
      return super.open(itemInfo, readOptions);
    }

    @Override
    public List<GoogleCloudStorageItemInfo> listObjectInfo(
        String bucketName, String objectNamePrefix, ListObjectOptions listOptions)
        throws IOException {
      listCalls.incrementAndGet();
      lastListOptions = listOptions;
      if (listError != null) {
        throw listError;
      }
      List<GoogleCloudStorageItemInfo> stubbed = stubbedListResult;
      if (stubbed != null) {
        stubbedListResult = null;
        return stubbed;
      }
      List<GoogleCloudStorageItemInfo> all =
          super.listObjectInfo(bucketName, objectNamePrefix, listOptions);
      if (!"bucket,name".equals(listOptions.getFields())) {
        return all;
      }
      List<GoogleCloudStorageItemInfo> projected = new ArrayList<>(all.size());
      for (GoogleCloudStorageItemInfo item : all) {
        projected.add(
            GoogleCloudStorageItemInfo.createObject(
                new StorageResourceId(item.getBucketName(), item.getObjectName()),
                /* creationTime= */ 0L,
                /* modificationTime= */ 0L,
                /* size= */ 0L,
                /* contentType= */ null,
                /* contentEncoding= */ null,
                /* metadata= */ null,
                /* contentGeneration= */ 0L,
                /* metaGeneration= */ 0L,
                /* verificationAttributes= */ null));
      }
      return projected;
    }

    @Override
    public void move(Map<StorageResourceId, StorageResourceId> sourceToDestinationObjectsMap)
        throws IOException {
      moveCalls.incrementAndGet();
      if (moveError != null) {
        throw moveError;
      }
      for (StorageResourceId src : sourceToDestinationObjectsMap.keySet()) {
        if (src.hasGenerationId()) {
          GoogleCloudStorageItemInfo current =
              super.getItemInfo(new StorageResourceId(src.getBucketName(), src.getObjectName()));
          if (!current.exists() || current.getContentGeneration() != src.getGenerationId()) {
            throw new IOException(
                String.format(
                    "412 Precondition Failed for %s: expected gen=%d, actual=%d",
                    src, src.getGenerationId(), current.getContentGeneration()));
          }
        }
      }
      movedObjectCount.addAndGet(sourceToDestinationObjectsMap.size());
      lastMoveMap = new LinkedHashMap<>(sourceToDestinationObjectsMap);
      super.move(sourceToDestinationObjectsMap);
      for (StorageResourceId dst : sourceToDestinationObjectsMap.values()) {
        recordParentFolders(dst.getObjectName());
      }
    }

    @Override
    public void copy(Map<StorageResourceId, StorageResourceId> sourceToDestinationObjectsMap)
        throws IOException {
      otherCalls.incrementAndGet();
      super.copy(sourceToDestinationObjectsMap);
    }

    @Override
    public void copy(
        String srcBucketName,
        List<String> srcObjectNames,
        String dstBucketName,
        List<String> dstObjectNames)
        throws IOException {
      otherCalls.incrementAndGet();
      super.copy(srcBucketName, srcObjectNames, dstBucketName, dstObjectNames);
    }

    @Override
    public void deleteObjects(List<StorageResourceId> fullObjectNames) throws IOException {
      deleteCalls.incrementAndGet();
      super.deleteObjects(fullObjectNames);
    }

    @Override
    public void createEmptyObject(StorageResourceId resourceId) throws IOException {
      createEmptyCalls.incrementAndGet();
      super.createEmptyObject(resourceId);
      recordParentFolders(resourceId.getObjectName());
    }

    @Override
    public void createEmptyObject(StorageResourceId resourceId, CreateObjectOptions options)
        throws IOException {
      createEmptyCalls.incrementAndGet();
      super.createEmptyObject(resourceId, options);
      recordParentFolders(resourceId.getObjectName());
    }

    @Override
    public void createEmptyObjects(List<StorageResourceId> resourceIds) throws IOException {
      createEmptyCalls.incrementAndGet();
      super.createEmptyObjects(resourceIds);
      for (StorageResourceId id : resourceIds) {
        recordParentFolders(id.getObjectName());
      }
    }

    @Override
    public void createEmptyObjects(List<StorageResourceId> resourceIds, CreateObjectOptions options)
        throws IOException {
      createEmptyCalls.incrementAndGet();
      super.createEmptyObjects(resourceIds, options);
      for (StorageResourceId id : resourceIds) {
        recordParentFolders(id.getObjectName());
      }
    }

    @Override
    public GoogleCloudStorageItemInfo getItemInfo(StorageResourceId resourceId) throws IOException {
      otherCalls.incrementAndGet();
      return super.getItemInfo(resourceId);
    }

    @Override
    public List<GoogleCloudStorageItemInfo> getItemInfos(List<StorageResourceId> resourceIds)
        throws IOException {
      otherCalls.incrementAndGet();
      return super.getItemInfos(resourceIds);
    }

    @Override
    public boolean isHnBucket(URI src) throws IOException {
      isHnBucketCalls.incrementAndGet();
      if (isHnBucketError != null) {
        throw isHnBucketError;
      }
      return hnBucket;
    }

    @Override
    public GoogleCloudStorageItemInfo getFolderInfo(StorageResourceId resourceId)
        throws IOException {
      otherCalls.incrementAndGet();
      if (hnBucket && hnFolders.contains(resourceId.toDirectoryId().getObjectName())) {
        return GoogleCloudStorageItemInfo.createInferredDirectory(resourceId.toDirectoryId());
      }
      return GoogleCloudStorageItemInfo.createNotFound(resourceId);
    }

    @Override
    public void createFolder(StorageResourceId resourceId, boolean recursive) throws IOException {
      createFolderCalls.incrementAndGet();
      if (hnBucket) {
        String folder = resourceId.toDirectoryId().getObjectName();
        hnFolders.add(folder);
        if (recursive) {
          recordParentFolders(folder);
        }
      }
    }

    @Override
    public ListPage<FolderInfo> listFolderInfoForPrefixPage(
        String bucketName,
        String folderNamePrefix,
        ListFolderOptions listFolderOptions,
        String pageToken)
        throws IOException {
      otherCalls.incrementAndGet();
      if (!hnBucket) {
        return super.listFolderInfoForPrefixPage(
            bucketName, folderNamePrefix, listFolderOptions, pageToken);
      }
      List<FolderInfo> result = new ArrayList<>();
      for (String folder : foldersUnder(folderNamePrefix)) {
        result.add(new FolderInfo(FolderInfo.createFolderInfoObject(bucketName, folder)));
      }
      return new ListPage<>(result, /* nextPageToken= */ null);
    }

    @Override
    public void deleteFolders(List<FolderInfo> folders) throws IOException {
      otherCalls.incrementAndGet();
      if (hnBucket) {
        for (FolderInfo folder : folders) {
          hnFolders.remove(folder.getFolderName());
        }
        return;
      }
      super.deleteFolders(folders);
    }
  }
}
