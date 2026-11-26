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

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.nullToEmpty;

import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

/**
 * A {@link FileOutputCommitter} (algorithm version 2) optimized for flat (non-HNS) GCS buckets.
 *
 * <p>When active, {@code commitTask} moves the task's files to the output directory with a flat
 * listing and a batched GCS move instead of recursive renames, {@code setupJob} skips creating the
 * {@code _temporary} directory, and {@code needsTaskCommit} returns {@code true}. All other
 * operations are inherited from {@link FileOutputCommitter}.
 *
 * <p>It falls back to {@link FileOutputCommitter} if the output path is {@code null} or not on GCS,
 * the bucket has hierarchical namespace enabled or its HNS status cannot be read, the algorithm
 * version is not 2, the {@code _SUCCESS} marker is disabled, {@code
 * mapreduce.fileoutputcommitter.concurrent.write.enabled} is {@code true}, or {@code
 * fs.gs.operation.move.enable} is {@code false}.
 *
 * <p>Unlike {@link FileOutputCommitter}, it does not recreate empty directories, and it overwrites
 * only output objects that have the same name as a committed file: it does not delete an existing
 * output directory whose name matches a committed file, or vice versa.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class GcsFileOutputCommitter extends FileOutputCommitter {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();
  // Dataproc Hadoop's concurrent-write mode suppresses _SUCCESS creation.
  @VisibleForTesting
  static final String FILEOUTPUTCOMMITTER_CONCURRENT_WRITE_ENABLED =
      "mapreduce.fileoutputcommitter.concurrent.write.enabled";

  private final boolean useGcsCommitter;

  /** Creates a GCS-optimized output committer for a task attempt. */
  public GcsFileOutputCommitter(@Nullable Path outputPath, TaskAttemptContext context)
      throws IOException {
    super(outputPath, context);
    this.useGcsCommitter = shouldUseGcsCommitter(getOutputPath(), context.getConfiguration());
  }

  /** Creates a GCS-optimized output committer for a job. */
  @InterfaceAudience.Private
  public GcsFileOutputCommitter(@Nullable Path outputPath, JobContext context) throws IOException {
    super(outputPath, context);
    this.useGcsCommitter = shouldUseGcsCommitter(getOutputPath(), context.getConfiguration());
  }

  /**
   * Returns {@code true} if {@code outputPath} is non-null and targets a flat (non-HNS) {@link
   * GoogleHadoopFileSystem} bucket with algorithm version 2, {@code _SUCCESS} marker creation
   * enabled, concurrent write disabled, and GCS move enabled; returns {@code false} to delegate
   * directly to {@link FileOutputCommitter} (including when the HNS bucket check fails with an
   * {@link IOException}).
   */
  private static boolean shouldUseGcsCommitter(@Nullable Path outputPath, Configuration conf)
      throws IOException {
    if (outputPath == null) {
      return false;
    }
    int algorithmVersion =
        conf.getInt(
            FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, FILEOUTPUTCOMMITTER_ALGORITHM_VERSION_DEFAULT);
    // Empty (0-file) writes rely on _SUCCESS to materialize outputPath/ when setupJob skips mkdirs.
    boolean markerEnabled = conf.getBoolean(SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, true);
    boolean concurrentWriteEnabled =
        conf.getBoolean(FILEOUTPUTCOMMITTER_CONCURRENT_WRITE_ENABLED, false);
    boolean moveEnabled =
        GoogleHadoopFileSystemConfiguration.GCS_OPERATION_MOVE_ENABLE.get(conf, conf::getBoolean);
    if (algorithmVersion != 2 || !markerEnabled || concurrentWriteEnabled || !moveEnabled) {
      logger.atFine().log(
          "Falling back to FileOutputCommitter for %s"
              + " (algorithm=%d, marker=%b, concurrentWrite=%b, move=%b)",
          outputPath, algorithmVersion, markerEnabled, concurrentWriteEnabled, moveEnabled);
      return false;
    }
    FileSystem fs = outputPath.getFileSystem(conf);
    if (!(fs instanceof GoogleHadoopFileSystem)) {
      logger.atFine().log(
          "Output path %s uses non-GCS FileSystem %s; falling back to FileOutputCommitter",
          outputPath, fs.getClass().getName());
      return false;
    }
    GoogleHadoopFileSystem ghfs = (GoogleHadoopFileSystem) fs;
    try {
      if (ghfs.getGcsFs().getGcs().isHnBucket(ghfs.getGcsPath(outputPath))) {
        logger.atFine().log(
            "Bucket for %s has hierarchical namespace enabled; falling back to"
                + " FileOutputCommitter",
            outputPath);
        return false;
      }
      return true;
    } catch (IOException e) {
      logger.atWarning().atMostEvery(1, TimeUnit.MINUTES).withCause(e).log(
          "Failed to check HNS status for %s; falling back to FileOutputCommitter", outputPath);
      return false;
    }
  }

  @VisibleForTesting
  boolean useGcsCommitter() {
    return useGcsCommitter;
  }

  @Override
  public void setupJob(JobContext context) throws IOException {
    if (!useGcsCommitter) {
      super.setupJob(context);
      return;
    }
    logger.atFine().log("Skipping setupJob mkdirs for GCS output path %s", getOutputPath());
  }

  @InterfaceAudience.Private
  @Override
  public boolean needsTaskCommit(TaskAttemptContext context, @Nullable Path taskAttemptPath)
      throws IOException {
    return useGcsCommitter || super.needsTaskCommit(context, taskAttemptPath);
  }

  @InterfaceAudience.Private
  @Override
  public void commitTask(TaskAttemptContext context, @Nullable Path taskAttemptPath)
      throws IOException {
    if (!useGcsCommitter) {
      super.commitTask(context, taskAttemptPath);
      return;
    }

    Path outputPath = getOutputPath();
    Path attemptPath = taskAttemptPath != null ? taskAttemptPath : getTaskAttemptPath(context);
    GoogleHadoopFileSystem ghfs =
        (GoogleHadoopFileSystem) outputPath.getFileSystem(context.getConfiguration());
    String outputPrefix = toObjectPrefix(ghfs.getGcsPath(outputPath));
    String attemptPrefix = toObjectPrefix(ghfs.getGcsPath(attemptPath));
    checkState(
        attemptPrefix.startsWith(outputPrefix) && !attemptPrefix.equals(outputPrefix),
        "Attempt prefix '%s' must be a child of output prefix '%s'",
        attemptPrefix,
        outputPrefix);

    ghfs.moveDirectoryContents(attemptPrefix, outputPrefix, context);
    logger.atFine().log("Committed task %s to %s", context.getTaskAttemptID(), outputPath);
  }

  @Override
  public String toString() {
    return String.format(
        "GcsFileOutputCommitter{useGcsCommitter=%s, %s}", useGcsCommitter, super.toString());
  }

  /**
   * Returns the object name prefix of {@code gcsPath} with a trailing slash, or {@code ""} for
   * bucket root.
   */
  private static String toObjectPrefix(URI gcsPath) {
    return nullToEmpty(
        StorageResourceId.fromUriPath(gcsPath, /* allowEmptyObjectName= */ true)
            .toDirectoryId()
            .getObjectName());
  }
}
