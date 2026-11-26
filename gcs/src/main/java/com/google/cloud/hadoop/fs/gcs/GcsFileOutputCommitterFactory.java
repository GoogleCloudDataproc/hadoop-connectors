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

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import javax.annotation.Nullable;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitterFactory;

/**
 * {@link PathOutputCommitterFactory} that creates {@link GcsFileOutputCommitter} for GCS paths and
 * delegates to standard {@link FileOutputCommitter} for non-GCS filesystems (e.g., {@code hdfs://},
 * {@code file://}) or {@code null} output paths.
 *
 * <p>Configure via Hadoop's scheme-scoped committer factory property:
 *
 * <ul>
 *   <li>{@code mapreduce.outputcommitter.factory.scheme.gs =
 *       com.google.cloud.hadoop.fs.gcs.GcsFileOutputCommitterFactory} (covers {@link
 *       org.apache.hadoop.mapreduce.lib.output.FileOutputFormat} callers such as Spark
 *       SQL/DataFrame ORC, CSV, JSON, text, Avro, and Hive SQL writes, and overrides Hadoop
 *       3.3.5+'s default {@code gs} binding to {@code ManifestCommitterFactory}). Note that global
 *       {@code mapreduce.outputcommitter.factory.class} takes precedence over {@code scheme.*} if
 *       set.
 *   <li>For Spark Parquet, set {@code spark.sql.parquet.output.committer.class =
 *       com.google.cloud.hadoop.fs.gcs.GcsFileOutputCommitter} together with {@code
 *       spark.hadoop.mapreduce.outputcommitter.factory.scheme.gs} so DSv1/DSv2 driver {@code
 *       setupJob} and executor {@code commitTask} stay aligned. Legacy {@code mapred} writes and
 *       direct {@code ParquetOutputFormat} ({@code saveAsNewAPIHadoopFile}) bypass {@link
 *       PathOutputCommitterFactory}.
 * </ul>
 *
 * <p>Do not enable globally for classic MapReduce jobs with reducers, because when active, {@link
 * GcsFileOutputCommitter#needsTaskCommit(TaskAttemptContext)} returns {@code true} locally without
 * checking staged files, so every map task goes through the task-commit handshake.
 *
 * <p>Requires {@code mapreduce.fileoutputcommitter.algorithm.version=2} (Spark defaults to {@code
 * 1}), {@code mapreduce.fileoutputcommitter.marksuccessfuljobs=true}, {@code
 * mapreduce.fileoutputcommitter.concurrent.write.enabled=false}, {@code
 * fs.gs.operation.move.enable=true}, and a flat (non-HNS) bucket; otherwise {@link
 * GcsFileOutputCommitter} delegates to {@link FileOutputCommitter}.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class GcsFileOutputCommitterFactory extends PathOutputCommitterFactory {

  /** Class name of this factory for Hadoop/Spark configuration binding. */
  public static final String NAME = GcsFileOutputCommitterFactory.class.getName();

  @Override
  public PathOutputCommitter createOutputCommitter(
      @Nullable Path outputPath, TaskAttemptContext context) throws IOException {
    checkNotNull(context, "context must not be null");
    if (outputPath != null
        && outputPath.getFileSystem(context.getConfiguration()) instanceof GoogleHadoopFileSystem) {
      return new GcsFileOutputCommitter(outputPath, context);
    }
    return createFileOutputCommitter(outputPath, context);
  }
}
