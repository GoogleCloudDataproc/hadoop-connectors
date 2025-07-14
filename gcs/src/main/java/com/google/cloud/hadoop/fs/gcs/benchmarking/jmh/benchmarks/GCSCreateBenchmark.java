/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.hadoop.fs.gcs.benchmarking.jmh.benchmarks;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import com.google.cloud.hadoop.fs.gcs.benchmarking.util.JMHArgs;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * A JMH benchmark to measure the performance of the {@code create} operation in Google Cloud
 * Storage.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@Warmup(
    iterations = GCSCreateBenchmark.DEFAULT_WARMUP_ITERATIONS,
    time = 1,
    timeUnit = TimeUnit.MILLISECONDS)
@Measurement(
    iterations = GCSCreateBenchmark.DEFAULT_MEASUREMENT_ITERATIONS,
    time = 1,
    timeUnit = TimeUnit.MILLISECONDS)
@Fork(value = 1)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class GCSCreateBenchmark {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  public static final int DEFAULT_WARMUP_ITERATIONS = 5;
  public static final int DEFAULT_MEASUREMENT_ITERATIONS = 10;

  @Param({"_path_not_set_"})
  private String pathString;

  @Param({"false"})
  private boolean overwrite;

  @Param({"755"})
  private String permissionOctalString;

  private GoogleHadoopFileSystem ghfs;
  private Path pathToCreate;
  private FsPermission permission;

  /**
   * Sets up the benchmark environment before the trial run.
   *
   * @throws IOException if the file system cannot be initialized.
   */
  @Setup(Level.Trial)
  public void setup() throws IOException {
    if ("_path_not_set_".equals(pathString)) {
      throw new IllegalArgumentException(
          "GCS path must be provided via the 'pathString' benchmark parameter.");
    }
    // Use the exact path and permissions provided for this run.
    this.pathToCreate = new Path(pathString);
    this.permission = new FsPermission(Short.parseShort(permissionOctalString, 8));

    Configuration conf = new Configuration();
    // Explicitly use the real GoogleHadoopFileSystem to bypass the benchmarking wrapper.
    conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");

    this.ghfs = new GoogleHadoopFileSystem();
    this.ghfs.initialize(this.pathToCreate.toUri(), conf);

    logger.atInfo().log(
        "Benchmark Setup: Ready to create file at %s with overwrite=%b and permissions=%s",
        pathToCreate, overwrite, permission);
  }

  /**
   * Cleans up resources after the trial run.
   *
   * @throws IOException if closing the file system or deleting the file fails.
   */
  @TearDown(Level.Trial)
  public void tearDown() throws IOException {
    if (this.ghfs != null) {
      try {
        if (ghfs.exists(pathToCreate)) {
          ghfs.delete(pathToCreate, false);
        }
      } catch (IOException e) {
        logger.atWarning().withCause(e).log("Failed to delete benchmark file: %s", pathToCreate);
      } finally {
        this.ghfs.close();
        logger.atInfo().log("Benchmark TearDown: Cleaned up and closed GCS filesystem.");
        this.ghfs = null;
      }
    }
  }

  /**
   * The core benchmark operation for the create method.
   *
   * @param bh A JMH Blackhole to consume the output.
   * @throws IOException if the create operation fails.
   */
  @Benchmark
  public void create_Operation(Blackhole bh) throws IOException {
    // A try-with-resources statement ensures the stream is closed, finalizing the file creation.
    try (FSDataOutputStream out =
        ghfs.create(pathToCreate, permission, overwrite, 0, (short) 0, 0L, null)) {
      bh.consume(out);
    }
  }

  /**
   * A static entry point to programmatically run this benchmark with specific parameters.
   *
   * @param hadoopPath The GCS path for creating the test file.
   * @param overwrite The overwrite flag to use for the create operation.
   * @param permission The FsPermission to use for the create operation.
   * @throws IOException if the benchmark runner fails to execute.
   */
  public static void runBenchmark(Path hadoopPath, boolean overwrite, FsPermission permission)
      throws IOException {
    try {
      // Fetch benchmark iteration counts from system properties (e.g., -Djmh.warmup.iterations=7).
      // If the properties are not set, fall back to the default constant values defined for this
      // benchmark.
      int warmupIterations = Integer.getInteger("jmh.warmup.iterations", DEFAULT_WARMUP_ITERATIONS);
      int measurementIterations =
          Integer.getInteger("jmh.measurement.iterations", DEFAULT_MEASUREMENT_ITERATIONS);

      // Append the operation's name to the session name or create a new session name if it does not
      // exist already.
      String[] jvmArgs = JMHArgs.fromEnv(GCSCreateBenchmark.class.getSimpleName());

      Options opt =
          new OptionsBuilder()
              .include(GCSCreateBenchmark.class.getSimpleName() + ".create_Operation")
              .param("pathString", hadoopPath.toString())
              .param("overwrite", String.valueOf(overwrite))
              // Converts permission flags (e.g., rwxrwxrwx) to their octal string representation
              // (e.g., "777")
              .param("permissionOctalString", Integer.toOctalString(permission.toShort()))
              .jvmArgs(jvmArgs)
              .warmupIterations(warmupIterations)
              .measurementIterations(measurementIterations)
              .build();

      new Runner(opt).run();

    } catch (RunnerException e) {
      throw new IOException("Failed to run JMH benchmark for create", e);
    }
  }
}
