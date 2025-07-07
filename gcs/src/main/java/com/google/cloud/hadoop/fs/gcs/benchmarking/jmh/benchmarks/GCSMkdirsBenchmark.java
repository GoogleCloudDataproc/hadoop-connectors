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
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
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
 * A JMH benchmark to measure the performance of the {@code mkdirs} operation on a Google Cloud
 * Storage path. This benchmark creates a directory and cleans it up for consistent measurements.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@Warmup(
    iterations = GCSMkdirsBenchmark.DEFAULT_WARMUP_ITERATIONS,
    time = 1,
    timeUnit = TimeUnit.MILLISECONDS)
@Measurement(
    iterations = GCSMkdirsBenchmark.DEFAULT_MEASUREMENT_ITERATIONS,
    time = 1,
    timeUnit = TimeUnit.MILLISECONDS)
@Fork(value = 1)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class GCSMkdirsBenchmark {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  public static final int DEFAULT_WARMUP_ITERATIONS = 5;
  public static final int DEFAULT_MEASUREMENT_ITERATIONS = 10;

  @Param({"_path_not_set_"})
  private String pathString;

  // The FsPermission for the new directory, represented as an octal string.
  // This is populated by the JMH runner via the OptionsBuilder.
  @Param({"755"})
  private String permissionOctalString;

  private GoogleHadoopFileSystem ghfs;
  private Path pathToCreate;
  private FsPermission permission;

  /**
   * Sets up the benchmark environment before the trial run. This method initializes the GCS file
   * system instance and ensures the target path for mkdirs does not exist. The setup time is not
   * included in the benchmark measurement.
   *
   * @throws IOException if the file system cannot be initialized or if pre-existing path cannot be
   *     deleted.
   */
  @Setup(Level.Trial)
  public void setup() throws IOException {
    if ("_path_not_set_".equals(pathString)) {
      throw new IllegalArgumentException(
          "GCS path must be provided via the 'pathString' benchmark parameter.");
    }
    this.pathToCreate = new Path(pathString);
    // Convert the octal string (eg 777) back to FsPermission (eg rwxrwxrwx)
    this.permission = new FsPermission(Short.parseShort(permissionOctalString, 8));

    Configuration conf = new Configuration();
    // Use the real GoogleHadoopFileSystem implementation for the benchmark.
    conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");

    this.ghfs = new GoogleHadoopFileSystem();

    this.ghfs.initialize(this.pathToCreate.toUri(), conf);

    logger.atInfo().log(
        "Benchmark Setup: Ready to benchmark mkdirs for '%s' with permission '%s'",
        pathToCreate, permission);
  }

  /**
   * Cleans up resources after each benchmark invocation. This method deletes the directory created
   * during the benchmark method, ensuring a clean state for the next invocation. Using
   * Level.Invocation provides the highest isolation for each measurement.
   */
  @TearDown(Level.Iteration)
  public void teardown() throws IOException {
    if (this.ghfs != null && this.pathToCreate != null) {
      try {
        ghfs.delete(pathToCreate, true); // true for recursive delete
        logger.atInfo().log("Benchmark Teardown: Deleted '%s' after invocation", pathToCreate);
      } catch (IOException e) {
        logger.atWarning().withCause(e).log(
            "Error during teardown for '%s': %s", pathToCreate, e.getMessage());
      }
    }
  }

  /**
   * Cleans up resources after the trial run. This method closes the GCS file system instance. The
   * teardown time is not included in the benchmark measurement.
   *
   * @throws IOException if closing the file system fails.
   */
  @TearDown(Level.Trial)
  public void teardownTrial() throws IOException {
    if (this.ghfs != null) {
      this.ghfs.close();
      logger.atInfo().log("Benchmark TearDown (Trial): Closed GCS filesystem instance.");
      this.ghfs = null;
    }
  }

  /**
   * The core benchmark operation. This method calls {@code mkdirs} and consumes the result using a
   * JMH Blackhole to prevent dead-code elimination by the compiler.
   *
   * @param bh A JMH Blackhole to consume the output, ensuring the operation is not optimized away.
   * @throws IOException if the mkdirs operation fails.
   */
  @Benchmark
  public void mkdirs_Operation(Blackhole bh) throws IOException {
    boolean success = ghfs.mkdirs(pathToCreate, permission);
    bh.consume(success);
  }

  /**
   * A static entry point to programmatically run this benchmark.
   *
   * @param hadoopPath The GCS path (e.g., "gs://your-bucket/some-dir/") to be benchmarked.
   * @param permission The FsPermission to use for the mkdirs operation.
   * @throws IOException if the benchmark runner fails to execute.
   */
  public static void runBenchmark(Path hadoopPath, FsPermission permission) throws IOException {
    try {
      // Fetch benchmark iteration counts from system properties (e.g., -Djmh.warmup.iterations=7).
      // If the properties are not set, fall back to the default constant values defined for this
      // benchmark.
      int warmupIterations = Integer.getInteger("jmh.warmup.iterations", DEFAULT_WARMUP_ITERATIONS);
      int measurementIterations =
          Integer.getInteger("jmh.measurement.iterations", DEFAULT_MEASUREMENT_ITERATIONS);
      Options opt =
          new OptionsBuilder()
              .include(GCSMkdirsBenchmark.class.getSimpleName() + ".mkdirs_Operation")
              .param("pathString", hadoopPath.toString())
              // Converts permission flags (e.g., rwxrwxrwx) to their octal string representation
              // (e.g., "777")
              .param("permissionOctalString", Integer.toOctalString(permission.toShort()))
              .warmupIterations(warmupIterations)
              .measurementIterations(measurementIterations)
              .build();

      new Runner(opt).run();

    } catch (RunnerException e) {
      throw new IOException("Failed to run JMH benchmark for mkdirs", e);
    }
  }
}
