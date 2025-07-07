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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * A JMH benchmark for a single, direct {@code delete} operation. This uses SingleShotTime to
 * measure the end-to-end latency of deleting the actual file or folder provided by the user
 * command.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SingleShotTime)
@Warmup(
    iterations = GCSDeleteBenchmark.DEFAULT_WARMUP_ITERATIONS,
    time = 1,
    timeUnit = TimeUnit.MILLISECONDS)
@Measurement(
    iterations = GCSDeleteBenchmark.DEFAULT_MEASUREMENT_ITERATIONS,
    time = 1,
    timeUnit = TimeUnit.MILLISECONDS)
@Fork(value = 1)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class GCSDeleteBenchmark {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  public static final int DEFAULT_WARMUP_ITERATIONS = 0;
  public static final int DEFAULT_MEASUREMENT_ITERATIONS = 1;

  @Param({"_path_not_set_"})
  private String pathString;

  @Param({"false"})
  private boolean recursive;

  private GoogleHadoopFileSystem ghfs;
  private Path pathToDelete;

  /**
   * Sets up the benchmark trial. Initializes the GCS client and verifies the target path exists.
   */
  @Setup(Level.Trial)
  public void setup() throws IOException {
    if ("_path_not_set_".equals(pathString)) {
      throw new IllegalArgumentException(
          "File path must be provided via JMH parameter 'pathString'.");
    }
    this.pathToDelete = new Path(pathString);

    Configuration conf = new Configuration();
    // Set the real GCS filesystem implementation to prevent an infinite recursive loop.
    conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");

    this.ghfs = new GoogleHadoopFileSystem();
    this.ghfs.initialize(pathToDelete.toUri(), conf);

    // Verify the path to be deleted exists before we start the measurement.
    if (!ghfs.exists(pathToDelete)) {
      throw new IOException(
          "Benchmark setup failed: Path to delete does not exist: " + pathToDelete);
    }
    logger.atInfo().log("Trial Setup: Ready to delete '%s'", pathToDelete);
  }

  /** Cleans up resources after the trial run by closing the filesystem client. */
  @TearDown(Level.Trial)
  public void tearDown() throws IOException {
    if (this.ghfs != null) {
      this.ghfs.close();
      logger.atInfo().log("Trial TearDown: Closed GCS filesystem instance.");
      this.ghfs = null;
    }
  }

  /**
   * The core benchmark. It deletes the actual file/folder specified by the user's command.
   *
   * @return The boolean result of the delete operation.
   * @throws IOException if the delete operation fails.
   */
  @Benchmark
  public boolean delete_Operation() throws IOException {
    return ghfs.delete(pathToDelete, recursive);
  }

  /**
   * A static entry point to programmatically run this benchmark.
   *
   * @param hadoopPath The exact GCS path to be deleted.
   * @param recursive The 'recursive' flag from the original command.
   * @throws IOException if the benchmark runner fails to execute.
   */
  public static void runBenchmark(Path hadoopPath, boolean recursive) throws IOException {
    try {
      // This is a SingleShotTime benchmark for a destructive operation (delete),
      // so iterations are fixed to 1 measurement and 0 warmups.
      Options opt =
          new OptionsBuilder()
              .include(GCSDeleteBenchmark.class.getSimpleName() + ".delete_Operation")
              .param("pathString", hadoopPath.toString())
              .param("recursive", String.valueOf(recursive))
              .warmupIterations(DEFAULT_WARMUP_ITERATIONS)
              .measurementIterations(DEFAULT_MEASUREMENT_ITERATIONS)
              .build();

      new Runner(opt).run();

    } catch (RunnerException e) {
      throw new IOException("Failed to run JMH benchmark for direct delete", e);
    }
  }
}
