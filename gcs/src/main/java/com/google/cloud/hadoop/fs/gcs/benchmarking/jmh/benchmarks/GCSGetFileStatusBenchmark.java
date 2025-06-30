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
package com.google.cloud.hadoop.fs.gcs.benchmarking.JMHBenchmarks;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import com.google.cloud.hadoop.fs.gcs.benchmarking.jmh.benchmarks.GCSDeleteBenchmark;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
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
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * A JMH benchmark to measure the performance of the {@code getFileStatus} operation on a single,
 * real Google Cloud Storage path provided by the user command.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
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
public class GCSGetFileStatusBenchmark {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  public static final int DEFAULT_WARMUP_ITERATIONS = 5;
  public static final int DEFAULT_MEASUREMENT_ITERATIONS = 10;
  @Param({"_path_not_set_"})
  private String pathString;

  private GoogleHadoopFileSystem ghfs;
  private Path pathToGetStatus;

  /**
   * Sets up the benchmark trial. Initializes the GCS filesystem and verifies the target path
   * exists. It does NOT create any temporary files.
   */
  @Setup(Level.Trial)
  public void setup() throws IOException {
    if ("_path_not_set_".equals(pathString)) {
      throw new IllegalArgumentException(
          "GCS path must be provided via the 'pathString' benchmark parameter.");
    }
    this.pathToGetStatus = new Path(pathString);

    Configuration conf = new Configuration();
    // Set the real GCS filesystem implementation to prevent an infinite recursive loop.
    conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");

    this.ghfs = new GoogleHadoopFileSystem();
    this.ghfs.initialize(this.pathToGetStatus.toUri(), conf);

    // This benchmark operates on the assumption that the source path already exists.
    // We verify it here to fail fast if it doesn't.
    if (!ghfs.exists(pathToGetStatus)) {
      throw new IOException(
          "Benchmark setup failed: The path '" + pathToGetStatus + "' does not exist.");
    }

    logger.atInfo().log("Trial Setup: Ready to get status of '%s'", pathToGetStatus);
  }

  /** Cleans up resources by closing the GCS filesystem client. */
  @TearDown(Level.Trial)
  public void tearDown() throws IOException {
    if (this.ghfs != null) {
      this.ghfs.close();
      logger.atInfo().log("Trial TearDown: Closed GCS filesystem instance.");
    }
  }

  /** The core benchmark. It gets the status of the file and consumes the result. */
  @Benchmark
  public void getFileStatus_operation(Blackhole bh) throws IOException {
    FileStatus fileStatus = ghfs.getFileStatus(pathToGetStatus);
    bh.consume(fileStatus);
  }

  /** Static entry point to run the benchmark from the wrapper class. */
  public static void runBenchmark(Path hadoopPath) throws IOException {
    try {
      int warmupIterations = Integer.getInteger("jmh.warmup.iterations", 5);
      int measurementIterations = Integer.getInteger("jmh.measurement.iterations", 10);
      Options opt =
          new OptionsBuilder()
              .include(GCSGetFileStatusBenchmark.class.getSimpleName() + ".getFileStatus_operation")
              .param("pathString", hadoopPath.toString())
              .warmupIterations(warmupIterations)
              .measurementIterations(measurementIterations)
              .build();

      new Runner(opt).run();

    } catch (RunnerException e) {
      throw new IOException("Failed to run JMH benchmark for getFileStatus", e);
    }
  }
}
