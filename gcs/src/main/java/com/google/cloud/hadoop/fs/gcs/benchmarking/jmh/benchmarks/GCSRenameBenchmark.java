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
import org.openjdk.jmh.annotations.OperationsPerInvocation;
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
 * A JMH benchmark to measure the performance of the {@code rename} operation on a Google Cloud
 * Storage path. This benchmark uses an alternating rename strategy (src->dst, dst->src) to allow
 * for multiple iterations while maintaining a consistent state.
 *
 * <p><b>NOTE:</b> This benchmark assumes the source file specified in the command already exists.
 * It will not create a dummy file.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
// Note: Total iterations (warmup + measurement) should be even to ensure the file ends up
// in the original source location before the wrapper class performs the final operation.
@Warmup(iterations = 1, time = 1, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 1, time = 1, timeUnit = TimeUnit.MILLISECONDS)
@Fork(value = 1)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class GCSRenameBenchmark {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  @Param({"_path_not_set_"})
  private String srcPathString;

  @Param({"_path_not_set_"})
  private String dstPathString;

  private GoogleHadoopFileSystem ghfs;
  private Path srcPath;
  private Path dstPath;

  /**
   * Sets up the benchmark trial. This method initializes the GCS filesystem and validates that the
   * source file specified by the user command actually exists. It does NOT create any dummy files.
   */
  @Setup(Level.Trial)
  public void setup() throws IOException {
    if ("_path_not_set_".equals(srcPathString) || "_path_not_set_".equals(dstPathString)) {
      throw new IllegalArgumentException(
          "Source and destination paths must be provided via JMH parameters.");
    }
    this.srcPath = new Path(srcPathString);
    this.dstPath = new Path(dstPathString);

    Configuration conf = new Configuration();
    conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");

    this.ghfs = new GoogleHadoopFileSystem();
    this.ghfs.initialize(this.srcPath.toUri(), conf);

    // This benchmark operates on the assumption that the source file already exists,
    // as provided by the user's 'hadoop fs -mv' command. We verify it here.
    if (!ghfs.exists(srcPath)) {
      throw new IOException("Benchmark setup failed: Source file does not exist at " + srcPath);
    }

    logger.atInfo().log(
        "Benchmark Setup: Verified source %s exists and destination %s is clear.",
        srcPath, dstPath);
  }

  /**
   * Cleans up resources after the trial run by closing the GCS filesystem client. This does NOT
   * delete any files, leaving the state ready for the final operation in the wrapper.
   */
  @TearDown(Level.Trial)
  public void tearDown() throws IOException {
    if (this.ghfs != null) {
      this.ghfs.close();
      logger.atInfo().log("Benchmark TearDown: Closed GCS filesystem instance.");
      this.ghfs = null;
    }
  }

  /**
   * The core benchmark operation. It renames a file from a source to a destination path, and then
   * immediately renames it back. This resets the state, allowing for multiple iterations.
   *
   * @param bh A JMH Blackhole to consume the boolean result, preventing dead-code elimination.
   * @throws IOException if the rename operation fails.
   */
  @Benchmark
  @OperationsPerInvocation(
      2) // This tells JMH this benchmark performs TWO operations. JMH will report the time as the
  // average of these two invocations.
  public void rename_operation(Blackhole bh) throws IOException {
    boolean success1 = ghfs.rename(srcPath, dstPath);
    boolean success2 = ghfs.rename(dstPath, srcPath);
    bh.consume(success1);
    bh.consume(success2);
  }

  /**
   * A static entry point to programmatically run this benchmark.
   *
   * @param srcPath The source GCS path for the rename operation.
   * @param dstPath The destination GCS path for the rename operation.
   * @throws IOException if the benchmark runner fails to execute.
   */
  public static void runBenchmark(Path srcPath, Path dstPath) throws IOException {

    try {
      int warmupIterations = Integer.getInteger("jmh.warmup.iterations", 1);
      int measurementIterations = Integer.getInteger("jmh.measurement.iterations", 1);
      Options opt =
          new OptionsBuilder()
              .include(GCSRenameBenchmark.class.getSimpleName() + ".rename_operation")
              .param("srcPathString", srcPath.toString())
              .param("dstPathString", dstPath.toString())
              .warmupIterations(warmupIterations)
              .measurementIterations(measurementIterations)
              .build();

      new Runner(opt).run();

    } catch (RunnerException e) {
      throw new IOException("Failed to run JMH benchmark for rename", e);
    }
  }
}
