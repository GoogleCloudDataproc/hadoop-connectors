/*
 * Copyright 2023 Google LLC
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
 * A JMH benchmark to measure the performance of the {@code listStatus} operation on a Google Cloud
 * Storage path. This benchmark operates on the real contents of the specified directory.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.MILLISECONDS)
@Fork(value = 1)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class GCSListStatusBenchmark {

  // The GCS path to the directory whose contents will be listed.
  // This is populated by the JMH runner via the OptionsBuilder.
  @Param({"_path_not_set_"})
  private String pathString;

  private GoogleHadoopFileSystem ghfs;
  private Path pathToList;

  /**
   * Sets up the benchmark environment before the trial run. This method initializes the GCS file
   * system instance. The setup time is not included in the benchmark measurement.
   *
   * @throws IOException if the file system cannot be initialized.
   */
  @Setup(Level.Trial)
  public void setup() throws IOException {
    if ("_path_not_set_".equals(pathString)) {
      throw new IllegalArgumentException(
          "GCS path must be provided via the 'pathString' benchmark parameter.");
    }
    this.pathToList = new Path(pathString);

    Configuration conf = new Configuration();
    // Use the real GoogleHadoopFileSystem implementation for the benchmark.
    // The wrapper class will have already intercepted this call.
    conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");

    this.ghfs = new GoogleHadoopFileSystem();
    this.ghfs.initialize(this.pathToList.toUri(), conf);

    System.out.println("Benchmark Setup: Ready to list contents of " + pathToList);
  }

  /**
   * Cleans up resources after the trial run. This method closes the GCS file system instance. The
   * teardown time is not included in the benchmark measurement.
   *
   * @throws IOException if closing the file system fails.
   */
  @TearDown(Level.Trial)
  public void tearDown() throws IOException {
    if (this.ghfs != null) {
      this.ghfs.close();
      System.out.println("Benchmark TearDown: Closed GCS filesystem instance.");
    }
  }

  /**
   * The core benchmark operation. This method calls {@code listStatus} and consumes the result
   * using a JMH Blackhole to prevent dead-code elimination by the compiler.
   *
   * @param bh A JMH Blackhole to consume the output, ensuring the operation is not optimized away.
   * @throws IOException if the listStatus operation fails.
   */
  @Benchmark
  public void listStatus_Operation(Blackhole bh) throws IOException {
    FileStatus[] fileStatuses = ghfs.listStatus(pathToList);
    bh.consume(fileStatuses);
  }

  /*
   * A static entry point to programmatically run this benchmark.
   *
   * @param hadoopPath The GCS path (e.g., "gs://your-bucket/some-dir/") to be benchmarked.
   * @throws IOException if the benchmark runner fails to execute.
   */
  public static void runBenchmark(Path hadoopPath) throws IOException {

    try {
      Options opt =
          new OptionsBuilder()
              .include(GCSListStatusBenchmark.class.getSimpleName() + ".listStatus_Operation")
              .param("pathString", hadoopPath.toString())
              .build();

      new Runner(opt).run();

    } catch (RunnerException e) {
      throw new IOException("Failed to run JMH benchmark for listStatus", e);
    }
  }
}
