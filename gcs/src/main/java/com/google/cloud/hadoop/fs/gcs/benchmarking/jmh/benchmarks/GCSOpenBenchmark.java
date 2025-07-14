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
import org.apache.hadoop.fs.FSDataInputStream;
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
 * A JMH benchmark to measure the performance of the {@code open} operation in Google Cloud Storage.
 *
 * <p>Before running this benchmark, ensure that the GCS path specified by the 'pathString'
 * parameter points to an existing file in your GCS bucket. The benchmark assumes the file is
 * already present and will not create it.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@Warmup(
    iterations = GCSOpenBenchmark.DEFAULT_WARMUP_ITERATIONS,
    time = 1,
    timeUnit = TimeUnit.MILLISECONDS)
@Measurement(
    iterations = GCSOpenBenchmark.DEFAULT_MEASUREMENT_ITERATIONS,
    time = 1,
    timeUnit = TimeUnit.MILLISECONDS)
@Fork(value = 1)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class GCSOpenBenchmark {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  public static final int DEFAULT_WARMUP_ITERATIONS = 5;
  public static final int DEFAULT_MEASUREMENT_ITERATIONS = 10;

  @Param({"_path_not_set_"})
  private String pathString;

  private GoogleHadoopFileSystem ghfs;
  private Path pathToOpen;

  /**
   * Sets up the benchmark environment before the trial run.
   *
   * <p>This method initializes the GoogleHadoopFileSystem. It assumes that the GCS path specified
   * by 'pathString' already exists and points to a file.
   *
   * @throws IOException if the file system cannot be initialized.
   */
  @Setup(Level.Trial)
  public void setup() throws IOException {
    if ("_path_not_set_".equals(pathString)) {
      throw new IllegalArgumentException(
          "GCS path must be provided via the 'pathString' benchmark parameter.");
    }
    this.pathToOpen = new Path(pathString);

    Configuration conf = new Configuration();
    // Explicitly use the real GoogleHadoopFileSystem for the benchmark.
    conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");

    this.ghfs = new GoogleHadoopFileSystem();
    this.ghfs.initialize(this.pathToOpen.toUri(), conf);

    // Verify the file exists before starting the benchmark. This will fail setup if it doesn't.
    if (!ghfs.exists(pathToOpen)) {
      throw new IOException(
          "Test file does not exist at path: "
              + pathToOpen
              + ". Please ensure the file exists before running the benchmark.");
    }
    logger.atInfo().log(
        "Benchmark Setup: Ready to open existing file at %s for open operation.", pathToOpen);
  }

  /**
   * Cleans up resources after the trial run.
   *
   * <p>This method closes the file system. It does not delete the file, as it was assumed to be
   * pre-existing.
   *
   * @throws IOException if closing the file system fails.
   */
  @TearDown(Level.Trial)
  public void tearDown() throws IOException {
    if (this.ghfs != null) {
      try {
        this.ghfs.close();
        logger.atInfo().log("Benchmark TearDown: Closed GCS filesystem.");
      } catch (IOException e) {
        logger.atWarning().withCause(e).log("Failed to close GCS filesystem.");
      } finally {
        this.ghfs = null;
      }
    }
  }

  /**
   * The core benchmark operation for the open method.
   *
   * <p>This method calls the `open` method on the GoogleHadoopFileSystem and consumes the stream
   * object to ensure it is initialized and ready for consumption, without reading its content.
   *
   * @param bh A JMH Blackhole to consume the output and prevent dead-code elimination.
   * @throws IOException if the open operation fails.
   */
  @Benchmark
  public void open_Operation(Blackhole bh) throws IOException {
    // A try-with-resources statement ensures the input stream is closed after each operation.
    try (FSDataInputStream in = ghfs.open(pathToOpen)) {
      bh.consume(in);
    }
  }

  /**
   * A static entry point to programmatically run this benchmark with specific parameters.
   *
   * @param hadoopPath The GCS path for the file to be opened during the benchmark.
   * @throws IOException if the benchmark runner fails to execute.
   */
  public static void runBenchmark(Path hadoopPath) throws IOException {
    try {
      int warmupIterations = Integer.getInteger("jmh.warmup.iterations", DEFAULT_WARMUP_ITERATIONS);
      int measurementIterations =
          Integer.getInteger("jmh.measurement.iterations", DEFAULT_MEASUREMENT_ITERATIONS);
      Options opt =
          new OptionsBuilder()
              .include(GCSOpenBenchmark.class.getSimpleName() + ".open_Operation")
              .param("pathString", hadoopPath.toString())
              .warmupIterations(warmupIterations)
              .measurementIterations(measurementIterations)
              .build();

      new Runner(opt).run();

    } catch (RunnerException e) {
      throw new IOException("Failed to run JMH benchmark for open", e);
    }
  }

  /**
   * Main method to allow direct execution of the benchmark from the command line.
   *
   * <p>Example usage: `java -jar benchmarks.jar
   * com.google.cloud.hadoop.fs.gcs.benchmarking.jmh.benchmarks.GCSOpenBenchmark -p
   * pathString=gs://your-bucket/existing-file.txt`
   *
   * @param args Command-line arguments. Expected to contain at least the GCS path.
   * @throws IOException if the benchmark runner fails.
   */
  public static void main(String[] args) throws IOException {
    if (args.length < 1) {
      System.err.println(
          "Usage: java " + GCSOpenBenchmark.class.getName() + " <gcs-path-to-existing-file>");
      System.exit(1);
    }
    Path testPath = new Path(args[0]);
    runBenchmark(testPath);
  }
}
