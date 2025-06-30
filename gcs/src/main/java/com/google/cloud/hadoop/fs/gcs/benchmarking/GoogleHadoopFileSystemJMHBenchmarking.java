package com.google.cloud.hadoop.fs.gcs.benchmarking;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import com.google.cloud.hadoop.fs.gcs.benchmarking.JMHBenchmarks.GCSListStatusBenchmark;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

/**
 * A wrapper around {@link GoogleHadoopFileSystem} that intercepts Hadoop FS commands and routes
 * them to the appropriate JMH benchmarks.
 */
public class GoogleHadoopFileSystemJMHBenchmarking extends GoogleHadoopFileSystem {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  /** A functional interface for a benchmark action that can throw an Exception. */
  @FunctionalInterface
  private interface BenchmarkAction {
    void run() throws Exception;
  }

  /**
   * Generic helper to run a benchmark, printing start/end banners and handling exceptions.
   *
   * @param operationName The name of the operation being benchmarked (e.g., "RENAME").
   * @param action The lambda expression containing the benchmark logic to execute.
   * @throws IOException if the benchmark fails.
   */
  private void runBenchmarkAndLog(String operationName, BenchmarkAction action) throws IOException {
    String banner = "======================================================";
    String startMessage = String.format("JMH BENCHMARK TRIGGERED FOR %s OPERATION!", operationName);
    String endMessage = String.format("JMH BENCHMARK FINISHED FOR %s.", operationName);

    // Use Flogger's formatted logging to avoid compile-time constant errors.
    logger.atInfo().log("%s", banner);
    logger.atInfo().log("  %s", startMessage);
    logger.atInfo().log("%s", banner);

    try {
      action.run();
    } catch (Exception e) {
      logger.atWarning().withCause(e).log("JMH benchmark failed to run for %s", operationName);
      throw new IOException("Failed to run JMH benchmark for " + operationName, e);
    } finally {
      logger.atInfo().log("%s", banner);
      logger.atInfo().log("  %s", endMessage);
      logger.atInfo().log("%s", banner);
    }
  }

  @Override
  public FileStatus[] listStatus(Path hadoopPath) throws IOException {
    runBenchmarkAndLog("LISTSTATUS", () -> GCSListStatusBenchmark.runBenchmark(hadoopPath));
    logger.atInfo().log("Benchmark complete. Now performing the actual 'listStatus' operation...");
    return super.listStatus(hadoopPath); // Run actual listStatus Operation after benchmarking it.
  }
}
