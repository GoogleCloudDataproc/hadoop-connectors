package com.google.cloud.hadoop.fs.gcs.benchmarking;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import com.google.cloud.hadoop.fs.gcs.benchmarking.jmh.benchmarks.GCSDeleteBenchmark;
import com.google.cloud.hadoop.fs.gcs.benchmarking.jmh.benchmarks.GCSListStatusBenchmark;
import com.google.cloud.hadoop.fs.gcs.benchmarking.jmh.benchmarks.GCSRenameBenchmark;
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
  private void runJMhBenchmarkAndLog(String operationName, BenchmarkAction action) throws IOException {
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
    runJMhBenchmarkAndLog("LISTSTATUS", () -> GCSListStatusBenchmark.runBenchmark(hadoopPath));
    logger.atInfo().log("Benchmark complete. Now performing the actual 'listStatus' operation...");
    return super.listStatus(hadoopPath); // Run actual listStatus Operation after benchmarking it.
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    runJMhBenchmarkAndLog("RENAME", () -> GCSRenameBenchmark.runBenchmark(src, dst));
    logger.atInfo().log("Benchmark complete. Now performing the actual 'rename' operation...");
    return super.rename(src, dst);
  }

  @Override
  public boolean delete(Path hadoopPath, boolean recursive) throws IOException {
    // Run the single-shot benchmark, which will delete the actual file/folder.
    runJMhBenchmarkAndLog("DELETE", () -> GCSDeleteBenchmark.runBenchmark(hadoopPath, recursive));

    // IMPORTANT: Because the benchmark itself performs the final delete, we DO NOT call
    // super.delete() here. We simply return true to indicate the user's command succeeded.
    logger.atInfo().log(
            "Benchmark complete. The path '%s' was deleted as part of the benchmark run.", hadoopPath);
    return true;
  }
}