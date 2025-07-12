package com.google.cloud.hadoop.fs.gcs.benchmarking;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import com.google.cloud.hadoop.fs.gcs.benchmarking.jmh.benchmarks.GCSCreateBenchmark;
import com.google.cloud.hadoop.fs.gcs.benchmarking.jmh.benchmarks.GCSDeleteBenchmark;
import com.google.cloud.hadoop.fs.gcs.benchmarking.jmh.benchmarks.GCSGetFileStatusBenchmark;
import com.google.cloud.hadoop.fs.gcs.benchmarking.jmh.benchmarks.GCSListStatusBenchmark;
import com.google.cloud.hadoop.fs.gcs.benchmarking.jmh.benchmarks.GCSMkdirsBenchmark;
import com.google.cloud.hadoop.fs.gcs.benchmarking.jmh.benchmarks.GCSOpenBenchmark;
import com.google.cloud.hadoop.fs.gcs.benchmarking.jmh.benchmarks.GCSRenameBenchmark;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

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
  private void runJMHBenchmarkAndLog(String operationName, BenchmarkAction action)
      throws IOException {
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
    runJMHBenchmarkAndLog("LISTSTATUS", () -> GCSListStatusBenchmark.runBenchmark(hadoopPath));
    logger.atInfo().log("Benchmark complete. Now performing the actual 'listStatus' operation...");

    // Run actual listStatus Operation after benchmarking it.
    return super.listStatus(hadoopPath);
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    runJMHBenchmarkAndLog("RENAME", () -> GCSRenameBenchmark.runBenchmark(src, dst));
    logger.atInfo().log("Benchmark complete. Now performing the actual 'rename' operation...");

    // Run actual rename Operation after benchmarking it.
    return super.rename(src, dst);
  }

  @Override
  public boolean delete(Path hadoopPath, boolean recursive) throws IOException {
    // Run the single-shot benchmark, which will delete the actual file/folder.
    runJMHBenchmarkAndLog("DELETE", () -> GCSDeleteBenchmark.runBenchmark(hadoopPath, recursive));

    // IMPORTANT: Because the benchmark itself performs the final delete, we DO NOT call
    // super.delete() here. We simply return true to indicate the user's command succeeded.
    logger.atInfo().log(
        "Benchmark complete. The path '%s' was deleted as part of the benchmark run.", hadoopPath);
    // This line is only reached if the benchmarked delete operation was successful.
    // A failure would have thrown an IOException, exiting this method prematurely.
    return true;
  }

  @Override
  public FileStatus getFileStatus(Path hadoopPath) throws IOException {
    runJMHBenchmarkAndLog(
        "GETFILESTATUS", () -> GCSGetFileStatusBenchmark.runBenchmark(hadoopPath));
    logger.atInfo().log(
        "Benchmark complete. Now performing the actual 'getFileStatus' operation...");

    // Run actual getFileStatus Operation after benchmarking it.
    return super.getFileStatus(hadoopPath);
  }

  @Override
  public boolean mkdirs(Path hadoopPath, FsPermission permission) throws IOException {
    runJMHBenchmarkAndLog("MKDIRS", () -> GCSMkdirsBenchmark.runBenchmark(hadoopPath, permission));
    logger.atInfo().log("Benchmark complete. Now performing the actual 'mkdirs' operation...");
    // Run actual mkdirs Operation after benchmarking it.
    return super.mkdirs(hadoopPath, permission);
  }

  @Override
  public FSDataOutputStream create(
      Path hadoopPath,
      FsPermission permission,
      boolean overwrite,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress)
      throws IOException {
    runJMHBenchmarkAndLog(
        "CREATE", () -> GCSCreateBenchmark.runBenchmark(hadoopPath, overwrite, permission));
    logger.atInfo().log("Benchmark complete. Now performing the actual Create operation...");
    // Run actual create operation after benchmarking it.
    return super.create(
        hadoopPath, permission, overwrite, bufferSize, replication, blockSize, progress);
  }

  @Override
  public FSDataInputStream open(Path hadoopPath, int bufferSize) throws IOException {
    runJMHBenchmarkAndLog("OPEN", () -> GCSOpenBenchmark.runBenchmark(hadoopPath));
    logger.atInfo().log("Benchmark complete. Now performing the actual Open operation...");
    // Run actual create operation after benchmarking it.
    return super.open(hadoopPath, bufferSize);
  }
}
