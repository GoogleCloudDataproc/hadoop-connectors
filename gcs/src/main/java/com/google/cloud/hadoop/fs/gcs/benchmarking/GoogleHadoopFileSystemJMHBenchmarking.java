package com.google.cloud.hadoop.fs.gcs.benchmarking;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import java.io.IOException;
/**
 * A wrapper around {@link GoogleHadoopFileSystem} that intercepts Hadoop FS commands and routes
 * them to the appropriate JMH benchmarks.
 */
public class GoogleHadoopFileSystemJMHBenchmarking extends GoogleHadoopFileSystem {

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
        String startMessage = String.format("JMH BENCHMARK TRIGGERED FOR %s OPERATION!", operationName);
        String endMessage = String.format("JMH BENCHMARK FINISHED FOR %s.", operationName);

        System.out.println("======================================================");
        System.out.println(String.format("  %-50s", startMessage));
        System.out.println("======================================================");

        try {
            action.run();
        } catch (Exception e) {
            System.err.println(
                    "JMH benchmark failed to run for " + operationName + ": " + e.getMessage());
            throw new IOException("Failed to run JMH benchmark for " + operationName, e);
        } finally {
            System.out.println("======================================================");
            System.out.println(String.format("  %-50s", endMessage));
            System.out.println("======================================================");
        }
    }

}