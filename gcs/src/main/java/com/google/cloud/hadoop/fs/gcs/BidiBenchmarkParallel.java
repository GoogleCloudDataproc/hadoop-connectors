package com.google.cloud.hadoop.fs.gcs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileRange;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * A benchmark that performs parallel vectored reads using 16 concurrent threads
 * on distinct files (1.parquet to 16.parquet) using a SINGLE FileSystem object
 * to test the shared thread pool limits.
 */
public class BidiBenchmarkParallel extends Configured implements Tool {

  private static final long ROW_GROUP_SIZE = 512L * 1024 * 1024; // 512 MB
  private static final int NUM_RANGES = 10;
  private static final int SMALL_READ_SIZE_MIN = 10 * 1024;
  private static final int SMALL_READ_SIZE_MAX = 90 * 1024;
  private static final int MEDIUM_READ_SIZE_MIN = 1 * 1024 * 1024;
  private static final int MEDIUM_READ_SIZE_MAX = 5 * 1024 * 1024;

  // Number of concurrent files/threads to process
  private static final int PARALLEL_FILES = 16;

  private static final String DEFAULT_FILE_URI = "gs://hadoop-benchmark-regional/";

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new Configuration(), new BidiBenchmark(), args);
    System.exit(exitCode);
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    String inputUri = (args.length > 0) ? args[0] : DEFAULT_FILE_URI;

    // --- Benchmark Configuration ---
    int iterations = conf.getInt("benchmark.iterations", 100);
    boolean initFsPerIteration = conf.getBoolean("benchmark.fs.init.per.iteration", true);
    String label = conf.get("benchmark.label", "Parallel_Scenario");
    String csvPath = conf.get("benchmark.output.csv", "benchmark_results.csv");

    // CRITICAL: Set the thread pool size to 128 as per requirement
    // This limit applies to the shared FileSystem object.
    conf.setInt("fs.gs.vectored.read.threads", 128);

    Path inputPath = new Path(inputUri);
    // Ensure we have the directory path to append 1.parquet...
    Path parentDir = inputPath.getName().endsWith(".parquet") ? inputPath.getParent() : inputPath;

    System.out.println("==================================================");
    System.out.println("Scenario: " + label);
    System.out.println("Base Directory: " + parentDir);
    System.out.println("Concurrency: " + PARALLEL_FILES + " threads (Files 1.parquet - 16.parquet)");
    System.out.println("Init FS per Iteration: " + initFsPerIteration);
    System.out.println("Vectored Read Threads: " + conf.get("fs.gs.vectored.read.threads"));
    System.out.println("Iterations: " + iterations);
    System.out.println("==================================================");

    // Get file size once from "1.parquet" (assuming all 16 files are same size ~5GB)
    long sampleFileSize;
    Path sampleFile = new Path(parentDir, "1.parquet");
    try (FileSystem tempFs = FileSystem.newInstance(sampleFile.toUri(), conf)) {
      if (!tempFs.exists(sampleFile)) {
        System.err.println("WARNING: Sample file " + sampleFile + " not found. Ensure dataset is set up.");
      }
      sampleFileSize = tempFs.getFileStatus(sampleFile).getLen();
    }
    System.out.println("Sample File Size: " + sampleFileSize + " bytes");

    // --- Metrics Containers ---
    List<Double> allOpenLatencies = Collections.synchronizedList(new ArrayList<>());
    List<Double> allReadLatencies = Collections.synchronizedList(new ArrayList<>());
    List<Double> allTotalLatencies = Collections.synchronizedList(new ArrayList<>());

    // Executor for the 16 client threads simulating parallel readers
    ExecutorService clientExecutor = Executors.newFixedThreadPool(PARALLEL_FILES);

    // Pre-initialize shared FS if we are NOT initializing per iteration
    FileSystem sharedFs = null;
    if (!initFsPerIteration) {
      System.out.println("Initializing Shared FileSystem...");
      sharedFs = FileSystem.newInstance(parentDir.toUri(), conf);
    }

    try {
      for (int i = 0; i < iterations; i++) {
        // The FS object to be used by ALL 16 threads in this iteration
        FileSystem fs;

        if (initFsPerIteration) {
          // Create a NEW, SINGLE FileSystem instance for this iteration
          fs = FileSystem.newInstance(parentDir.toUri(), conf);
        } else {
          // Reuse the persistent shared instance
          fs = sharedFs;
        }

        // Prepare 16 parallel tasks
        List<Callable<SingleRunMetrics>> tasks = new ArrayList<>();
        for (int fileIdx = 1; fileIdx <= PARALLEL_FILES; fileIdx++) {
          Path filePath = new Path(parentDir, fileIdx + ".parquet");
          // Pass the SAME 'fs' object to all threads
          final FileSystem currentFs = fs;
          tasks.add(() -> processFile(currentFs, filePath, sampleFileSize));
        }

        try {
          // Execute all 16 tasks in parallel and wait for completion
          List<Future<SingleRunMetrics>> results = clientExecutor.invokeAll(tasks);

          // Collect metrics
          for (Future<SingleRunMetrics> future : results) {
            try {
              SingleRunMetrics metrics = future.get();
              allOpenLatencies.add(metrics.openMs);
              allReadLatencies.add(metrics.readMs);
              allTotalLatencies.add(metrics.totalMs);
            } catch (ExecutionException e) {
              System.err.println("Task failed: " + e.getCause().getMessage());
            }
          }

        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException("Benchmark interrupted", e);
        } finally {
          // Close the FS if it was created specifically for this iteration
          if (initFsPerIteration && fs != null) {
            try { fs.close(); } catch (IOException ignored) {}
          }
        }

        if ((i + 1) % 10 == 0) {
          System.out.printf("Progress: %d/%d iterations completed%n", i + 1, iterations);
        }
      }
    } finally {
      clientExecutor.shutdown();
      if (sharedFs != null) try { sharedFs.close(); } catch (IOException ignored) {}
    }

    // Print Summary
    System.out.println("\n--- Summary (Aggregated over all threads) ---");
    Stats totalStats = calculateStats(allTotalLatencies);
    System.out.printf("P50: %.2f | P99: %.2f | Max: %.2f%n", totalStats.p50, totalStats.p99, totalStats.max);

    // Write to CSV
    writeToCsv(csvPath, label, initFsPerIteration, iterations,
        calculateStats(allOpenLatencies),
        calculateStats(allReadLatencies),
        totalStats);

    return 0;
  }

  /** Helper class to hold metrics for a single file operation */
  private static class SingleRunMetrics {
    double openMs;
    double readMs;
    double totalMs;

    SingleRunMetrics(double openMs, double readMs) {
      this.openMs = openMs;
      this.readMs = readMs;
      this.totalMs = openMs + readMs;
    }
  }

  /** The workload executed by each of the 16 threads */
  private SingleRunMetrics processFile(FileSystem fs, Path path, long fileSize) throws IOException {
    // Each thread calculates its own random ranges
    List<FileRange> ranges = calculateRanges(fileSize);
    FSDataInputStream in = null;
    double openMs = 0;
    double readMs = 0;

    try {
      // 1. Open
      long startOpen = System.nanoTime();
      in = fs.open(path);
      long endOpen = System.nanoTime();
      openMs = (endOpen - startOpen) / 1_000_000.0;

      // 2. Read (Vectored)
      long startRead = System.nanoTime();
      in.readVectored(ranges, ByteBuffer::allocate);

      // Wait for completion (simulating consumption)
      for (FileRange range : ranges) {
        range.getData().join();
      }
      long endRead = System.nanoTime();
      readMs = (endRead - startRead) / 1_000_000.0;

    } finally {
      if (in != null) try { in.close(); } catch (IOException ignored) {}
    }

    return new SingleRunMetrics(openMs, readMs);
  }

  // --- Utility Methods (Stats, CSV, Ranges) ---

  private static class Stats {
    double p50, p90, p99, max;
  }

  private Stats calculateStats(List<Double> data) {
    Stats s = new Stats();
    if (data.isEmpty()) return s;
    synchronized (data) {
      Collections.sort(data);
      int n = data.size();
      s.p50 = data.get((int) (n * 0.50));
      s.p90 = data.get((int) (n * 0.90));
      s.p99 = data.get((int) (n * 0.99));
      s.max = data.get(n - 1);
    }
    return s;
  }

  private void writeToCsv(String path, String label, boolean initPerIter, int iterations,
      Stats open, Stats read, Stats total) {
    File file = new File(path);
    boolean writeHeader = !file.exists();

    try (FileWriter fw = new FileWriter(file, true);
        PrintWriter pw = new PrintWriter(fw)) {

      if (writeHeader) {
        pw.println("Timestamp,Label,InitFsPerIter,Iterations,Threads,Open_P50,Open_P99,Read_P50,Read_P99,Total_P50,Total_P90,Total_P99,Max_Total");
      }

      pw.printf("%s,%s,%b,%d,%d,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f%n",
          Instant.now().toString(), label, initPerIter, iterations, PARALLEL_FILES,
          open.p50, open.p99, read.p50, read.p99,
          total.p50, total.p90, total.p99, total.max);

      System.out.println("Results appended to: " + path);
    } catch (IOException e) {
      System.err.println("Failed to write to CSV: " + e.getMessage());
    }
  }

  private List<FileRange> calculateRanges(long fileSize) {
    Random random = new Random();
    List<FileRange> ranges = new ArrayList<>();
    long maxStartIndex = Math.max(0, fileSize - ROW_GROUP_SIZE);
    long blockStart = (long) (random.nextDouble() * (maxStartIndex + 1));
    long blockEnd = blockStart + ROW_GROUP_SIZE;

    int attempts = 0;
    while (ranges.size() < NUM_RANGES && attempts < 1000) {
      attempts++;
      int readLength = random.nextBoolean() ?
          random.nextInt(SMALL_READ_SIZE_MAX - SMALL_READ_SIZE_MIN + 1) + SMALL_READ_SIZE_MIN :
          random.nextInt(MEDIUM_READ_SIZE_MAX - MEDIUM_READ_SIZE_MIN + 1) + MEDIUM_READ_SIZE_MIN;

      long maxOffset = blockEnd - readLength;
      long offset = blockStart + (long) (random.nextDouble() * (maxOffset - blockStart + 1));

      boolean overlaps = false;
      for (FileRange existing : ranges) {
        if (offset < existing.getOffset() + existing.getLength() &&
            existing.getOffset() < offset + readLength) {
          overlaps = true; break;
        }
      }
      if (!overlaps) ranges.add(FileRange.createFileRange(offset, readLength));
    }
    ranges.sort(Comparator.comparingLong(FileRange::getOffset));
    return ranges;
  }
}
