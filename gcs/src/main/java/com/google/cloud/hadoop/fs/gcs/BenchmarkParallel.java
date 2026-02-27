package com.google.cloud.hadoop.fs.gcs;

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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileRange;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * A benchmark that performs parallel vectored reads using 16 concurrent threads. Dumps both summary
 * statistics and raw per-iteration data.
 */
public class BenchmarkParallel extends Configured implements Tool {

  private static final long ROW_GROUP_SIZE = 512L * 1024 * 1024;
  private static final int NUM_RANGES = 10;
  private static final int SMALL_READ_SIZE_MIN = 10 * 1024;
  private static final int SMALL_READ_SIZE_MAX = 90 * 1024;
  private static final int MEDIUM_READ_SIZE_MIN = 1 * 1024 * 1024;
  private static final int MEDIUM_READ_SIZE_MAX = 5 * 1024 * 1024;
  private static final int PARALLEL_FILES = 16;

  private static final String DEFAULT_FILE_URI = "gs://hadoop-benchmark-regional/";

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new Configuration(), new BenchmarkParallel(), args);
    System.exit(exitCode);
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    String inputUri = (args.length > 0) ? args[0] : DEFAULT_FILE_URI;

    int iterations = conf.getInt("benchmark.iterations", 100);
    boolean initFsPerIteration = conf.getBoolean("benchmark.fs.init.per.iteration", false);
    String label = conf.get("benchmark.label", "Parallel_Scenario");
    String summaryCsvPath = conf.get("benchmark.output.csv", "benchmark_summary.csv");
    String rawCsvPath = conf.get("benchmark.output.raw.csv", "benchmark_raw.csv");

    conf.setInt("fs.gs.vectored.read.threads", 128);
    conf.setInt("fs.gs.analytics-core.read.thread.count", 128);

    Path inputPath = new Path(inputUri);
    Path parentDir = inputPath.getName().endsWith(".parquet") ? inputPath.getParent() : inputPath;

    System.out.println("==================================================");
    System.out.println("Scenario: " + label);
    System.out.println("Concurrency: " + PARALLEL_FILES + " threads");
    System.out.println("Init FS per Iteration: " + initFsPerIteration);
    System.out.println("Raw Data Output: " + rawCsvPath);
    System.out.println("==================================================");

    long sampleFileSize;
    Path sampleFile = new Path(parentDir, "1.parquet");
    try (FileSystem tempFs = FileSystem.newInstance(sampleFile.toUri(), conf)) {
      if (!tempFs.exists(sampleFile)) {
        System.err.println("WARNING: Sample file " + sampleFile + " not found.");
      }
      sampleFileSize = tempFs.getFileStatus(sampleFile).getLen();
    }

    List<Double> allOpenLatencies = Collections.synchronizedList(new ArrayList<>());
    List<Double> allReadLatencies = Collections.synchronizedList(new ArrayList<>());
    List<Double> allTotalLatencies = Collections.synchronizedList(new ArrayList<>());
    List<Double> allVectoredReadLatencies = Collections.synchronizedList(new ArrayList<>());

    ExecutorService clientExecutor = Executors.newFixedThreadPool(PARALLEL_FILES);

    FileSystem sharedFs = null;
    if (!initFsPerIteration) {
      sharedFs = FileSystem.newInstance(parentDir.toUri(), conf);
    }

    // Initialize Raw Data Writer
    try (PrintWriter rawWriter = new PrintWriter(new FileWriter(rawCsvPath, true))) {
      if (new File(rawCsvPath).length() == 0) {
        rawWriter.println(
            "Timestamp,Iteration,Thread_Idx,Open_Ms,Read_Ms,Total_Ms,Avg_Vectored_Read_Ms,Read_Type");
      }

      for (int i = 0; i < iterations; i++) {
        FileSystem fs;
        if (initFsPerIteration) {
          fs = FileSystem.newInstance(parentDir.toUri(), conf);
        } else {
          fs = sharedFs;
        }

        List<Callable<SingleRunMetrics>> tasks = new ArrayList<>();
        for (int fileIdx = 1; fileIdx <= PARALLEL_FILES; fileIdx++) {
          Path filePath = new Path(parentDir, fileIdx + ".parquet");
          final FileSystem currentFs = fs;
          tasks.add(() -> processFile(currentFs, filePath, sampleFileSize));
        }

        try {
          List<Future<SingleRunMetrics>> results = clientExecutor.invokeAll(tasks);

          // Dump Raw Data
          long totalVectoredReadDurationNs = 0;
          List<VectoredReadMetric> metrics = new ArrayList<>(VectoredReadMetric.getAll());
          VectoredReadMetric.clear();

          long avgVectoredReadDurationNs = 0;
          String readType = "Unknown";
          if (!metrics.isEmpty()) {
            readType = metrics.get(0).type;
            for (VectoredReadMetric m : metrics) {
              totalVectoredReadDurationNs += m.durationNs;
              allVectoredReadLatencies.add(m.durationNs / 1_000_000.0);
            }
            avgVectoredReadDurationNs = totalVectoredReadDurationNs / metrics.size();
          }

          // We can't easily attribute specific metrics to specific threads in this simple
          // global queue model
          // without more complex correlation ID.
          // So we will just log the AVERAGE duration per operation for this iteration
          // across all threads
          // in a separate column or just append it to the first thread's row?
          // Actually, the user asked for "metrics... to csv".
          // Let's modify the CSV to include "Avg_Vectored_Read_Ms" and "Read_Type" for
          // the iteration.

          int threadIdx = 1;
          for (Future<SingleRunMetrics> future : results) {
            try {
              SingleRunMetrics singleRunMetrics = future.get();

              // Aggregates
              allOpenLatencies.add(singleRunMetrics.openMs);
              allReadLatencies.add(singleRunMetrics.readMs);
              allTotalLatencies.add(singleRunMetrics.totalMs);

              // Raw Dump
              rawWriter.printf(
                  "%s,%d,%d,%.2f,%.2f,%.2f,%.2f,%s%n",
                  Instant.now().toString(),
                  i,
                  threadIdx++,
                  singleRunMetrics.openMs,
                  singleRunMetrics.readMs,
                  singleRunMetrics.totalMs,
                  avgVectoredReadDurationNs / 1_000_000.0, // Convert ns to ms
                  readType);

            } catch (ExecutionException e) {
              System.err.println("Task failed: " + e.getCause().getMessage());
            }
          }
          rawWriter.flush();

        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException("Benchmark interrupted", e);
        } finally {
          if (initFsPerIteration && fs != null) {
            try {
              fs.close();
            } catch (IOException ignored) {
            }
          }
        }

        if ((i + 1) % 10 == 0) {
          System.out.printf("Progress: %d/%d iterations completed%n", i + 1, iterations);
        }
      }
    } finally {
      clientExecutor.shutdown();
      if (sharedFs != null)
        try {
          sharedFs.close();
        } catch (IOException ignored) {
        }
    }

    Stats totalStats = calculateStats(allTotalLatencies);
    System.out.printf(
        "\nSummary - P50: %.2f | P99: %.2f | Max: %.2f%n",
        totalStats.p50, totalStats.p99, totalStats.max);

    writeSummaryCsv(
        summaryCsvPath,
        label,
        initFsPerIteration,
        iterations,
        calculateStats(allOpenLatencies),
        calculateStats(allReadLatencies),
        totalStats,
        calculateStats(allVectoredReadLatencies));

    return 0;
  }

  private static class SingleRunMetrics {
    double openMs, readMs, totalMs;

    SingleRunMetrics(double openMs, double readMs) {
      this.openMs = openMs;
      this.readMs = readMs;
      this.totalMs = openMs + readMs;
    }
  }

  private SingleRunMetrics processFile(FileSystem fs, Path path, long fileSize) throws IOException {
    List<FileRange> ranges = calculateRanges(fileSize);
    FSDataInputStream in = null;
    double openMs = 0;
    double readMs = 0;

    try {
      long startOpen = System.nanoTime();
      in = fs.open(path);
      long endOpen = System.nanoTime();
      openMs = (endOpen - startOpen) / 1_000_000.0;

      long startRead = System.nanoTime();
      in.readVectored(ranges, ByteBuffer::allocate);

      for (FileRange range : ranges) {
        range.getData().join();
      }
      long endRead = System.nanoTime();
      readMs = (endRead - startRead) / 1_000_000.0;

    } finally {
      if (in != null)
        try {
          in.close();
        } catch (IOException ignored) {
        }
    }

    return new SingleRunMetrics(openMs, readMs);
  }

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

  private void writeSummaryCsv(
      String path,
      String label,
      boolean initPerIter,
      int iterations,
      Stats open,
      Stats read,
      Stats total,
      Stats vectoredRead) {
    File file = new File(path);
    boolean writeHeader = !file.exists();

    try (FileWriter fw = new FileWriter(file, true);
        PrintWriter pw = new PrintWriter(fw)) {

      if (writeHeader) {
        pw.println(
            "Timestamp,Label,InitFsPerIter,Iterations,Threads,Open_P50,Open_P99,Read_P50,Read_P99,Total_P50,Total_P90,Total_P99,Max_Total,Vectored_Read_P50,Vectored_Read_P99");
      }

      pw.printf(
          "%s,%s,%b,%d,%d,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f%n",
          Instant.now().toString(),
          label,
          initPerIter,
          iterations,
          PARALLEL_FILES,
          open.p50,
          open.p99,
          read.p50,
          read.p99,
          total.p50,
          total.p90,
          total.p99,
          total.max,
          vectoredRead.p50,
          vectoredRead.p99);

      System.out.println("Summary appended to: " + path);
    } catch (IOException e) {
      System.err.println("Failed to write to summary CSV: " + e.getMessage());
    }
  }

  private List<FileRange> calculateRanges(long fileSize) {
    ThreadLocalRandom random = ThreadLocalRandom.current();
    List<FileRange> ranges = new ArrayList<>();
    long maxStartIndex = Math.max(0, fileSize - ROW_GROUP_SIZE);
    long blockStart = (long) (random.nextDouble() * (maxStartIndex + 1));
    long blockEnd = blockStart + ROW_GROUP_SIZE;

    int attempts = 0;
    while (ranges.size() < NUM_RANGES && attempts < 1000) {
      attempts++;
      int readLength =
          random.nextBoolean()
              ? random.nextInt(SMALL_READ_SIZE_MAX - SMALL_READ_SIZE_MIN + 1) + SMALL_READ_SIZE_MIN
              : random.nextInt(MEDIUM_READ_SIZE_MAX - MEDIUM_READ_SIZE_MIN + 1)
                  + MEDIUM_READ_SIZE_MIN;

      long maxOffset = blockEnd - readLength;
      long offset = blockStart + (long) (random.nextDouble() * (maxOffset - blockStart + 1));

      boolean overlaps = false;
      for (FileRange existing : ranges) {
        if (offset < existing.getOffset() + existing.getLength()
            && existing.getOffset() < offset + readLength) {
          overlaps = true;
          break;
        }
      }
      if (!overlaps) ranges.add(FileRange.createFileRange(offset, readLength));
    }
    ranges.sort(Comparator.comparingLong(FileRange::getOffset));
    return ranges;
  }
}
