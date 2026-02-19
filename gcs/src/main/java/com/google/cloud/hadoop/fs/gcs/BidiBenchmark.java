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
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileRange;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BidiBenchmark extends Configured implements Tool {

  private static final long ROW_GROUP_SIZE = 512L * 1024 * 1024;
  private static final int NUM_RANGES = 10;
  private static final int SMALL_READ_SIZE_MIN = 10 * 1024;
  private static final int SMALL_READ_SIZE_MAX = 90 * 1024;
  private static final int MEDIUM_READ_SIZE_MIN = 1 * 1024 * 1024;
  private static final int MEDIUM_READ_SIZE_MAX = 5 * 1024 * 1024;

  private static final String DEFAULT_FILE_URI = "gs://hadoop-benchmark-regional/1.parquet";

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new Configuration(), new BidiBenchmark(), args);
    System.exit(exitCode);
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    String fileUri = (args.length > 0) ? args[0] : DEFAULT_FILE_URI;

    // Configuration Flags
    int iterations = conf.getInt("benchmark.iterations", 100);
    boolean initFsPerIteration = conf.getBoolean("benchmark.fs.init.per.iteration", true);
    String label = conf.get("benchmark.label", "Unknown_Scenario");
    String csvPath = conf.get("benchmark.output.csv", "benchmark_results.csv");
    // New config for raw data output
    String rawCsvPath = conf.get("benchmark.output.raw.csv", "benchmark_raw.csv");

    Path path = new Path(fileUri);

    System.out.println("==================================================");
    System.out.println("Scenario: " + label);
    System.out.println("Target: " + fileUri);
    System.out.println("Init FS per Iteration: " + initFsPerIteration);
    System.out.println("Raw Data Output: " + rawCsvPath);
    System.out.println("==================================================");

    long fileSize;
    try (FileSystem tempFs = FileSystem.newInstance(path.toUri(), conf)) {
      fileSize = tempFs.getFileStatus(path).getLen();
    }

    List<Double> openLatencies = new ArrayList<>();
    List<Double> readLatencies = new ArrayList<>();
    List<Double> totalLatencies = new ArrayList<>();

    FileSystem sharedFs = null;
    if (!initFsPerIteration) {
      sharedFs = FileSystem.newInstance(path.toUri(), conf);
    }

    // Initialize Raw Data Writer
    try (PrintWriter rawWriter = new PrintWriter(new FileWriter(rawCsvPath, true))) {
      // Write Header if file is empty
      if (new File(rawCsvPath).length() == 0) {
        rawWriter.println("Timestamp,Iteration,Open_Ms,Read_Ms,Total_Ms");
      }

      try {
        for (int i = 0; i < iterations; i++) {
          List<FileRange> ranges = calculateRanges(fileSize);

          FileSystem fs = null;
          FSDataInputStream in = null;
          double openDurationMs = 0;
          double readDurationMs = 0;

          try {
            if (initFsPerIteration) {
              fs = FileSystem.newInstance(path.toUri(), conf);
            } else {
              fs = sharedFs;
            }

            long openStart = System.nanoTime();
            in = fs.open(path);
            long openEnd = System.nanoTime();
            openDurationMs = (openEnd - openStart) / 1_000_000.0;

            long readStart = System.nanoTime();
            in.readVectored(ranges, ByteBuffer::allocate);
            for (FileRange range : ranges) {
              range.getData().join();
            }
            long readEnd = System.nanoTime();
            readDurationMs = (readEnd - readStart) / 1_000_000.0;

          } catch (Exception e) {
            System.err.printf("Iteration %d failed: %s%n", i, e.getMessage());
            if (initFsPerIteration && fs != null)
              try {
                fs.close();
              } catch (IOException ignored) {
              }
            continue;
          } finally {
            if (in != null)
              try {
                in.close();
              } catch (IOException ignored) {
              }
            if (initFsPerIteration && fs != null)
              try {
                fs.close();
              } catch (IOException ignored) {
              }
          }

          double totalMs = openDurationMs + readDurationMs;
          openLatencies.add(openDurationMs);
          readLatencies.add(readDurationMs);
          totalLatencies.add(totalMs);

          // Dump Raw Data
          rawWriter.printf(
              "%s,%d,%.2f,%.2f,%.2f%n",
              Instant.now().toString(), i, openDurationMs, readDurationMs, totalMs);
          // Flush to ensure data is written immediately (useful if benchmark crashes)
          rawWriter.flush();

          if ((i + 1) % 100 == 0) {
            System.out.printf("Progress: %d/%d%n", i + 1, iterations);
          }
        }
      } finally {
        if (sharedFs != null)
          try {
            sharedFs.close();
          } catch (IOException ignored) {
          }
      }
    }

    // Print to Console
    System.out.println("\n--- Summary ---");
    Stats totalStats = calculateStats(totalLatencies);
    System.out.printf(
        "P50: %.2f | P99: %.2f | Max: %.2f%n", totalStats.p50, totalStats.p99, totalStats.max);

    // Write to Summary CSV
    writeToCsv(
        csvPath,
        label,
        initFsPerIteration,
        iterations,
        calculateStats(openLatencies),
        calculateStats(readLatencies),
        totalStats);

    return 0;
  }

  private static class Stats {
    double p50, p90, p99, max;
  }

  private Stats calculateStats(List<Double> data) {
    Stats s = new Stats();
    if (data.isEmpty()) return s;
    Collections.sort(data);
    int n = data.size();
    s.p50 = data.get((int) (n * 0.50));
    s.p90 = data.get((int) (n * 0.90));
    s.p99 = data.get((int) (n * 0.99));
    s.max = data.get(n - 1);
    return s;
  }

  private void writeToCsv(
      String path,
      String label,
      boolean initPerIter,
      int iterations,
      Stats open,
      Stats read,
      Stats total) {
    File file = new File(path);
    boolean writeHeader = !file.exists();

    try (FileWriter fw = new FileWriter(file, true);
        PrintWriter pw = new PrintWriter(fw)) {

      if (writeHeader) {
        pw.println(
            "Timestamp,Label,InitFsPerIter,Iterations,Open_P50,Open_P99,Read_P50,Read_P99,Total_P50,Total_P90,Total_P99,Max_Total");
      }

      pw.printf(
          "%s,%s,%b,%d,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f%n",
          Instant.now().toString(),
          label,
          initPerIter,
          iterations,
          open.p50,
          open.p99,
          read.p50,
          read.p99,
          total.p50,
          total.p90,
          total.p99,
          total.max);

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
