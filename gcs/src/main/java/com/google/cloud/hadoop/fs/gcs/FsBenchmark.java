package com.google.cloud.hadoop.fs.gcs;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.util.Collections.newSetFromMap;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toMap;

import com.google.common.collect.ImmutableList;
import com.google.common.flogger.GoogleLogger;
import com.google.common.util.concurrent.Futures;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * File system operations benchmark tool.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * hadoop jar /usr/lib/hadoop/lib/gcs-connector.jar com.google.cloud.hadoop.fs.gcs.FsBenchmark \
 *     {read,random-read} --file=gs://<bucket_name> [--no-warmup] [--verbose]
 * }</pre>
 */
public class FsBenchmark extends Configured implements Tool {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  public static void main(String[] args) throws Exception {
    // Let ToolRunner handle generic command-line options
    int result = ToolRunner.run(new FsBenchmark(), args);
    System.exit(result);
  }

  private FsBenchmark() {
    super(new Configuration());
  }

  @Override
  public int run(String[] args) throws IOException {
    String cmd = args[0];
    Map<String, String> cmdArgs =
        ImmutableList.copyOf(args).subList(1, args.length).stream()
            .collect(
                toMap(
                    arg -> arg.split("=")[0],
                    arg -> arg.contains("=") ? arg.split("=")[1] : "",
                    (u, v) -> {
                      throw new IllegalStateException(String.format("Duplicate key %s", u));
                    },
                    HashMap::new));

    URI testUri = new Path(cmdArgs.getOrDefault("--file", cmdArgs.get("--bucket"))).toUri();
    FileSystem fs = FileSystem.get(testUri, getConf());

    int res = 0;
    try {
      res = runWithInstrumentation(fs, cmd, cmdArgs);
    } catch (Throwable e) {
      logger.atSevere().withCause(e).log(
          "Failed to execute '%s' command with arguments: %s", cmd, cmdArgs);
    }
    System.out.println(res == 0 ? "Success!" : "Failure!");
    return res;
  }

  /** Helper to dispatch ToolRunner.run but with try/catch, progress-reporting, and statistics. */
  private int runWithInstrumentation(FileSystem fs, String cmd, Map<String, String> cmdArgs) {
    Statistics statistics = FileSystem.getStatistics().get(fs.getScheme());

    Optional<ScheduledExecutorService> progressReporter = Optional.empty();
    Future<?> statsFuture = immediateVoidFuture();
    if (cmdArgs.containsKey("--verbose")) {
      progressReporter = Optional.of(newSingleThreadScheduledExecutor());
      statsFuture =
          progressReporter
              .get()
              .scheduleWithFixedDelay(
                  () -> System.out.printf("Progress stats: %s%n", statistics),
                  parseLong(cmdArgs.getOrDefault("--verbose-delay-seconds", "5")),
                  parseLong(cmdArgs.getOrDefault("--verbose-interval-seconds", "15")),
                  SECONDS);
    }

    try {
      return runInternal(fs, cmd, cmdArgs);
    } finally {
      statsFuture.cancel(/* mayInterruptIfRunning= */ true);
      progressReporter.ifPresent(ExecutorService::shutdownNow);
      System.out.printf("Final stats: %s%n", statistics);
    }
  }

  /**
   * Add a few custom commands that we handle, delegating all else to superclass.
   *
   * <ul>
   *   <li>{@code concurrent-seek <path> <num_threads> [read_size]} - Benchmark random seek and read
   *       performance with multiple threads.
   *   <li>{@code read <path> <read_size> <reads_number>} - Benchmark sequential read performance in
   *       single thread.
   *   <li>{@code seek <path>} - Benchmarks the performance of performing forward seeks of varying
   *       strides through the specified file and prints results to stdout.
   * </ul>
   */
  private int runInternal(FileSystem fs, String cmd, Map<String, String> cmdArgs) {
    switch (cmd) {
      case "read":
        return benchmarkRead(fs, cmdArgs);
      case "random-read":
        return benchmarkRandomRead(fs, cmdArgs);
    }
    throw new IllegalArgumentException("Unknown command: " + cmd);
  }

  private int benchmarkRead(FileSystem fs, Map<String, String> args) {
    if (args.size() < 1) {
      System.err.println(
          "Usage: read"
              + " --file=gs://${BUCKET}/path/to/test/object"
              + " [--read-size=<read buffer size in bytes>]"
              + " [--num-reads=<number of times to fully read test file>]"
              + " [--num-threads=<number of threads to run test>]");
      return 1;
    }

    Path testFile = new Path(args.get("--file"));

    warmup(
        args,
        () ->
            benchmarkRead(
                fs, testFile, /* readSize= */ 1024, /* numReads= */ 1, /* numThreads= */ 2));

    benchmarkRead(
        fs,
        testFile,
        parseInt(args.getOrDefault("--read-size", String.valueOf(1024))),
        parseInt(args.getOrDefault("--num-reads", String.valueOf(1))),
        parseInt(args.getOrDefault("--num-threads", String.valueOf(1))));

    return 0;
  }

  private void benchmarkRead(
      FileSystem fs, Path testFile, int readSize, int numReads, int numThreads) {
    System.out.printf(
        "Running read test using %d bytes reads to fully read '%s' file %d times in %d threads%n",
        readSize, testFile, numReads, numThreads);

    Set<LongSummaryStatistics> readFileBytesList = newSetFromMap(new ConcurrentHashMap<>());
    Set<LongSummaryStatistics> readFileTimeNsList = newSetFromMap(new ConcurrentHashMap<>());
    Set<LongSummaryStatistics> readCallBytesList = newSetFromMap(new ConcurrentHashMap<>());
    Set<LongSummaryStatistics> readCallTimeNsList = newSetFromMap(new ConcurrentHashMap<>());

    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch initLatch = new CountDownLatch(numThreads);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch stopLatch = new CountDownLatch(numThreads);
    List<Future<?>> futures = new ArrayList<>(numThreads);
    for (int i = 0; i < numThreads; i++) {
      futures.add(
          executor.submit(
              () -> {
                LongSummaryStatistics readFileBytes = newLongSummaryStatistics(readFileBytesList);
                LongSummaryStatistics readFileTimeNs = newLongSummaryStatistics(readFileTimeNsList);
                LongSummaryStatistics readCallBytes = newLongSummaryStatistics(readCallBytesList);
                LongSummaryStatistics readCallTimeNs = newLongSummaryStatistics(readCallTimeNsList);

                byte[] readBuffer = new byte[readSize];

                initLatch.countDown();
                startLatch.await();
                try {
                  for (int j = 0; j < numReads; j++) {
                    try (FSDataInputStream input = fs.open(testFile)) {
                      long readStart = System.nanoTime();
                      long fileBytesRead = 0;
                      int bytesRead;
                      do {
                        long readCallStart = System.nanoTime();
                        bytesRead = input.read(readBuffer);
                        if (bytesRead > 0) {
                          fileBytesRead += bytesRead;
                          readCallBytes.accept(bytesRead);
                        }
                        readCallTimeNs.accept(System.nanoTime() - readCallStart);
                      } while (bytesRead >= 0);

                      readFileBytes.accept(fileBytesRead);
                      readFileTimeNs.accept(System.nanoTime() - readStart);
                    }
                  }
                } finally {
                  stopLatch.countDown();
                }
                return null;
              }));
    }
    executor.shutdown();

    awaitUnchecked(initLatch);
    long startTimeNs = System.nanoTime();
    startLatch.countDown();
    awaitUnchecked(stopLatch);
    long runtimeNs = System.nanoTime() - startTimeNs;

    // Verify that all threads completed without errors
    futures.forEach(Futures::getUnchecked);

    printTimeStats("Read call time", readCallTimeNsList);
    printSizeStats("Read call data", readCallBytesList);
    printThroughputStats("Read call throughput", readCallTimeNsList, readCallBytesList);

    printTimeStats("Read file time", readFileTimeNsList);
    printSizeStats("Read file data", readFileBytesList);
    printThroughputStats("Read file throughput", readFileTimeNsList, readFileBytesList);

    System.out.printf(
        "Read average throughput (MiB/s): %.3f%n",
        bytesToMebibytes(combineStats(readFileBytesList).getSum()) / nanosToSeconds(runtimeNs));
  }

  private int benchmarkRandomRead(FileSystem fs, Map<String, String> args) {
    if (args.size() < 1) {
      System.err.println(
          "Usage: random-read"
              + " --file=gs://${BUCKET}/path/to/test/object"
              + " [--num-open=<number of file open>]"
              + " [--read-size=<read buffer size in bytes>]"
              + " [--num-reads=<number of random reads per file open>]"
              + " [--num-threads=<number of threads to run test>]");
      return 1;
    }

    Path testFile = new Path(args.get("--file"));

    warmup(
        args,
        () ->
            benchmarkRandomRead(
                fs,
                testFile,
                /* numOpen= */ 5,
                /* readSize= */ 1024,
                /* numReads= */ 20,
                /* numThreads= */ 5));

    benchmarkRandomRead(
        fs,
        testFile,
        parseInt(args.getOrDefault("--num-open", String.valueOf(1))),
        parseInt(args.getOrDefault("--read-size", String.valueOf(1024))),
        parseInt(args.getOrDefault("--num-reads", String.valueOf(100))),
        parseInt(args.getOrDefault("--num-threads", String.valueOf(1))));

    return 0;
  }

  private void benchmarkRandomRead(
      FileSystem fs, Path testFile, int numOpen, int readSize, int numReads, int numThreads) {
    System.out.printf(
        "Running random read test that reads %d bytes from '%s' file %d times per %d open"
            + " operations in %d threads%n",
        readSize, testFile, numReads, numOpen, numThreads);

    Set<LongSummaryStatistics> openLatencyNsList = newSetFromMap(new ConcurrentHashMap<>());
    Set<LongSummaryStatistics> seekLatencyNsList = newSetFromMap(new ConcurrentHashMap<>());
    Set<LongSummaryStatistics> readLatencyNsList = newSetFromMap(new ConcurrentHashMap<>());
    Set<LongSummaryStatistics> closeLatencyNsList = newSetFromMap(new ConcurrentHashMap<>());

    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch initLatch = new CountDownLatch(numThreads);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch stopLatch = new CountDownLatch(numThreads);
    List<Future<?>> futures = new ArrayList<>(numThreads);
    for (int i = 0; i < numThreads; i++) {
      futures.add(
          executor.submit(
              () -> {
                FileStatus fileStatus = fs.getFileStatus(testFile);
                long fileSize = fileStatus.getLen();
                long maxReadPositionExclusive = fileSize - readSize + 1;

                LongSummaryStatistics openLatencyNs = newLongSummaryStatistics(openLatencyNsList);
                LongSummaryStatistics seekLatencyNs = newLongSummaryStatistics(seekLatencyNsList);
                LongSummaryStatistics readLatencyNs = newLongSummaryStatistics(readLatencyNsList);
                LongSummaryStatistics closeLatencyNs = newLongSummaryStatistics(closeLatencyNsList);

                ThreadLocalRandom random = ThreadLocalRandom.current();
                byte[] readBuffer = new byte[readSize];

                initLatch.countDown();
                startLatch.await();
                try {
                  for (int j = 0; j < numOpen; j++) {
                    try {
                      long seekPos = random.nextLong(maxReadPositionExclusive);

                      long openStart = System.nanoTime();
                      FSDataInputStream input = fs.open(testFile);
                      openLatencyNs.accept(System.nanoTime() - openStart);
                      try {
                        for (int k = 0; k < numReads; k++) {
                          long seekStart = System.nanoTime();
                          input.seek(seekPos);
                          seekLatencyNs.accept(System.nanoTime() - seekStart);

                          long readStart = System.nanoTime();
                          int numRead = input.read(readBuffer);
                          readLatencyNs.accept(System.nanoTime() - readStart);

                          if (numRead != readSize) {
                            System.err.printf(
                                "Read %d bytes from %d bytes at offset %d!%n",
                                numRead, readSize, seekPos);
                          }
                        }
                      } finally {
                        long closeStart = System.nanoTime();
                        input.close();
                        closeLatencyNs.accept(System.nanoTime() - closeStart);
                      }
                    } catch (Throwable e) {
                      logger.atSevere().withCause(e).log("Failed random read from '%s'", testFile);
                    }
                  }
                } finally {
                  stopLatch.countDown();
                }

                return null;
              }));
    }
    executor.shutdown();

    awaitUnchecked(initLatch);
    long startTime = System.nanoTime();
    startLatch.countDown();
    awaitUnchecked(stopLatch);
    double runtimeSeconds = nanosToSeconds(System.nanoTime() - startTime);
    long operations = combineStats(readLatencyNsList).getCount();

    // Verify that all threads completed without errors
    futures.forEach(Futures::getUnchecked);

    printTimeStats("Open latency ", combineStats(openLatencyNsList));
    printTimeStats("Seek latency ", combineStats(seekLatencyNsList));
    printTimeStats("Read latency ", combineStats(readLatencyNsList));
    printTimeStats("Close latency", combineStats(closeLatencyNsList));
    System.out.printf(
        "Average QPS: %.3f (%d in total %.3fs)%n",
        operations / runtimeSeconds, operations, runtimeSeconds);
  }

  private static void warmup(Map<String, String> args, Runnable warmupFn) {
    if (args.containsKey("--no-warmup")) {
      System.out.println("=== Skipping warmup ===");
      return;
    }

    System.out.println("=== Running warmup ===");
    ExecutorService warmupExecutor = newSingleThreadExecutor();
    try {
      warmupExecutor.submit(warmupFn).get();
    } catch (ExecutionException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new RuntimeException("Benchmark warmup failed", e);
    } finally {
      warmupExecutor.shutdownNow();
    }
    System.out.println("=== Finished warmup ===\n");
  }

  private static LongSummaryStatistics newLongSummaryStatistics(
      Collection<LongSummaryStatistics> openLatencyNsList) {
    LongSummaryStatistics openLatencyNs = new LongSummaryStatistics();
    openLatencyNsList.add(openLatencyNs);
    return openLatencyNs;
  }

  private static void awaitUnchecked(CountDownLatch latch) {
    try {
      latch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("CountDownLatch.await interrupted", e);
    }
  }

  private static void printTimeStats(String name, Collection<LongSummaryStatistics> timeStats) {
    printTimeStats(name, combineStats(timeStats));
  }

  private static void printTimeStats(String name, LongSummaryStatistics timeStats) {
    System.out.printf(
        "%s (ms): min=%.5f, average=%.5f, max=%.5f (count=%d)%n",
        name,
        nanosToMillis(timeStats.getMin()),
        nanosToMillis(timeStats.getAverage()),
        nanosToMillis(timeStats.getMax()),
        timeStats.getCount());
  }

  private static void printSizeStats(String name, Collection<LongSummaryStatistics> sizeStats) {
    printSizeStats(name, combineStats(sizeStats));
  }

  private static void printSizeStats(String name, LongSummaryStatistics sizeStats) {
    System.out.printf(
        "%s (MiB): min=%.5f, average=%.5f, max=%.5f (count=%d)%n",
        name,
        bytesToMebibytes(sizeStats.getMin()),
        bytesToMebibytes(sizeStats.getAverage()),
        bytesToMebibytes(sizeStats.getMax()),
        sizeStats.getCount());
  }

  private static void printThroughputStats(
      String name,
      Collection<LongSummaryStatistics> timeStats,
      Collection<LongSummaryStatistics> sizeStats) {
    printThroughputStats(name, combineStats(timeStats), combineStats(sizeStats).getAverage());
  }

  private static void printThroughputStats(
      String name, LongSummaryStatistics timeStats, double bytesProcessed) {
    System.out.printf(
        "%s (MiB/s): min=%.3f, average=%.3f, max=%.3f (count=%d)%n",
        name,
        bytesToMebibytes(bytesProcessed) / nanosToSeconds(timeStats.getMax()),
        bytesToMebibytes(bytesProcessed) / nanosToSeconds(timeStats.getAverage()),
        bytesToMebibytes(bytesProcessed) / nanosToSeconds(timeStats.getMin()),
        timeStats.getCount());
  }

  private static LongSummaryStatistics combineStats(Collection<LongSummaryStatistics> stats) {
    return stats.stream()
        .collect(
            LongSummaryStatistics::new,
            LongSummaryStatistics::combine,
            LongSummaryStatistics::combine);
  }

  private static double nanosToMillis(double nanos) {
    return nanos / 1_000_000.0;
  }

  private static double nanosToSeconds(double nanos) {
    return nanos / 1_000_000_000.0;
  }

  private static double bytesToMebibytes(double bytes) {
    return bytes / 1024 / 1024;
  }
}
