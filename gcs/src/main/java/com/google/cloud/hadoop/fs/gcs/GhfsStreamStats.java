package com.google.cloud.hadoop.fs.gcs;

import javax.annotation.Nonnull;

class GhfsStreamStats {
  private final GhfsStorageStatistics storageStatistics;
  private final GhfsStatistic durationStat;
  private final Object context;
  private long maxLatencyNs;
  private int operationCount;
  private long minLatencyNs;
  private long totalNs;

  GhfsStreamStats(
      @Nonnull GhfsStorageStatistics storageStatistics,
      GhfsStatistic durationStat,
      Object context) {
    this.storageStatistics = storageStatistics;
    this.durationStat = durationStat;
    this.context = context;
  }

  void close() {
    if (operationCount == 0) {
      return;
    }

    storageStatistics.updateStats(
        durationStat,
        toMillis(minLatencyNs),
        toMillis(maxLatencyNs),
        toMillis(totalNs),
        operationCount,
        context);
    this.totalNs = 0;
    this.operationCount = 0;
  }

  void updateWriteStreamStats(int len, long start) {
    updateStats(start);
    storageStatistics.streamWriteBytes(len);
  }

  void updateReadStreamSeekStats(long start) {
    updateStats(start);
  }

  void updateReadStreamStats(int len, long start) {
    updateStats(start);
    storageStatistics.streamReadBytes(len);
  }

  private static long toMillis(long nano) {
    return nano / 1000_000;
  }

  private void updateStats(long start) {
    long latency = System.nanoTime() - start;
    this.maxLatencyNs = Math.max(latency, this.maxLatencyNs);
    if (operationCount == 0) {
      this.minLatencyNs = latency;
    } else {
      this.minLatencyNs = Math.min(latency, this.minLatencyNs);
    }

    this.totalNs += latency;
    this.operationCount++;
  }
}
