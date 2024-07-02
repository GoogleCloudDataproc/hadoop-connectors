/*
 * Copyright 2023 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.fs.gcs;

import javax.annotation.Nonnull;

/** Manages a specific duration statistic */
class GhfsStreamStats {
  private final GhfsGlobalStorageStatistics storageStatistics;
  private final GhfsStatistic durationStat;
  private final Object context;
  private long maxLatencyNs;
  private int operationCount;
  private long minLatencyNs;
  private long totalNs;

  GhfsStreamStats(
      @Nonnull GhfsGlobalStorageStatistics storageStatistics,
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

  void updateVectoredReadStreamStats(long start) {
    updateStats(start);
  }

  void updateVectoredBytesRead(int len) {}

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
