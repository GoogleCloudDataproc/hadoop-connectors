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

class GhfsStreamStats {
  private final GhfsGlobalStorageStatistics storageStatistics;
  private final GhfsStatistic durationStat;
  private final Object context;
  private long maxLatencyMs;
  private int operationCount;
  private long minLatencyMs;
  private long totalMs;

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
        durationStat, minLatencyMs, maxLatencyMs, totalMs, operationCount, context);
    this.totalMs = 0;
    this.operationCount = 0;
    this.maxLatencyMs = 0;
    this.minLatencyMs = 0;
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

  private static long toMillis(long nano) {
    return nano / 1000_000;
  }

  private void updateStats(long start) {
    long latencyMs = toMillis(System.nanoTime() - start);
    this.maxLatencyMs = Math.max(latencyMs, this.maxLatencyMs);
    if (operationCount == 0) {
      this.minLatencyMs = latencyMs;
    } else {
      this.minLatencyMs = Math.min(latencyMs, this.minLatencyMs);
    }

    this.totalMs += latencyMs;
    this.operationCount++;
  }
}
