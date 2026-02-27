package com.google.cloud.hadoop.fs.gcs;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class VectoredReadMetric {
  public final String type;
  public final long durationNs;

  public VectoredReadMetric(String type, long durationNs) {
    this.type = type;
    this.durationNs = durationNs;
  }

  // Global queue to store metrics for the current benchmark run.
  // In a real production system, this would be a proper metrics system.
  // For this benchmark, a static queue is sufficient and simple.
  private static final Queue<VectoredReadMetric> metrics = new ConcurrentLinkedQueue<>();

  public static void add(String type, long durationNs) {
    metrics.add(new VectoredReadMetric(type, durationNs));
  }

  public static Queue<VectoredReadMetric> getAll() {
    return metrics;
  }

  public static void clear() {
    metrics.clear();
  }
}
