package com.google.cloud.hadoop.fs.gcs;

import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class VectoredReadMetric {
  public final String type;
  public final long durationNs;
  public final List<String> requestedRanges;

  public VectoredReadMetric(String type, long durationNs) {
    this(type, durationNs, Collections.emptyList());
  }

  public VectoredReadMetric(String type, long durationNs, List<String> requestedRanges) {
    this.type = type;
    this.durationNs = durationNs;
    this.requestedRanges = requestedRanges;
  }

  // Global queue to store metrics for the current benchmark run.
  // In a real production system, this would be a proper metrics system.
  // For this benchmark, a static queue is sufficient and simple.
  private static final Queue<VectoredReadMetric> metrics = new ConcurrentLinkedQueue<>();

  public static void add(String type, long durationNs) {
    metrics.add(new VectoredReadMetric(type, durationNs));
  }

  public static void add(String type, long durationNs, List<String> requestedRanges) {
    metrics.add(new VectoredReadMetric(type, durationNs, requestedRanges));
  }

  public static Queue<VectoredReadMetric> getAll() {
    return metrics;
  }

  public static void clear() {
    metrics.clear();
  }
}
