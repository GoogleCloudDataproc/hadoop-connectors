/*
 * Copyright 2024 Google LLC. All Rights Reserved.
 * ...
 */
package com.google.cloud.hadoop.util;

import io.grpc.Status;

/** Event carried over EventBus when a gRPC request completes. */
public class GrpcRequestCompletionEvent {
  private final Status status;
  private final long durationMillis;

  public GrpcRequestCompletionEvent(Status status, long durationMillis) {
    this.status = status;
    this.durationMillis = durationMillis;
  }

  public Status getStatus() {
    return status;
  }

  public long getDurationMillis() {
    return durationMillis;
  }
}
