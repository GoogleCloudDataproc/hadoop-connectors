/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.util;

import java.net.URI;

/** Event for providing GCS read metrics. */
public class GcsReadMetricEvent {

  /** Types of read metric events. */
  public enum Type {
    CONNECTION,
    DATA_TRANSFER
  }

  private final Type type;
  private final long durationMs;
  private final URI streamPath;
  private final boolean latencyThresholdBreached;

  public static GcsReadMetricEvent ofConnection(long durationMs, URI streamPath) {
    return new GcsReadMetricEvent(Type.CONNECTION, durationMs, streamPath, false);
  }

  public static GcsReadMetricEvent ofDataTransfer(
      long durationMs, URI streamPath, boolean latencyThresholdBreached) {
    return new GcsReadMetricEvent(
        Type.DATA_TRANSFER, durationMs, streamPath, latencyThresholdBreached);
  }

  private GcsReadMetricEvent(
      Type type, long durationMs, URI streamPath, boolean latencyThresholdBreached) {
    this.type = type;
    this.durationMs = durationMs;
    this.streamPath = streamPath;
    this.latencyThresholdBreached = latencyThresholdBreached;
  }

  public Type getType() {
    return type;
  }

  public long getDurationMs() {
    return durationMs;
  }

  public URI getStreamPath() {
    return streamPath;
  }

  public boolean isLatencyThresholdBreached() {
    return latencyThresholdBreached;
  }
}
