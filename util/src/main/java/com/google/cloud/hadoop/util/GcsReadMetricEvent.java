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
  private final long connectionDurationMs;
  private final long dataTransferDurationMs;
  private final URI streamPath;
  private final boolean latencyThresholdBreached;

  public static GcsReadMetricEvent ofConnection(long durationMs, URI streamPath) {
    return new GcsReadMetricEvent(durationMs, 0, streamPath, false);
  }

  public static GcsReadMetricEvent ofDataTransfer(long durationMs, URI streamPath) {
    return ofDataTransfer(durationMs, streamPath, false);
  }

  public static GcsReadMetricEvent ofDataTransfer(
      long durationMs, URI streamPath, boolean latencyThresholdBreached) {
    return new GcsReadMetricEvent(0, durationMs, streamPath, latencyThresholdBreached);
  }

  public GcsReadMetricEvent(
      long connectionDurationMs,
      long dataTransferDurationMs,
      URI streamPath,
      boolean latencyThresholdBreached) {
    this.connectionDurationMs = connectionDurationMs;
    this.dataTransferDurationMs = dataTransferDurationMs;
    this.streamPath = streamPath;
    this.latencyThresholdBreached = latencyThresholdBreached;
  }

  public long getConnectionDurationMs() {
    return connectionDurationMs;
  }

  public long getDataTransferDurationMs() {
    return dataTransferDurationMs;
  }

  public URI getStreamPath() {
    return streamPath;
  }

  public boolean isLatencyThresholdBreached() {
    return latencyThresholdBreached;
  }
}
