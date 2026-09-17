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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.base.Ticker;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Decorates the GCS object content stream so that the time spent pulling bytes off the socket is
 * attributed to the GCS API, not to the caller.
 *
 * <p>Without this, {@code gcsApiTime} only covers the time until the response headers arrive
 * (time-to-first-byte); the payload transfer, which dominates for large reads, is invisible.
 *
 * <p>The stopwatch accumulates across every read on this stream and is reported once on {@link
 * #close()}, so the conversion to milliseconds is truncated exactly once per HTTP request - the
 * same precision trade-off {@code RequestTracker} already makes for time-to-first-byte.
 *
 * <p>Not thread safe. One instance corresponds to one HTTP response body, which is consumed by a
 * single reader.
 */
public class GcsReadDurationTrackerStream extends FilterInputStream {

  private final Stopwatch stopwatch;
  private boolean reported;

  public GcsReadDurationTrackerStream(InputStream delegate) {
    this(delegate, Ticker.systemTicker());
  }

  @VisibleForTesting
  GcsReadDurationTrackerStream(InputStream delegate, Ticker ticker) {
    super(checkNotNull(delegate, "delegate cannot be null"));
    this.stopwatch = Stopwatch.createUnstarted(ticker);
  }

  @Override
  public int read() throws IOException {
    stopwatch.start();
    try {
      return super.read();
    } finally {
      stopwatch.stop();
    }
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    stopwatch.start();
    try {
      return super.read(b, off, len);
    } finally {
      stopwatch.stop();
    }
  }

  @Override
  public void close() throws IOException {
    try {
      super.close();
    } finally {
      reportDuration();
    }
  }

  /** Publishes the accumulated transfer time. Guarded so a repeated close does not double count. */
  private void reportDuration() {
    if (reported) {
      return;
    }
    reported = true;

    long durationMs = stopwatch.elapsed().toMillis();
    if (durationMs > 0) {
      postToEventQueue(new GcsDataTransferEvent(durationMs));
    }
  }

  /** Publishes the event. Overridden in tests to observe without touching the static event bus. */
  protected void postToEventQueue(GcsDataTransferEvent event) {
    GoogleCloudStorageEventBus.postDataTransferEvent(event);
  }
}
