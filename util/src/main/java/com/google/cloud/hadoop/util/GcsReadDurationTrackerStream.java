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

import com.google.api.client.http.HttpHeaders;
import com.google.common.flogger.GoogleLogger;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.concurrent.TimeUnit;

/** A decorator for {@link InputStream} that tracks the data transfer duration. */
public class GcsReadDurationTrackerStream extends FilterInputStream {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();
  private static final String UPLOAD_ID_HEADER = "x-guploader-uploadid";

  private final URI streamPath;
  private final Object responseHeaders;
  private final long latencyLoggingThresholdMs;
  private long cumulativeTransferDurationMs;

  public GcsReadDurationTrackerStream(
      InputStream delegate,
      URI streamPath,
      Object responseHeaders,
      long latencyLoggingThresholdMs) {
    super(delegate);
    this.streamPath = streamPath;
    this.responseHeaders = responseHeaders;
    this.latencyLoggingThresholdMs = latencyLoggingThresholdMs;
    this.cumulativeTransferDurationMs = 0;
  }

  @Override
  public int read() throws IOException {
    long startTime = System.currentTimeMillis();
    try {
      return super.read();
    } finally {
      cumulativeTransferDurationMs += System.currentTimeMillis() - startTime;
    }
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    long startTime = System.currentTimeMillis();
    try {
      return super.read(b, off, len);
    } finally {
      cumulativeTransferDurationMs += System.currentTimeMillis() - startTime;
    }
  }

  @Override
  public void close() throws IOException {
    try {
      super.close();
    } finally {
      boolean latencyThresholdBreached = cumulativeTransferDurationMs > latencyLoggingThresholdMs;
      if (latencyThresholdBreached) {
        String uploadId =
            responseHeaders instanceof HttpHeaders
                ? ((HttpHeaders) responseHeaders).getFirstHeaderStringValue(UPLOAD_ID_HEADER)
                : String.valueOf(responseHeaders);
        logger.atInfo().atMostEvery(10, TimeUnit.SECONDS).log(
            "Detected high latency for %s. durationMs=%d; upload_id=%s",
            streamPath, cumulativeTransferDurationMs, uploadId);
      }
      GoogleCloudStorageEventBus.postReadMetricEvent(
          GcsReadMetricEvent.ofDataTransfer(
              cumulativeTransferDurationMs, streamPath, latencyThresholdBreached));
    }
  }
}
