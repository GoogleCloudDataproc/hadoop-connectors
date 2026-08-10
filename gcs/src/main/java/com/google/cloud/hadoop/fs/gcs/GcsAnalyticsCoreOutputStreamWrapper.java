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

package com.google.cloud.hadoop.fs.gcs;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.gcs.analyticscore.core.GoogleCloudStorageOutputStream;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.io.OutputStream;

/**
 * A synchronized wrapper around the non-thread-safe GoogleCloudStorageOutputStream.
 *
 * <p>TODO(user): Implement GoogleCloudStorageItemInfo.Provider once gcs-analytics-core exposes the
 * finalized generation ID upon close.
 */
class GcsAnalyticsCoreOutputStreamWrapper extends OutputStream {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private final GoogleCloudStorageOutputStream delegate;
  private boolean closed = false;

  public GcsAnalyticsCoreOutputStreamWrapper(GoogleCloudStorageOutputStream delegate) {
    this.delegate = checkNotNull(delegate, "delegate cannot be null");
  }

  @Override
  public synchronized void write(int b) throws IOException {
    logger.atFiner().log("write(int)");
    delegate.write(b);
  }

  @Override
  public synchronized void write(byte[] b, int off, int len) throws IOException {
    logger.atFine().log("write(byte[], off=%d, len=%d)", off, len);
    delegate.write(b, off, len);
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      logger.atFiner().log("close(): Stream already closed, ignoring.");
      return;
    }
    try {
      delegate.close();
    } finally {
      closed = true;
    }
  }
}
