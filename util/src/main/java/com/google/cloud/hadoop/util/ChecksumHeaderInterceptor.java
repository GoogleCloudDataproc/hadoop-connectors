/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.util;

import com.google.api.client.http.HttpExecuteInterceptor;
import com.google.api.client.http.HttpMethods;
import com.google.api.client.http.HttpRequest;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.util.function.Supplier;

/** A {@link HttpExecuteInterceptor} that adds trailing checksum header. */
public class ChecksumHeaderInterceptor implements HttpExecuteInterceptor {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  public static final String CHECKSUM_HEADER = "x-goog-hash";

  private final HttpExecuteInterceptor chainedInterceptor;

  public ChecksumHeaderInterceptor(HttpExecuteInterceptor chainedInterceptor) {
    this.chainedInterceptor = chainedInterceptor;
  }

  @Override
  public void intercept(HttpRequest request) throws IOException {
    logger.atFiner().log(
        "intercept(%s %s): Intercepting request", request.getRequestMethod(), request.getUrl());

    if (chainedInterceptor != null) {
      chainedInterceptor.intercept(request);
    }

    if (!HttpMethods.PUT.equals(request.getRequestMethod())) {
      return;
    }

    Supplier<String> checksumSupplier = ChecksumContext.getChecksumSupplier();
    if (checksumSupplier == null) {
      return;
    }

    String contentRange = request.getHeaders().getContentRange();

    logger.atFiner().log(
        "Analyzing PUT request for trailing checksum. Content-Range: %s", contentRange);

    // Detect if this is the FINAL chunk
    // Intermediate chunks end with "/*", final chunks include the total size.
    if (contentRange == null || contentRange.endsWith("/*")) {
      return;
    }

    logger.atFiner().log("Detected final upload chunk. Calculating checksum...");

    // Finalize the checksum and encode it
    // Since the WriteChannel is closed by now, the hasher contains the full object checksum
    String finalChecksum = checksumSupplier.get();

    // Set the trailing checksum header
    // GCS will validate this checksum against the data received before finalizing the object
    request.getHeaders().set(CHECKSUM_HEADER, "crc32c=" + finalChecksum);

    logger.atFiner().log(
        "Set trailing checksum header '%s' to 'crc32c=%s'", CHECKSUM_HEADER, finalChecksum);
  }
}
