/*
 * Copyright 2025 Google LLC
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
import com.google.api.client.http.HttpRequest;
import com.google.common.flogger.GoogleLogger;
import com.google.common.hash.Hasher;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Ints;
import java.io.IOException;

/** A {@link HttpExecuteInterceptor} that adds trailing checksum header. */
public class ChecksumHeaderInterceptor implements HttpExecuteInterceptor {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  public static final String TRAILING_CHECKSUM_HEADER = "x-goog-hash";

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

    Hasher hasher = ChecksumContext.CURRENT_HASHER.get();

    if (hasher != null && "PUT".equals(request.getRequestMethod())) {
      String contentRange = request.getHeaders().getContentRange();

      logger.atInfo().log(
          "intercept: Analyzing PUT request for trailing checksum. Content-Range: %s",
          contentRange);

      // Detect if this is the FINAL chunk
      // Intermediate chunks end with "/*" (e.g., "bytes 0-100/*")
      // The final chunk includes the total size (e.g., "bytes 100-200/201")
      if (contentRange != null && !contentRange.endsWith("/*")) {
        logger.atFiner().log("intercept: Detected final upload chunk. Calculating checksum...");

        // Finalize the checksum and encode it
        // Since the WriteChannel is closed by now, the hasher contains the full object checksum
        String finalChecksum =
            BaseEncoding.base64().encode(Ints.toByteArray(hasher.hash().asInt()));

        // Set the trailing checksum header
        // GCS will validate this checksum against the data received before finalizing the object
        request.getHeaders().set(TRAILING_CHECKSUM_HEADER, "crc32c=" + finalChecksum);

        logger.atFiner().log(
            "intercept: Set trailing checksum header '%s' to 'crc32c=%s'",
            TRAILING_CHECKSUM_HEADER, finalChecksum);
      }
    }
  }
}
