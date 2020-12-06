/*
 * Copyright 2014 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.google.cloud.hadoop.util;

import com.google.api.client.http.HttpResponseException;
import java.io.IOException;

/**
 * This abstract class is designed to tell if an exception is transient and should result in a retry
 * or not, and should result in a returned exception to the caller. Meant to be used with a {@link
 * ResilientOperation}.
 *
 * @param <X> The type of exception you are checking and could possibly return.
 */
@FunctionalInterface
public interface RetryDeterminer<X extends Exception> {

  /** Retries exception when either {@code SOCKET_ERRORS} or {@code SERVER_ERRORS} would retry. */
  RetryDeterminer<Exception> DEFAULT =
      new RetryDeterminer<Exception>() {
        @Override
        public boolean shouldRetry(Exception e) {
          return e instanceof IOException
              && (SOCKET_ERRORS.shouldRetry((IOException) e)
                  || SERVER_ERRORS.shouldRetry((IOException) e));
        }
      };

  /** Always retries. */
  RetryDeterminer<Exception> ALL_ERRORS = e -> true;

  /** Socket errors retry determiner retries on socket exceptions. */
  RetryDeterminer<IOException> SOCKET_ERRORS = IoExceptionHelper::isSocketError;

  /** Server errors RetryDeterminer decides to retry on HttpResponseExceptions that return a 500. */
  RetryDeterminer<IOException> SERVER_ERRORS =
      e ->
          e instanceof HttpResponseException
              // TODO: Find out what we should do for 500 codes that are not always transient.
              && ((HttpResponseException) e).getStatusCode() / 100 == 5;

  /** A rate limited determiner that uses a default {@link ApiErrorExtractor}. */
  RetryDeterminer<IOException> RATE_LIMIT_ERRORS = ApiErrorExtractor.INSTANCE::rateLimited;

  /**
   * Determines if we should attempt a retry depending on the caught exception.
   *
   * <p>To indicate that no retry should be made, return false. If no retry, the exception should be
   * returned to the user.
   *
   * @param e Exception of type X that can be examined to determine if a retry is possible.
   * @return true if should retry, false otherwise
   */
  boolean shouldRetry(X e);
}
