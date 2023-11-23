/*
 * Copyright 2023 Google LLC
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

/** Translates exceptions from API calls into ErrorType */
public interface ErrorTypeExtractor {

  enum ErrorType {
    NOT_FOUND,
    OUT_OF_RANGE,
    ALREADY_EXISTS,
    FAILED_PRECONDITION,
    UNKNOWN
  }

  ErrorType getErrorType(Exception exception);

  /** Determines if the given exception indicates that bucket already exists. */
  boolean bucketAlreadyExists(Exception e);
}
