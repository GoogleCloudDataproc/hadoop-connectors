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

import io.grpc.Status;

/**
 * Implementation for {@link ErrorTypeExtractor} for exception specifically thrown from gRPC path.
 */
public class GrpcErrorTypeExtractor implements ErrorTypeExtractor {

  public static final GrpcErrorTypeExtractor INSTANCE = new GrpcErrorTypeExtractor();

  private GrpcErrorTypeExtractor() {}

  @Override
  public ErrorType getErrorType(Exception error) {
    switch (Status.fromThrowable(error).getCode()) {
      case NOT_FOUND:
        return ErrorType.NOT_FOUND;
      case OUT_OF_RANGE:
        return ErrorType.OUT_OF_RANGE;
      default:
        return ErrorType.UNKNOWN;
    }
  }
}
