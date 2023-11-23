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
import io.grpc.StatusRuntimeException;
import javax.annotation.Nullable;

/**
 * Implementation for {@link ErrorTypeExtractor} for exception specifically thrown from gRPC path.
 */
public class GrpcErrorTypeExtractor implements ErrorTypeExtractor {

  public static final GrpcErrorTypeExtractor INSTANCE = new GrpcErrorTypeExtractor();

  private static final String BUCKET_ALREADY_EXISTS_MESSAGE =
      "FAILED_PRECONDITION: Your previous request to create the named bucket succeeded and you already own it.";

  private GrpcErrorTypeExtractor() {}

  @Override
  public ErrorType getErrorType(Exception error) {
    switch (Status.fromThrowable(error).getCode()) {
      case NOT_FOUND:
        return ErrorType.NOT_FOUND;
      case OUT_OF_RANGE:
        return ErrorType.OUT_OF_RANGE;
      case ALREADY_EXISTS:
        return ErrorType.ALREADY_EXISTS;
      case FAILED_PRECONDITION:
        return ErrorType.FAILED_PRECONDITION;
      default:
        return ErrorType.UNKNOWN;
    }
  }

  @Override
  public boolean bucketAlreadyExists(Exception e) {
    ErrorType errorType = getErrorType(e);
    if (errorType == ErrorType.ALREADY_EXISTS) {
      return true;
    }
    // The gRPC API currently throws a FAILED_PRECONDITION status code instead of ALREADY_EXISTS,
    // so we handle both these conditions in the interim.
    // TODO: remove once the status codes are fixed.
    else if (errorType == ErrorType.FAILED_PRECONDITION) {
      StatusRuntimeException statusRuntimeException = getStatusRuntimeException(e);
      return statusRuntimeException != null
          && BUCKET_ALREADY_EXISTS_MESSAGE.equals(statusRuntimeException.getMessage());
    }
    return false;
  }

  /** Extracts StatusRuntimeException from the Exception, if it exists. */
  @Nullable
  private StatusRuntimeException getStatusRuntimeException(Exception e) {
    Throwable cause = e;
    // Keeping a counter to break early from the loop to avoid infinite loop condition due to
    // cyclic exception chains.
    int currentExceptionDepth = 0, maxChainDepth = 1000;
    while (cause != null && currentExceptionDepth < maxChainDepth) {
      if (cause instanceof StatusRuntimeException) {
        return (StatusRuntimeException) cause;
      }
      cause = cause.getCause();
      currentExceptionDepth++;
    }
    return null;
  }
}
