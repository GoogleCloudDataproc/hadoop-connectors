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

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.hadoop.util.ErrorTypeExtractor.ErrorType;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.Test;

public class GrpcErrorTypeExtractorTest {

  private static final GrpcErrorTypeExtractor typeExtractor = GrpcErrorTypeExtractor.INSTANCE;

  @Test
  public void testNotFound() {
    Exception ex = new StatusRuntimeException(Status.NOT_FOUND);
    assertThat(typeExtractor.getErrorType(ex)).isEqualTo(ErrorType.NOT_FOUND);
  }

  @Test
  public void testOutOfRange() {
    Exception ex = new StatusRuntimeException(Status.OUT_OF_RANGE);
    assertThat(typeExtractor.getErrorType(ex)).isEqualTo(ErrorType.OUT_OF_RANGE);
  }

  @Test
  public void testBucketAlreadyExistsFailedPreconditionException() {
    Exception ex =
        new Exception(
            new StatusRuntimeException(
                Status.FAILED_PRECONDITION.withDescription(
                    "Your previous request to create the named bucket succeeded and you already own it.")));
    assertThat(typeExtractor.bucketAlreadyExists(ex)).isEqualTo(true);
  }

  @Test
  public void testBucketAlreadyExists() {
    Exception ex = new Exception(new StatusRuntimeException(Status.ALREADY_EXISTS));
    assertThat(typeExtractor.bucketAlreadyExists(ex)).isEqualTo(true);
  }

  @Test
  public void testBucketAlreadyExistsInvalidException() {
    Exception ex = new StatusRuntimeException(Status.ABORTED);
    assertThat(typeExtractor.bucketAlreadyExists(ex)).isEqualTo(false);
  }
}
