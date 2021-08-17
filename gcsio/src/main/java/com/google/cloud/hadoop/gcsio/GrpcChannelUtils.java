/*
 * Copyright 2021 Google Inc. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.gcsio;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

/** Utility methods for gRPC channel */
final class GrpcChannelUtils {

  private GrpcChannelUtils() {}

  static final String V2_BUCKET_NAME_PREFIX = "projects/_/buckets/";

  /**
   * @param v1BucketName name of the bucket
   * @return bucket name in to GCS v2 bucket name format
   */
  static String toV2BucketName(String v1BucketName) {
    checkArgument(!isNullOrEmpty(v1BucketName), "v1BucketName cannot be null or empty");
    return V2_BUCKET_NAME_PREFIX + v1BucketName;
  }
}
