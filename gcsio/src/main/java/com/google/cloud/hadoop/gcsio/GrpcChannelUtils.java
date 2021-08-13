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
