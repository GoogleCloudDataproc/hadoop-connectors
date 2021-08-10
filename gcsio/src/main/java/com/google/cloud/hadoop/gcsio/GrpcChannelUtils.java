package com.google.cloud.hadoop.gcsio;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

public class GrpcChannelUtils {

  static final String V2_BUCKET_NAME_PREFIX = "projects/_/buckets/";

  /**
   * @param bucketName name of the bucket
   * @return bucket name in to GCS v2 bucket name format
   */
  static String toV2BucketName(String bucketName) {
    checkArgument(!isNullOrEmpty(bucketName), "bucketName cannot be null or empty");
    return V2_BUCKET_NAME_PREFIX + bucketName;
  }
}
