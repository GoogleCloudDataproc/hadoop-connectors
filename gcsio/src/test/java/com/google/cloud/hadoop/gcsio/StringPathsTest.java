package com.google.cloud.hadoop.gcsio;

import static com.google.cloud.hadoop.gcsio.StringPaths.V2_BUCKET_NAME_PREFIX;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import org.junit.Test;

public class StringPathsTest {

  @Test
  public void toV2BucketNameConvertsV1BucketName() {
    String bucketName = "bucketName";
    String v2BucketName = StringPaths.toV2BucketName(bucketName);
    assertThat(v2BucketName).isNotNull();
    assertThat(v2BucketName).isNotEmpty();
    assertThat(v2BucketName).isEqualTo(V2_BUCKET_NAME_PREFIX + bucketName);
  }

  @Test
  public void toV2BucketNameThrowsExceptionOnNull() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          StringPaths.toV2BucketName(null);
        });
  }

  @Test
  public void toV2BucketNameThrowsExceptionOnEmpty() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          StringPaths.toV2BucketName("");
        });
  }
}
