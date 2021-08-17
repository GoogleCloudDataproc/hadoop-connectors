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

import static com.google.cloud.hadoop.gcsio.GrpcChannelUtils.V2_BUCKET_NAME_PREFIX;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GrpcChannelUtilsTest {

  @Test
  public void toV2BucketNameConvertsV1BucketName() {
    String bucketName = "bucketName";
    String v2BucketName = GrpcChannelUtils.toV2BucketName(bucketName);
    assertThat(v2BucketName).isNotNull();
    assertThat(v2BucketName).isNotEmpty();
    assertThat(v2BucketName).isEqualTo(V2_BUCKET_NAME_PREFIX + bucketName);
  }

  @Test
  public void toV2BucketNameThrowsExceptionOnNull() {
    assertThrows(IllegalArgumentException.class, () -> GrpcChannelUtils.toV2BucketName(null));
  }

  @Test
  public void toV2BucketNameThrowsExceptionOnEmpty() {
    assertThrows(IllegalArgumentException.class, () -> GrpcChannelUtils.toV2BucketName(""));
  }
}
