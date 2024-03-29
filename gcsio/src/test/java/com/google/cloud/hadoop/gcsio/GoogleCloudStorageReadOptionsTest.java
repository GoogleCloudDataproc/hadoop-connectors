/*
 * Copyright 2019 Google LLC
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

package com.google.cloud.hadoop.gcsio;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GoogleCloudStorageReadOptionsTest {

  @Test
  public void build_throwsException_whenInplaceSeekLimitLowerThanZero() {
    long inplaceSeekLimit = -123;
    GoogleCloudStorageReadOptions.Builder builder =
        GoogleCloudStorageReadOptions.builder().setInplaceSeekLimit(inplaceSeekLimit);

    IllegalStateException e = assertThrows(IllegalStateException.class, builder::build);

    assertThat(e)
        .hasMessageThat()
        .isEqualTo("inplaceSeekLimit must be non-negative! Got " + inplaceSeekLimit);
  }
}
