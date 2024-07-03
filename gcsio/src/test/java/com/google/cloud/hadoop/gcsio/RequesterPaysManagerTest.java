/*
 * Copyright 2024 Google LLC
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cloud.hadoop.util.RequesterPaysOptions;
import com.google.cloud.hadoop.util.RequesterPaysOptions.RequesterPaysMode;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class RequesterPaysManagerTest {
  private final String requesterPaysBucket = "RequesterPays-enabled";
  private final List<String> requesterPaysBuckets = Arrays.asList(requesterPaysBucket);
  private final String requesterPayDisabledBucket = "RequesterPays-disabled";

  private int shouldRequesterPayCallCountCounter = 0;

  @Test
  public void requesterPaysAutoMode() {
    RequesterPaysOptions options =
        RequesterPaysOptions.DEFAULT.toBuilder().setMode(RequesterPaysMode.AUTO).build();
    RequesterPaysManager manager = new RequesterPaysManager(options, this::shouldRequesterPays);

    // cache lookup will not be done if mode is `Auto`

    assertThat(shouldRequesterPayCallCountCounter).isEqualTo(0);
    assertTrue(manager.requesterShouldPay(requesterPaysBucket));
    assertThat(shouldRequesterPayCallCountCounter).isEqualTo(1);
    // any other call should be served from cache
    assertTrue(manager.requesterShouldPay(requesterPaysBucket));
    assertThat(shouldRequesterPayCallCountCounter).isEqualTo(1);

    // user-project header is only set for requests belonging to requesterPays enabled buckets
    assertFalse(manager.requesterShouldPay(requesterPayDisabledBucket));
    assertThat(shouldRequesterPayCallCountCounter).isEqualTo(2);
    // any other call should be served from cache
    assertFalse(manager.requesterShouldPay(requesterPayDisabledBucket));
    assertThat(shouldRequesterPayCallCountCounter).isEqualTo(2);
  }

  @Test
  public void requesterPaysCustomMode() {
    RequesterPaysOptions options =
        RequesterPaysOptions.DEFAULT
            .toBuilder()
            .setMode(RequesterPaysMode.CUSTOM)
            .setBuckets(requesterPaysBuckets)
            .build();
    RequesterPaysManager manager = new RequesterPaysManager(options, this::shouldRequesterPays);

    // cache lookup will not be done if mode is `Custom`
    // user-project header is only set for requests belonging to allowlisted buckets
    assertTrue(manager.requesterShouldPay(requesterPaysBucket));
    assertThat(shouldRequesterPayCallCountCounter).isEqualTo(0);

    assertFalse(manager.requesterShouldPay(requesterPayDisabledBucket));
    assertThat(shouldRequesterPayCallCountCounter).isEqualTo(0);
  }

  @Test
  public void requesterPaysEnableMode() {
    RequesterPaysOptions options =
        RequesterPaysOptions.DEFAULT.toBuilder().setMode(RequesterPaysMode.ENABLED).build();
    RequesterPaysManager manager = new RequesterPaysManager(options, this::shouldRequesterPays);
    // cache lookup not done if mode is `Enabled`
    // user-project header is set for all requests
    assertTrue(manager.requesterShouldPay(requesterPaysBucket));
    assertThat(shouldRequesterPayCallCountCounter).isEqualTo(0);
    assertTrue(manager.requesterShouldPay(requesterPayDisabledBucket));
    assertThat(shouldRequesterPayCallCountCounter).isEqualTo(0);
  }

  @Test
  public void requesterPaysDisableMode() {
    RequesterPaysOptions options =
        RequesterPaysOptions.DEFAULT.toBuilder().setMode(RequesterPaysMode.DISABLED).build();
    RequesterPaysManager manager = new RequesterPaysManager(options, this::shouldRequesterPays);

    // cache lookup is not done if mode is `Disabled`
    // user-project header is not set for any request
    assertFalse(manager.requesterShouldPay(requesterPaysBucket));
    assertThat(shouldRequesterPayCallCountCounter).isEqualTo(0);
    assertFalse(manager.requesterShouldPay(requesterPayDisabledBucket));
    assertThat(shouldRequesterPayCallCountCounter).isEqualTo(0);
  }

  private boolean shouldRequesterPays(String bucketName) {
    // updating counter of function call
    shouldRequesterPayCallCountCounter++;
    return requesterPaysBuckets.contains(bucketName);
  }
}
