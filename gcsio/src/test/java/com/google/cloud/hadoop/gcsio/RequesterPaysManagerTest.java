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
  private final String requesterPayBucket = "RequesterPays-enabled";
  private final List<String> requesterPaysBuckets = Arrays.asList(requesterPayBucket);
  private final String payDisabledBucket = "RequesterPays-disabled";

  private int shouldRequesterPayCallCountCounter = 0;

  @Test
  public void requesterPaysAutoMode() {
    RequesterPaysOptions options =
        RequesterPaysOptions.DEFAULT.toBuilder().setMode(RequesterPaysMode.AUTO).build();
    RequesterPaysManager manager = new RequesterPaysManager(options, this::shouldRequesterPays);

    assertThat(shouldRequesterPayCallCountCounter).isEqualTo(0);
    assertTrue(manager.requesterShouldPay(requesterPayBucket));
    assertThat(shouldRequesterPayCallCountCounter).isEqualTo(1);
    // any other call should be served from cache
    assertTrue(manager.requesterShouldPay(requesterPayBucket));
    assertThat(shouldRequesterPayCallCountCounter).isEqualTo(1);

    assertFalse(manager.requesterShouldPay(payDisabledBucket));
    assertThat(shouldRequesterPayCallCountCounter).isEqualTo(2);
    // any other call should be served from cache
    assertFalse(manager.requesterShouldPay(payDisabledBucket));
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

    assertTrue(manager.requesterShouldPay(requesterPayBucket));
    // shouldRequesterPay is not called for custom mode
    assertThat(shouldRequesterPayCallCountCounter).isEqualTo(0);

    assertFalse(manager.requesterShouldPay(payDisabledBucket));
    // shouldRequesterPay is not called for custom mode
    assertThat(shouldRequesterPayCallCountCounter).isEqualTo(0);
  }

  @Test
  public void requesterPaysEnableMode() {
    RequesterPaysOptions options =
        RequesterPaysOptions.DEFAULT.toBuilder().setMode(RequesterPaysMode.ENABLED).build();
    RequesterPaysManager manager = new RequesterPaysManager(options, this::shouldRequesterPays);

    assertTrue(manager.requesterShouldPay(requesterPayBucket));
    // shouldRequesterPay is not called for custom mode
    assertThat(shouldRequesterPayCallCountCounter).isEqualTo(0);

    assertTrue(manager.requesterShouldPay(payDisabledBucket));
    // shouldRequesterPay is not called for custom mode
    assertThat(shouldRequesterPayCallCountCounter).isEqualTo(0);
  }

  @Test
  public void requesterPaysDisableMode() {
    RequesterPaysOptions options =
        RequesterPaysOptions.DEFAULT.toBuilder().setMode(RequesterPaysMode.DISABLED).build();
    RequesterPaysManager manager = new RequesterPaysManager(options, this::shouldRequesterPays);

    assertFalse(manager.requesterShouldPay(requesterPayBucket));
    // shouldRequesterPay is not called for custom mode
    assertThat(shouldRequesterPayCallCountCounter).isEqualTo(0);

    assertFalse(manager.requesterShouldPay(payDisabledBucket));
    // shouldRequesterPay is not called for custom mode
    assertThat(shouldRequesterPayCallCountCounter).isEqualTo(0);
  }

  private boolean shouldRequesterPays(String bucketName) {
    // updating counter of function call
    shouldRequesterPayCallCountCounter++;
    return requesterPaysBuckets.contains(bucketName);
  }
}
