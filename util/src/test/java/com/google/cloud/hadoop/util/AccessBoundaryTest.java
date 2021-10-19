/*
 * Copyright 2014 Google Inc. All Rights Reserved.
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

package com.google.cloud.hadoop.util;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.hadoop.util.AccessBoundary.Action;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class AccessBoundaryTest {

  /**
   * This test is to confirm the enum names don't change announced. The Action enum is a downstream
   * contract that shouldn't change current values to avoid breaking changes.
   */
  @Test
  public void Action_enumNames() {
    assertThat(Action.LIST_OBJECTS.name()).isEqualTo("LIST_OBJECTS");
    assertThat(Action.READ_OBJECTS.name()).isEqualTo("READ_OBJECTS");
    assertThat(Action.WRITE_OBJECTS.name()).isEqualTo("WRITE_OBJECTS");
    assertThat(Action.EDIT_OBJECTS.name()).isEqualTo("EDIT_OBJECTS");
    assertThat(Action.GET_BUCKETS.name()).isEqualTo("GET_BUCKETS");
    assertThat(Action.CREATE_BUCKETS.name()).isEqualTo("CREATE_BUCKETS");
    assertThat(Action.DELETE_BUCKETS.name()).isEqualTo("DELETE_BUCKETS");
    assertThat(Action.LIST_BUCKETS.name()).isEqualTo("LIST_BUCKETS");
  }
}
