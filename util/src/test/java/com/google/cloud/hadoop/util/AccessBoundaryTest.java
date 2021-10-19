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

import static org.junit.Assert.assertEquals;

import com.google.cloud.hadoop.util.AccessBoundary.Action;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class AccessBoundaryTest {

  /**
   * This test is to confirm the enum names don't change announced.
   * The Action enum is a downstream contract that shouldn't change current values to avoid breaking
   * changes.
   */
  @Test
  public void Action_enumNames() {
    assertEquals("LIST_OBJECTS", Action.LIST_OBJECTS.name());
    assertEquals("READ_OBJECTS", Action.READ_OBJECTS.name());
    assertEquals("WRITE_OBJECTS", Action.WRITE_OBJECTS.name());
    assertEquals("EDIT_OBJECTS", Action.EDIT_OBJECTS.name());
    assertEquals("GET_BUCKETS", Action.GET_BUCKETS.name());
    assertEquals("CREATE_BUCKETS", Action.CREATE_BUCKETS.name());
    assertEquals("DELETE_BUCKETS", Action.DELETE_BUCKETS.name());
    assertEquals("LIST_BUCKETS", Action.LIST_BUCKETS.name());
  }
}
