/*
 * Copyright 2021 Google LLC
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.util;

import com.google.auto.value.AutoValue;

/** An access boundary used to generate a downscoped access token. */
@AutoValue
public abstract class AccessBoundary {
  public static AccessBoundary create(String bucketName, String objectName, Action action) {
    return new AutoValue_AccessBoundary(bucketName, objectName, action);
  }

  public abstract String bucketName();

  public abstract String objectName();

  public abstract Action action();

  public enum Action {
    UNSPECIFIED_ACTION("UNSPECIFIED_ACTION"),
    LIST_OBJECTS("LIST_OBJECTS"),
    READ_OBJECTS("READ_OBJECTS"),
    WRITE_OBJECTS("WRITE_OBJECTS"),
    EDIT_OBJECTS("EDIT_OBJECTS"),
    DELETE_OBJECTS("DELETE_OBJECTS"),
    GET_BUCKETS("GET_BUCKETS"),
    CREATE_BUCKETS("CREATE_BUCKETS"),
    DELETE_BUCKETS("DELETE_BUCKETS"),
    LIST_BUCKETS("LIST_BUCKETS");

    private String name;

    Action(String name) {
      this.name = name;
    }

    @Override
    public String toString() {
      return this.name;
    }
  }
}
