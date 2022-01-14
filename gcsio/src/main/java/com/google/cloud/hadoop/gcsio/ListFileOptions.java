/*
 * Copyright 2020 Google LLC
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

package com.google.cloud.hadoop.gcsio;

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl.OBJECT_FIELDS;

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;

/** Options that can be specified when listing files in the {@link GoogleCloudStorageFileSystem}. */
@AutoValue
public abstract class ListFileOptions {

  /** List all files in the directory. */
  public static final ListFileOptions DEFAULT = builder().build();

  public static Builder builder() {
    return new AutoValue_ListFileOptions.Builder().setFields(OBJECT_FIELDS).setRecursive(true);
  }

  public abstract Builder toBuilder();

  /**
   * Comma separated list of object fields to include in the list response.
   *
   * <p>See <a
   * href="https://cloud.google.com/storage/docs/json_api/v1/objects#resource-representations">
   * object resource</a> for reference.
   */
  @Nullable
  public abstract String getFields();

  public abstract boolean getRecursive();


  /** Builder for {@link ListFileOptions} */
  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setFields(String delimiter);

    public abstract Builder setRecursive(boolean recursive);

    public abstract ListFileOptions build();
  }
}
