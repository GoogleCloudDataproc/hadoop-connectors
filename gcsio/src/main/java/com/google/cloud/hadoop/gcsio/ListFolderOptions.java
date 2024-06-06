/*
 * Copyright 2024 Google LLC
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

import com.google.auto.value.AutoValue;

/**
 * Options that can be specified when listing Folders in the {@link GoogleCloudStorage} and is valid
 * for only HN enabled bucket
 */
@AutoValue
public abstract class ListFolderOptions {

  /** List all folders in the directory. */
  public static final ListFolderOptions DEFAULT = builder().build();

  public static Builder builder() {
    return new AutoValue_ListFolderOptions.Builder().setPageSize(5000);
  }

  public abstract Builder toBuilder();

  /** Maximum folder resources in a single page */
  public abstract int getPageSize();

  /** Builder for {@link ListObjectOptions} */
  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setPageSize(int pageSize);

    public abstract ListFolderOptions build();
  }
}
