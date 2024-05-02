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

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorage.MAX_RESULTS_UNLIMITED;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorage.PATH_DELIMITER;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions.MAX_LIST_ITEMS_PER_CALL_DEFAULT;

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;

/**
 * Options that can be specified when listing Folders in the {@link GoogleCloudStorage} and is valid
 * for only HN enabled bucket
 */
@AutoValue
public abstract class ListFolderOptions {

  /** List all folders in the directory. */
  public static final ListFolderOptions DEFAULT = builder().build();

  /** List all folders with the prefix. */
  public static final ListFolderOptions DEFAULT_FOLDER_LIST = builder().setDelimiter(null).build();

  public static Builder builder() {
    return new AutoValue_ListFolderOptions.Builder()
        .setDelimiter(PATH_DELIMITER)
        .setMaxResults(MAX_RESULTS_UNLIMITED)
        .setPageSize(MAX_LIST_ITEMS_PER_CALL_DEFAULT);
  }

  public abstract Builder toBuilder();

  /** Delimiter to use (typically {@code /}), otherwise {@code null}. */
  @Nullable
  public abstract String getDelimiter();

  /** Maximum number of results to return, unlimited if negative or zero. */
  public abstract long getMaxResults();

  /** Maximum folder resources in a single page */
  public abstract long getPageSize();

  /**
   * Comma separated list of folder fields to include in the list response.
   *
   * <p>See <a href="">folder resource</a> for reference.
   */
  @Nullable
  public abstract String getFields();

  /** Builder for {@link ListObjectOptions} */
  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setDelimiter(String delimiter);

    public abstract Builder setMaxResults(long maxResults);

    public abstract Builder setPageSize(long pageSize);

    public abstract Builder setFields(String fields);

    public abstract ListFolderOptions build();
  }
}
