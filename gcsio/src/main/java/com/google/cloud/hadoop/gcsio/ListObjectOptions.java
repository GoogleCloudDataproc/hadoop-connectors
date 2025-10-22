/*
 * Copyright 2020 Google LLC
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

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorage.LIST_MAX_RESULTS;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorage.MAX_RESULTS_UNLIMITED;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorage.PATH_DELIMITER;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl.OBJECT_FIELDS;

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;

/** Options that can be specified when listing objects in the {@link GoogleCloudStorage}. */
@AutoValue
public abstract class ListObjectOptions {

  /** List all objects in the directory. */
  public static final ListObjectOptions DEFAULT = builder().build();

  /** List objects starting from an offset. */
  public static final ListObjectOptions DEFAULT_USING_START_OFFSET =
      builder().setDelimiter(null).setMaxResults(LIST_MAX_RESULTS).build();

  /** List all objects with the prefix. */
  public static final ListObjectOptions DEFAULT_FLAT_LIST = builder().setDelimiter(null).build();

  public static Builder builder() {
    return new AutoValue_ListObjectOptions.Builder()
        .setDelimiter(PATH_DELIMITER)
        .setIncludePrefix(false)
        .setIncludeFoldersAsPrefixes(false)
        .setMaxResults(MAX_RESULTS_UNLIMITED)
        .setFields(OBJECT_FIELDS);
  }

  public abstract Builder toBuilder();

  /** Delimiter to use (typically {@code /}), otherwise {@code null}. */
  @Nullable
  public abstract String getDelimiter();

  /** Whether to include prefix object in the result. */
  public abstract boolean isIncludePrefix();

  /** Whether to include empty folders in the result. */
  public abstract boolean isIncludeFoldersAsPrefixes();

  /** Maximum number of results to return, unlimited if negative or zero. */
  public abstract long getMaxResults();

  /**
   * Comma separated list of object fields to include in the list response.
   *
   * <p>See <a
   * href="https://cloud.google.com/storage/docs/json_api/v1/objects#resource-representations">
   * object resource</a> for reference.
   */
  @Nullable
  public abstract String getFields();

  /** Builder for {@link ListObjectOptions} */
  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setDelimiter(String delimiter);

    public abstract Builder setIncludePrefix(boolean includePrefix);

    public abstract Builder setIncludeFoldersAsPrefixes(boolean includeFoldersAsPrefixes);

    public abstract Builder setMaxResults(long maxResults);

    public abstract Builder setFields(String fields);

    public abstract ListObjectOptions build();
  }
}
