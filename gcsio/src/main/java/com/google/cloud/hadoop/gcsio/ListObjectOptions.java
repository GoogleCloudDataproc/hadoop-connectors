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

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorage.MAX_RESULTS_UNLIMITED;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorage.PATH_DELIMITER;

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;

/** Options that can be specified when listing objects in the {@link GoogleCloudStorage}. */
@AutoValue
public abstract class ListObjectOptions {

  /** List all objects in the directory. */
  public static final ListObjectOptions DEFAULT = builder().build();

  /** List all objects with the prefix. */
  public static final ListObjectOptions DEFAULT_FLAT_LIST = builder().setDelimiter(null).build();

  public static Builder builder() {
    return new AutoValue_ListObjectOptions.Builder()
        .setDelimiter(PATH_DELIMITER)
        .setMaxResults(MAX_RESULTS_UNLIMITED);
  }

  public abstract Builder toBuilder();

  /** Delimiter to use (typically {@code /}), otherwise {@code null}. */
  @Nullable
  public abstract String getDelimiter();

  /** Maximum number of results to return, unlimited if negative or zero. */
  public abstract long getMaxResults();

  /** Builder for {@link ListObjectOptions} */
  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setDelimiter(String delimiter);

    public abstract Builder setMaxResults(long maxResults);

    public abstract ListObjectOptions build();
  }
}
