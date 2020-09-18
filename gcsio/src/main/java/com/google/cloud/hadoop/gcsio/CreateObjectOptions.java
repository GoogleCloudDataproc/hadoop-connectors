/*
 * Copyright 2014 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.google.cloud.hadoop.gcsio;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import javax.annotation.Nullable;

/** Options that can be specified when creating a file in the {@link GoogleCloudStorage}. */
@AutoValue
public abstract class CreateObjectOptions {

  public static final String CONTENT_TYPE_DEFAULT = "application/octet-stream";

  public static final CreateObjectOptions DEFAULT_NO_OVERWRITE = builder().build();
  public static final CreateObjectOptions DEFAULT_OVERWRITE =
      builder().setOverwriteExisting(true).build();

  public static Builder builder() {
    return new AutoValue_CreateObjectOptions.Builder()
        .setContentEncoding(null)
        .setContentType(CONTENT_TYPE_DEFAULT)
        .setEnsureEmptyObjectsMetadataMatch(true)
        .setMetadata(ImmutableMap.of())
        .setOverwriteExisting(false);
  }

  public abstract Builder toBuilder();

  /** Content encoding for the created object. */
  @Nullable
  public abstract String getContentEncoding();

  /** Content type for the created object. */
  @Nullable
  public abstract String getContentType();

  /**
   * When creating an empty object and certain types of errors occur, any existing object is checked
   * for an exact metadata match to the metadata in this {@link CreateObjectOptions} before
   * accepting the creation as successful. If {@code false}, then on error for creating empty
   * objects, as long as an appropriate empty object already exists, even if it holds different
   * metadata than provided in this {@link CreateObjectOptions} instance, it may be considered
   * created successfully.
   */
  public abstract boolean isEnsureEmptyObjectsMetadataMatch();

  /** A metadata to apply to the create object. */
  public abstract ImmutableMap<String, byte[]> getMetadata();

  /** Whether to overwrite any existing objects with the same name */
  public abstract boolean isOverwriteExisting();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setContentEncoding(String contentEncoding);

    public abstract Builder setContentType(String contentType);

    public abstract Builder setEnsureEmptyObjectsMetadataMatch(
        boolean ensureEmptyObjectsMetadataMatch);

    public abstract Builder setMetadata(Map<String, byte[]> metadata);

    public abstract Builder setOverwriteExisting(boolean overwriteExisting);

    protected abstract CreateObjectOptions autoBuild();

    public CreateObjectOptions build() {
      CreateObjectOptions options = autoBuild();
      checkArgument(
          !options.getMetadata().containsKey("Content-Encoding"),
          "The Content-Encoding must be provided explicitly via the 'contentEncoding' parameter");
      checkArgument(
          !options.getMetadata().containsKey("Content-Type"),
          "The Content-Type must be provided explicitly via the 'contentType' parameter");
      return options;
    }
  }
}
