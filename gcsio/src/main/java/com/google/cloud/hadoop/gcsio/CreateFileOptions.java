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

/**
 * Options that can be specified when creating a file in the {@link GoogleCloudStorageFileSystem}.
 */
@AutoValue
public abstract class CreateFileOptions {

  public static final CreateFileOptions DEFAULT_NO_OVERWRITE = builder().build();
  public static final CreateFileOptions DEFAULT_OVERWRITE =
      builder().setOverwriteExisting(true).build();

  public static Builder builder() {
    return new AutoValue_CreateFileOptions.Builder()
        .setAttributes(ImmutableMap.of())
        .setContentType(CreateObjectOptions.CONTENT_TYPE_DEFAULT)
        .setEnsureNoDirectoryConflict(true)
        .setOverwriteExisting(false)
        .setOverwriteGenerationId(StorageResourceId.UNKNOWN_GENERATION_ID);
  }

  public abstract Builder toBuilder();

  /** Extended attributes to set when creating a file. */
  public abstract ImmutableMap<String, byte[]> getAttributes();

  /** Content-type to set when creating a file. */
  @Nullable
  public abstract String getContentType();

  /**
   * If true, makes sure there isn't already a directory object of the same name. If false, you run
   * the risk of creating hard-to-cleanup/access files whose names collide with directory names. If
   * already sure no such directory exists, then this is safe to set for improved performance.
   */
  public abstract boolean isEnsureNoDirectoryConflict();

  /** Whether to overwrite an existing file with the same name. */
  public abstract boolean isOverwriteExisting();

  /**
   * Generation of existing object to overwrite. Ignored if set to {@link
   * StorageResourceId#UNKNOWN_GENERATION_ID}, but otherwise this is used instead of {@code
   * overwriteExisting}, where 0 indicates no existing object, and otherwise an existing object will
   * only be overwritten by the newly created file if its generation matches this provided
   * generationId.
   */
  public abstract long getOverwriteGenerationId();

  /** Builder for {@link CreateFileOptions} */
  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setAttributes(Map<String, byte[]> attributes);

    public abstract Builder setContentType(String contentType);

    public abstract Builder setEnsureNoDirectoryConflict(boolean ensureNoDirectoryConflict);

    public abstract Builder setOverwriteGenerationId(long overwriteGenerationId);

    public abstract Builder setOverwriteExisting(boolean overwriteExisting);

    abstract CreateFileOptions autoBuild();

    public CreateFileOptions build() {
      CreateFileOptions options = autoBuild();
      checkArgument(
          !options.getAttributes().containsKey("Content-Type"),
          "The Content-Type attribute must be provided explicitly via the 'contentType' parameter");
      return options;
    }
  }
}
