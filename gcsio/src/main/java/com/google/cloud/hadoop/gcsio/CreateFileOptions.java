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
        .setEnsureParentDirectoriesExist(true)
        .setExistingGenerationId(StorageResourceId.UNKNOWN_GENERATION_ID)
        .setOverwriteExisting(false);
  }

  public abstract Builder toBuilder();

  /** Extended attributes to set when creating a file. */
  public abstract ImmutableMap<String, byte[]> getAttributes();

  /** Content-type to set when creating a file. */
  public abstract String getContentType();

  /**
   * If true, makes sure there isn't already a directory object of the same name. If false, you run
   * the risk of creating hard-to-cleanup/access files whose names collide with directory names. If
   * already sure no such directory exists, then this is safe to set for improved performance.
   */
  public abstract boolean isEnsureNoDirectoryConflict();

  /**
   * If true, ensures parent directories exist, creating them on-demand if they don't. If false, you
   * run the risk of creating objects without parent directories, which may degrade or break the
   * behavior of some filesystem functionality. If already sure parent directories exist, then this
   * is safe to set for improved performance.
   */
  public abstract boolean isEnsureParentDirectoriesExist();

  /**
   * Generation of existing object. Ignored if set to StorageResourceId.UNKNOWN_GENERATION_ID, but
   * otherwise this is used instead of {@code overwriteExisting}, where 0 indicates no existing
   * object, and otherwise an existing object will only be overwritten by the newly created file if
   * its generation matches this provided generationId.
   */
  public abstract long getExistingGenerationId();

  /** Whether to overwrite an existing file with the same name. */
  public abstract boolean isOverwriteExisting();

  /** Builder for {@link CreateFileOptions} */
  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setAttributes(Map<String, byte[]> attributes);

    public abstract Builder setContentType(String contentType);

    public abstract Builder setEnsureNoDirectoryConflict(boolean ensureNoDirectoryConflict);

    public abstract Builder setEnsureParentDirectoriesExist(boolean ensureParentDirectoriesExist);

    public abstract Builder setExistingGenerationId(long existingGenerationId);

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
