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
import java.time.Duration;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Options that can be specified when creating a file in the {@link GoogleCloudStorageFileSystem}.
 */
@AutoValue
public abstract class CreateFileOptions {

  public static final CreateFileOptions DEFAULT_APPEND =
      builder()
          .setWriteMode(WriteMode.APPEND)
          .setWriteGenerationId(StorageResourceId.UNKNOWN_GENERATION_ID)
          .build();
  public static final CreateFileOptions DEFAULT_CREATE_NEW = builder().build();
  public static final CreateFileOptions DEFAULT_OVERWRITE =
      builder()
          .setWriteMode(WriteMode.OVERWRITE)
          .setWriteGenerationId(StorageResourceId.UNKNOWN_GENERATION_ID)
          .build();

  public enum WriteMode {
    /** Write new bytes to the end of the existing file rather than the beginning. */
    APPEND,
    /** Creates a new file for write and fails if file already exists. */
    CREATE_NEW,
    /** Creates a new file for write or overwrites an existing file if it already exists. */
    OVERWRITE;
  }

  public static Builder builder() {
    return new AutoValue_CreateFileOptions.Builder()
        .setAttributes(ImmutableMap.of())
        .setContentType(CreateObjectOptions.CONTENT_TYPE_DEFAULT)
        .setEnsureNoDirectoryConflict(true)
        .setMinSyncInterval(Duration.ofSeconds(10))
        .setWriteMode(WriteMode.CREATE_NEW)
        .setWriteGenerationId(0);
  }

  public abstract Builder toBuilder();

  /** Extended attributes to set when creating a file. */
  public abstract ImmutableMap<String, byte[]> getAttributes();

  /** Content-type to set when creating a file. */
  @Nullable
  public abstract String getContentType();

  /** Configures the minimum time interval (milliseconds) between consecutive sync/flush calls */
  public abstract Duration getMinSyncInterval();

  /**
   * If true, makes sure there isn't already a directory object of the same name. If false, you run
   * the risk of creating hard-to-cleanup/access files whose names collide with directory names. If
   * already sure no such directory exists, then this is safe to set for improved performance.
   */
  public abstract boolean isEnsureNoDirectoryConflict();

  /** Whether to overwrite an existing file with the same name. */
  public abstract WriteMode getWriteMode();

  /**
   * Generation of existing object to overwrite. Ignored if set to {@link
   * StorageResourceId#UNKNOWN_GENERATION_ID}, but otherwise this is used instead of {@code
   * overwriteExisting}, where 0 indicates no existing object, and otherwise an existing object will
   * only be overwritten by the newly created file if its generation matches this provided
   * generationId.
   */
  public abstract long getWriteGenerationId();

  /** Builder for {@link CreateFileOptions} */
  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setAttributes(Map<String, byte[]> attributes);

    public abstract Builder setContentType(String contentType);

    public abstract Builder setEnsureNoDirectoryConflict(boolean ensureNoDirectoryConflict);

    public abstract Builder setMinSyncInterval(Duration interval);

    public abstract Builder setWriteGenerationId(long generationId);

    public abstract Builder setWriteMode(WriteMode mode);

    abstract CreateFileOptions autoBuild();

    public CreateFileOptions build() {
      CreateFileOptions options = autoBuild();
      checkArgument(
          !options.getAttributes().containsKey("Content-Type"),
          "The Content-Type attribute must be provided explicitly via the 'contentType' parameter");
      switch (options.getWriteMode()) {
        case APPEND:
          checkArgument(options.getWriteGenerationId() == StorageResourceId.UNKNOWN_GENERATION_ID);
          break;
        case CREATE_NEW:
          checkArgument(options.getWriteGenerationId() == 0);
          break;
        case OVERWRITE:
          checkArgument(options.getWriteGenerationId() != 0);
          break;
      }
      return options;
    }
  }
}
