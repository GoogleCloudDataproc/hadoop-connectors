/*
 * Copyright 2014 Google Inc.
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

import com.google.auto.value.AutoValue;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/** Configurable options for the {@link GoogleCloudStorageFileSystem} class. */
@AutoValue
public abstract class GoogleCloudStorageFileSystemOptions {

  public static GoogleCloudStorageFileSystemOptions DEFAULT = builder().build();

  /** Cloud Storage client to use. */
  public enum ClientType {
    HTTP_API_CLIENT,
    STORAGE_CLIENT,
  }

  public static Builder builder() {
    return new AutoValue_GoogleCloudStorageFileSystemOptions.Builder()
        .setBucketDeleteEnabled(false)
        .setClientType(ClientType.HTTP_API_CLIENT)
        .setCloudStorageOptions(GoogleCloudStorageOptions.DEFAULT)
        .setEnsureNoConflictingItems(true)
        .setMarkerFilePattern((String) null)
        .setPerformanceCacheEnabled(false)
        .setPerformanceCacheOptions(PerformanceCachingGoogleCloudStorageOptions.DEFAULT)
        .setStatusParallelEnabled(true)
        .setListDirectoryFirstEnabled(false)
        .setCloudLoggingEnabled(false);
  }

  public abstract Builder toBuilder();

  public abstract boolean isPerformanceCacheEnabled();

  public abstract ClientType getClientType();

  public abstract PerformanceCachingGoogleCloudStorageOptions getPerformanceCacheOptions();

  public abstract GoogleCloudStorageOptions getCloudStorageOptions();

  public abstract boolean isBucketDeleteEnabled();

  @Nullable
  public abstract Pattern getMarkerFilePattern();

  public abstract boolean isStatusParallelEnabled();

  public abstract boolean isListDirectoryFirstEnabled();

  public abstract boolean isCloudLoggingEnabled();

  public abstract boolean isEnsureNoConflictingItems();

  public void throwIfNotValid() {
    getCloudStorageOptions().throwIfNotValid();
  }

  /** Mutable builder for {@link GoogleCloudStorageFileSystemOptions}. */
  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setClientType(ClientType clientType);

    public abstract Builder setPerformanceCacheEnabled(boolean performanceCacheEnabled);

    public abstract Builder setPerformanceCacheOptions(
        PerformanceCachingGoogleCloudStorageOptions options);

    public abstract Builder setCloudStorageOptions(GoogleCloudStorageOptions options);

    public abstract Builder setBucketDeleteEnabled(boolean bucketDeleteEnabled);

    abstract Builder setMarkerFilePattern(Pattern markerFilePattern);

    public Builder setMarkerFilePattern(String markerFilePattern) {
      return setMarkerFilePattern(
          markerFilePattern == null ? null : Pattern.compile("^(.+/)?" + markerFilePattern + "$"));
    }

    /**
     * Enables parallel execution of GCS requests in {@code listFileInfo} and {@code getFileInfo}
     * methods to reduce latency.
     */
    public abstract Builder setStatusParallelEnabled(boolean statusParallelEnabled);

    /**
     * Makes {@code listFileInfo} list a path without a trailing '/' as a directory first, and
     * look up the object with that name only if the directory listing is empty. This saves an
     * {@code objects.get} on every directory. When both an object {@code a} and a directory {@code
     * a/} exist, the directory's contents are returned instead of the object.
     */
    public abstract Builder setListDirectoryFirstEnabled(boolean listDirectoryFirstEnabled);

    public abstract Builder setCloudLoggingEnabled(boolean cloudLoggingEnabled);

    public abstract Builder setEnsureNoConflictingItems(boolean ensureNoConflictingItems);

    public abstract GoogleCloudStorageFileSystemOptions build();
  }
}
