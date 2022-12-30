/*
 * Copyright 2013 Google Inc.
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
import java.time.Duration;

/** Configurable options for {@link PerformanceCachingGoogleCloudStorage}. */
@AutoValue
public abstract class PerformanceCachingGoogleCloudStorageOptions {

  public static final PerformanceCachingGoogleCloudStorageOptions DEFAULT = builder().build();

  public static Builder builder() {
    return new AutoValue_PerformanceCachingGoogleCloudStorageOptions.Builder()
        .setMaxEntryAge(Duration.ofSeconds(5));
  }

  public abstract Builder toBuilder();

  /** Gets the max age of an item in cache in milliseconds. */
  public abstract Duration getMaxEntryAge();

  /** Builder class for PerformanceCachingGoogleCloudStorageOptions. */
  @AutoValue.Builder
  public abstract static class Builder {

    /** Sets the max age of an item in cache in milliseconds. */
    public abstract Builder setMaxEntryAge(Duration maxEntryAge);

    public abstract PerformanceCachingGoogleCloudStorageOptions build();
  }
}
