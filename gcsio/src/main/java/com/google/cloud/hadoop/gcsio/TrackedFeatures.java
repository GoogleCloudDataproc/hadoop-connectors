/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * WARNING: This file is parsed by an internal tool that relies on the format of this file.
 * Any changes that violate the rules below can break the tool and cause unwanted failures.
 * ### Rules for Editing This File ###
 * 1. **Constants First:** All enum constants MUST be declared at the top of
 *     the enum, before any other fields or methods.
 * 2. **Strict Constant Format:** Each constant MUST follow this exact pattern:
 *   `CONSTANT_NAME(integer_value),`
 * 3. **Single Integer Constructor:** The enum constructor MUST accept only a
 *   single `int` argument.
 */
package com.google.cloud.hadoop.gcsio;

/**
 * Enum representing features tracked via a bit field. Each feature corresponds to a specific bit
 * position.
 */
public enum TrackedFeatures {
  FADVISE_AUTO(0),

  FADVISE_RANDOM(1),

  FADVISE_SEQUENTIAL(2),

  FADVISE_AUTORANDOM(3),

  HIERARCHICAL_NAMESPACE_ENABLED(4),

  HNS_OPTIMIZATIONS_ENABLED(5),

  PERFORMANCE_CACHE_ENABLED(6),

  CLOUD_LOGGING_ENABLED(7),

  TRACE_LOG_ENABLED(8),

  OPERATION_TRACE_LOG_ENABLED(9),

  DIRECT_UPLOAD_ENABLED(10),

  BIDI_ENABLED(11),

  RENAME_API(12);

  private final int bitPosition;

  TrackedFeatures(int bitPosition) {
    this.bitPosition = bitPosition;
  }

  public int getBitPosition() {
    return bitPosition;
  }
}
