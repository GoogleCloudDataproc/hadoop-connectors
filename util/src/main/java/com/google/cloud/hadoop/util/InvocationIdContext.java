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

package com.google.cloud.hadoop.util;

import static com.google.cloud.hadoop.util.interceptors.InvocationIdInterceptor.GCCL_INVOCATION_ID_PREFIX;

import com.google.common.annotations.VisibleForTesting;
import java.util.UUID;

/**
 * A utility class to manage a unique invocation ID for each thread using {@link
 * InheritableThreadLocal}. The invocation ID is used to track and identify requests across
 * different components.
 */
public class InvocationIdContext {

  /** Thread-local storage for the invocation ID. The ID is inherited by child threads. */
  private static final InheritableThreadLocal<String> INVOCATION_ID =
      new InheritableThreadLocal<>() {
        @Override
        protected String initialValue() {
          return "";
        }
      };

  /**
   * Retrieves the current thread's invocation ID.
   *
   * @return the invocation ID for the current thread, or an empty string if not set
   */
  public static String getInvocationId() {
    return INVOCATION_ID.get();
  }

  /**
   * Generates and sets a new unique invocation ID for the current thread. The ID is prefixed with
   * {@code GCCL_INVOCATION_ID_PREFIX}.
   */
  public static void setInvocationId() {
    String uuid = UUID.randomUUID().toString().substring(0, 8);
    INVOCATION_ID.set(GCCL_INVOCATION_ID_PREFIX + uuid);
  }

  /** Clears the invocation ID for the current thread. */
  @VisibleForTesting
  static void clear() {
    INVOCATION_ID.remove();
  }
}
