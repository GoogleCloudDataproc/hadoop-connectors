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

  /** Number of hex characters of the operation ID (the first group of a UUID). */
  private static final int OPERATION_ID_LENGTH = 8;

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
    INVOCATION_ID.set(newInvocationId());
  }

  /** Generates a new operation ID, prefixed with {@code gccl-invocation-id/}. */
  public static String newInvocationId() {
    return GCCL_INVOCATION_ID_PREFIX
        + UUID.randomUUID().toString().substring(0, OPERATION_ID_LENGTH);
  }

  /**
   * Generates a unique {@code gccl-invocation-id/<uuid>} token for a single HTTP request. If the
   * current thread has an invocation ID, its 8 hex characters replace the first UUID group so the
   * request can be correlated with the connector operation, e.g. {@code
   * gccl-invocation-id/a1b2c3d4-4907-4e6f-829f-eb303246e83b} for {@code
   * gccl-invocation-id/a1b2c3d4}. Does not modify the thread's invocation ID.
   */
  public static String newRequestInvocationId() {
    String requestUuid = UUID.randomUUID().toString();
    String operationId = getOperationId();
    if (operationId.isEmpty()) {
      return GCCL_INVOCATION_ID_PREFIX + requestUuid;
    }
    return GCCL_INVOCATION_ID_PREFIX + operationId + requestUuid.substring(OPERATION_ID_LENGTH);
  }

  /** Returns the current thread's operation ID without the prefix, or an empty string. */
  private static String getOperationId() {
    String invocationId = INVOCATION_ID.get();
    if (invocationId.startsWith(GCCL_INVOCATION_ID_PREFIX)) {
      invocationId = invocationId.substring(GCCL_INVOCATION_ID_PREFIX.length());
    }
    return invocationId.length() >= OPERATION_ID_LENGTH
        ? invocationId.substring(0, OPERATION_ID_LENGTH)
        : "";
  }

  /** Clears the invocation ID for the current thread. */
  @VisibleForTesting
  static void clear() {
    INVOCATION_ID.remove();
  }
}
