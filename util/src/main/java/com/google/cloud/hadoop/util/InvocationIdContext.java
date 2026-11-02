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
import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import java.util.UUID;

/**
 * A utility class to manage a unique invocation ID for each thread. The invocation ID is used to
 * track and identify requests of a connector operation across different components. It is stored in
 * a plain {@link ThreadLocal} and is set only through an {@link AutoCloseable} {@link Scope}, so
 * that it never outlives the operation or leaks into other threads.
 */
public class InvocationIdContext {

  /** Number of hex characters of the operation ID (the first group of a UUID). */
  private static final int OPERATION_ID_LENGTH = 8;

  private static final ThreadLocal<String> INVOCATION_ID = ThreadLocal.withInitial(() -> "");

  private static final Scope NOOP_SCOPE = () -> {};

  /**
   * A scope during which an invocation ID is active on the current thread. Scopes must be closed in
   * reverse order of opening, on the thread that opened them. Closing a scope more than once has no
   * further effect.
   */
  public interface Scope extends AutoCloseable {
    /** Ends the scope and restores the invocation ID that was active before it was opened. */
    @Override
    void close();
  }

  /**
   * Marks the beginning of a connector operation on the current thread. If an ID is already active
   * (nested call), it is reused and the returned scope is a no-op; otherwise a new ID is set until
   * the returned scope is closed. Use with try-with-resources.
   */
  public static Scope openScope() {
    return INVOCATION_ID.get().isEmpty() ? openScopeWithId(newInvocationId()) : NOOP_SCOPE;
  }

  /**
   * Activates the given invocation ID on the current thread until the returned scope is closed,
   * which restores the previously active ID. If the given ID is already active, the returned scope
   * is a no-op.
   *
   * @throws IllegalArgumentException if {@code invocationId} is null or empty
   */
  public static Scope openScopeWithId(String invocationId) {
    checkArgument(!Strings.isNullOrEmpty(invocationId), "invocationId must not be empty");
    String previousId = INVOCATION_ID.get();
    if (invocationId.equals(previousId)) {
      return NOOP_SCOPE;
    }
    INVOCATION_ID.set(invocationId);
    return new Scope() {
      private boolean closed;

      @Override
      public void close() {
        if (closed) {
          return;
        }
        closed = true;
        if (previousId.isEmpty()) {
          INVOCATION_ID.remove();
        } else {
          INVOCATION_ID.set(previousId);
        }
      }
    };
  }

  /**
   * Retrieves the current thread's invocation ID.
   *
   * @return the invocation ID for the current thread, or an empty string if not set
   */
  public static String getInvocationId() {
    return INVOCATION_ID.get();
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
