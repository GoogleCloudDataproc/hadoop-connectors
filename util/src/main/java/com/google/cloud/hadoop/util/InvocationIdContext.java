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
  public static void clear() {
    INVOCATION_ID.remove();
  }
}
