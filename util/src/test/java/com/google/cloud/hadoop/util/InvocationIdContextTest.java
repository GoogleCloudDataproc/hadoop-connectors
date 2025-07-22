package com.google.cloud.hadoop.util;

import static org.junit.Assert.*;

import com.google.cloud.hadoop.util.interceptors.InvocationIdInterceptor;
import org.junit.Before;
import org.junit.Test;

public class InvocationIdContextTest {

  @Before
  public void clearInvocationId() {
    // Ensure the INVOCATION_ID is cleared before each test
    InvocationIdContext.clear();
  }

  @Test
  public void testInitialValue() {
    // Test that the initial value of INVOCATION_ID is an empty string
    assertEquals("", InvocationIdContext.getInvocationId());
  }

  @Test
  public void testSetInvocationId() {
    // Set a new invocation ID and verify it is not empty
    InvocationIdContext.setInvocationId();

    String invocationId = InvocationIdContext.getInvocationId();
    String uuidPart =
        invocationId.substring(InvocationIdInterceptor.GCCL_INVOCATION_ID_PREFIX.length());

    // Verify that the invocation ID is not empty
    assertNotEquals("", invocationId);
    // Verify the format of the invocation ID
    assertTrue(invocationId.startsWith(InvocationIdInterceptor.GCCL_INVOCATION_ID_PREFIX));
    assertEquals(8, uuidPart.length());
  }

  @Test
  public void testClearInvocationId() {
    // Set an invocation ID, clear it, and verify it is reset to an empty string
    InvocationIdContext.setInvocationId();

    InvocationIdContext.clear();

    assertEquals("", InvocationIdContext.getInvocationId());
  }

  @Test
  public void testThreadLocalIsolation() throws InterruptedException {
    // Test that the INVOCATION_ID is isolated across threads
    InvocationIdContext.setInvocationId();
    String mainThreadId = InvocationIdContext.getInvocationId();

    Thread thread =
        new Thread(
            () -> {
              InvocationIdContext.setInvocationId();
              assertNotEquals(mainThreadId, InvocationIdContext.getInvocationId());
            });
    thread.start();
    thread.join();

    // Ensure the main thread's invocation ID remains unchanged
    assertEquals(mainThreadId, InvocationIdContext.getInvocationId());
  }

  @Test
  public void testChildThreadInheritsInvocationId() throws InterruptedException {
    // Set an invocation ID in the parent thread
    InvocationIdContext.setInvocationId();
    String parentThreadId = InvocationIdContext.getInvocationId();

    // Create a child thread and verify it inherits the same invocation ID
    Thread childThread =
        new Thread(() -> assertEquals(parentThreadId, InvocationIdContext.getInvocationId()));
    childThread.start();
    childThread.join();

    // Ensure the parent thread's invocation ID remains unchanged
    assertEquals(parentThreadId, InvocationIdContext.getInvocationId());
  }
}
