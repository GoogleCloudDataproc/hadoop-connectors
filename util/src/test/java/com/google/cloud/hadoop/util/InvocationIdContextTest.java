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
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.*;

import com.google.cloud.hadoop.util.interceptors.InvocationIdInterceptor;
import java.util.UUID;
import java.util.regex.Pattern;
import org.junit.Before;
import org.junit.Test;

public class InvocationIdContextTest {

  @Before
  public void clearInvocationId() {
    // Ensure the INVOCATION_ID is cleared before each test
    InvocationIdContext.clear();
  }

  @Test
  public void getInvocationId_initialContext_returnsEmptyString() {
    // Test that the initial value of INVOCATION_ID is an empty string
    assertEquals("", InvocationIdContext.getInvocationId());
  }

  @Test
  public void setInvocationId_noExistingId_generatesValidId() {
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
  public void clear_whenIdExists_resetsToEmptyString() {
    // Set an invocation ID, clear it, and verify it is reset to an empty string
    InvocationIdContext.setInvocationId();

    InvocationIdContext.clear();

    assertEquals("", InvocationIdContext.getInvocationId());
  }

  @Test
  public void setInvocationId_inNewThread_doesNotAffectParentThreadId()
      throws InterruptedException {
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
  public void getInvocationId_inChildThread_inheritsParentId() throws InterruptedException {
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

  @Test
  public void newInvocationId_hasPrefixAndEightHexChars_andDoesNotSetContext() {
    String invocationId = InvocationIdContext.newInvocationId();

    assertThat(invocationId)
        .matches("^" + Pattern.quote(GCCL_INVOCATION_ID_PREFIX) + "[0-9a-f]{8}$");
    assertThat(InvocationIdContext.newInvocationId()).isNotEqualTo(invocationId);
    // Generating an ID does not activate it.
    assertThat(InvocationIdContext.getInvocationId()).isEmpty();
  }

  @Test
  public void newRequestInvocationId_withoutOperation_isFullUuidAndDoesNotSetContext() {
    String requestId = InvocationIdContext.newRequestInvocationId();

    assertThat(requestId).matches(REQUEST_ID_REGEX);
    assertThat(requestId).hasLength(GCCL_INVOCATION_ID_PREFIX.length() + 36);
    // Must be parseable as a UUID.
    assertThat(UUID.fromString(requestId.substring(GCCL_INVOCATION_ID_PREFIX.length())))
        .isNotNull();
    // Generating a request ID must never activate an operation ID on the thread.
    assertThat(InvocationIdContext.getInvocationId()).isEmpty();
    assertThat(InvocationIdContext.newRequestInvocationId()).isNotEqualTo(requestId);
  }

  @Test
  public void newRequestInvocationId_withOperation_sharesOperationPrefixAndIsUnique() {
    InvocationIdContext.setInvocationId();
    String operationId = InvocationIdContext.getInvocationId();

    String requestId1 = InvocationIdContext.newRequestInvocationId();
    String requestId2 = InvocationIdContext.newRequestInvocationId();

    assertThat(requestId1).matches(REQUEST_ID_REGEX);
    assertThat(requestId2).matches(REQUEST_ID_REGEX);
    assertThat(requestId1).startsWith(operationId + "-");
    assertThat(requestId2).startsWith(operationId + "-");
    assertThat(requestId1).isNotEqualTo(requestId2);
    assertThat(UUID.fromString(requestId1.substring(GCCL_INVOCATION_ID_PREFIX.length())))
        .isNotNull();
    // The operation ID is left untouched.
    assertThat(InvocationIdContext.getInvocationId()).isEqualTo(operationId);
  }

  private static final String REQUEST_ID_REGEX =
      "^"
          + Pattern.quote(GCCL_INVOCATION_ID_PREFIX)
          + "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$";
}
