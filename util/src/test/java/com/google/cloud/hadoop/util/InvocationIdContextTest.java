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

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class InvocationIdContextTest {

  private static final Pattern OPERATION_ID_PATTERN =
      Pattern.compile("^" + Pattern.quote(GCCL_INVOCATION_ID_PREFIX) + "[0-9a-f]{8}$");

  private static final Pattern REQUEST_ID_PATTERN =
      Pattern.compile(
          "^"
              + Pattern.quote(GCCL_INVOCATION_ID_PREFIX)
              + "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$");

  @Before
  @After
  public void clearInvocationId() {
    InvocationIdContext.clear();
  }

  @Test
  public void getInvocationId_initialContext_returnsEmptyString() {
    assertThat(InvocationIdContext.getInvocationId()).isEmpty();
  }

  @Test
  public void newInvocationId_hasPrefixAndEightHexChars() {
    String invocationId = InvocationIdContext.newInvocationId();

    assertThat(invocationId).matches(OPERATION_ID_PATTERN);
    assertThat(InvocationIdContext.newInvocationId()).isNotEqualTo(invocationId);
    // Generating an ID does not activate it.
    assertThat(InvocationIdContext.getInvocationId()).isEmpty();
  }

  @Test
  public void openScope_setsIdInsideAndClearsOnClose() {
    String invocationId;
    try (InvocationIdContext.Scope ignored = InvocationIdContext.openScope()) {
      invocationId = InvocationIdContext.getInvocationId();
      assertThat(invocationId).matches(OPERATION_ID_PATTERN);
    }

    assertThat(InvocationIdContext.getInvocationId()).isEmpty();

    try (InvocationIdContext.Scope ignored = InvocationIdContext.openScope()) {
      assertThat(InvocationIdContext.getInvocationId()).isNotEqualTo(invocationId);
    }
  }

  @Test
  public void openScope_clearsIdWhenOperationThrows() {
    try (InvocationIdContext.Scope ignored = InvocationIdContext.openScope()) {
      throw new IllegalStateException("operation failed");
    } catch (IllegalStateException expected) {
      // ignore
    }

    assertThat(InvocationIdContext.getInvocationId()).isEmpty();
  }

  @Test
  @SuppressWarnings("deprecation")
  public void setInvocationId_deprecated_setsIdUntilCleared() {
    InvocationIdContext.setInvocationId();
    String invocationId = InvocationIdContext.getInvocationId();

    assertThat(invocationId).matches(OPERATION_ID_PATTERN);

    InvocationIdContext.setInvocationId();
    assertThat(InvocationIdContext.getInvocationId()).isNotEqualTo(invocationId);

    InvocationIdContext.clear();
    assertThat(InvocationIdContext.getInvocationId()).isEmpty();
  }

  @Test
  @SuppressWarnings("deprecation")
  public void openScope_afterDeprecatedSetInvocationId_usesNewIdAndRestoresOldOnClose() {
    InvocationIdContext.setInvocationId();
    String unscopedId = InvocationIdContext.getInvocationId();

    // An ID that was not set by a scope must not be reused by a new operation.
    try (InvocationIdContext.Scope ignored = InvocationIdContext.openScope()) {
      assertThat(InvocationIdContext.getInvocationId()).matches(OPERATION_ID_PATTERN);
      assertThat(InvocationIdContext.getInvocationId()).isNotEqualTo(unscopedId);
    }

    assertThat(InvocationIdContext.getInvocationId()).isEqualTo(unscopedId);
  }

  @Test
  public void openScope_nestedInScopeWithId_reusesThatId() {
    String streamId = InvocationIdContext.newInvocationId();

    try (InvocationIdContext.Scope outer = InvocationIdContext.openScopeWithId(streamId)) {
      try (InvocationIdContext.Scope inner = InvocationIdContext.openScope()) {
        assertThat(InvocationIdContext.getInvocationId()).isEqualTo(streamId);
      }
      assertThat(InvocationIdContext.getInvocationId()).isEqualTo(streamId);
    }

    assertThat(InvocationIdContext.getInvocationId()).isEmpty();
  }

  @Test
  public void openScope_nestedInEmptyScopeWithId_setsNewId() {
    // A worker task submitted from a thread without an ID runs in an empty propagation scope; it
    // must still be able to start its own operation.
    try (InvocationIdContext.Scope outer = InvocationIdContext.openScopeWithId("")) {
      try (InvocationIdContext.Scope inner = InvocationIdContext.openScope()) {
        assertThat(InvocationIdContext.getInvocationId()).matches(OPERATION_ID_PATTERN);
      }
      assertThat(InvocationIdContext.getInvocationId()).isEmpty();
    }

    assertThat(InvocationIdContext.getInvocationId()).isEmpty();
  }

  @Test
  public void openScope_nested_reusesOuterIdAndKeepsItUntilOuterCloses() {
    try (InvocationIdContext.Scope outer = InvocationIdContext.openScope()) {
      String outerId = InvocationIdContext.getInvocationId();

      try (InvocationIdContext.Scope inner = InvocationIdContext.openScope()) {
        assertThat(InvocationIdContext.getInvocationId()).isEqualTo(outerId);
      }

      // Closing the inner scope must not clear the outer operation's ID.
      assertThat(InvocationIdContext.getInvocationId()).isEqualTo(outerId);
    }

    assertThat(InvocationIdContext.getInvocationId()).isEmpty();
  }

  @Test
  public void openScopeWithId_overridesAndRestoresPreviousId() {
    String streamId = InvocationIdContext.newInvocationId();

    try (InvocationIdContext.Scope outer = InvocationIdContext.openScope()) {
      String outerId = InvocationIdContext.getInvocationId();

      try (InvocationIdContext.Scope inner = InvocationIdContext.openScopeWithId(streamId)) {
        assertThat(InvocationIdContext.getInvocationId()).isEqualTo(streamId);
      }

      assertThat(InvocationIdContext.getInvocationId()).isEqualTo(outerId);
    }

    assertThat(InvocationIdContext.getInvocationId()).isEmpty();
  }

  @Test
  public void openScopeWithId_withoutPreviousId_clearsOnClose() {
    String streamId = InvocationIdContext.newInvocationId();

    try (InvocationIdContext.Scope ignored = InvocationIdContext.openScopeWithId(streamId)) {
      assertThat(InvocationIdContext.getInvocationId()).isEqualTo(streamId);
    }

    assertThat(InvocationIdContext.getInvocationId()).isEmpty();
  }

  @Test
  public void openScopeWithId_emptyId_deactivatesIdForScope() {
    try (InvocationIdContext.Scope outer = InvocationIdContext.openScope()) {
      String outerId = InvocationIdContext.getInvocationId();

      try (InvocationIdContext.Scope inner = InvocationIdContext.openScopeWithId("")) {
        assertThat(InvocationIdContext.getInvocationId()).isEmpty();
      }
      try (InvocationIdContext.Scope inner = InvocationIdContext.openScopeWithId(null)) {
        assertThat(InvocationIdContext.getInvocationId()).isEmpty();
      }

      assertThat(InvocationIdContext.getInvocationId()).isEqualTo(outerId);
    }
  }

  @Test
  public void openScope_inChildThread_doesNotInheritParentId() throws InterruptedException {
    AtomicReference<String> childInitialId = new AtomicReference<>();
    AtomicReference<String> childScopedId = new AtomicReference<>();

    try (InvocationIdContext.Scope ignored = InvocationIdContext.openScope()) {
      String parentId = InvocationIdContext.getInvocationId();

      Thread child =
          new Thread(
              () -> {
                childInitialId.set(InvocationIdContext.getInvocationId());
                try (InvocationIdContext.Scope childScope = InvocationIdContext.openScope()) {
                  childScopedId.set(InvocationIdContext.getInvocationId());
                }
              });
      child.start();
      child.join();

      // Plain ThreadLocal: nothing is inherited, the child mints its own ID in its own scope.
      assertThat(childInitialId.get()).isEmpty();
      assertThat(childScopedId.get()).matches(OPERATION_ID_PATTERN);
      assertThat(childScopedId.get()).isNotEqualTo(parentId);
      // The child's scope does not affect the parent.
      assertThat(InvocationIdContext.getInvocationId()).isEqualTo(parentId);
    }
  }

  @Test
  public void newRequestInvocationId_withoutOperation_isFullUuidAndDoesNotSetContext() {
    String requestId = InvocationIdContext.newRequestInvocationId();

    assertThat(requestId).matches(REQUEST_ID_PATTERN);
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
    try (InvocationIdContext.Scope ignored = InvocationIdContext.openScope()) {
      String operationId = InvocationIdContext.getInvocationId();

      String requestId1 = InvocationIdContext.newRequestInvocationId();
      String requestId2 = InvocationIdContext.newRequestInvocationId();

      assertThat(requestId1).matches(REQUEST_ID_PATTERN);
      assertThat(requestId2).matches(REQUEST_ID_PATTERN);
      assertThat(requestId1).startsWith(operationId + "-");
      assertThat(requestId2).startsWith(operationId + "-");
      assertThat(requestId1).isNotEqualTo(requestId2);
      assertThat(UUID.fromString(requestId1.substring(GCCL_INVOCATION_ID_PREFIX.length())))
          .isNotNull();
      // The operation ID is left untouched.
      assertThat(InvocationIdContext.getInvocationId()).isEqualTo(operationId);
    }
  }
}
