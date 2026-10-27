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
import static org.junit.Assert.assertThrows;

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
  public void newInvocationId_hasPrefixAndEightHexChars_andDoesNotSetContext() {
    String invocationId = InvocationIdContext.newInvocationId();

    assertThat(invocationId).matches(OPERATION_ID_PATTERN);
    assertThat(InvocationIdContext.newInvocationId()).isNotEqualTo(invocationId);
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
    assertThrows(
        IllegalStateException.class,
        () -> {
          try (InvocationIdContext.Scope ignored = InvocationIdContext.openScope()) {
            throw new IllegalStateException("operation failed");
          }
        });

    assertThat(InvocationIdContext.getInvocationId()).isEmpty();
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
  public void openScope_nested_reusesOuterIdAndKeepsItUntilOuterCloses() {
    try (InvocationIdContext.Scope outer = InvocationIdContext.openScope()) {
      String outerId = InvocationIdContext.getInvocationId();

      try (InvocationIdContext.Scope inner = InvocationIdContext.openScope()) {
        assertThat(InvocationIdContext.getInvocationId()).isEqualTo(outerId);
      }

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
  public void openScopeWithId_sameIdAlreadyActive_keepsIdUntilOuterCloses() {
    String streamId = InvocationIdContext.newInvocationId();

    try (InvocationIdContext.Scope outer = InvocationIdContext.openScopeWithId(streamId)) {
      try (InvocationIdContext.Scope inner = InvocationIdContext.openScopeWithId(streamId)) {
        assertThat(InvocationIdContext.getInvocationId()).isEqualTo(streamId);
      }
      assertThat(InvocationIdContext.getInvocationId()).isEqualTo(streamId);
    }

    assertThat(InvocationIdContext.getInvocationId()).isEmpty();
  }

  @Test
  public void openScopeWithId_nullOrEmptyId_throws() {
    assertThrows(IllegalArgumentException.class, () -> InvocationIdContext.openScopeWithId(""));
    assertThrows(IllegalArgumentException.class, () -> InvocationIdContext.openScopeWithId(null));

    assertThat(InvocationIdContext.getInvocationId()).isEmpty();
  }

  @Test
  public void scopeClose_calledTwice_doesNotRestoreStaleId() {
    InvocationIdContext.Scope outer = InvocationIdContext.openScope();
    InvocationIdContext.Scope inner =
        InvocationIdContext.openScopeWithId(InvocationIdContext.newInvocationId());

    inner.close();
    outer.close();
    inner.close();
    outer.close();

    assertThat(InvocationIdContext.getInvocationId()).isEmpty();
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

      assertThat(childInitialId.get()).isEmpty();
      assertThat(childScopedId.get()).matches(OPERATION_ID_PATTERN);
      assertThat(childScopedId.get()).isNotEqualTo(parentId);
      assertThat(InvocationIdContext.getInvocationId()).isEqualTo(parentId);
    }
  }

  @Test
  public void newRequestInvocationId_withoutOperation_isFullUuidAndDoesNotSetContext() {
    String requestId = InvocationIdContext.newRequestInvocationId();

    assertThat(requestId).matches(REQUEST_ID_PATTERN);
    assertThat(uuidOf(requestId).toString()).isEqualTo(uuidPartOf(requestId));
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
      assertThat(uuidOf(requestId1).toString()).isEqualTo(uuidPartOf(requestId1));
      assertThat(InvocationIdContext.getInvocationId()).isEqualTo(operationId);
    }
  }

  private static String uuidPartOf(String requestId) {
    return requestId.substring(GCCL_INVOCATION_ID_PREFIX.length());
  }

  private static UUID uuidOf(String requestId) {
    return UUID.fromString(uuidPartOf(requestId));
  }
}
