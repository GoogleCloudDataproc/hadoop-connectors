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
  public void setInvocationId_noExistingId_generatesValidId() {
    InvocationIdContext.setInvocationId();

    assertThat(InvocationIdContext.getInvocationId()).matches(OPERATION_ID_PATTERN);
  }

  @Test
  public void clear_whenIdExists_resetsToEmptyString() {
    InvocationIdContext.setInvocationId();

    InvocationIdContext.clear();

    assertThat(InvocationIdContext.getInvocationId()).isEmpty();
  }

  @Test
  public void setInvocationId_inNewThread_doesNotAffectParentThreadId()
      throws InterruptedException {
    InvocationIdContext.setInvocationId();
    String mainThreadId = InvocationIdContext.getInvocationId();
    AtomicReference<String> childThreadId = new AtomicReference<>();

    Thread thread =
        new Thread(
            () -> {
              InvocationIdContext.setInvocationId();
              childThreadId.set(InvocationIdContext.getInvocationId());
            });
    thread.start();
    thread.join();

    assertThat(childThreadId.get()).isNotEqualTo(mainThreadId);
    assertThat(InvocationIdContext.getInvocationId()).isEqualTo(mainThreadId);
  }

  @Test
  public void getInvocationId_inChildThread_inheritsParentId() throws InterruptedException {
    InvocationIdContext.setInvocationId();
    String parentThreadId = InvocationIdContext.getInvocationId();
    AtomicReference<String> childThreadId = new AtomicReference<>();

    Thread childThread = new Thread(() -> childThreadId.set(InvocationIdContext.getInvocationId()));
    childThread.start();
    childThread.join();

    assertThat(childThreadId.get()).isEqualTo(parentThreadId);
    assertThat(InvocationIdContext.getInvocationId()).isEqualTo(parentThreadId);
  }

  @Test
  public void newInvocationId_hasPrefixAndEightHexChars_andDoesNotSetContext() {
    String invocationId = InvocationIdContext.newInvocationId();

    assertThat(invocationId).matches(OPERATION_ID_PATTERN);
    assertThat(InvocationIdContext.newInvocationId()).isNotEqualTo(invocationId);
    assertThat(InvocationIdContext.getInvocationId()).isEmpty();
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
    InvocationIdContext.setInvocationId();
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

  private static String uuidPartOf(String requestId) {
    return requestId.substring(GCCL_INVOCATION_ID_PREFIX.length());
  }

  private static UUID uuidOf(String requestId) {
    return UUID.fromString(uuidPartOf(requestId));
  }
}
