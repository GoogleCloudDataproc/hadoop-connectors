/*
 * Copyright 2026 Google LLC
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

import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class InvocationIdPropagatingExecutorServiceTest {

  private ExecutorService executor;

  @Before
  public void setUp() {
    // Single thread so that the same worker is reused across tasks and leaks would be observable.
    executor = InvocationIdPropagatingExecutorService.wrap(Executors.newSingleThreadExecutor());
  }

  @After
  public void tearDown() {
    executor.shutdownNow();
    InvocationIdContext.clear();
  }

  private static final Callable<String> GET_INVOCATION_ID = InvocationIdContext::getInvocationId;

  @Test
  public void wrap_isIdempotent() {
    assertThat(InvocationIdPropagatingExecutorService.wrap(executor)).isSameInstanceAs(executor);
  }

  @Test
  public void submit_propagatesSubmitterIdAndCleansUpWorker() throws Exception {
    String operationId;
    String idSeenByTask;
    try (InvocationIdContext.Scope ignored = InvocationIdContext.openScope()) {
      operationId = InvocationIdContext.getInvocationId();
      idSeenByTask = executor.submit(GET_INVOCATION_ID).get();
    }

    assertThat(idSeenByTask).isEqualTo(operationId);
    // The worker thread must not retain the ID once the task has finished.
    assertThat(executor.submit(GET_INVOCATION_ID).get()).isEmpty();
  }

  @Test
  public void submit_capturesIdAtSubmitTimeNotAtExecutionTime() throws Exception {
    CountDownLatch release = new CountDownLatch(1);
    // Block the single worker so that the next task is queued while the scope is still active.
    executor.submit(
        () -> {
          release.await();
          return null;
        });

    Future<String> queued;
    String operationId;
    try (InvocationIdContext.Scope ignored = InvocationIdContext.openScope()) {
      operationId = InvocationIdContext.getInvocationId();
      queued = executor.submit(GET_INVOCATION_ID);
    }
    // Scope is closed on the submitter before the task runs.
    assertThat(InvocationIdContext.getInvocationId()).isEmpty();
    release.countDown();

    assertThat(queued.get(10, TimeUnit.SECONDS)).isEqualTo(operationId);
  }

  @Test
  public void execute_propagatesSubmitterId() throws Exception {
    CountDownLatch done = new CountDownLatch(1);
    String[] idSeenByTask = new String[1];
    String operationId;
    try (InvocationIdContext.Scope ignored = InvocationIdContext.openScope()) {
      operationId = InvocationIdContext.getInvocationId();
      executor.execute(
          () -> {
            idSeenByTask[0] = InvocationIdContext.getInvocationId();
            done.countDown();
          });
      assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
    }

    assertThat(idSeenByTask[0]).isEqualTo(operationId);
    assertThat(executor.submit(GET_INVOCATION_ID).get()).isEmpty();
  }

  @Test
  public void invokeAllAndInvokeAny_propagateSubmitterId() throws Exception {
    String operationId;
    List<Future<String>> all;
    String any;
    try (InvocationIdContext.Scope ignored = InvocationIdContext.openScope()) {
      operationId = InvocationIdContext.getInvocationId();
      all = executor.invokeAll(ImmutableList.of(GET_INVOCATION_ID, GET_INVOCATION_ID));
      any = executor.invokeAny(ImmutableList.of(GET_INVOCATION_ID));
    }

    for (Future<String> future : all) {
      assertThat(future.get()).isEqualTo(operationId);
    }
    assertThat(any).isEqualTo(operationId);
    assertThat(executor.submit(GET_INVOCATION_ID).get()).isEmpty();
  }

  @Test
  public void submit_withoutActiveId_taskSeesNoId() throws Exception {
    assertThat(executor.submit(GET_INVOCATION_ID).get()).isEmpty();
  }

  @Test
  public void submit_taskOpeningOwnScope_doesNotLeakIntoNextTask() throws Exception {
    String idInsideOwnScope =
        executor
            .submit(
                () -> {
                  try (InvocationIdContext.Scope ignored = InvocationIdContext.openScope()) {
                    return InvocationIdContext.getInvocationId();
                  }
                })
            .get();

    assertThat(idInsideOwnScope).isNotEmpty();
    assertThat(executor.submit(GET_INVOCATION_ID).get()).isEmpty();
  }
}
