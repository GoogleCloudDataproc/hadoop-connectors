/*
 * Copyright 2019 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.util;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.Lists;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link LazyExecutorService}. */
@RunWith(JUnit4.class)
public class LazyExecutorServiceTest {
  @Test
  public void testConstructorWithBackingService() {
    LazyExecutorService backingService = new LazyExecutorService();
    LazyExecutorService lazyExecutorService = new LazyExecutorService(backingService);
    assertThat(lazyExecutorService.isShutdown()).isFalse();
  }

  @Test
  public void testIsTerminated() {
    LazyExecutorService lazyExecutorService = new LazyExecutorService();
    lazyExecutorService.shutdown();
    assertThat(lazyExecutorService.isTerminated()).isTrue();
  }

  @Test
  public void testAwaitTermination() throws Exception {
    LazyExecutorService lazyExecutorService = new LazyExecutorService();
    assertThat(lazyExecutorService.awaitTermination(1, TimeUnit.MILLISECONDS)).isFalse();
    lazyExecutorService.shutdown();
    assertThat(lazyExecutorService.awaitTermination(1, TimeUnit.MILLISECONDS)).isTrue();
  }

  @Test
  public void testSubmitTask() {
    LazyExecutorService lazyExecutorService = new LazyExecutorService();
    Runnable runnable = () -> {};
    assertThat(lazyExecutorService.submit(runnable).isDone()).isTrue();
  }

  @Test
  public void testSubmitTaskWithResult() {
    LazyExecutorService lazyExecutorService = new LazyExecutorService();
    Runnable runnable = () -> {};
    assertThat(lazyExecutorService.submit(runnable, null).isDone()).isTrue();
  }

  @Test
  public void testInvokeAllSubmitAllTasks() throws Exception {
    LazyExecutorService lazyExecutorService = new LazyExecutorService();
    Callable callable = () -> null;
    List<Callable<Void>> monitorTasks = Lists.newLinkedList();
    monitorTasks.add(callable);
    monitorTasks.add(callable);

    assertThat(lazyExecutorService.invokeAll(monitorTasks).size()).isEqualTo(2);
  }

  @Test
  public void testInvokeAllSubmitAllTasksWithTimeout() throws Exception {
    LazyExecutorService lazyExecutorService = new LazyExecutorService();
    Callable callable = () -> null;
    List<Callable<Void>> monitorTasks = Lists.newLinkedList();
    monitorTasks.add(callable);
    monitorTasks.add(callable);

    assertThat(lazyExecutorService.invokeAll(monitorTasks, 1, TimeUnit.MILLISECONDS).size())
        .isEqualTo(2);
  }

  @Test
  public void testBackingService_shouldBeShutDownWithMainService() {
    LazyExecutorService backingService = new LazyExecutorService();
    LazyExecutorService lazyExecutorService = new LazyExecutorService(backingService);
    backingService.shutdown();
    assertThat(lazyExecutorService.isShutdown()).isTrue();
  }

  @Test
  public void testSubmitTaskToDeadExecutorService_shouldThrowRejectedExecutionException() {
    LazyExecutorService lazyExecutorService = new LazyExecutorService();
    lazyExecutorService.shutdown();
    Runnable runnable = () -> {};
    assertThrows(RejectedExecutionException.class, () -> lazyExecutorService.submit(runnable));
  }

  @Test
  public void testCancelledTask() {
    LazyExecutorService backingService = new LazyExecutorService();
    LazyExecutorService lazyExecutorService = new LazyExecutorService(backingService);
    Runnable runnable = () -> {};
    Future future = lazyExecutorService.submit(runnable);
    lazyExecutorService.shutdownNow();

    assertThat(future.isCancelled()).isTrue();
    assertThat(future.isDone()).isTrue();
    assertThrows(CancellationException.class, () -> future.get());
    assertThrows(CancellationException.class, () -> future.get(1, TimeUnit.MILLISECONDS));
    assertThat(future.cancel(true)).isFalse();
  }
}
