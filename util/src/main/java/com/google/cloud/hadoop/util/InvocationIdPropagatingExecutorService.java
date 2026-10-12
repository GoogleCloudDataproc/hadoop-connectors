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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;

import com.google.common.util.concurrent.ForwardingExecutorService;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * An {@link ExecutorService} decorator that runs each task with the {@link InvocationIdContext
 * invocation ID} of the thread that submitted it. The ID is captured at submission and is active on
 * the worker thread only for the duration of the task.
 */
public final class InvocationIdPropagatingExecutorService extends ForwardingExecutorService {

  private final ExecutorService delegate;

  private InvocationIdPropagatingExecutorService(ExecutorService delegate) {
    this.delegate = checkNotNull(delegate, "delegate must not be null");
  }

  /** Wraps the given executor so that it propagates the submitter's invocation ID. */
  public static ExecutorService wrap(ExecutorService delegate) {
    return delegate instanceof InvocationIdPropagatingExecutorService
        ? delegate
        : new InvocationIdPropagatingExecutorService(delegate);
  }

  /** Returns a {@link Runnable} that runs {@code task} with the current thread's invocation ID. */
  public static Runnable wrap(Runnable task) {
    checkNotNull(task, "task must not be null");
    String invocationId = InvocationIdContext.getInvocationId();
    return () -> {
      try (InvocationIdContext.Scope ignored = InvocationIdContext.openScopeWithId(invocationId)) {
        task.run();
      }
    };
  }

  /** Returns a {@link Callable} that runs {@code task} with the current thread's invocation ID. */
  public static <T> Callable<T> wrap(Callable<T> task) {
    checkNotNull(task, "task must not be null");
    String invocationId = InvocationIdContext.getInvocationId();
    return () -> {
      try (InvocationIdContext.Scope ignored = InvocationIdContext.openScopeWithId(invocationId)) {
        return task.call();
      }
    };
  }

  @Override
  protected ExecutorService delegate() {
    return delegate;
  }

  @Override
  public void execute(Runnable command) {
    delegate.execute(wrap(command));
  }

  @Override
  public Future<?> submit(Runnable task) {
    return delegate.submit(wrap(task));
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    return delegate.submit(wrap(task), result);
  }

  @Override
  public <T> Future<T> submit(Callable<T> task) {
    return delegate.submit(wrap(task));
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
      throws InterruptedException {
    return delegate.invokeAll(wrapAll(tasks));
  }

  @Override
  public <T> List<Future<T>> invokeAll(
      Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException {
    return delegate.invokeAll(wrapAll(tasks), timeout, unit);
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
      throws InterruptedException, ExecutionException {
    return delegate.invokeAny(wrapAll(tasks));
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    return delegate.invokeAny(wrapAll(tasks), timeout, unit);
  }

  private static <T> List<Callable<T>> wrapAll(Collection<? extends Callable<T>> tasks) {
    return tasks.stream()
        .map(InvocationIdPropagatingExecutorService::<T>wrap)
        .collect(toImmutableList());
  }
}
