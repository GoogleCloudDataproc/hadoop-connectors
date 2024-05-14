/*
 * Copyright 2024 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.gcsio;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;

import com.google.cloud.hadoop.util.GoogleCloudStorageEventBus;
import com.google.common.flogger.GoogleLogger;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * BatchExecutor provides a means to manually batch requests using a thread pool. Execution is
 * performed by the underlying {@link #requestsExecutor} ExecutorService.
 *
 * <p>Expected usage is to create a new BatchExecutor instance per client operation that represents
 * logical grouping of requests(delete, copy, get).
 *
 * <p>Instance of this class can not be used again after {@link #shutdown()} method has been called.
 */
class BatchExecutor {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private final ExecutorService requestsExecutor;

  private final Queue<Future<Void>> responseFutures = new ConcurrentLinkedQueue<>();

  public BatchExecutor(int numThreads) {
    this.requestsExecutor =
        numThreads == 0 ? newDirectExecutorService() : newRequestExecutor(numThreads);
  }

  private static ExecutorService newRequestExecutor(int numThreads) {
    ThreadPoolExecutor executor =
        new ThreadPoolExecutor(
            /* corePoolSize= */ numThreads,
            /* maximumPoolSize= */ numThreads,
            /* keepAliveTime= */ 10L,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(numThreads * 20),
            new ThreadFactoryBuilder()
                .setNameFormat("gcs-manual-batching-pool-%d")
                .setDaemon(true)
                .build());
    executor.allowCoreThreadTimeOut(true);
    executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
    return executor;
  }

  /** Adds a task to the execution queue. */
  public <T> void queue(Callable<T> task, FutureCallback<T> callback) {
    checkState(
        !requestsExecutor.isShutdown() && !requestsExecutor.isTerminated(),
        "requestExecutor should not be terminated to queue request");

    responseFutures.add(
        requestsExecutor.submit(
            () -> {
              execute(task, callback);
              return null;
            }));
  }

  private static <T> void execute(Callable<T> task, FutureCallback<T> callback) {
    checkArgument(callback != null, "FutureCallBack cannot be null : %s", callback);
    try {
      T result = task.call();
      callback.onSuccess(result);
    } catch (Throwable throwable) {
      callback.onFailure(throwable);
    }
  }

  /** Awaits until all tasks are terminated and then shutdowns the executor. */
  public void shutdown() throws IOException {
    try {
      awaitRequestsCompletion();
      checkState(responseFutures.isEmpty(), "responseFutures should be empty after flush");
    } finally {
      requestsExecutor.shutdown();
      try {
        if (!requestsExecutor.awaitTermination(1, TimeUnit.SECONDS)) {
          logger.atWarning().log("Forcibly shutting down manual batching thread pool.");
          requestsExecutor.shutdownNow();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.atFine().withCause(e).log(
            "Failed to await termination: forcibly shutting down manual batching thread pool.");
        requestsExecutor.shutdownNow();
      }
    }
  }

  /** Awaits until all sent requests are completed */
  private void awaitRequestsCompletion() throws IOException {
    while (!responseFutures.isEmpty()) {
      getFromFuture(responseFutures.remove());
    }
  }

  static <T> T getFromFuture(Future<T> future) throws IOException {
    try {
      return future.get();
    } catch (ExecutionException | InterruptedException e) {
      GoogleCloudStorageEventBus.postOnException();
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new IOException(
          String.format(
              "Failed to get result: %s with message : %s",
              e instanceof ExecutionException ? e.getCause() : e, e.getMessage()),
          e);
    }
  }
}
