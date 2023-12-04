package com.google.cloud.hadoop.gcsio;

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemImpl.getFromFuture;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.flogger.GoogleLogger;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * GrpcManualBatchExecutor provides a means to manually batch gRPC requests using a thread pool.
 * Execution is performed by the requestsExecutor ExecutorService.
 *
 * <p>Expected usage is to create a new GrpcManualBatchExecutor instance per client operation that
 * represents logical grouping of requests(delete, copy, get).
 *
 * <p>Instance of this class can not be used again after {@link #shutdown()} method has been called.
 */
public class GrpcManualBatchExecutor {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private final ExecutorService requestsExecutor;

  private final Queue<Future<Void>> responseFutures = new ConcurrentLinkedQueue<>();

  GrpcManualBatchExecutor(int numThreads) {
    ThreadPoolExecutor executor =
        new ThreadPoolExecutor(
            /* corePoolSize= */ numThreads,
            /* maximumPoolSize= */ numThreads,
            /* keepAliveTime= */ 10L,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(numThreads * 20),
            new ThreadFactoryBuilder()
                .setNameFormat("gcs-grpc-manual-batching-pool-%d")
                .setDaemon(true)
                .build());
    executor.allowCoreThreadTimeOut(true);
    executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
    this.requestsExecutor = executor;
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
    try {
      T result = task.call();
      callback.onSuccess(result);
    } catch (Throwable throwable) {
      callback.onFailure(throwable);
    }
  }

  /** Awaits until all tasks are terminated and then shutdowns the executor. */
  public void shutdown() throws IOException {
    awaitRequestsCompletion();
    try {
      checkState(responseFutures.isEmpty(), "responseFutures should be empty after flush");
    } finally {
      requestsExecutor.shutdown();
      try {
        if (!requestsExecutor.awaitTermination(1, TimeUnit.SECONDS)) {
          logger.atWarning().log("Forcibly shutting down grpc manual batching thread pool.");
          requestsExecutor.shutdownNow();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.atFine().withCause(e).log(
            "Failed to await termination: forcibly shutting down grpc manual batching thread pool.");
        requestsExecutor.shutdownNow();
      }
    }
  }

  /** Awaits until all sent requests are completed. Should be serialized */
  private void awaitRequestsCompletion() throws IOException {
    while (!responseFutures.isEmpty()) {
      getFromFuture(responseFutures.remove());
    }
  }
}
