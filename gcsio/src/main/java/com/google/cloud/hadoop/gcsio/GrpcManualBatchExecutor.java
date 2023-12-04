package com.google.cloud.hadoop.gcsio;

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemImpl.getFromFuture;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.flogger.GoogleLogger;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * GrpcManualBatchExecutor provides a means to manually batch gRPC requests using a thread pool.
 *
 * <p> Expected usage is to create a new GrpcManualBatchExecutor instance per client operation
 * that represents logical grouping of requests.
 *
 * <p> Instance of this class can not be used again after {@link #shutdown()} method call.
 */
public class GrpcManualBatchExecutor {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private final ListeningExecutorService requestsExecutor;

  private final Queue<Future> responseFutures = new ConcurrentLinkedQueue<>();

  GrpcManualBatchExecutor(int maxRequestPerBatch) {
    ThreadPoolExecutor service = new ThreadPoolExecutor(
        /* corePoolSize= */ maxRequestPerBatch,
        /* maximumPoolSize= */ maxRequestPerBatch,
        /* keepAliveTime= */ 10L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
        new ThreadFactoryBuilder().setNameFormat("gcs-grpc-manual-batching-pool-%d").setDaemon(true)
            .build());
    service.allowCoreThreadTimeOut(true);
    this.requestsExecutor = MoreExecutors.listeningDecorator(service);
  }

  /**
   * Adds a task to the execution queue.
   */
  public <T> void queue(Callable<T> task, FutureCallback<T> callback) {
    checkState(!requestsExecutor.isShutdown() && !requestsExecutor.isTerminated(),
        "requestExecutor should not be terminated to queue request");
    ListenableFuture<T> future = requestsExecutor.submit(task);
    if (callback != null) {
      Futures.addCallback(future, callback, requestsExecutor);
    }
    responseFutures.add(future);
  }

  /**
   * Awaits until all tasks are terminated and then shutdowns the executor.
   */
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


  /**
   * Awaits until all sent requests are completed. Should be serialized
   */
  private void awaitRequestsCompletion() throws IOException {
    while (!responseFutures.isEmpty()) {
      getFromFuture(responseFutures.remove());
    }
  }

}
