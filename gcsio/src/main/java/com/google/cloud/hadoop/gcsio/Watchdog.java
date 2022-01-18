/*
 * Copyright 2022 Google Inc. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.hadoop.gcsio;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.flogger.GoogleLogger;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.grpc.ClientCall;
import io.grpc.Context.CancellableContext;
import io.grpc.stub.StreamObserver;
import java.time.Clock;
import java.time.Duration;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentHashMap.KeySetView;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

/**
 * Prevents the streams from hanging indefinitely. This middleware garbage collects idle streams in
 * case the user forgot to close a ServerStream or if a connection is reset and GRPC does not get
 * notified.
 *
 * <p>For every {@code checkInterval}, this class checks thresholds:
 *
 * <ul>
 *   <li>idleTimeout: the amount of time to wait before assuming that the caller forgot to close the
 *       stream and forcefully closing the stream. This is measured from the last time the caller
 *       had no outstanding demand. Duration.ZERO disables the timeout.
 * </ul>
 */
final class Watchdog implements Runnable {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private final KeySetView<WatchdogStream, Boolean> openStreams = ConcurrentHashMap.newKeySet();

  private final Duration scheduleInterval;

  private final Clock clock = Clock.systemUTC();

  private final ScheduledExecutorService executor =
      newSingleThreadScheduledExecutor(
          new ThreadFactoryBuilder()
              .setNameFormat("gcs-background-watchdog-pool-%d")
              .setDaemon(true)
              .build());

  /** returns a Watchdog which is scheduled at the provided interval. */
  public static Watchdog create(Duration scheduleInterval) {
    Watchdog watchdog = new Watchdog(scheduleInterval);
    watchdog.start();
    return watchdog;
  }

  private Watchdog(Duration scheduleInterval) {
    this.scheduleInterval = scheduleInterval;
  }

  private void start() {
    executor.scheduleAtFixedRate(
        this, scheduleInterval.toMillis(), scheduleInterval.toMillis(), MILLISECONDS);
  }

  /**
   * Wraps the target {@code StreamObserver} with timing constraints. Used for ServerStreamingRPC
   * calls
   *
   * @param requestContext request to be cancelled when the timer is overrun
   * @param responseIterator responseIterator to monitor the state of the RPC
   * @param idleTimeout Idle time duration that triggers client call cancellation
   * @param <T> Type reference for the RPC response
   * @return {@link Iterator}
   */
  public <T> Iterator<T> watch(
      CancellableContext requestContext,
      Iterator<T> responseIterator,
      @Nonnull Duration idleTimeout) {
    checkNotNull(responseIterator, "responseIterator can't be null");
    checkNotNull(idleTimeout, "idleTimeout can't be null");

    if (idleTimeout.isZero()) {
      return responseIterator;
    }

    ServerStreamingRPCWatchdogStream<T> stream =
        new ServerStreamingRPCWatchdogStream<>(requestContext, responseIterator, idleTimeout);
    openStreams.add(stream);
    return stream;
  }

  /**
   * Wraps the target {@code StreamObserver} with timing constraints. Used for ClientStreamingRPC
   * calls
   *
   * @param clientCall ClientCall to be cancelled when the timer is overrun
   * @param streamObserver Observer instance to monitor the state of the RPC
   * @param idleTimeout Idle time duration that triggers client call cancellation
   * @param <T> Type reference for the RPC response
   * @return {@link StreamObserver}
   */
  public <R, T> StreamObserver<R> watch(
      ClientCall<R, T> clientCall,
      StreamObserver<R> streamObserver,
      @Nonnull Duration idleTimeout) {
    checkNotNull(streamObserver, "streamObserver can't be null");
    checkNotNull(idleTimeout, "idleTimeout can't be null");

    if (idleTimeout.isZero()) {
      return streamObserver;
    }

    ClientStreamingRPCWatchdogStream<R, T> stream =
        new ClientStreamingRPCWatchdogStream<>(clientCall, streamObserver, idleTimeout);
    openStreams.add(stream);
    return stream;
  }

  @Override
  public void run() {
    try {
      runUnsafe();
    } catch (RuntimeException t) {
      logger.atSevere().withCause(t).log("Caught throwable in periodic Watchdog run. Continuing.");
    }
  }

  @VisibleForTesting
  KeySetView<WatchdogStream, Boolean> getOpenStreams() {
    return openStreams;
  }

  private void runUnsafe() {
    openStreams.removeIf(WatchdogStream::cancelIfStale);
  }

  /** clean up resources used by the class */
  public void shutdown() {
    executor.shutdown();
  }

  enum State {
    /** Stream has been started, but doesn't have any outstanding requests. */
    IDLE,
    /** Stream is awaiting a response from upstream. */
    WAITING,
    /**
     * Stream received a response from upstream, and is awaiting outerResponseObserver processing.
     */
    DELIVERING
  }

  interface WatchdogStream {
    /**
     * Checks if this stream has overrun any of timeouts and cancels it if it does.
     *
     * @return True if the stream was canceled.
     */
    boolean cancelIfStale();
  }

  class ServerStreamingRPCWatchdogStream<T> implements Iterator<T>, WatchdogStream {
    private final Object lock = new Object();

    private final Duration waitTimeout;

    private final Iterator<T> innerIterator;

    private final CancellableContext requestContext;

    @GuardedBy("lock")
    private long lastActivityAt = clock.millis();

    @GuardedBy("lock")
    private State state = State.IDLE;

    ServerStreamingRPCWatchdogStream(
        CancellableContext requestContext, Iterator<T> iterator, Duration waitTimeout) {
      this.waitTimeout = waitTimeout;
      this.requestContext = requestContext;
      this.innerIterator = iterator;
    }

    @Override
    public T next() {
      synchronized (lock) {
        lastActivityAt = clock.millis();
      }
      T next = innerIterator.next();
      this.state = State.DELIVERING;
      return next;
    }

    @Override
    public boolean cancelIfStale() {
      if (innerIterator == null || requestContext == null) {
        return false;
      }

      Throwable throwable = null;
      synchronized (lock) {
        long waitTime = clock.millis() - lastActivityAt;
        if (this.state == State.WAITING
            && (!waitTimeout.isZero() && waitTime >= waitTimeout.toMillis())) {
          throwable = new TimeoutException("Canceled due to timeout waiting for next response");
        }
      }
      if (throwable != null) {
        requestContext.cancel(throwable);
        return true;
      }
      return false;
    }

    @Override
    public boolean hasNext() {
      boolean hasNext = false;
      try {
        this.state = State.WAITING;
        hasNext = innerIterator.hasNext();
      } finally {
        // stream is complete successfully with no more items or has thrown an exception
        if (!hasNext) openStreams.remove(this);
      }
      return hasNext;
    }
  }

  class ClientStreamingRPCWatchdogStream<R, T> implements StreamObserver<R>, WatchdogStream {

    private final Object lock = new Object();

    private final Duration waitTimeout;

    private final StreamObserver<R> innerStreamObserver;

    private final ClientCall<R, T> clientCall;

    @GuardedBy("lock")
    private long lastActivityAt = clock.millis();

    @GuardedBy("lock")
    private State state = State.IDLE;

    public ClientStreamingRPCWatchdogStream(
        ClientCall<R, T> clientCall, StreamObserver<R> innerStreamObserver, Duration waitTimeout) {
      this.clientCall = clientCall;
      this.innerStreamObserver = innerStreamObserver;
      this.waitTimeout = waitTimeout;
    }

    @Override
    public boolean cancelIfStale() {
      if (innerStreamObserver == null || clientCall == null) {
        return false;
      }

      Throwable throwable = null;
      synchronized (lock) {
        long waitTime = clock.millis() - lastActivityAt;
        if (this.state == State.WAITING
            && (!waitTimeout.isZero() && waitTime >= waitTimeout.toMillis())) {
          throwable = new TimeoutException("Canceled due to timeout waiting for next response");
        }
      }
      if (throwable != null) {
        clientCall.cancel(throwable.getMessage(), throwable);
        return true;
      }
      return false;
    }

    @Override
    public void onNext(R value) {
      synchronized (lock) {
        lastActivityAt = clock.millis();
      }
      this.state = State.WAITING;
      innerStreamObserver.onNext(value);
      this.state = State.IDLE;
    }

    @Override
    public void onError(Throwable t) {
      openStreams.remove(this);
      innerStreamObserver.onError(t);
    }

    @Override
    public void onCompleted() {
      openStreams.remove(this);
      innerStreamObserver.onCompleted();
    }
  }
}
