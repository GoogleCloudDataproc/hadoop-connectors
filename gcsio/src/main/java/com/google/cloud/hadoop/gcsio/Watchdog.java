/*
 * Copyright 2021 Google LLC
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *     * Neither the name of Google LLC nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.google.cloud.hadoop.gcsio;

import com.google.api.core.ApiClock;
import com.google.common.base.Preconditions;
import com.google.common.flogger.GoogleLogger;
import io.grpc.ClientCall;
import io.grpc.Context.CancellableContext;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
public final class Watchdog implements Runnable {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  // Dummy value to convert the ConcurrentHashMap into a Set
  private static final Object PRESENT = new Object();
  private final ConcurrentHashMap<WatchdogStream, Object> openStreams = new ConcurrentHashMap<>();

  private final ApiClock clock;
  private final Duration scheduleInterval;
  private final ScheduledExecutorService executor;

  /** returns a Watchdog which is scheduled at the provided interval. */
  public static Watchdog create(
      ApiClock clock, Duration scheduleInterval, ScheduledExecutorService executor) {
    Watchdog watchdog = new Watchdog(clock, scheduleInterval, executor);
    watchdog.start();
    return watchdog;
  }

  private Watchdog(ApiClock clock, Duration scheduleInterval, ScheduledExecutorService executor) {
    this.clock = Preconditions.checkNotNull(clock, "clock can't be null");
    this.scheduleInterval = scheduleInterval;
    this.executor = executor;
  }

  private void start() {
    executor.scheduleAtFixedRate(
        this, scheduleInterval.toMillis(), scheduleInterval.toMillis(), TimeUnit.MILLISECONDS);
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
    Preconditions.checkNotNull(responseIterator, "responseIterator can't be null");
    Preconditions.checkNotNull(idleTimeout, "idleTimeout can't be null");

    if (idleTimeout.isZero()) {
      return responseIterator;
    }

    ServerStreamingRPCWatchdogStream<T> stream =
        new ServerStreamingRPCWatchdogStream<>(requestContext, responseIterator, idleTimeout);
    openStreams.put(stream, PRESENT);
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
    Preconditions.checkNotNull(streamObserver, "streamObserver can't be null");
    Preconditions.checkNotNull(idleTimeout, "idleTimeout can't be null");

    if (idleTimeout.isZero()) {
      return streamObserver;
    }

    ClientStreamingRPCWatchdogStream<R, T> stream =
        new ClientStreamingRPCWatchdogStream<>(clientCall, streamObserver, idleTimeout);
    openStreams.put(stream, PRESENT);
    return stream;
  }

  @Override
  public void run() {
    try {
      runUnsafe();
    } catch (RuntimeException t) {
      logger.atSevere().log("Caught throwable in periodic Watchdog run. Continuing.", t);
    }
  }

  // package-private method to test the state of watchdog
  ConcurrentHashMap<WatchdogStream, Object> getOpenStreams() {
    return openStreams;
  }

  private void runUnsafe() {
    Iterator<Entry<WatchdogStream, Object>> it = openStreams.entrySet().iterator();

    while (it.hasNext()) {
      WatchdogStream stream = it.next().getKey();
      if (stream.cancelIfStale()) {
        it.remove();
      }
    }
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
    private long lastActivityAt = clock.millisTime();

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
        lastActivityAt = clock.millisTime();
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
        long waitTime = clock.millisTime() - lastActivityAt;
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
    private long lastActivityAt = clock.millisTime();

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
        long waitTime = clock.millisTime() - lastActivityAt;
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
        lastActivityAt = clock.millisTime();
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
