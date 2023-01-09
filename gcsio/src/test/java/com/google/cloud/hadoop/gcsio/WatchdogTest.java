/*
 * Copyright 2022 Google LLC
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

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.Lists;
import com.google.common.flogger.GoogleLogger;
import com.google.storage.v2.ReadObjectResponse;
import com.google.storage.v2.WriteObjectRequest;
import com.google.storage.v2.WriteObjectResponse;
import io.grpc.ClientCall;
import io.grpc.Context;
import io.grpc.Context.CancellableContext;
import io.grpc.internal.NoopClientCall;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class WatchdogTest {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private Watchdog watchdog;

  private final Duration waitTime = Duration.ofSeconds(5);
  private final Duration zeroWaitTime = Duration.ofSeconds(0);

  @Before
  public void setUp() throws Exception {
    Duration checkInterval = Duration.ofSeconds(2);
    watchdog = Watchdog.create(checkInterval);
  }

  @Test
  public void watchPassThroughClientStreamingRPC() {
    ClientCall<WriteObjectRequest, WriteObjectResponse> clientCall = new NoopClientCall<>();
    StreamObserverStub<WriteObjectRequest> streamObserver = new StreamObserverStub<>();
    StreamObserver<WriteObjectRequest> watch = watchdog.watch(clientCall, streamObserver, waitTime);
    assertThat(watchdog).isNotNull();
    assertThat(watchdog.getOpenStreams()).hasSize(1);
    WriteObjectRequest value = WriteObjectRequest.newBuilder().build();
    watch.onNext(value);
    assertThat(streamObserver.getObjects()).containsExactly(value);
    Throwable t = new TimeoutException("Request timeout out");
    watch.onError(t);
    assertThat(streamObserver.getErrors()).containsExactly(t);
    watch.onCompleted();
    assertThat(streamObserver.isCompleted()).isTrue();
    assertThat(watchdog.getOpenStreams().isEmpty()).isTrue();
  }

  @Test
  public void watchPassThroughServerStreamingRPC() {
    CancellableContext requestContext = Context.current().withCancellation();
    ReadObjectResponse defaultInstance = ReadObjectResponse.getDefaultInstance();
    Response<ReadObjectResponse> validResponse = new Response<>(defaultInstance);
    Response<ReadObjectResponse> errorResponse =
        new Response<>(new RuntimeException("Read timeout out"));
    List<Response<ReadObjectResponse>> responseList =
        Lists.newArrayList(validResponse, errorResponse);
    ResponseIteratorStub<ReadObjectResponse> responseIterator =
        new ResponseIteratorStub<>(responseList);
    Iterator<ReadObjectResponse> watch = watchdog.watch(requestContext, responseIterator, waitTime);
    ReadObjectResponse next = watch.next();
    assertThat(next).isEqualTo(validResponse.object);
    assertThat(watchdog).isNotNull();
    assertThat(watchdog.getOpenStreams()).hasSize(1);
    assertThrows(RuntimeException.class, watch::hasNext);
    assertThat(watchdog.getOpenStreams().isEmpty()).isTrue();
  }

  @Test
  public void watchOnClientStreamingRPCTimeout() {
    NoopClientCallStub<WriteObjectRequest, WriteObjectResponse> clientCall =
        new NoopClientCallStub<>();
    StreamObserver<WriteObjectRequest> timeoutStreamObserver =
        new StreamObserver<>() {
          @Override
          public void onNext(WriteObjectRequest value) {
            logger.atInfo().log("Sleeping for 10 seconds");
            sleepUninterruptibly(Duration.ofSeconds(10));
          }

          @Override
          public void onError(Throwable t) {}

          @Override
          public void onCompleted() {}
        };
    StreamObserver<WriteObjectRequest> watch =
        watchdog.watch(clientCall, timeoutStreamObserver, waitTime);
    WriteObjectRequest value = WriteObjectRequest.newBuilder().build();
    watch.onNext(value);
    assertThat(clientCall.cancelled).isTrue();
    assertThat(clientCall.cause).isInstanceOf(TimeoutException.class);
  }

  @Test
  public void watchOnServerStreamingRPCTimeout() {
    CancellableContext requestContext = Context.current().withCancellation();
    Iterator<ReadObjectResponse> responseIterator =
        new Iterator<>() {
          @Override
          public boolean hasNext() {
            logger.atInfo().log("Sleeping for 10 seconds");
            sleepUninterruptibly(Duration.ofSeconds(10));
            return true;
          }

          @Override
          public ReadObjectResponse next() {
            return null;
          }
        };
    Iterator<ReadObjectResponse> watch = watchdog.watch(requestContext, responseIterator, waitTime);
    assertThat(watch.hasNext()).isTrue();
    assertThat(requestContext.isCancelled()).isTrue();
  }

  @Test
  public void watchMultipleStreams() {
    NoopClientCallStub<WriteObjectRequest, WriteObjectResponse> clientCall =
        new NoopClientCallStub<>();
    StreamObserver<WriteObjectRequest> timeoutStreamObserver =
        new StreamObserver<>() {
          @Override
          public void onNext(WriteObjectRequest value) {
            logger.atInfo().log("Sleeping for 10 seconds");
            sleepUninterruptibly(Duration.ofSeconds(10));
          }

          @Override
          public void onError(Throwable t) {}

          @Override
          public void onCompleted() {}
        };

    CancellableContext requestContext = Context.current().withCancellation();
    Iterator<ReadObjectResponse> responseIterator =
        new Iterator<>() {
          @Override
          public boolean hasNext() {
            logger.atInfo().log("Sleeping for 10 seconds");
            sleepUninterruptibly(Duration.ofSeconds(10));
            return true;
          }

          @Override
          public ReadObjectResponse next() {
            return null;
          }
        };
    Iterator<ReadObjectResponse> clientStreamingRPCWatch =
        watchdog.watch(requestContext, responseIterator, waitTime);
    StreamObserver<WriteObjectRequest> serverStreamingRPCWatch =
        watchdog.watch(clientCall, timeoutStreamObserver, waitTime);
    assertThat(watchdog).isNotNull();
    assertThat(watchdog.getOpenStreams()).hasSize(2);
    boolean actual = clientStreamingRPCWatch.hasNext();
    WriteObjectRequest value = WriteObjectRequest.newBuilder().build();
    serverStreamingRPCWatch.onNext(value);

    assertThat(actual).isTrue();
    assertThat(requestContext.isCancelled()).isTrue();
    assertThat(clientCall.cancelled).isTrue();
    assertThat(clientCall.cause).isInstanceOf(TimeoutException.class);
    assertThat(watchdog.getOpenStreams().isEmpty()).isTrue();
  }

  @Test
  public void watchOnClientStreamingRPCWithoutTimeout() {
    NoopClientCallStub<WriteObjectRequest, WriteObjectResponse> clientCall =
        new NoopClientCallStub<>();
    StreamObserver<WriteObjectRequest> timeoutStreamObserver =
        new StreamObserver<>() {
          @Override
          public void onNext(WriteObjectRequest value) {
            logger.atInfo().log("Sleeping for 10 seconds");
            sleepUninterruptibly(Duration.ofSeconds(10));
          }

          @Override
          public void onError(Throwable t) {}

          @Override
          public void onCompleted() {}
        };
    StreamObserver<WriteObjectRequest> watch =
        watchdog.watch(clientCall, timeoutStreamObserver, zeroWaitTime);
    WriteObjectRequest value = WriteObjectRequest.newBuilder().build();
    watch.onNext(value);
    assertThat(clientCall.cancelled).isFalse();
    assertThat(clientCall.cause).isNull();
  }

  @Test
  public void watchOnServerStreamingRPCWithoutTimeout() {
    CancellableContext requestContext = Context.current().withCancellation();
    Iterator<ReadObjectResponse> responseIterator =
        new Iterator<>() {
          @Override
          public boolean hasNext() {
            logger.atInfo().log("Sleeping for 10 seconds");
            sleepUninterruptibly(Duration.ofSeconds(10));
            return true;
          }

          @Override
          public ReadObjectResponse next() {
            return null;
          }
        };
    Iterator<ReadObjectResponse> watch =
        watchdog.watch(requestContext, responseIterator, zeroWaitTime);
    assertThat(watch.hasNext()).isTrue();
    assertThat(requestContext.isCancelled()).isFalse();
  }

  static final class StreamObserverStub<T> implements StreamObserver<T> {

    List<T> objects = new ArrayList<>();
    List<Throwable> errors = new ArrayList<>();
    boolean completed;

    @Override
    public void onNext(T t) {
      objects.add(t);
    }

    @Override
    public void onError(Throwable throwable) {
      errors.add(throwable);
    }

    @Override
    public void onCompleted() {
      completed = true;
    }

    public List<T> getObjects() {
      return objects;
    }

    public List<Throwable> getErrors() {
      return errors;
    }

    public boolean isCompleted() {
      return completed;
    }
  }

  static final class ResponseIteratorStub<T> implements Iterator<T> {
    private final Iterator<Response<T>> objects;

    public ResponseIteratorStub(List<Response<T>> objects) {
      this.objects = objects.listIterator();
    }

    // With gRPC read Stream, hasNext invokes a network call, and can throw an exception.
    @Override
    public boolean hasNext() {
      boolean hasNext = objects.hasNext();
      if (hasNext) {
        Response<T> next = objects.next();
        if (next.throwable != null) {
          throw next.throwable;
        }
      }
      return hasNext;
    }

    @Override
    public T next() {
      return objects.next().object;
    }
  }

  static final class Response<T> {
    private final T object;
    private final RuntimeException throwable;

    public Response(T object) {
      this.object = object;
      this.throwable = null;
    }

    public Response(RuntimeException throwable) {
      this.throwable = throwable;
      this.object = null;
    }
  }

  static final class NoopClientCallStub<ReqT, ResT> extends NoopClientCall<ReqT, ResT> {
    boolean cancelled;
    Throwable cause;

    @Override
    public void cancel(String message, Throwable cause) {
      cancelled = true;
      this.cause = cause;
    }
  }
}
