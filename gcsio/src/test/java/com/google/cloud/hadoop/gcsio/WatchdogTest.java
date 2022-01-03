package com.google.cloud.hadoop.gcsio;

import static com.google.common.truth.Truth.assertThat;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.flogger.GoogleLogger;
import com.google.storage.v2.ReadObjectResponse;
import com.google.storage.v2.WriteObjectRequest;
import com.google.storage.v2.WriteObjectResponse;
import io.grpc.ClientCall;
import io.grpc.Context;
import io.grpc.Context.CancellableContext;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.Iterator;
import java.util.concurrent.ScheduledExecutorService;
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
    ScheduledExecutorService executor = newSingleThreadScheduledExecutor();
    watchdog = Watchdog.create(checkInterval, executor);
  }

  @Test
  public void testPassThroughClientStreamingRPC() {
    ClientCall<WriteObjectRequest, WriteObjectResponse> clientCall = mock(ClientCall.class);
    StreamObserver<WriteObjectRequest> streamObserver = mock(StreamObserver.class);
    StreamObserver<WriteObjectRequest> watch = watchdog.watch(clientCall, streamObserver, waitTime);
    WriteObjectRequest value = WriteObjectRequest.newBuilder().build();
    watch.onNext(value);
    verify(streamObserver).onNext(value);
    Throwable t = mock(Throwable.class);
    watch.onError(t);
    verify(streamObserver).onError(t);
    watch.onCompleted();
    verify(streamObserver).onCompleted();
  }

  @Test
  public void testPassThroughServerStreamingRPC() {
    CancellableContext requestContext = Context.current().withCancellation();
    Iterator<ReadObjectResponse> responseIterator = mock(Iterator.class);
    Iterator<ReadObjectResponse> watch = watchdog.watch(requestContext, responseIterator, waitTime);
    watch.next();
    verify(responseIterator).next();
    assertThat(watchdog).isNotNull();
    assertThat(watchdog.getOpenStreams()).isNotNull();
    assertThat(watchdog.getOpenStreams().size()).isEqualTo(1);
    when(responseIterator.hasNext()).thenReturn(false);
    assertThat(watch.hasNext()).isFalse();
    verify(responseIterator).hasNext();
    assertThat(watchdog.getOpenStreams()).isNotNull();
    assertThat(watchdog.getOpenStreams().size()).isEqualTo(0);
  }

  @Test
  public void testClientStreamingRPCTimeout() {
    ClientCall<WriteObjectRequest, WriteObjectResponse> clientCall = mock(ClientCall.class);
    StreamObserver<WriteObjectRequest> timeoutStreamObserver =
        new StreamObserver() {
          @Override
          public void onNext(Object value) {
            try {
              logger.atInfo().log("Sleeping for 10 seconds");
              Thread.sleep(10000);
            } catch (InterruptedException e) {
              logger.atSevere().log("thread interrupted ", e);
            }
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
    verify(clientCall).cancel(anyString(), isA(TimeoutException.class));
  }

  @Test
  public void testServerStreamingRPCTimeout() {
    CancellableContext requestContext = Context.current().withCancellation();
    Iterator<ReadObjectResponse> responseIterator =
        new Iterator<ReadObjectResponse>() {
          @Override
          public boolean hasNext() {
            try {
              logger.atInfo().log("Sleeping for 10 seconds");
              Thread.sleep(10000);
            } catch (InterruptedException e) {
              logger.atSevere().log("thread interrupted ", e);
            }
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
  public void testMultipleWatches() {
    ClientCall<WriteObjectRequest, WriteObjectResponse> clientCall = mock(ClientCall.class);
    StreamObserver<WriteObjectRequest> timeoutStreamObserver =
        new StreamObserver() {
          @Override
          public void onNext(Object value) {
            try {
              logger.atInfo().log("Sleeping for 10 seconds");
              Thread.sleep(10000);
            } catch (InterruptedException e) {
              logger.atSevere().log("thread interrupted ", e);
            }
          }

          @Override
          public void onError(Throwable t) {}

          @Override
          public void onCompleted() {}
        };

    CancellableContext requestContext = Context.current().withCancellation();
    Iterator<ReadObjectResponse> responseIterator =
        new Iterator<ReadObjectResponse>() {
          @Override
          public boolean hasNext() {
            try {
              logger.atInfo().log("Sleeping for 10 seconds");
              Thread.sleep(10000);
            } catch (InterruptedException e) {
              logger.atSevere().log("thread interrupted ", e);
            }
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
    assertThat(watchdog.getOpenStreams()).isNotNull();
    assertThat(watchdog.getOpenStreams().size()).isEqualTo(2);

    boolean actual = clientStreamingRPCWatch.hasNext();
    WriteObjectRequest value = WriteObjectRequest.newBuilder().build();
    serverStreamingRPCWatch.onNext(value);

    assertThat(actual).isTrue();
    assertThat(requestContext.isCancelled()).isTrue();
    verify(clientCall).cancel(anyString(), isA(TimeoutException.class));
    assertThat(watchdog.getOpenStreams().size()).isEqualTo(0);
  }

  @Test
  public void testClientStreamingRPCWithoutTimeout() {
    ClientCall<WriteObjectRequest, WriteObjectResponse> clientCall = mock(ClientCall.class);
    StreamObserver<WriteObjectRequest> timeoutStreamObserver =
        new StreamObserver() {
          @Override
          public void onNext(Object value) {
            try {
              logger.atInfo().log("Sleeping for 10 seconds");
              Thread.sleep(10000);
            } catch (InterruptedException e) {
              logger.atSevere().log("thread interrupted ", e);
            }
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
    verify(clientCall, never()).cancel(anyString(), isA(TimeoutException.class));
  }

  @Test
  public void testServerStreamingRPCWithoutTimeout() {
    CancellableContext requestContext = Context.current().withCancellation();
    Iterator<ReadObjectResponse> responseIterator =
        new Iterator<ReadObjectResponse>() {
          @Override
          public boolean hasNext() {
            try {
              logger.atInfo().log("Sleeping for 10 seconds");
              Thread.sleep(10000);
            } catch (InterruptedException e) {
              logger.atSevere().log("thread interrupted ", e);
            }
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
}
