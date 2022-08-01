package com.google.cloud.hadoop.gcsio;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptors;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.testing.TestMethodDescriptors;
import java.util.ArrayList;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentMatchers;

@RunWith(JUnit4.class)
public class InvocationIdGrpcChannelInterceptorTest {

  private InvocationIdGrpcChannelInterceptor invocationIdInterceptor;

  private Channel channel = mock(Channel.class);

  private BaseClientCall call = new BaseClientCall();
  private final MethodDescriptor<Void, Void> method = TestMethodDescriptors.voidMethod();

  @Before
  public void setUp() throws Exception {
    invocationIdInterceptor = new InvocationIdGrpcChannelInterceptor();
    when(channel.newCall(
            ArgumentMatchers.<MethodDescriptor<String, Integer>>any(), any(CallOptions.class)))
        .thenReturn(call);
  }
  // Verify if InvocationId is getting added to metadata
  @Test
  public void examineInvocationIdHeader() {
    Channel intercepted = ClientInterceptors.intercept(channel, invocationIdInterceptor);
    ClientCall.Listener<Void> listener = mock(ClientCall.Listener.class);
    ClientCall<Void, Void> interceptedCall = intercepted.newCall(method, CallOptions.DEFAULT);
    // start() on the intercepted call will eventually reach the call created by the real channel
    interceptedCall.start(listener, new Metadata());
    // The headers passed to the real channel call will contain the information inserted by the
    // interceptor.
    assertSame(listener, call.listener);
    assertThat(call.headers.get(InvocationIdGrpcChannelInterceptor.GOOG_API_CLIENT))
        .contains(InvocationIdGrpcChannelInterceptor.GCCL_INVOCATION_ID);
  }

  // Verify if invocationId is getting appended if header is already existing.
  @Test
  public void invocationIdAppendedToExistingHeader() {
    Channel intercepted = ClientInterceptors.intercept(channel, invocationIdInterceptor);
    ClientCall.Listener<Void> listener = mock(ClientCall.Listener.class);
    ClientCall<Void, Void> interceptedCall = intercepted.newCall(method, CallOptions.DEFAULT);

    Metadata header = new Metadata();
    String dummyHeaderValue = "DummyValue";
    header.put(InvocationIdGrpcChannelInterceptor.GOOG_API_CLIENT, dummyHeaderValue);
    // start() on the intercepted call will eventually reach the call created by the real channel
    interceptedCall.start(listener, header);
    // The headers passed to the real channel call will contain the information inserted by the
    // interceptor.
    assertThat(call.headers.get(InvocationIdGrpcChannelInterceptor.GOOG_API_CLIENT))
        .contains(dummyHeaderValue);
    assertThat(call.headers.get(InvocationIdGrpcChannelInterceptor.GOOG_API_CLIENT))
        .contains(InvocationIdGrpcChannelInterceptor.GCCL_INVOCATION_ID);
  }

  private static class BaseClientCall extends ClientCall<String, Integer> {
    private boolean started;
    private boolean done;
    private ClientCall.Listener<Integer> listener;
    private Metadata headers;
    private List<Integer> requests = new ArrayList<>();
    private List<String> messages = new ArrayList<>();
    private boolean halfClosed;

    @Override
    public void start(ClientCall.Listener<Integer> listener, Metadata headers) {
      checkNotDone();
      started = true;
      this.listener = listener;
      this.headers = headers;
    }

    @Override
    public void request(int numMessages) {
      checkNotDone();
      checkStarted();
      requests.add(numMessages);
    }

    @Override
    public void cancel(String message, Throwable cause) {
      checkNotDone();
    }

    @Override
    public void halfClose() {
      checkNotDone();
      checkStarted();
      this.halfClosed = true;
    }

    @Override
    public void sendMessage(String message) {
      checkNotDone();
      checkStarted();
      messages.add(message);
    }

    private void checkNotDone() {
      if (done) {
        throw new IllegalStateException("no more methods should be called");
      }
    }

    private void checkStarted() {
      if (!started) {
        throw new IllegalStateException("should have called start");
      }
    }
  }
}
