package com.google.cloud.hadoop.gcsio;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import java.util.UUID;

public final class GrpcChannelInvocationIdInterceptor implements ClientInterceptor {

  static final Metadata.Key<String> GOOG_API_CLIENT =
      Metadata.Key.of("x-goog-api-client", Metadata.ASCII_STRING_MARSHALLER);

  static final String GCCL_INVOCATION_ID = "gccl-invocation-id";

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
    return new InvocationIdAttachingClientCall<>(next.newCall(method, callOptions));
  }

  // Non private to avoid synthetic class
  GrpcChannelInvocationIdInterceptor() {}

  private final class InvocationIdAttachingClientCall<ReqT, RespT>
      extends SimpleForwardingClientCall<ReqT, RespT> {

    // Non private to avoid synthetic class
    InvocationIdAttachingClientCall(ClientCall<ReqT, RespT> call) {
      super(call);
    }

    @Override
    public void start(Listener<RespT> responseListener, Metadata headers) {
      appendInvocationId(headers);
      super.start(responseListener, headers);
    }

    private void appendInvocationId(final Metadata header) {
      UUID uuid = UUID.randomUUID();
      String invocationId = String.format("%s/%s", GCCL_INVOCATION_ID, uuid.toString());
      final String value = header.get(GOOG_API_CLIENT);
      if (value != null) {
        // First remove the value from Metadata, as overrides are not supported
        header.remove(GOOG_API_CLIENT, value);
        // append space separated invocationId
        String appendedValue = value + " " + invocationId;
        header.put(GOOG_API_CLIENT, appendedValue);
      } else {
        header.put(GOOG_API_CLIENT, invocationId);
      }
    }
  }
}
