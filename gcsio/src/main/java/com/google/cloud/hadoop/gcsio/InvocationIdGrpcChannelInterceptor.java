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

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import java.util.UUID;

/**
 * Interceptor to inject invocationId i.e. a unique random id, to every Request.
 */
final class InvocationIdGrpcChannelInterceptor implements ClientInterceptor {

  static final Metadata.Key<String> GOOG_API_CLIENT =
      Metadata.Key.of("x-goog-api-client", Metadata.ASCII_STRING_MARSHALLER);

  static final String GCCL_INVOCATION_ID_PREFIX = "gccl-invocation-id";

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
    return new InvocationIdAttachingClientCall<>(next.newCall(method, callOptions));
  }

  // Non private to avoid synthetic class
  InvocationIdGrpcChannelInterceptor() {}

  private final class InvocationIdAttachingClientCall<ReqT, RespT>
      extends SimpleForwardingClientCall<ReqT, RespT> {

    // Non private to avoid synthetic class
    InvocationIdAttachingClientCall(ClientCall<ReqT, RespT> call) {
      super(call);
    }

    @Override
    public void start(Listener<RespT> responseListener, Metadata headers) {
      appendInvocationId(headers);
      Listener interceptedListner = new SimpleForwardingClientCallListener<RespT>(responseListener) {
        @Override
        public void onHeaders(Metadata headers) {
          super.onHeaders(headers);
        }
      };
      super.start(responseListener, headers);
    }

    private void appendInvocationId(Metadata header) {
      UUID uuid = UUID.randomUUID();
      String invocationId = String.format("%s/%s", GCCL_INVOCATION_ID_PREFIX, uuid.toString());
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
