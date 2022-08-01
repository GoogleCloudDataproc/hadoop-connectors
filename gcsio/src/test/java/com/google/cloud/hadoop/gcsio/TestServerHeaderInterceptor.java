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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import java.util.ArrayList;
import java.util.List;

// Header interceptor to capture the header information received at the server side
class TestServerHeaderInterceptor implements ServerInterceptor {
  private List<Metadata> allMeta = new ArrayList<>();

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> serverCall,
      Metadata metadata,
      ServerCallHandler<ReqT, RespT> serverCallHandler) {
    this.allMeta.add(metadata);

    return serverCallHandler.startCall(serverCall, metadata);
  }

  void verifyAllRequestsHasGoogRequestParamsHeader(String bucket, int expectedCallCount) {
    assertEquals(expectedCallCount, allMeta.size());
    for (Metadata metadata : allMeta) {
      assertEquals(
          String.format("bucket=%s", bucket),
          metadata.get(StorageStubProvider.GOOG_REQUEST_PARAMS));
    }
  }

  void verifyAllRequestsHasInvocationId() {
    for (Metadata metadata : allMeta) {
      assertThat(metadata.get(GrpcChannelInvocationIdInterceptor.GOOG_API_CLIENT))
          .contains("gccl-invocation-id");
    }
  }
}
