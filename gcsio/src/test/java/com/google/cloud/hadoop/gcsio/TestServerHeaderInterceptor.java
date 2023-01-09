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

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import java.util.ArrayList;
import java.util.List;

// Header interceptor to capture the header information received at the server side
class TestServerHeaderInterceptor implements ServerInterceptor {
  private final List<Metadata> allMeta = new ArrayList<>();

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> serverCall,
      Metadata metadata,
      ServerCallHandler<ReqT, RespT> serverCallHandler) {
    this.allMeta.add(metadata);

    return serverCallHandler.startCall(serverCall, metadata);
  }

  void verifyAllRequestsHasGoogRequestParamsHeader(String bucket, int expectedCallCount) {
    assertThat(allMeta).hasSize(expectedCallCount);
    for (Metadata metadata : allMeta) {
      assertThat(metadata.get(StorageStubProvider.GOOG_REQUEST_PARAMS))
          .isEqualTo("bucket=" + bucket);
    }
  }
}
