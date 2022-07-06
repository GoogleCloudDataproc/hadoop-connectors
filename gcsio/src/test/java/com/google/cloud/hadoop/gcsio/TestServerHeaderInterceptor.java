package com.google.cloud.hadoop.gcsio;

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
}
