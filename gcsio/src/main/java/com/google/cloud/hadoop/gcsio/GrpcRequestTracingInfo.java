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

import com.google.common.annotations.VisibleForTesting;
import java.util.OptionalLong;

/**
 * Additional gRPC request specific information that will be emitted in {@link
 * GoogleCloudStorageGrpcTracingInterceptor}.
 */
@VisibleForTesting
public class GrpcRequestTracingInfo {
  final String api = "grpc";
  final String objectName;
  final String requestType;

  private GrpcRequestTracingInfo(String objectName, String requestType) {
    this.objectName = objectName;
    this.requestType = requestType;
  }

  static GrpcRequestTracingInfo getReadRequestTraceInfo(
      String objectName, long objectGeneration, long offset, OptionalLong bytesToRead) {
    return new ReadRequestTracingInfo(objectName, objectGeneration, offset, bytesToRead);
  }

  static GrpcRequestTracingInfo getWriteRequestTraceInfo(String objectName) {
    return new GrpcRequestTracingInfo(objectName, "write");
  }

  public String getApi() {
    return this.api;
  }

  public String getObjectName() {
    return this.objectName;
  }

  public String getRequestType() {
    return this.requestType;
  }

  private static class ReadRequestTracingInfo extends GrpcRequestTracingInfo {
    final long objectGeneration;
    final long offset;
    final OptionalLong bytesToRead;

    ReadRequestTracingInfo(
        String objectName, long objectGeneration, long offset, OptionalLong bytesToRead) {
      super(objectName, "read");
      this.objectGeneration = objectGeneration;
      this.offset = offset;
      this.bytesToRead = bytesToRead;
    }
  }
}
