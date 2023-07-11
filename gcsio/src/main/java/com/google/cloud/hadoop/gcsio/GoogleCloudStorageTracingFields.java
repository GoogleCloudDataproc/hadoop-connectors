/*
 * Copyright 2023 Google LLC
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

public enum GoogleCloudStorageTracingFields {

  // GRPC specific fields
  RPC_METHOD("rpcMethod"),
  IDEMPOTENCY_TOKEN("idempotency-token"),
  REQUEST_COUNTER("requestCounter"),
  RESPONSE_COUNTER("responseCounter"),

  // Common fields
  RESOURCE("resource"),
  UPLOAD_ID("uploadId"),
  WRITE_OFFSET("writeOffset"),

  FINALIZE_WRITE("finalizeWrite"),
  CONTENT_LENGTH("contentLength"),
  PERSISTED_SIZE("persistedSize"),
  READ_OFFSET("readOffset"),
  READ_LIMIT("readLimit"),
  REQUEST_START_OFFSET("requestStartOffset"),
  BYTES_READ("bytesRead"),
  STATUS("status");

  public final String name;

  GoogleCloudStorageTracingFields(String name) {
    this.name = name;
  }
}
