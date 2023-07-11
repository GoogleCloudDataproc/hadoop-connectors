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

  private final String name;

  GoogleCloudStorageTracingFields(String name) {
    this.name = name;
  }
}
