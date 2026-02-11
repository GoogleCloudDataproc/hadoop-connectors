package com.google.cloud.hadoop.gcsio;

import com.google.api.services.storage.StorageRequest;
import com.google.api.services.storage.model.StorageObject;
import java.io.IOException;

public class GoogleCloudStorageObjectsRequest<T extends StorageRequest<?>>
    implements StorageObjectRequest<T> {
  private final T request;
  private final String operationName;

  public GoogleCloudStorageObjectsRequest(T request, String operationName) {
    this.request = request;
    this.operationName = operationName;
  }

  @Override
  public StorageObject execute() throws IOException {
    // Long startTime = System.currentTimeMillis();
    return (StorageObject) request.execute();
    // increment GCS_API_TOTAL metric by System.currentTimeMillis() - startTime;
    // if time is greater > threshold then log
  }

  @Override
  public String getOperationName() {
    return operationName;
  }
}
