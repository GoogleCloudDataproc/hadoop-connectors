package com.google.cloud.hadoop.gcsio;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
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
    try {
      return (StorageObject) request.execute(); // Execute the specific GCS request
    } catch (GoogleJsonResponseException e) {
      // Specific error handling for JSON response exceptions
      // Example: retry logic, logging, etc.
      throw new IOException(
          String.format("%s request failed: %s", operationName, e.getMessage()), e);
    } catch (IOException e) {
      // Generic IOException handling
      // Example: logging, rethrowing, etc.
      throw new IOException(
          String.format("%s request failed: %s", operationName, e.getMessage()), e);
    }
  }

  @Override
  public String getOperationName() {
    return operationName;
  }
}
