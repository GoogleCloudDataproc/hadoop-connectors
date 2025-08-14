package com.google.cloud.hadoop.gcsio;

import com.google.api.services.storage.model.StorageObject;
import java.io.IOException;

public interface StorageObjectRequest<T> {
  StorageObject execute() throws IOException;

  String getOperationName(); // Method to retrieve the operation name
}
