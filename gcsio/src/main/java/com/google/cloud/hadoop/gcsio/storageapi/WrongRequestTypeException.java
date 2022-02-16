package com.google.cloud.hadoop.gcsio.storageapi;

/** Thrown when calling execute or executeMedia methods on request types not supporting them. */
public class WrongRequestTypeException extends RuntimeException {
  public WrongRequestTypeException(String message) {
    super(message);
  }
}
