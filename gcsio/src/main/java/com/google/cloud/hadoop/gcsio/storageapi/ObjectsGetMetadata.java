package com.google.cloud.hadoop.gcsio.storageapi;

import com.google.api.client.http.HttpResponse;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.Storage.Objects.Get;
import com.google.common.annotations.VisibleForTesting;

/**
 * Overrides Storage.Objects.Get and provides specific functions for getting object metadata to
 * decouple getting object media from getting metadata.
 */
public class ObjectsGetMetadata extends Get {

  @VisibleForTesting
  ObjectsGetMetadata(Storage.Objects objects, String bucket, String object) {
    objects.super(bucket, object);
  }

  /**
   * Throws {@link WrongRequestTypeException}. GetMetadata is only meant for getting object
   * metadata. Users should call {@link Get#execute()} instead.
   *
   * @return nothing. This method never return normally.
   */
  @Override
  public HttpResponse executeMedia() {
    throw new WrongRequestTypeException("Shouldn't Call executeMedia on GetMetadata requests. ");
  }
}
