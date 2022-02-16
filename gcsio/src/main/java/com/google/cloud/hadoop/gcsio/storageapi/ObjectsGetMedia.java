package com.google.cloud.hadoop.gcsio.storageapi;

import com.google.api.services.storage.Storage;
import com.google.api.services.storage.Storage.Objects.Get;
import com.google.api.services.storage.model.StorageObject;
import com.google.common.annotations.VisibleForTesting;

/**
 * Overrides Storage.Objects.Get and provides specific functions for getting object media to
 * decouple getting object media from getting metadata.
 */
public class ObjectsGetMedia extends Get {

  @VisibleForTesting
  ObjectsGetMedia(Storage.Objects objects, String bucket, String object) {
    objects.super(bucket, object);
  }

  /**
   * Throws {@link WrongRequestTypeException}. GetMedia is only meant for getting object data. Users
   * should call {@link Get#executeMedia()} instead.
   *
   * @return nothing. This method never return normally.
   */
  @Override
  public StorageObject execute() {
    throw new WrongRequestTypeException("Shouldn't Call execute on GetMedia type. ");
  }
}
