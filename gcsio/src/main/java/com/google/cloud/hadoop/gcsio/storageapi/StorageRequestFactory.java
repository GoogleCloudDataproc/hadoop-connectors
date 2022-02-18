package com.google.cloud.hadoop.gcsio.storageapi;

import com.google.api.services.storage.Storage;

/** Factory class to create request types that overrides storage api. */
public class StorageRequestFactory {

  // The Storage object to use to instantiate the requests.
  private final Storage storage;

  /**
   * Instantiates a RequestFactory. The overridden request types requires a storage instance since
   * the types are inner classes of {@link Storage}.
   *
   * @param storage a google api storage object.
   */
  public StorageRequestFactory(Storage storage) {
    this.storage = storage;
  }

  /** Instantiates a {@link ObjectsGetData} request. */
  public ObjectsGetData objectsGetData(String bucketName, String objectName) {
    return new ObjectsGetData(storage.objects(), bucketName, objectName);
  }

  /** Instantiates a {@link ObjectsGetMetadata} request. */
  public ObjectsGetMetadata objectsGetMetadata(String bucketName, String objectName) {
    return new ObjectsGetMetadata(storage.objects(), bucketName, objectName);
  }
}
