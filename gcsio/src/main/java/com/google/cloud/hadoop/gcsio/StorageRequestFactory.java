package com.google.cloud.hadoop.gcsio;

import com.google.api.client.http.HttpResponse;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.Storage.Objects.Get;
import com.google.api.services.storage.model.StorageObject;
import com.google.common.annotations.VisibleForTesting;

/** Factory class to create request types that overrides storage api. */
class StorageRequestFactory {

  // The Storage object to use to instantiate the requests.
  private final Storage storage;

  /**
   * Instantiates a RequestFactory. The overridden request types requires a storage instance since
   * the types are inner classes of {@link Storage}.
   *
   * @param storage a google api storage object.
   */
  StorageRequestFactory(Storage storage) {
    this.storage = storage;
  }

  /** Instantiates a {@link ObjectsGetData} request. */
  ObjectsGetData objectsGetData(String bucketName, String objectName) {
    return new ObjectsGetData(storage.objects(), bucketName, objectName);
  }

  /** Instantiates a {@link ObjectsGetMetadata} request. */
  ObjectsGetMetadata objectsGetMetadata(String bucketName, String objectName) {
    return new ObjectsGetMetadata(storage.objects(), bucketName, objectName);
  }

  /** Thrown when calling execute or executeMedia methods on request types not supporting them. */
  static class WrongRequestTypeException extends RuntimeException {
    WrongRequestTypeException(String message) {
      super(message);
    }
  }

  /**
   * Overrides Storage.Objects.Get and provides specific functions for getting object data to
   * decouple getting object data from getting metadata.
   */
  static class ObjectsGetData extends Get {

    @VisibleForTesting
    private ObjectsGetData(Storage.Objects objects, String bucket, String object) {
      objects.super(bucket, object);
    }

    /**
     * Throws {@link WrongRequestTypeException}. GetData is only meant for getting object data.
     * Users should call {@link Get#executeMedia()} instead.
     *
     * @return nothing. This method never return normally.
     */
    @Override
    public StorageObject execute() {
      throw new WrongRequestTypeException("Shouldn't Call execute on GetData requests. ");
    }
  }

  /**
   * Overrides Storage.Objects.Get and provides specific functions for getting object metadata to
   * decouple getting object media from getting metadata.
   */
  static class ObjectsGetMetadata extends Get {

    @VisibleForTesting
    private ObjectsGetMetadata(Storage.Objects objects, String bucket, String object) {
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
}
