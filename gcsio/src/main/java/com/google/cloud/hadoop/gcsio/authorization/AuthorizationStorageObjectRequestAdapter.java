package com.google.cloud.hadoop.gcsio.authorization;

import com.google.api.client.http.json.JsonHttpContent;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.StorageRequest;
import com.google.api.services.storage.model.Bucket;
import com.google.api.services.storage.model.ComposeRequest;
import com.google.api.services.storage.model.StorageObject;
import com.google.common.base.Strings;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class AuthorizationStorageObjectRequestAdapter {

  /**
   * Translates a {@link StorageRequest} to a list of {@link GcsResourceAndActions} messages
   *
   * @param request the {@link StorageRequest} instance
   * @return a list of {@link GcsResourceAndActions} messages corresponding to the request
   */
  public static <RequestT extends StorageRequest<?>> List<GcsResourceAndActions> fromStorageObjectRequest(
      RequestT request) {
    if (request instanceof Storage.Objects.List) {
      return translateObjectListRequest((Storage.Objects.List) request);
    } else if (request instanceof Storage.Objects.Insert) {
      return translateObjectInsertRequest((Storage.Objects.Insert) request);
    } else if (request instanceof Storage.Objects.Compose) {
      return translateObjectComposeRequest((Storage.Objects.Compose) request);
    } else if (request instanceof Storage.Objects.Get) {
      return translateObjectGetRequest((Storage.Objects.Get) request);
    } else if (request instanceof Storage.Objects.Delete) {
      return translateObjectDeleteRequest((Storage.Objects.Delete) request);
    } else if (request instanceof Storage.Objects.Rewrite) {
      return translateObjectRewriteRequest((Storage.Objects.Rewrite) request);
    } else if (request instanceof Storage.Objects.Copy) {
      return translateObjectCopyRequest((Storage.Objects.Copy) request);
    } else if (request instanceof Storage.Objects.Patch) {
      return translateObjectPatchRequest((Storage.Objects.Patch) request);
    } else if (request instanceof Storage.Buckets.Get) {
      return translateBucketGetRequest((Storage.Buckets.Get) request);
    } else if (request instanceof Storage.Buckets.Insert) {
      return translateBucketInsertRequest((Storage.Buckets.Insert) request);
    } else if (request instanceof Storage.Buckets.Delete) {
      return translateBucketDeleteRequest((Storage.Buckets.Delete) request);
    } else if (request instanceof Storage.Buckets.List) {
      return translateBucketListRequest((Storage.Buckets.List) request);
    }

    // We enumerated all object and bucket operations used by the GCS connector already. If still
    // no match then it should be an exception.
    throw new InvalidParameterException(
        "StorageRequest not used by the connector: " + request.getClass().getSimpleName());
  }

  private static List<GcsResourceAndActions> translateObjectListRequest(
      Storage.Objects.List request) {
    String prefix = request.getPrefix();
    if (Strings.isNullOrEmpty(prefix)) {
      prefix = "/";
    }
    return Collections
        .singletonList(new GcsResourceAndActions(request.getBucket(), prefix, "read"));
  }

  private static List<GcsResourceAndActions> translateObjectInsertRequest(
      Storage.Objects.Insert request) {
    String path = ((StorageObject) getData(request)).getName();
    return Collections.singletonList(
        new GcsResourceAndActions(request.getBucket(), path, "write"));
  }

  private static List<GcsResourceAndActions> translateObjectComposeRequest(
      Storage.Objects.Compose request) {
    List<GcsResourceAndActions> resourceAndActions = new ArrayList<>();
    // Read access on all the sources
    ((ComposeRequest) getData(request)).getSourceObjects().stream()
        .forEach(source -> resourceAndActions.add(
            new GcsResourceAndActions(request.getDestinationBucket(), source.getName(), "read")));
    // Write access on the destination
    resourceAndActions.add(
        new GcsResourceAndActions(request.getDestinationBucket(), request.getDestinationObject(),
            "write"));
    return resourceAndActions;
  }

  private static List<GcsResourceAndActions> translateObjectGetRequest(
      Storage.Objects.Get request) {
    return Collections.singletonList(
        new GcsResourceAndActions(request.getBucket(), request.getObject(), "read"));
  }

  private static List<GcsResourceAndActions> translateObjectDeleteRequest(
      Storage.Objects.Delete request) {
    return Collections.singletonList(
        new GcsResourceAndActions(request.getBucket(), request.getObject(), "write"));
  }

  private static List<GcsResourceAndActions> translateObjectRewriteRequest(
      Storage.Objects.Rewrite request) {
    List<GcsResourceAndActions> resourceAndActions = new ArrayList<>();
    resourceAndActions.add(
        new GcsResourceAndActions(request.getSourceBucket(), request.getSourceObject(), "read"));
    resourceAndActions.add(
        new GcsResourceAndActions(request.getDestinationBucket(), request.getDestinationObject(),
            "write"));
    return resourceAndActions;
  }

  private static List<GcsResourceAndActions> translateObjectCopyRequest(
      Storage.Objects.Copy request) {
    List<GcsResourceAndActions> resourceAndActions = new ArrayList<>();
    resourceAndActions.add(
        new GcsResourceAndActions(request.getSourceBucket(), request.getSourceObject(), "read"));
    resourceAndActions.add(
        new GcsResourceAndActions(request.getDestinationBucket(), request.getDestinationObject(),
            "write"));
    return resourceAndActions;
  }

  private static List<GcsResourceAndActions> translateObjectPatchRequest(
      Storage.Objects.Patch request) {
    return Collections.singletonList(
        new GcsResourceAndActions(request.getBucket(), request.getObject(), "write"));
  }

  private static List<GcsResourceAndActions> translateBucketGetRequest(
      Storage.Buckets.Get request) {
    return Collections.singletonList(
        new GcsResourceAndActions(request.getBucket(), "/", "read"));
  }

  private static List<GcsResourceAndActions> translateBucketInsertRequest(
      Storage.Buckets.Insert request) {
    String bucketName = ((Bucket) getData(request)).getName();
    return Collections.singletonList(
        new GcsResourceAndActions(bucketName, "/", "write"));
  }

  private static List<GcsResourceAndActions> translateBucketDeleteRequest(
      Storage.Buckets.Delete request) {
    return Collections.singletonList(
        new GcsResourceAndActions(request.getBucket(), "/", "write"));
  }

  private static List<GcsResourceAndActions> translateBucketListRequest(
      Storage.Buckets.List request) {
    return Collections.singletonList(new GcsResourceAndActions());
  }

  private static Object getData(StorageRequest request) {
    return ((JsonHttpContent) request.getHttpContent()).getData();
  }
}
