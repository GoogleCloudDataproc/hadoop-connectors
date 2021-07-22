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
   * Translates a {@link StorageRequest} to a list of {@link GcsResourceAndAction} messages
   *
   * @param request the {@link StorageRequest} instance
   * @return a list of {@link GcsResourceAndAction} messages corresponding to the request
   */
  public static <RequestT extends StorageRequest<?>> List<GcsResourceAndAction> fromStorageObjectRequest(
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

  private static List<GcsResourceAndAction> translateObjectListRequest(
      Storage.Objects.List request) {
    String prefix = request.getPrefix();
    if (Strings.isNullOrEmpty(prefix)) {
      prefix = "/";
    }
    return Collections
        .singletonList(new GcsResourceAndAction(request.getBucket(), prefix, "read"));
  }

  private static List<GcsResourceAndAction> translateObjectInsertRequest(
      Storage.Objects.Insert request) {
    String path = ((StorageObject) getData(request)).getName();
    return Collections.singletonList(
        new GcsResourceAndAction(request.getBucket(), path, "write"));
  }

  private static List<GcsResourceAndAction> translateObjectComposeRequest(
      Storage.Objects.Compose request) {
    List<GcsResourceAndAction> resourceAndActions = new ArrayList<>();
    // Read access on all the sources
    ((ComposeRequest) getData(request)).getSourceObjects().stream()
        .forEach(source -> resourceAndActions.add(
            new GcsResourceAndAction(request.getDestinationBucket(), source.getName(), "read")));
    // Write access on the destination
    resourceAndActions.add(
        new GcsResourceAndAction(request.getDestinationBucket(), request.getDestinationObject(),
            "write"));
    return resourceAndActions;
  }

  private static List<GcsResourceAndAction> translateObjectGetRequest(
      Storage.Objects.Get request) {
    return Collections.singletonList(
        new GcsResourceAndAction(request.getBucket(), request.getObject(), "read"));
  }

  private static List<GcsResourceAndAction> translateObjectDeleteRequest(
      Storage.Objects.Delete request) {
    return Collections.singletonList(
        new GcsResourceAndAction(request.getBucket(), request.getObject(), "write"));
  }

  private static List<GcsResourceAndAction> translateObjectRewriteRequest(
      Storage.Objects.Rewrite request) {
    List<GcsResourceAndAction> resourceAndActions = new ArrayList<>();
    resourceAndActions.add(
        new GcsResourceAndAction(request.getSourceBucket(), request.getSourceObject(), "read"));
    resourceAndActions.add(
        new GcsResourceAndAction(request.getDestinationBucket(), request.getDestinationObject(),
            "write"));
    return resourceAndActions;
  }

  private static List<GcsResourceAndAction> translateObjectCopyRequest(
      Storage.Objects.Copy request) {
    List<GcsResourceAndAction> resourceAndActions = new ArrayList<>();
    resourceAndActions.add(
        new GcsResourceAndAction(request.getSourceBucket(), request.getSourceObject(), "read"));
    resourceAndActions.add(
        new GcsResourceAndAction(request.getDestinationBucket(), request.getDestinationObject(),
            "write"));
    return resourceAndActions;
  }

  private static List<GcsResourceAndAction> translateObjectPatchRequest(
      Storage.Objects.Patch request) {
    return Collections.singletonList(
        new GcsResourceAndAction(request.getBucket(), request.getObject(), "write"));
  }

  private static List<GcsResourceAndAction> translateBucketGetRequest(
      Storage.Buckets.Get request) {
    return Collections.singletonList(
        new GcsResourceAndAction(request.getBucket(), "/", "read"));
  }

  private static List<GcsResourceAndAction> translateBucketInsertRequest(
      Storage.Buckets.Insert request) {
    String bucketName = ((Bucket) getData(request)).getName();
    return Collections.singletonList(
        new GcsResourceAndAction(bucketName, "/", "write"));
  }

  private static List<GcsResourceAndAction> translateBucketDeleteRequest(
      Storage.Buckets.Delete request) {
    return Collections.singletonList(
        new GcsResourceAndAction(request.getBucket(), "/", "write"));
  }

  private static List<GcsResourceAndAction> translateBucketListRequest(
      Storage.Buckets.List request) {
    return Collections.singletonList(new GcsResourceAndAction());
  }

  private static Object getData(StorageRequest request) {
    return ((JsonHttpContent) request.getHttpContent()).getData();
  }
}
