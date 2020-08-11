package com.google.cloud.hadoop.gcsio.authorization;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;

import com.google.api.client.http.json.JsonHttpContent;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.StorageRequest;
import com.google.api.services.storage.model.Bucket;
import com.google.api.services.storage.model.ComposeRequest;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.hadoop.gcsio.UriPaths;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.flogger.GoogleLogger;
import java.net.URI;
import java.nio.file.AccessDeniedException;
import java.util.List;
import java.util.Map;

/** Authorize storage requests using user specified authorization handler. */
public class StorageRequestAuthorizer {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private final AuthorizationHandler authorizationHandler;

  /**
   * Create and init AuthorizationHandler from Class definition.
   *
   * @param handlerClass Class object of Authorization handler.
   * @param properties String to String map of properties.
   */
  public StorageRequestAuthorizer(
      Class<? extends AuthorizationHandler> handlerClass, Map<String, String> properties) {
    checkNotNull(handlerClass, "handlerClass can not be null");
    checkNotNull(properties, "properties can not be null");
    try {
      this.authorizationHandler = handlerClass.getDeclaredConstructor().newInstance();
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException("Can not instantiate authorization handler.", e);
    }
    this.authorizationHandler.setProperties(properties);
  }

  /**
   * Use dependency injection to allow more flexible authorization behavior.
   *
   * @param authorizationHandler An already initialized {@link AuthorizationHandler}.
   */
  @VisibleForTesting
  StorageRequestAuthorizer(AuthorizationHandler authorizationHandler) {
    this.authorizationHandler =
        checkNotNull(authorizationHandler, "authorizationHandler can not be null");
  }

  /**
   * Authorize a storage request using {@link AuthorizationHandler}.
   *
   * @param request Storage request to be authorized.
   * @throws AccessDeniedException Thrown when access denied by AuthorizationHandler.
   */
  public void authorize(StorageRequest<?> request) throws AccessDeniedException {
    logger.atFine().log("authorizeStorageRequest(%s)", request);

    // Objects
    if (request instanceof Storage.Objects.List) {
      Storage.Objects.List listRequest = (Storage.Objects.List) request;
      authorizationHandler.handleListObjects(
          getGcsUri(listRequest.getBucket(), listRequest.getPrefix()));
    } else if (request instanceof Storage.Objects.Insert) {
      Storage.Objects.Insert insertRequest = (Storage.Objects.Insert) request;
      authorizationHandler.handleInsertObject(
          getGcsUri(insertRequest.getBucket(), ((StorageObject) getData(request)).getName()));
    } else if (request instanceof Storage.Objects.Compose) {
      Storage.Objects.Compose composeRequest = (Storage.Objects.Compose) request;
      String bucket = composeRequest.getDestinationBucket();
      URI destination = getGcsUri(bucket, composeRequest.getDestinationObject());
      List<URI> sources =
          ((ComposeRequest) getData(request))
              .getSourceObjects().stream()
                  .map(source -> getGcsUri(bucket, source.getName()))
                  .collect(toImmutableList());
      authorizationHandler.handleComposeObject(destination, sources);
    } else if (request instanceof Storage.Objects.Get) {
      Storage.Objects.Get getRequest = (Storage.Objects.Get) request;
      authorizationHandler.handleGetObject(
          getGcsUri(getRequest.getBucket(), getRequest.getObject()));
    } else if (request instanceof Storage.Objects.Delete) {
      Storage.Objects.Delete deleteRequest = (Storage.Objects.Delete) request;
      authorizationHandler.handleDeleteObject(
          getGcsUri(deleteRequest.getBucket(), deleteRequest.getObject()));
    } else if (request instanceof Storage.Objects.Rewrite) {
      Storage.Objects.Rewrite rewriteRequest = (Storage.Objects.Rewrite) request;
      authorizationHandler.handleRewriteObject(
          getGcsUri(rewriteRequest.getSourceBucket(), rewriteRequest.getSourceObject()),
          getGcsUri(rewriteRequest.getDestinationBucket(), rewriteRequest.getDestinationObject()));

    } else if (request instanceof Storage.Objects.Copy) {
      Storage.Objects.Copy copyRequest = (Storage.Objects.Copy) request;
      authorizationHandler.handleCopyObject(
          getGcsUri(copyRequest.getSourceBucket(), copyRequest.getSourceObject()),
          getGcsUri(copyRequest.getDestinationBucket(), copyRequest.getDestinationObject()));
    } else if (request instanceof Storage.Objects.Patch) {
      Storage.Objects.Patch patchRequest = (Storage.Objects.Patch) request;
      authorizationHandler.handlePatchObject(
          getGcsUri(patchRequest.getBucket(), patchRequest.getObject()));
    }

    // Buckets
    else if (request instanceof Storage.Buckets.List) {
      authorizationHandler.handleListBuckets(((Storage.Buckets.List) request).getProject());
    } else if (request instanceof Storage.Buckets.Insert) {
      authorizationHandler.handleInsertBucket(
          ((Storage.Buckets.Insert) request).getProject(),
          getGcsUri(((Bucket) getData(request)).getName(), /* objectPath= */ null));
    } else if (request instanceof Storage.Buckets.Get) {
      authorizationHandler.handleGetBucket(
          getGcsUri(((Storage.Buckets.Get) request).getBucket(), /* objectPath= */ null));
    } else if (request instanceof Storage.Buckets.Delete) {
      authorizationHandler.handleDeleteBucket(
          getGcsUri(((Storage.Buckets.Delete) request).getBucket(), /* objectPath= */ null));
    }

    // Unhandled request types. All storage request should be covered by the previous checks.
    else {
      throw new RuntimeException("Unhandled storage request type. Request: " + request);
    }
  }

  private static Object getData(StorageRequest<?> request) {
    return ((JsonHttpContent) request.getHttpContent()).getData();
  }

  /**
   * Create a URI of GCS bucket from bucket name and object path.
   *
   * @param bucketName A string means GCS bucket name.
   * @param objectPath A prefix of object path.
   * @return URI of the GCS object.
   */
  private static URI getGcsUri(String bucketName, String objectPath) {
    return UriPaths.fromStringPathComponents(
        bucketName, objectPath, /* allowEmptyObjectName= */ true);
  }
}
