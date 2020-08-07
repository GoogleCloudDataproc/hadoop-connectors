package com.google.cloud.hadoop.gcsio.authorization;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.client.http.json.JsonHttpContent;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.StorageRequest;
import com.google.api.services.storage.model.ComposeRequest;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.hadoop.gcsio.UriPaths;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.flogger.GoogleLogger;
import java.net.URI;
import java.nio.file.AccessDeniedException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Authorize storage requests using user specified authorization handler. */
public class StorageRequestAuthorizer {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();
  private static final String NULL_HANDLER_MSG = "handlerClass can not be null.";

  private AuthorizationHandler authorizationHandler = null;

  /**
   * Create and init AuthorizationHandler from Class definition.
   *
   * @param handlerClass Class object of Authorization handler.
   * @param properties String to String map of properties.
   */
  public StorageRequestAuthorizer(
      Class<? extends AuthorizationHandler> handlerClass, Map<String, String> properties) {
    checkNotNull(handlerClass, NULL_HANDLER_MSG);
    try {
      authorizationHandler = handlerClass.getDeclaredConstructor().newInstance();
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException("Can not instantiate authorization handler.", e);
    }
    authorizationHandler.setProperties(properties);
  }

  /**
   * Use dependency injection to allow more flexible authorization behavior.
   *
   * @param authorizationHandler An already initialized AuthorizationHandler.
   */
  @VisibleForTesting
  public StorageRequestAuthorizer(AuthorizationHandler authorizationHandler) {
    checkNotNull(authorizationHandler, NULL_HANDLER_MSG);
    this.authorizationHandler = authorizationHandler;
  }

  /**
   * Authorize a storage request using AuthorizationHandler.
   *
   * @param request Storage request to be authorized.
   * @param bucketName The bucket name of storage request.
   * @param <RequestT> Storage request type.
   * @throws AccessDeniedException Thrown when access denied by AuthorizationHandler.
   */
  public <RequestT extends StorageRequest<?>> void authorize(RequestT request, String bucketName)
      throws AccessDeniedException {
    logger.atFine().log(
        "authorizeStorageRequest(%s, %s) Entering authorization handler.", request, bucketName);

    // Object
    if (request instanceof Storage.Objects.List) {
      authorizationHandler.handleListObject(
          createGCSURI(
              ((Storage.Objects.List) request).getBucket(),
              ((Storage.Objects.List) request).getPrefix()));
    } else if (request instanceof Storage.Objects.Insert) {
      authorizationHandler.handleInsertObject(
          createGCSURI(
              bucketName,
              ((StorageObject) ((JsonHttpContent) request.getHttpContent()).getData()).getName()));
    } else if (request instanceof Storage.Objects.Compose) {
      URI destinationURI =
          createGCSURI(bucketName, ((Storage.Objects.Compose) request).getDestinationObject());
      // read all source objects
      List<URI> sourceList = new ArrayList<>();
      for (ComposeRequest.SourceObjects source :
          ((ComposeRequest) ((JsonHttpContent) request.getHttpContent()).getData())
              .getSourceObjects()) {
        sourceList.add(createGCSURI(bucketName, source.getName()));
      }
      authorizationHandler.handleComposeObject(destinationURI, sourceList);
    } else if (request instanceof Storage.Objects.Get) {
      authorizationHandler.handleGetObject(
          createGCSURI(bucketName, ((Storage.Objects.Get) request).getObject()));
    } else if (request instanceof Storage.Objects.Delete) {
      authorizationHandler.handleDeleteObject(
          createGCSURI(bucketName, ((Storage.Objects.Delete) request).getObject()));
    } else if (request instanceof Storage.Objects.Rewrite) {
      authorizationHandler.handleRewriteObject(
          createGCSURI(
              ((Storage.Objects.Rewrite) request).getSourceBucket(),
              ((Storage.Objects.Rewrite) request).getSourceObject()),
          createGCSURI(
              ((Storage.Objects.Rewrite) request).getDestinationBucket(),
              ((Storage.Objects.Rewrite) request).getDestinationObject()));

    } else if (request instanceof Storage.Objects.Copy) {
      authorizationHandler.handleCopyObject(
          createGCSURI(
              ((Storage.Objects.Copy) request).getSourceBucket(),
              ((Storage.Objects.Copy) request).getSourceObject()),
          createGCSURI(
              ((Storage.Objects.Copy) request).getDestinationBucket(),
              ((Storage.Objects.Copy) request).getDestinationObject()));
    } else if (request instanceof Storage.Objects.Patch) {
      authorizationHandler.handlePatchObject(
          createGCSURI(bucketName, ((Storage.Objects.Patch) request).getObject()));
    }

    // Bucket
    else if (request instanceof Storage.Buckets.List) {
      authorizationHandler.handleListBucket();
    } else if (request instanceof Storage.Buckets.Insert) {
      authorizationHandler.handleInsertBucket(createGCSURI(bucketName, null));
    } else if (request instanceof Storage.Buckets.Get) {
      authorizationHandler.handleGetBucket(createGCSURI(bucketName, null));
    } else if (request instanceof Storage.Buckets.Delete) {
      authorizationHandler.handleDeleteBucket(createGCSURI(bucketName, null));
    }

    // Unhandled request types. All storage request should be covered by the previous checks.
    else {
      throw new RuntimeException(
          "Unhandled storage request type encountered during StorageRequestAuthorizer.authorize. Request: "
              + request);
    }
  }

  /**
   * Create a URI of GCS bucket from bucket name and object path.
   *
   * @param bucketName A string means GCS bucket name.
   * @param objectPath A prefix of object path.
   * @return URI of the GCS object.
   */
  @VisibleForTesting
  static URI createGCSURI(String bucketName, String objectPath) {
    return UriPaths.fromStringPathComponents(bucketName, objectPath, true);
  }
}
