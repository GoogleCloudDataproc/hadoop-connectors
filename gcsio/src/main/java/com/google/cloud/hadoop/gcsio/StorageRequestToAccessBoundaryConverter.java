/*
 * Copyright 2021 Google LLC
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.gcsio;

import com.google.api.client.http.json.JsonHttpContent;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.StorageRequest;
import com.google.api.services.storage.model.Bucket;
import com.google.api.services.storage.model.ComposeRequest;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.hadoop.util.AccessBoundary;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import java.security.InvalidParameterException;
import java.util.Collections;
import java.util.List;

/** A utility class to convert StorageRequest to a list of AccessBoundary objects. */
class StorageRequestToAccessBoundaryConverter {

  /**
   * Translates a {@link StorageRequest} to a list of {@link AccessBoundary} messages
   *
   * @param request the {@link StorageRequest} instance
   * @return a list of {@link AccessBoundary} messages corresponding to the request
   */
  public static <RequestT extends StorageRequest<?>> List<AccessBoundary> fromStorageObjectRequest(
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
        "StorageRequest not used by the connector: " + request.getClass().getCanonicalName());
  }

  private static List<AccessBoundary> translateObjectListRequest(Storage.Objects.List request) {
    String prefix = request.getPrefix();
    if (Strings.isNullOrEmpty(prefix)) {
      prefix = "/";
    }
    return Collections.singletonList(AccessBoundary.create(request.getBucket(), prefix, "read"));
  }

  private static List<AccessBoundary> translateObjectInsertRequest(Storage.Objects.Insert request) {
    String path = ((StorageObject) getData(request)).getName();
    return Collections.singletonList(AccessBoundary.create(request.getBucket(), path, "write"));
  }

  private static List<AccessBoundary> translateObjectComposeRequest(
      Storage.Objects.Compose request) {
    ImmutableList.Builder<AccessBoundary> listBuilder = ImmutableList.builder();
    // Read access on all the sources
    ((ComposeRequest) getData(request))
        .getSourceObjects()
        .forEach(
            source ->
                listBuilder.add(
                    AccessBoundary.create(
                        request.getDestinationBucket(), source.getName(), "read")));
    // Write access on the destination
    listBuilder.add(
        AccessBoundary.create(
            request.getDestinationBucket(), request.getDestinationObject(), "write"));
    return listBuilder.build();
  }

  private static List<AccessBoundary> translateObjectGetRequest(Storage.Objects.Get request) {
    return Collections.singletonList(
        AccessBoundary.create(request.getBucket(), request.getObject(), "read"));
  }

  private static List<AccessBoundary> translateObjectDeleteRequest(Storage.Objects.Delete request) {
    return Collections.singletonList(
        AccessBoundary.create(request.getBucket(), request.getObject(), "write"));
  }

  private static List<AccessBoundary> translateObjectRewriteRequest(
      Storage.Objects.Rewrite request) {
    ImmutableList.Builder<AccessBoundary> listBuilder = ImmutableList.builder();
    listBuilder.add(
        AccessBoundary.create(request.getSourceBucket(), request.getSourceObject(), "read"));
    listBuilder.add(
        AccessBoundary.create(
            request.getDestinationBucket(), request.getDestinationObject(), "write"));
    return listBuilder.build();
  }

  private static List<AccessBoundary> translateObjectCopyRequest(Storage.Objects.Copy request) {
    ImmutableList.Builder<AccessBoundary> listBuilder = ImmutableList.builder();
    listBuilder.add(
        AccessBoundary.create(request.getSourceBucket(), request.getSourceObject(), "read"));
    listBuilder.add(
        AccessBoundary.create(
            request.getDestinationBucket(), request.getDestinationObject(), "write"));
    return listBuilder.build();
  }

  private static List<AccessBoundary> translateObjectPatchRequest(Storage.Objects.Patch request) {
    return Collections.singletonList(
        AccessBoundary.create(request.getBucket(), request.getObject(), "write"));
  }

  private static List<AccessBoundary> translateBucketGetRequest(Storage.Buckets.Get request) {
    return Collections.singletonList(AccessBoundary.create(request.getBucket(), "/", "read"));
  }

  private static List<AccessBoundary> translateBucketInsertRequest(Storage.Buckets.Insert request) {
    String bucketName = ((Bucket) getData(request)).getName();
    return Collections.singletonList(AccessBoundary.create(bucketName, "/", "write"));
  }

  private static List<AccessBoundary> translateBucketDeleteRequest(Storage.Buckets.Delete request) {
    return Collections.singletonList(AccessBoundary.create(request.getBucket(), "/", "write"));
  }

  private static List<AccessBoundary> translateBucketListRequest(Storage.Buckets.List request) {
    return Collections.singletonList(AccessBoundary.create("", "", "read"));
  }

  private static Object getData(StorageRequest<?> request) {
    return ((JsonHttpContent) request.getHttpContent()).getData();
  }
}
