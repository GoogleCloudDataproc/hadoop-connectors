/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.gcsio;

import static com.google.auth.http.AuthHttpConstants.AUTHORIZATION;

import com.google.cloud.hadoop.util.AccessBoundary;
import com.google.cloud.hadoop.util.AccessBoundary.Action;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.flogger.GoogleLogger;
import com.google.storage.v2.ComposeObjectRequest;
import com.google.storage.v2.ComposeObjectRequest.SourceObject;
import com.google.storage.v2.DeleteObjectRequest;
import com.google.storage.v2.ReadObjectRequest;
import com.google.storage.v2.StartResumableWriteRequest;
import com.google.storage.v2.WriteObjectRequest;
import com.google.storage.v2.WriteObjectSpec;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Interceptor to set the downscoped token.
 *
 * <p>When downscoping is enabled, the downscoped token is generated for each request. Java storage
 * client currently does not have the capability to set this for each of the requests. As a work
 * around the credentials will be set from the interceptor.
 *
 * <p>This is a short term solution till Java storage client adds support for setting credentials at
 * a request level.
 */
class GoogleCloudStorageClientGrpcDownscopingInterceptor implements ClientInterceptor {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  @VisibleForTesting
  static final String GOOGLE_STORAGE_V_2_STORAGE_COMPOSE_OBJECT =
      "google.storage.v2.Storage/ComposeObject";

  @VisibleForTesting
  static final String GOOGLE_STORAGE_V_2_STORAGE_DELETE_OBJECT =
      "google.storage.v2.Storage/DeleteObject";

  @VisibleForTesting
  static final String GOOGLE_STORAGE_V_2_STORAGE_READ_OBJECT =
      "google.storage.v2.Storage/ReadObject";

  ;

  @VisibleForTesting
  static final String GOOGLE_STORAGE_V_2_STORAGE_START_RESUMABLE_WRITE =
      "google.storage.v2.Storage/StartResumableWrite";

  @VisibleForTesting
  static final String GOOGLE_STORAGE_V_2_STORAGE_WRITE_OBJECT =
      "google.storage.v2.Storage/WriteObject";

  @VisibleForTesting private static final int BUCKET_PREFIX_LENGTH = "projects/_/buckets/".length();

  @VisibleForTesting
  static Metadata.Key<String> AUTH_KEY =
      Metadata.Key.of(AUTHORIZATION, Metadata.ASCII_STRING_MARSHALLER);

  private final Function<List<AccessBoundary>, String> downscopingFunction;

  public GoogleCloudStorageClientGrpcDownscopingInterceptor(
      Function<List<AccessBoundary>, String> downscopedAccessToken) {
    this.downscopingFunction = downscopedAccessToken;
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
    logger.atFinest().log("interceptCall(): method=%s", method.getFullMethodName());
    return new SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
      private int flowControlRequests;
      private final String methodName = method.getFullMethodName();
      ;
      private Metadata headers;
      private Listener<RespT> responseListener;

      @Override
      public void start(Listener<RespT> responseListener, Metadata headers) {
        this.responseListener = responseListener;
        this.headers = headers;

        logger.atFinest().log("start(): method=%s", methodName);
      }

      @Override
      public void request(int numMessages) {
        if (headers != null) {
          this.flowControlRequests += numMessages;
        } else {
          super.request(numMessages);
        }
      }

      @Override
      public void sendMessage(ReqT message) {
        if (headers != null) {
          setAuthHeader(message);
          super.start(responseListener, headers);
          headers = null;
          if (flowControlRequests != 0) {
            super.request(flowControlRequests);
            flowControlRequests = 0;
          }
        }
        super.sendMessage(message);
      }

      private void setAuthHeader(ReqT message) {
        if (headers == null) {
          return;
        }

        String token = getDownScopedToken(message);
        if (token != null) {
          logger.atFinest().log("Setting down-scoped auth token");
          if (!headers.containsKey(AUTH_KEY)) {
            headers.put(AUTH_KEY, "Bearer " + token);
          }
        }
      }

      private String getDownScopedToken(ReqT message) {
        List<AccessBoundary> accessBoundaries = getAccessBoundaries(message);
        return getDownscopedToken(accessBoundaries);
      }

      private String getBucketName(String bucketLongName) {
        return bucketLongName.substring(BUCKET_PREFIX_LENGTH);
      }

      private List<AccessBoundary> getAccessBoundaries(ReqT message) {
        if (GOOGLE_STORAGE_V_2_STORAGE_READ_OBJECT.equals(methodName)) {
          ReadObjectRequest readObjectRequest = (ReadObjectRequest) message;
          String bucketName = getBucketName(readObjectRequest.getBucket());
          String objectName = readObjectRequest.getObject();

          return ImmutableList.of(
              AccessBoundary.create(bucketName, objectName, Action.READ_OBJECTS));
        }

        if (GOOGLE_STORAGE_V_2_STORAGE_WRITE_OBJECT.equals(methodName)) {
          WriteObjectSpec writeObjectSpec = ((WriteObjectRequest) message).getWriteObjectSpec();
          com.google.storage.v2.Object obj = writeObjectSpec.getResource();

          String theBucket = obj.getBucket();
          if (theBucket.length() == 0) {
            // TODO: Check why this is required.
            return ImmutableList.of();
          }

          return ImmutableList.of(
              AccessBoundary.create(getBucketName(theBucket), obj.getName(), Action.WRITE_OBJECTS));
        }

        if (GOOGLE_STORAGE_V_2_STORAGE_DELETE_OBJECT.equals(methodName)) {
          DeleteObjectRequest deleteObjectRequest = (DeleteObjectRequest) message;

          return ImmutableList.of(
              AccessBoundary.create(
                  getBucketName(deleteObjectRequest.getBucket()),
                  deleteObjectRequest.getObject(),
                  Action.DELETE_OBJECTS));
        }

        if (GOOGLE_STORAGE_V_2_STORAGE_START_RESUMABLE_WRITE.equals(methodName)) {
          com.google.storage.v2.Object obj =
              ((StartResumableWriteRequest) message).getWriteObjectSpec().getResource();

          AccessBoundary accessBoundary =
              AccessBoundary.create(
                  getBucketName(obj.getBucket()), obj.getName(), Action.WRITE_OBJECTS);

          return ImmutableList.of(accessBoundary);
        }

        if (GOOGLE_STORAGE_V_2_STORAGE_COMPOSE_OBJECT.equals(methodName)) {
          ComposeObjectRequest composeObjectRequest = (ComposeObjectRequest) message;
          com.google.storage.v2.Object dest = composeObjectRequest.getDestination();

          List<SourceObject> sourceObjects = composeObjectRequest.getSourceObjectsList();
          String bucketName = getBucketName(dest.getBucket());

          List<AccessBoundary> accessBoundaries = new ArrayList<>(sourceObjects.size() + 1);
          accessBoundaries.add(
              AccessBoundary.create(bucketName, dest.getName(), Action.WRITE_OBJECTS));

          for (SourceObject so : sourceObjects) {
            // TODO: Get bucket from so.
            accessBoundaries.add(
                AccessBoundary.create(bucketName, so.getName(), Action.READ_OBJECTS));
          }

          return accessBoundaries;
        }

        logger.atSevere().log("Unexpected method `%s`", methodName);
        return ImmutableList.of();
      }

      private String getDownscopedToken(List<AccessBoundary> accessBoundaries) {
        logger.atFinest().log(
            "Getting downscoped token for %s; method=%s", accessBoundaries, methodName);
        if (accessBoundaries.size() == 0) {
          return null;
        }

        try {
          return downscopingFunction.apply(accessBoundaries);
        } catch (Throwable e) {
          logger.atSevere().withCause(e).log(
              "Getting down-scoped token failed. details=%s", e.getMessage());
          throw e;
        }
      }
    };
  }
}
