/*
 * Copyright 2024 Google LLC. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.fs.gcs;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpResponseException;
import com.google.cloud.hadoop.util.GcsRequestExecutionEvent;
import com.google.common.eventbus.Subscribe;
import io.grpc.Status;
import java.io.IOException;
import javax.annotation.Nonnull;

/* Stores the subscriber methods corresponding to GoogleCloudStorageEventBus */
public class GoogleCloudStorageEventSubscriber {
  private static GhfsGlobalStorageStatistics storageStatistics;

  public GoogleCloudStorageEventSubscriber(GhfsGlobalStorageStatistics storageStatistics) {
    this.storageStatistics = storageStatistics;
  }

  /**
   * Updating the required gcs specific statistics based on HttpResponseException.
   *
   * @param responseException contains statusCode based on which metrics are updated
   */
  @Subscribe
  private void subscriberOnHttpResponseException(@Nonnull HttpResponseException responseException) {
    updateGcsIOSpecificStatistics(responseException.getStatusCode());
  }

  /**
   * Updating the required gcs specific statistics based on GoogleJsonResponseException.
   *
   * @param responseException contains statusCode based on which metrics are updated
   */
  @Subscribe
  private void subscriberOnGoogleJsonResponseException(
      @Nonnull GoogleJsonResponseException responseException) {
    updateGcsIOSpecificStatistics(responseException.getStatusCode());
  }

  /**
   * Updating the required gcs specific statistics based on HttpResponse.
   *
   * @param responseStatus status code from HTTP response
   */
  @Subscribe
  private void subscriberOnHttpResponseStatus(@Nonnull Integer responseStatus) {
    updateGcsIOSpecificStatistics(responseStatus);
    incrementStatusCode(responseStatus);
  }

  @Subscribe
  private void subscriberOnGcsRequest(@Nonnull GcsRequestExecutionEvent event) {
    storageStatistics.incrementGcsTotalRequestCount();
  }

  @Subscribe
  private void subscriberOnGrpcStatus(@Nonnull Status status) {
    updateGcsIOSpecificStatistics(grpcToHttpStatusCodeMapping(status));
  }

  /**
   * Updating the EXCEPTION_COUNT
   *
   * @param exception
   */
  @Subscribe
  private void subscriberOnException(IOException exception) {
    storageStatistics.incrementGcsExceptionCount();
  }

  /**
   * Updating the required gcs specific statistics based on httpresponse.
   *
   * @param statusCode
   */
  protected void updateGcsIOSpecificStatistics(int statusCode) {

    if (statusCode >= 400 && statusCode < 500) {
      storageStatistics.incrementGcsClientSideCounter();

      if (statusCode == 429) {
        storageStatistics.incrementRateLimitingCounter();
      }
    }
    if (statusCode >= 500 && statusCode < 600) {
      storageStatistics.incrementGcsServerSideCounter();
    }
  }

  private int grpcToHttpStatusCodeMapping(Status grpcStatusCode) {
    // using code.proto as reference
    // https://github.com/googleapis/googleapis/blob/master/google/rpc/code.proto
    switch (grpcStatusCode.getCode()) {
      case OK:
        return 200;
      case CANCELLED:
        return 499;
      case INVALID_ARGUMENT:
      case FAILED_PRECONDITION:
      case OUT_OF_RANGE:
        return 400;
      case DEADLINE_EXCEEDED:
        return 504;
      case NOT_FOUND:
        return 404;
      case ALREADY_EXISTS:
      case ABORTED:
        return 409;
      case PERMISSION_DENIED:
        return 403;
      case RESOURCE_EXHAUSTED:
        return 429;
      case UNIMPLEMENTED:
        return 501;
      case UNAVAILABLE:
        return 503;
      case UNAUTHENTICATED:
        return 401;
      case UNKNOWN:
      case INTERNAL:
      case DATA_LOSS:
      default:
        return 500;
    }
  }

  private void incrementStatusCode(int statusCode) {
    switch (statusCode) {
      case 400:
        storageStatistics.incrementGcsClientBadRequestCount();
        break;
      case 401:
        storageStatistics.incrementGcsClientUnauthorizedResponseCount();
        break;
      case 404:
        storageStatistics.incrementGcsClientNotFoundResponseCount();
        break;
      case 408:
        storageStatistics.incrementGcsClientRequestTimeoutCount();
        break;
      case 410:
        storageStatistics.incrementGcsClientGoneResponseCount();
        break;
      case 412:
        storageStatistics.incrementGcsClientPreconditionFailedResponseCount();
        break;
      case 416:
        storageStatistics.incrementGcsClientRequestedRangeNotSatisfiableCount();
        break;
      case 500:
        storageStatistics.incrementGcsServerInternalErrorCount();
        break;
      case 502:
        storageStatistics.incrementGcsServerBadGatewayCount();
        break;
      case 503:
        storageStatistics.incrementGcsServerServiceUnavailableCount();
        break;
      case 504:
        storageStatistics.incrementGcsServerTimeoutCount();
        break;
    }
  }
}
