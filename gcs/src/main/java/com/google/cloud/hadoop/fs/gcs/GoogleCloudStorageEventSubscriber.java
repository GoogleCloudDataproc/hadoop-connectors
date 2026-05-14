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

import static com.google.cloud.hadoop.gcsio.StatisticTypeEnum.TYPE_DURATION;
import static com.google.cloud.hadoop.gcsio.StatisticTypeEnum.TYPE_DURATION_TOTAL;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics;
import com.google.cloud.hadoop.gcsio.StatisticTypeEnum;
import com.google.cloud.hadoop.util.GCSChecksumFailureEvent;
import com.google.cloud.hadoop.util.GcsJsonApiEvent;
import com.google.cloud.hadoop.util.GcsJsonApiEvent.EventType;
import com.google.cloud.hadoop.util.GcsJsonApiEvent.RequestType;
import com.google.cloud.hadoop.util.GcsRequestExecutionEvent;
import com.google.cloud.hadoop.util.IGcsJsonApiEvent;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.eventbus.Subscribe;
import com.google.common.flogger.GoogleLogger;
import io.grpc.Status;
import java.io.IOException;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;

/* Stores the subscriber methods corresponding to GoogleCloudStorageEventBus */
public class GoogleCloudStorageEventSubscriber {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private static final Map<RequestType, GhfsStatistic> requestToGhfsStatMap =
      getHadoopFileSystemMap();
  private static final Map<RequestType, GoogleCloudStorageStatistics> requestToGcsStatMap =
      getGcsStatMap();

  private static GhfsGlobalStorageStatistics storageStatistics;
  private static GoogleCloudStorageEventSubscriber INSTANCE = null;

  private GoogleCloudStorageEventSubscriber(GhfsGlobalStorageStatistics storageStatistics) {
    this.storageStatistics = storageStatistics;
  }

  /*
   * Singleton class such that registration of subscriber methods is only once.
   * */
  public static synchronized GoogleCloudStorageEventSubscriber getInstance(
      @Nonnull GhfsGlobalStorageStatistics storageStatistics) {
    if (INSTANCE == null) {
      logger.atFiner().log("Subscriber class invoked for first time");
      INSTANCE = new GoogleCloudStorageEventSubscriber(storageStatistics);
    }
    return INSTANCE;
  }

  @VisibleForTesting
  protected static void reset() {
    INSTANCE = null;
  }

  @Subscribe
  private void subscriberOnGcsRequest(@Nonnull GcsRequestExecutionEvent event) {
    storageStatistics.incrementGcsTotalRequestCount();
  }

  @Subscribe
  private void subscriberOnGrpcStatus(@Nonnull Status status) {
    updateGcsIOSpecificStatistics(grpcToHttpStatusCodeMapping(status));
  }

  @Subscribe
  private void subscriberOnGcsRequestExecutionEvent(IGcsJsonApiEvent event) {
    EventType eventType = event.getEventType();
    Object eventContext = event.getContext();
    if (eventType == EventType.STARTED) {
      storageStatistics.incrementGcsTotalRequestCount();
    } else if (eventType == EventType.RESPONSE) {
      long duration = (long) event.getProperty(GcsJsonApiEvent.DURATION);
      int statusCode = (int) event.getProperty(GcsJsonApiEvent.STATUS_CODE);

      incrementStatusCode(statusCode);
      updateGcsIOSpecificStatistics(statusCode);

      storageStatistics.incrementCounter(GoogleCloudStorageStatistics.GCS_API_TIME, duration);

      RequestType requestType = (RequestType) event.getProperty(GcsJsonApiEvent.REQUEST_TYPE);
      if (requestToGcsStatMap.containsKey(requestType)) {
        updateMetric(requestToGcsStatMap.get(requestType), duration, eventContext);
      } else if (requestToGhfsStatMap.containsKey(requestType)) {
        updateMetric(requestToGhfsStatMap.get(requestType), duration, eventContext);
      } else {
        // Not expected. If this happens some of the requests may not be tracked.
        logger.atSevere().atMostEvery(1, TimeUnit.MINUTES).log(
            "Unexpected error type %s. context=%s", requestType, eventContext);
      }
    } else if (eventType == EventType.BACKOFF) {
      long backOffTime = (long) event.getProperty(GcsJsonApiEvent.BACKOFF_TIME);
      storageStatistics.increment(GoogleCloudStorageStatistics.GCS_BACKOFF_COUNT);
      storageStatistics.incrementCounter(
          GoogleCloudStorageStatistics.GCS_BACKOFF_TIME, backOffTime);
    } else if (eventType == EventType.EXCEPTION) {
      storageStatistics.incrementGcsExceptionCount();
    }
  }

  private void updateMetric(GhfsStatistic stat, long duration, Object eventContext) {
    storageStatistics.incrementCounter(stat, 1);

    if (stat.getType() == TYPE_DURATION || stat.getType() == TYPE_DURATION_TOTAL) {
      storageStatistics.updateStats(stat, duration, eventContext);
    }
  }

  private void updateMetric(GoogleCloudStorageStatistics stat, long duration, Object eventContext) {
    storageStatistics.incrementCounter(stat, 1);

    if (stat.getType() == StatisticTypeEnum.TYPE_DURATION
        || stat.getType() == TYPE_DURATION_TOTAL) {
      storageStatistics.updateStats(stat, duration, eventContext);
    }
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

  @Subscribe
  private void onGcsChecksumFailure(GCSChecksumFailureEvent exception) {
    storageStatistics.incrementWriteChecksumFailureCount();
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

  private static Map<RequestType, GhfsStatistic> getHadoopFileSystemMap() {
    EnumMap<RequestType, GhfsStatistic> result = new EnumMap<>(RequestType.class);
    result.put(RequestType.DELETE, GhfsStatistic.ACTION_HTTP_DELETE_REQUEST);
    result.put(RequestType.PATCH, GhfsStatistic.ACTION_HTTP_PATCH_REQUEST);
    result.put(RequestType.POST, GhfsStatistic.ACTION_HTTP_POST_REQUEST);
    result.put(RequestType.PUT, GhfsStatistic.ACTION_HTTP_PUT_REQUEST);

    return result;
  }

  private static Map<RequestType, GoogleCloudStorageStatistics> getGcsStatMap() {
    EnumMap<RequestType, GoogleCloudStorageStatistics> result = new EnumMap<>(RequestType.class);
    result.put(RequestType.GET_MEDIA, GoogleCloudStorageStatistics.GCS_GET_MEDIA_REQUEST);
    result.put(RequestType.GET_METADATA, GoogleCloudStorageStatistics.GCS_METADATA_REQUEST);
    result.put(RequestType.GET_OTHER, GoogleCloudStorageStatistics.GCS_GET_OTHER_REQUEST);
    result.put(RequestType.LIST_DIR, GoogleCloudStorageStatistics.GCS_LIST_DIR_REQUEST);
    result.put(RequestType.LIST_FILE, GoogleCloudStorageStatistics.GCS_LIST_FILE_REQUEST);

    return result;
  }
}
