/*
 * Copyright 2024 Google LLC. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.fs.gcs;

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.EXCEPTION_COUNT;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.GCS_API_CLIENT_BAD_REQUEST_COUNT;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.GCS_API_CLIENT_GONE_RESPONSE_COUNT;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.GCS_API_CLIENT_NOT_FOUND_RESPONSE_COUNT;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.GCS_API_CLIENT_PRECONDITION_FAILED_RESPONSE_COUNT;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.GCS_API_CLIENT_RATE_LIMIT_COUNT;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.GCS_API_CLIENT_REQUESTED_RANGE_NOT_SATISFIABLE_COUNT;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.GCS_API_CLIENT_REQUEST_TIMEOUT_COUNT;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.GCS_API_CLIENT_SIDE_ERROR_COUNT;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.GCS_API_CLIENT_UNAUTHORIZED_RESPONSE_COUNT;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.GCS_API_REQUEST_COUNT;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.GCS_API_SERVER_BAD_GATEWAY_COUNT;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.GCS_API_SERVER_INTERNAL_ERROR_COUNT;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.GCS_API_SERVER_SERVICE_UNAVAILABLE_COUNT;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.GCS_API_SERVER_SIDE_ERROR_COUNT;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.GCS_API_SERVER_TIMEOUT_COUNT;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.GCS_METADATA_REQUEST;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.WRITE_CHECKSUM_FAILURE_COUNT;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.hadoop.util.GcsJsonApiEvent;
import com.google.cloud.hadoop.util.GcsRequestExecutionEvent;
import com.google.cloud.hadoop.util.GoogleCloudStorageEventBus;
import com.google.cloud.hadoop.util.IGcsJsonApiEvent;
import com.google.common.flogger.GoogleLogger;
import io.grpc.Status;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.fs.StorageStatistics.LongStatistic;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GoogleCloudStorageStatisticsTest {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();
  private GhfsGlobalStorageStatistics storageStatistics = new GhfsGlobalStorageStatistics();
  protected GoogleCloudStorageEventSubscriber subscriber =
      GoogleCloudStorageEventSubscriber.getInstance(storageStatistics);

  @Before
  public void setUp() throws Exception {
    GoogleCloudStorageEventBus.register(subscriber);
  }

  @After
  public void cleanup() throws Exception {
    GoogleCloudStorageEventBus.unregister(subscriber);
    GoogleCloudStorageEventSubscriber.reset();
  }

  private void verifyStatistics(GhfsGlobalStorageStatistics expectedStats) {
    Iterator<LongStatistic> statsIterator = expectedStats.getLongStatistics();
    boolean metricsVerified = true;
    while (statsIterator.hasNext()) {
      LongStatistic stats = statsIterator.next();
      Long value = storageStatistics.getLong(stats.getName());

      if (stats.getValue() != value) {
        logger.atWarning().log(
            "Metric values not matching. for: %s, expected: %d, got: %d",
            stats.getName(), stats.getValue(), value);
        metricsVerified = false;
        break;
      }
    }

    assertThat(metricsVerified).isTrue();
  }

  @Test
  public void test_multiple_register_of_statistics() throws Exception {
    GoogleCloudStorageEventBus.register(subscriber);
    GoogleCloudStorageEventBus.register(subscriber);
    GoogleCloudStorageEventBus.register(
        GoogleCloudStorageEventSubscriber.getInstance(storageStatistics));
    GoogleCloudStorageEventBus.register(
        GoogleCloudStorageEventSubscriber.getInstance(storageStatistics));

    GoogleCloudStorageEventBus.onGcsRequest(new GcsRequestExecutionEvent());
    GhfsGlobalStorageStatistics verifyCounterStats = new GhfsGlobalStorageStatistics();
    verifyCounterStats.incrementCounter(GCS_API_REQUEST_COUNT, 1);
    verifyStatistics(verifyCounterStats);
  }

  @Test
  public void gcs_requestCounter() throws Exception {
    GoogleCloudStorageEventBus.onGcsRequest(new GcsRequestExecutionEvent());
    GhfsGlobalStorageStatistics verifyCounterStats = new GhfsGlobalStorageStatistics();
    verifyCounterStats.incrementCounter(GCS_API_REQUEST_COUNT, 1);
    verifyStatistics(verifyCounterStats);
  }

  @Test
  public void gcs_rateLimitCounter() {
    // verify for http event i.e. via Apiary
    GoogleCloudStorageEventBus.postGcsJsonApiEvent(new TestGcsApiEvent(429));
    GhfsGlobalStorageStatistics verifyCounterStats = new GhfsGlobalStorageStatistics();
    verifyCounterStats.incrementCounter(GCS_METADATA_REQUEST, 1);
    verifyCounterStats.incrementCounter(GCS_API_CLIENT_RATE_LIMIT_COUNT, 1);
    verifyCounterStats.incrementCounter(GCS_API_CLIENT_SIDE_ERROR_COUNT, 1);
    verifyStatistics(verifyCounterStats);

    storageStatistics.reset();

    // verify for gRPC event i.e. via java-storage
    verifyCounterStats = new GhfsGlobalStorageStatistics();
    verifyCounterStats.incrementCounter(GCS_API_CLIENT_RATE_LIMIT_COUNT, 1);
    verifyCounterStats.incrementCounter(GCS_API_CLIENT_SIDE_ERROR_COUNT, 1);
    GoogleCloudStorageEventBus.onGrpcStatus(Status.RESOURCE_EXHAUSTED);
    verifyStatistics(verifyCounterStats);
  }

  @Test
  public void gcs_clientSideErrorCounter() {
    GoogleCloudStorageEventBus.postGcsJsonApiEvent(new TestGcsApiEvent(404));
    GhfsGlobalStorageStatistics verifyCounterStats = new GhfsGlobalStorageStatistics();
    verifyCounterStats.incrementCounter(GCS_METADATA_REQUEST, 1);
    verifyCounterStats.incrementCounter(GCS_API_CLIENT_SIDE_ERROR_COUNT, 1);
    verifyCounterStats.incrementCounter(GCS_API_CLIENT_NOT_FOUND_RESPONSE_COUNT, 1);
    verifyStatistics(verifyCounterStats);
  }

  @Test
  public void gcs_grpcCancelledStatusCounter() {
    GoogleCloudStorageEventBus.onGrpcStatus(Status.CANCELLED);
    GhfsGlobalStorageStatistics verifyCounterStats = new GhfsGlobalStorageStatistics();
    verifyCounterStats.incrementCounter(GCS_API_CLIENT_SIDE_ERROR_COUNT, 1);
    // verify for gRPC event i.e. via java-storage
    verifyStatistics(verifyCounterStats);
  }

  @Test
  public void gcs_serverSideErrorCounter() {
    GoogleCloudStorageEventBus.postGcsJsonApiEvent(new TestGcsApiEvent(503));
    GhfsGlobalStorageStatistics verifyCounterStats = new GhfsGlobalStorageStatistics();
    verifyCounterStats.incrementCounter(GCS_METADATA_REQUEST, 1);
    verifyCounterStats.incrementCounter(GCS_API_SERVER_SIDE_ERROR_COUNT, 1);
    verifyCounterStats.incrementCounter(GCS_API_SERVER_SERVICE_UNAVAILABLE_COUNT, 1);
    verifyStatistics(verifyCounterStats);
  }

  @Test
  public void gcs_grpcInternalStatusCounter() {
    // verify for gRPC event i.e. via java-storage
    GoogleCloudStorageEventBus.onGrpcStatus(Status.INTERNAL);
    GhfsGlobalStorageStatistics verifyCounterStats = new GhfsGlobalStorageStatistics();
    verifyCounterStats.incrementCounter(GCS_API_SERVER_SIDE_ERROR_COUNT, 1);
    verifyStatistics(verifyCounterStats);
  }

  @Test
  public void gcs_exceptionCounter() {
    GoogleCloudStorageEventBus.postOnException();
    GhfsGlobalStorageStatistics verifyCounterStats = new GhfsGlobalStorageStatistics();
    verifyCounterStats.incrementCounter(EXCEPTION_COUNT, 1);
    verifyStatistics(verifyCounterStats);
  }

  @Test
  public void gcs_clientBadRequestCount() throws IOException {
    GoogleCloudStorageEventBus.postGcsJsonApiEvent(new TestGcsApiEvent(400));
    GhfsGlobalStorageStatistics verifyCounterStats = new GhfsGlobalStorageStatistics();
    verifyCounterStats.incrementCounter(GCS_METADATA_REQUEST, 1);
    verifyCounterStats.incrementCounter(GCS_API_CLIENT_BAD_REQUEST_COUNT, 1);
    verifyCounterStats.incrementCounter(GCS_API_CLIENT_SIDE_ERROR_COUNT, 1);
    verifyStatistics(verifyCounterStats);
  }

  @Test
  public void gcs_writeChecksumFailureCount() throws IOException {
    GoogleCloudStorageEventBus.postWriteChecksumFailure();
    GhfsGlobalStorageStatistics verifyCounterStats = new GhfsGlobalStorageStatistics();
    verifyCounterStats.incrementCounter(WRITE_CHECKSUM_FAILURE_COUNT, 1);
    verifyStatistics(verifyCounterStats);
  }

  private class TestGcsApiEvent implements IGcsJsonApiEvent {
    private final int statusCode;

    TestGcsApiEvent(int statusCode) {
      this.statusCode = statusCode;
    }

    @Override
    public GcsJsonApiEvent.EventType getEventType() {
      return GcsJsonApiEvent.EventType.RESPONSE;
    }

    @Override
    public Object getContext() {
      return "";
    }

    @Override
    public String getMethod() {
      return "GET";
    }

    @Override
    public Object getProperty(String key) {
      if (key.equals(GcsJsonApiEvent.DURATION)) {
        return 0L;
      }

      if (key.equals(GcsJsonApiEvent.STATUS_CODE)) {
        return this.statusCode;
      }

      if (key.equals(GcsJsonApiEvent.REQUEST_TYPE)) {
        return GcsJsonApiEvent.RequestType.GET_METADATA;
      }

      throw new IllegalStateException("Unknown property: " + key);
    }
  }

  @Test
  public void gcs_clientUnauthorizedResponseCount() {
    GoogleCloudStorageEventBus.postGcsJsonApiEvent(new TestGcsApiEvent(401));
    GhfsGlobalStorageStatistics verifyCounterStats = new GhfsGlobalStorageStatistics();
    verifyCounterStats.incrementCounter(GCS_METADATA_REQUEST, 1);
    verifyCounterStats.incrementCounter(GCS_API_CLIENT_UNAUTHORIZED_RESPONSE_COUNT, 1);
    verifyCounterStats.incrementCounter(GCS_API_CLIENT_SIDE_ERROR_COUNT, 1);
    verifyStatistics(verifyCounterStats);
  }

  @Test
  public void gcs_clientNotFoundResponseCount() {
    GoogleCloudStorageEventBus.postGcsJsonApiEvent(new TestGcsApiEvent(404));
    GhfsGlobalStorageStatistics verifyCounterStats = new GhfsGlobalStorageStatistics();
    verifyCounterStats.incrementCounter(GCS_METADATA_REQUEST, 1);
    verifyCounterStats.incrementCounter(GCS_API_CLIENT_NOT_FOUND_RESPONSE_COUNT, 1);
    verifyCounterStats.incrementCounter(GCS_API_CLIENT_SIDE_ERROR_COUNT, 1);
    verifyStatistics(verifyCounterStats);
  }

  @Test
  public void gcs_clientRequestTimeoutCount() {
    GoogleCloudStorageEventBus.postGcsJsonApiEvent(new TestGcsApiEvent(408));
    GhfsGlobalStorageStatistics verifyCounterStats = new GhfsGlobalStorageStatistics();
    verifyCounterStats.incrementCounter(GCS_METADATA_REQUEST, 1);
    verifyCounterStats.incrementCounter(GCS_API_CLIENT_REQUEST_TIMEOUT_COUNT, 1);
    verifyCounterStats.incrementCounter(GCS_API_CLIENT_SIDE_ERROR_COUNT, 1);
    verifyStatistics(verifyCounterStats);
  }

  @Test
  public void gcs_clientGoneResponseCount() {
    GoogleCloudStorageEventBus.postGcsJsonApiEvent(new TestGcsApiEvent(410));
    GhfsGlobalStorageStatistics verifyCounterStats = new GhfsGlobalStorageStatistics();
    verifyCounterStats.incrementCounter(GCS_METADATA_REQUEST, 1);
    verifyCounterStats.incrementCounter(GCS_API_CLIENT_GONE_RESPONSE_COUNT, 1);
    verifyCounterStats.incrementCounter(GCS_API_CLIENT_SIDE_ERROR_COUNT, 1);
    verifyStatistics(verifyCounterStats);
  }

  @Test
  public void gcs_clientPreconditionFailedResponseCount() {
    GoogleCloudStorageEventBus.postGcsJsonApiEvent(new TestGcsApiEvent(412));
    GhfsGlobalStorageStatistics verifyCounterStats = new GhfsGlobalStorageStatistics();
    verifyCounterStats.incrementCounter(GCS_METADATA_REQUEST, 1);
    verifyCounterStats.incrementCounter(GCS_API_CLIENT_PRECONDITION_FAILED_RESPONSE_COUNT, 1);
    verifyCounterStats.incrementCounter(GCS_API_CLIENT_SIDE_ERROR_COUNT, 1);
    verifyStatistics(verifyCounterStats);
  }

  @Test
  public void gcs_clientRequestedRangeNotSatisfiableCount() {
    GoogleCloudStorageEventBus.postGcsJsonApiEvent(new TestGcsApiEvent(416));
    GhfsGlobalStorageStatistics verifyCounterStats = new GhfsGlobalStorageStatistics();
    verifyCounterStats.incrementCounter(GCS_METADATA_REQUEST, 1);
    verifyCounterStats.incrementCounter(GCS_API_CLIENT_REQUESTED_RANGE_NOT_SATISFIABLE_COUNT, 1);
    verifyCounterStats.incrementCounter(GCS_API_CLIENT_SIDE_ERROR_COUNT, 1);
    verifyStatistics(verifyCounterStats);
  }

  @Test
  public void gcs_serverInternalErrorCount() {
    GoogleCloudStorageEventBus.postGcsJsonApiEvent(new TestGcsApiEvent(500));
    GhfsGlobalStorageStatistics verifyCounterStats = new GhfsGlobalStorageStatistics();
    verifyCounterStats.incrementCounter(GCS_METADATA_REQUEST, 1);
    verifyCounterStats.incrementCounter(GCS_API_SERVER_INTERNAL_ERROR_COUNT, 1);
    verifyCounterStats.incrementCounter(GCS_API_SERVER_SIDE_ERROR_COUNT, 1);
    verifyStatistics(verifyCounterStats);
  }

  @Test
  public void gcs_serverBadGatewayCount() {
    GoogleCloudStorageEventBus.postGcsJsonApiEvent(new TestGcsApiEvent(502));
    GhfsGlobalStorageStatistics verifyCounterStats = new GhfsGlobalStorageStatistics();
    verifyCounterStats.incrementCounter(GCS_METADATA_REQUEST, 1);
    verifyCounterStats.incrementCounter(GCS_API_SERVER_BAD_GATEWAY_COUNT, 1);
    verifyCounterStats.incrementCounter(GCS_API_SERVER_SIDE_ERROR_COUNT, 1);
    verifyStatistics(verifyCounterStats);
  }

  @Test
  public void gcs_serverServiceUnavailableCount() {
    GoogleCloudStorageEventBus.postGcsJsonApiEvent(new TestGcsApiEvent(503));
    GhfsGlobalStorageStatistics verifyCounterStats = new GhfsGlobalStorageStatistics();
    verifyCounterStats.incrementCounter(GCS_METADATA_REQUEST, 1);
    verifyCounterStats.incrementCounter(GCS_API_SERVER_SERVICE_UNAVAILABLE_COUNT, 1);
    verifyCounterStats.incrementCounter(GCS_API_SERVER_SIDE_ERROR_COUNT, 1);
    verifyStatistics(verifyCounterStats);
  }

  @Test
  public void gcs_serverTimeoutCount() {
    GoogleCloudStorageEventBus.postGcsJsonApiEvent(new TestGcsApiEvent(504));
    GhfsGlobalStorageStatistics verifyCounterStats = new GhfsGlobalStorageStatistics();
    verifyCounterStats.incrementCounter(GCS_METADATA_REQUEST, 1);
    verifyCounterStats.incrementCounter(GCS_API_SERVER_TIMEOUT_COUNT, 1);
    verifyCounterStats.incrementCounter(GCS_API_SERVER_SIDE_ERROR_COUNT, 1);
    verifyStatistics(verifyCounterStats);
  }
}
