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
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.GCS_CLIENT_RATE_LIMIT_COUNT;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.GCS_CLIENT_SIDE_ERROR_COUNT;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.GCS_REQUEST_COUNT;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics.GCS_SERVER_SIDE_ERROR_COUNT;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.hadoop.util.GcsRequestExecutionEvent;
import com.google.cloud.hadoop.util.GoogleCloudStorageEventBus;
import com.google.common.flogger.GoogleLogger;
import io.grpc.Status;
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
  private GhfsGlobalStorageStatistics subscriber = new GhfsGlobalStorageStatistics();
  protected GoogleCloudStorageEventSubscriber subscriber =
      new GoogleCloudStorageEventSubscriber(storageStatistics);

  @Before
  public void setUp() throws Exception {

    GoogleCloudStorageEventBus.register(subscriber);
  }

  @After
  public void cleanup() throws Exception {

    GoogleCloudStorageEventBus.unregister(subscriber);
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
  public void gcs_requestCounter() throws Exception {
    GoogleCloudStorageEventBus.onGcsRequest(new GcsRequestExecutionEvent());
    GhfsGlobalStorageStatistics verifyCounterStats = new GhfsGlobalStorageStatistics();
    verifyCounterStats.incrementCounter(GCS_REQUEST_COUNT, 1);
    verifyStatistics(verifyCounterStats);
  }

  @Test
  public void gcs_rateLimitCounter() {
    // verify for http event i.e. via Apiary
    GoogleCloudStorageEventBus.postOnHttpResponseStatus(429);
    GhfsGlobalStorageStatistics verifyCounterStats = new GhfsGlobalStorageStatistics();
    verifyCounterStats.incrementCounter(GCS_CLIENT_RATE_LIMIT_COUNT, 1);
    verifyCounterStats.incrementCounter(GCS_CLIENT_SIDE_ERROR_COUNT, 1);
    verifyStatistics(verifyCounterStats);

    storageStatistics.reset();

    // verify for gRPC event i.e. via java-storage
    GoogleCloudStorageEventBus.onGrpcStatus(Status.RESOURCE_EXHAUSTED);
    verifyStatistics(verifyCounterStats);
  }

  @Test
  public void gcs_clientSideErrorCounter() {
    GoogleCloudStorageEventBus.postOnHttpResponseStatus(404);
    GhfsGlobalStorageStatistics verifyCounterStats = new GhfsGlobalStorageStatistics();
    verifyCounterStats.incrementCounter(GCS_CLIENT_SIDE_ERROR_COUNT, 1);
    verifyStatistics(verifyCounterStats);

    storageStatistics.reset();

    // verify for gRPC event i.e. via java-storage
    GoogleCloudStorageEventBus.onGrpcStatus(Status.CANCELLED);
    verifyStatistics(verifyCounterStats);
  }

  @Test
  public void gcs_serverSideErrorCounter() {
    GoogleCloudStorageEventBus.postOnHttpResponseStatus(503);
    GhfsGlobalStorageStatistics verifyCounterStats = new GhfsGlobalStorageStatistics();
    verifyCounterStats.incrementCounter(GCS_SERVER_SIDE_ERROR_COUNT, 1);
    verifyStatistics(verifyCounterStats);

    storageStatistics.reset();

    // verify for gRPC event i.e. via java-storage
    GoogleCloudStorageEventBus.onGrpcStatus(Status.INTERNAL);
    verifyStatistics(verifyCounterStats);
  }

  @Test
  public void gcs_ExceptionCounter() {
    GoogleCloudStorageEventBus.postOnException();
    GhfsGlobalStorageStatistics verifyCounterStats = new GhfsGlobalStorageStatistics();
    verifyCounterStats.incrementCounter(EXCEPTION_COUNT, 1);
    verifyStatistics(verifyCounterStats);
  }
}
