/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.util;

import static com.google.common.truth.Truth.assertThat;

import com.google.api.client.http.HttpHeaders;
import com.google.common.eventbus.Subscribe;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GcsReadDurationTrackerStreamTest {

  private final List<GcsReadMetricEvent> events = new ArrayList<>();
  private final URI path = URI.create("gs://bucket/obj");

  @Before
  public void setUp() {
    GoogleCloudStorageEventBus.reset();
    GoogleCloudStorageEventBus.register(
        new Object() {
          @Subscribe
          public void onGcsReadMetricEvent(GcsReadMetricEvent event) {
            if (event.getType() == GcsReadMetricEvent.Type.DATA_TRANSFER) {
              events.add(event);
            }
          }
        });
  }

  @Test
  public void testDurationTracking() throws IOException {
    byte[] data = new byte[100];
    ByteArrayInputStream bais =
        new ByteArrayInputStream(data) {
          @Override
          public synchronized int read(byte[] b, int off, int len) {
            try {
              Thread.sleep(100);
            } catch (InterruptedException e) {
              // ignore
            }
            return super.read(b, off, len);
          }
        };

    GcsReadDurationTrackerStream stream = new GcsReadDurationTrackerStream(bais, path, null, 1000L);
    stream.read(new byte[10]);
    stream.close();

    assertThat(events).hasSize(1);
    GcsReadMetricEvent event = events.get(0);
    assertThat(event.getDurationMs()).isAtLeast(100L);
    assertThat(event.getStreamPath()).isEqualTo(path);
  }

  @Test
  public void testLatencyThresholdBreached() throws IOException {
    ByteArrayInputStream bais =
        new ByteArrayInputStream(new byte[10]) {
          @Override
          public synchronized int read(byte[] b, int off, int len) {
            try {
              Thread.sleep(1100);
            } catch (InterruptedException e) {
              // ignore
            }
            return super.read(b, off, len);
          }
        };
    HttpHeaders headers = new HttpHeaders();
    headers.set("x-guploader-uploadid", "test-upload-id");

    GcsReadDurationTrackerStream stream =
        new GcsReadDurationTrackerStream(bais, path, headers, 1000L);
    stream.read(new byte[1]);
    stream.close();

    assertThat(events).hasSize(1);
    assertThat(events.get(0).isLatencyThresholdBreached()).isTrue();
  }
}
