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
import static org.junit.Assert.assertThrows;

import com.google.common.base.Ticker;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GcsReadDurationTrackerStreamTest {

  private TestTicker ticker;
  private List<GcsDataTransferEvent> events;

  @Before
  public void setUp() {
    ticker = new TestTicker();
    events = new ArrayList<>();
  }

  @Test
  public void read_doesNotPostBeforeClose() throws IOException {
    GcsReadDurationTrackerStream stream = newStream(/* millisPerRead= */ 5);

    stream.read();
    stream.read(new byte[1], 0, 1);

    assertThat(events).isEmpty();
  }

  @Test
  public void close_postsAccumulatedReadDuration() throws IOException {
    GcsReadDurationTrackerStream stream = newStream(/* millisPerRead= */ 5);

    stream.read();
    stream.read(new byte[1], 0, 1);
    stream.close();

    assertThat(events).hasSize(1);
    assertThat(events.get(0).getDurationMs()).isEqualTo(10);
  }

  @Test
  public void close_doesNotCountTimeOutsideRead() throws IOException {
    GcsReadDurationTrackerStream stream = newStream(/* millisPerRead= */ 5);

    stream.read();
    // Time the caller spends processing the bytes must not be attributed to the GCS API.
    ticker.advanceMillis(1000);
    stream.close();

    assertThat(events).hasSize(1);
    assertThat(events.get(0).getDurationMs()).isEqualTo(5);
  }

  @Test
  public void repeatedClose_postsOnce() throws IOException {
    GcsReadDurationTrackerStream stream = newStream(/* millisPerRead= */ 5);

    stream.read();
    stream.close();
    stream.close();

    assertThat(events).hasSize(1);
  }

  @Test
  public void subMillisecondTransfer_postsNothing() throws IOException {
    GcsReadDurationTrackerStream stream = newStream(/* millisPerRead= */ 0);

    stream.read();
    stream.close();

    assertThat(events).isEmpty();
  }

  @Test
  public void nullDelegate_failsFast() {
    assertThrows(NullPointerException.class, () -> new GcsReadDurationTrackerStream(null, ticker));
  }

  /**
   * Returns a stream whose delegate advances the test ticker on every read, and which captures
   * published events instead of putting them on the static event bus.
   */
  private GcsReadDurationTrackerStream newStream(long millisPerRead) {
    InputStream delegate =
        new InputStream() {
          @Override
          public int read() {
            ticker.advanceMillis(millisPerRead);
            return 1;
          }

          @Override
          public int read(byte[] b, int off, int len) {
            ticker.advanceMillis(millisPerRead);
            return len;
          }
        };

    return new GcsReadDurationTrackerStream(delegate, ticker) {
      @Override
      protected void postToEventQueue(GcsDataTransferEvent event) {
        events.add(event);
      }
    };
  }

  /** Ticker with a manual time value used for testing the stream. */
  private static class TestTicker extends Ticker {

    private long time;

    @Override
    public long read() {
      return time;
    }

    public void advanceMillis(long millis) {
      time += TimeUnit.NANOSECONDS.convert(millis, TimeUnit.MILLISECONDS);
    }
  }
}
