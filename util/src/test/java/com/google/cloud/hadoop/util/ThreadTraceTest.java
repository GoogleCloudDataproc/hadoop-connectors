/*
 * Copyright 2023 Google Inc.
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

import com.google.cloud.hadoop.util.ThreadTrace.ThreadTraceEvent;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ThreadTraceTest {
  private ThreadTrace threadTrace;
  private List<ThreadTraceEvent> events;
  private String key1;
  private String key2;
  private String val1;
  private Map<Long, List<ThreadTraceEvent>> subEvents;

  @Before
  public void before() throws Exception {
    this.threadTrace = new ThreadTrace("some");
    this.key1 = getRandomVal();
    this.key2 = getRandomVal();
    this.val1 = getRandomVal();

    this.events = (List<ThreadTraceEvent>) getPrivateField("events").get(threadTrace);
    this.subEvents =
        (Map<Long, List<ThreadTraceEvent>>) getPrivateField("subEvents").get(threadTrace);
  }

  private Field getPrivateField(String fieldName) throws NoSuchFieldException {
    Field eventsField = threadTrace.getClass().getDeclaredField(fieldName);
    eventsField.setAccessible(true);
    return eventsField;
  }

  @Test
  public void null_trackingId_throws() {
    assertThrows(NullPointerException.class, () -> new ThreadTrace(null));
  }

  @Test
  public void annotate_emtpy_event_ignored() {
    threadTrace.annotate(key1, val1);
    assertThat(events).hasSize(0);
  }

  @Test
  public void annotate_adds_to_last() {
    ThreadTraceEvent threadTraceEvent = threadTrace.starOperationEvent(getRandomVal());

    assertThat(events).hasSize(1);
    assertThat(events.get(0)).isEqualTo(threadTraceEvent);
    assertThat(threadTraceEvent.get(key1)).isNull();

    threadTrace.annotate(key1, val1);

    assertThat(threadTraceEvent.get(key1)).isEqualTo(val1);
  }

  @Test
  public void max_annotate_limit_test() {
    ThreadTraceEvent threadTraceEvent = threadTrace.starOperationEvent(getRandomVal());

    for (int i = 0; i < ThreadTraceEvent.MAX_PROPERTY_SIZE - 3; i++) {
      String theKey = Integer.toString(i);
      String theVal = getRandomVal();
      threadTrace.annotate(theKey, theVal);
      assertThat(threadTraceEvent.get(theKey)).isEqualTo(theVal);
    }

    threadTrace.annotate(key1, val1);
    assertThat(threadTraceEvent.get(key1)).isNull();
  }

  @Test
  public void no_events_added_when_full() {
    for (int i = 0; i < ThreadTrace.MAX_EVENTS_SIZE; i++) {
      assertThat(threadTrace.starOperationEvent(getRandomVal())).isNotNull();
      assertThat(events).hasSize(i + 1);
    }

    assertThat(threadTrace.starOperationEvent(getRandomVal())).isNull();
    assertThat(events).hasSize(ThreadTrace.MAX_EVENTS_SIZE);
  }

  @Test
  public void end_operation_event_when_full() {
    for (int i = 0; i < ThreadTrace.MAX_EVENTS_SIZE - 1; i++) {
      assertThat(threadTrace.starOperationEvent(getRandomVal())).isNotNull();
      assertThat(events).hasSize(i + 1);
    }

    ThreadTraceEvent ss = threadTrace.starOperationEvent(getRandomVal());
    assertThat(events).hasSize(ThreadTrace.MAX_EVENTS_SIZE);
    threadTrace.endOperationEvent(ss);
    assertThat(events).hasSize(ThreadTrace.MAX_EVENTS_SIZE);
  }

  @Test
  public void sub_trace_udpates_parent() throws ExecutionException, InterruptedException {
    ExecutorService service = Executors.newFixedThreadPool(1);
    try {
      assertThat(subEvents).hasSize(0);
      for (int i = 1; i < ThreadTrace.MAX_EVENTS_SIZE; i++) {
        long threadId = submitSubTask(service);
        assertThat(subEvents).hasSize(1);

        List<ThreadTraceEvent> singleThreadEvents = subEvents.get(threadId);
        assertThat(singleThreadEvents).hasSize(i);
      }

      long threadId = submitSubTask(service);
      List<ThreadTraceEvent> singleThreadEvents = subEvents.get(threadId);
      assertThat(singleThreadEvents).hasSize(ThreadTrace.MAX_EVENTS_SIZE);
    } finally {
      service.shutdown();
    }
  }

  private Long submitSubTask(ExecutorService service)
      throws InterruptedException, ExecutionException {
    return service
        .submit(
            () -> {
              ThreadTrace subTrace = threadTrace.getSubTrace();
              ThreadTraceEvent start = subTrace.starOperationEvent("some");
              subTrace.endOperationEvent(start);
              return Thread.currentThread().getId();
            })
        .get();
  }

  static String getRandomVal() {
    return String.format("rand-%d", ThreadLocalRandom.current().nextInt());
  }
}
