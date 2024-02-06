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

import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TraceFactoryTest {
  private TestLogHandler assertingHandler;

  private static final Logger LOGGER = Logger.getLogger(TraceOperation.class.getName());
  private String name;
  private String context;
  private String name2;

  @Before
  public void setUp() throws IOException {
    assertingHandler = new TestLogHandler();
    LOGGER.setUseParentHandlers(false);
    LOGGER.addHandler(assertingHandler);
    LOGGER.setLevel(Level.INFO);
    name = getRandomString();
    name2 = getRandomString();
    context = getRandomString();
  }

  @After
  public void verifyAndRemoveAssertingHandler() {
    LOGGER.removeHandler(assertingHandler);
  }

  @Test
  public void disabled_uses_singleton() {
    ITraceFactory first = TraceFactory.get(false);
    ITraceFactory second = TraceFactory.get(false);

    assertThat(first).isEqualTo(second);
  }

  @Test
  public void enabled_creates_new() {
    ITraceFactory first = TraceFactory.get(true);
    ITraceFactory second = TraceFactory.get(true);

    assertThat(first).isNotEqualTo(second);
    assertThat(this.assertingHandler.logs).hasSize(0);
  }

  @Test
  public void enabled_creates_trace() {
    ITraceFactory traceFactory = TraceFactory.get(true);

    assertThreadTraceIsNull();
    try (ITraceOperation traceOperation = traceFactory.createRootWithLogging(name, context)) {
      ThreadTrace tt = getThreadTrace();
      assertThat(tt).isNotNull();

      assertThat(tt.getTrackingId()).startsWith(String.format("%s(%s)", name, context));

      assertThat(this.assertingHandler.logs).hasSize(0);
    }

    assertThreadTraceIsNull();
    assertThat(this.assertingHandler.logs).hasSize(1);
    assertThat(this.getEventAtLogIndex(0).size()).isEqualTo(1);

    assertEventAtIndex(0, 0, ImmutableMap.of("name", name, "type", "MERGED"));
  }

  @Test
  public void multiple_nested_trace_operations() {
    ITraceFactory traceFactory = TraceFactory.get(true);

    assertThreadTraceIsNull();
    try (ITraceOperation traceOperation = traceFactory.createRootWithLogging(name, context)) {
      try (ITraceOperation operation = TraceOperation.addToExistingTrace(name2)) {
        ThreadTrace tt = getThreadTrace();
        assertThat(tt).isNotNull();

        assertThat(tt.getTrackingId()).startsWith(String.format("%s(%s)", name, context));

        assertThat(this.assertingHandler.logs).hasSize(0);
      }

      assertThat(this.assertingHandler.logs).hasSize(0);
    }

    assertThreadTraceIsNull();
    assertThat(this.assertingHandler.logs).hasSize(1);
    assertThat(this.getEventAtLogIndex(0).size()).isEqualTo(3);

    assertEventAtIndex(0, 0, ImmutableMap.of("name", name, "type", "START"));
    assertEventAtIndex(0, 1, ImmutableMap.of("name", name2, "type", "MERGED"));
    assertEventAtIndex(0, 2, ImmutableMap.of("name", name, "type", "END"));
  }

  @Test
  public void multithreaded_trace_operations() throws ExecutionException, InterruptedException {
    ITraceFactory traceFactory = TraceFactory.get(true);
    int threadPoolSize = 2;
    ExecutorService threadPool = Executors.newFixedThreadPool(threadPoolSize);
    try {
      assertThreadTraceIsNull();
      List<String> expectedSubEventNames = runMultithreaded(traceFactory, threadPool, 10);

      assertThreadTraceIsNull();
      assertThat(this.assertingHandler.logs).hasSize(1);
      assertThat(this.getEventAtLogIndex(0).size()).isEqualTo(1);
      validateSubEvents(0, threadPoolSize, expectedSubEventNames);
    } finally {
      threadPool.shutdown();
    }
  }

  @Test
  public void multiple_sequential_trace_operations() {
    ITraceFactory traceFactory = TraceFactory.get(true);
    assertThreadTraceIsNull();
    try (ITraceOperation traceOperation = traceFactory.createRootWithLogging(name, context)) {
      ThreadTrace tt = getThreadTrace();

      assertThat(tt).isNotNull();
      assertThat(tt.getTrackingId()).startsWith(String.format("%s(%s)", name, context));
      assertThat(this.assertingHandler.logs).hasSize(0);
    }

    assertThreadTraceIsNull();

    try (ITraceOperation traceOperation = traceFactory.createRootWithLogging(name2, context)) {
      ThreadTrace tt = getThreadTrace();

      assertThat(tt).isNotNull();
      assertThat(tt.getTrackingId()).startsWith(String.format("%s(%s)", name2, context));
      assertThat(this.assertingHandler.logs).hasSize(1);
    }

    assertThat(this.assertingHandler.logs).hasSize(2);
    assertThat(this.getEventAtLogIndex(1).size()).isEqualTo(1);
    assertEventAtIndex(1, 0, ImmutableMap.of("name", name2, "type", "MERGED"));
  }

  @Test
  public void max_events_test() {
    ITraceFactory traceFactory = TraceFactory.get(true);
    assertThreadTraceIsNull();
    try (ITraceOperation traceOperation = traceFactory.createRootWithLogging(name, context)) {
      ThreadTrace tt = getThreadTrace();

      assertThat(tt).isNotNull();
      assertThat(tt.getTrackingId()).startsWith(String.format("%s(%s)", name, context));
      assertThat(this.assertingHandler.logs).hasSize(0);

      for (int i = 0; i < ThreadTrace.MAX_EVENTS_SIZE * 2; i++) {
        try (ITraceOperation to = TraceOperation.addToExistingTrace(Integer.toString(i))) {}
      }
    }

    assertThat(this.assertingHandler.logs).hasSize(1);
    assertThat(this.getEventAtLogIndex(0).size()).isEqualTo(ThreadTrace.MAX_EVENTS_SIZE);
  }

  @Test
  @Ignore
  public void max_subevents_test() throws ExecutionException, InterruptedException {
    ITraceFactory traceFactory = TraceFactory.get(true);
    int threadPoolSize = 2;
    ExecutorService threadPool = Executors.newFixedThreadPool(threadPoolSize);
    try {
      assertThreadTraceIsNull();
      runMultithreaded(traceFactory, threadPool, 100);

      assertThreadTraceIsNull();
      assertThat(this.assertingHandler.logs).hasSize(1);
      assertThat(this.getEventAtLogIndex(0).size()).isEqualTo(1);
      validateSubEventsSize(0, threadPoolSize, threadPoolSize * ThreadTrace.MAX_EVENTS_SIZE);
    } finally {
      threadPool.shutdown();
    }
  }

  @Test
  public void disabled_doesnot_have_trace() {
    ITraceFactory traceFactory = TraceFactory.get(false);

    assertThreadTraceIsNull();
    try (ITraceOperation to =
        traceFactory.createRootWithLogging(getRandomString(), getRandomString())) {
      assertThreadTraceIsNull();
    }

    assertThreadTraceIsNull();
  }

  private List<String> runMultithreaded(
      ITraceFactory traceFactory, ExecutorService threadPool, int numOperations) {
    List<String> subEventNames = new ArrayList<>();
    List<Future<Object>> futures = new ArrayList<>();
    try (ITraceOperation to = traceFactory.createRootWithLogging(name, context)) {
      for (int i = 0; i < numOperations; i++) {
        final String eventName = String.format("op_%d", i);
        subEventNames.add(eventName);
        Future<Object> future =
            threadPool.submit(
                () -> {
                  try (ITraceOperation child =
                      TraceOperation.getChildTrace(to.getTrace(), eventName)) {
                    return null;
                  }
                });

        futures.add(future);
      }

      futures.forEach(
          future -> {
            try {
              future.get();
            } catch (Exception e) {
            }
          });

      return subEventNames;
    }
  }

  private void validateSubEvents(
      int logIndex, int expectedSize, List<String> expectedSubEventNames) {
    Map<String, List<Map<String, Object>>> subEvents = getSubEventsAtIndex(logIndex);
    assertThat(subEvents).hasSize(expectedSize);

    List<Map<String, Object>> allSubEvents = new ArrayList<>();
    for (String threadId : subEvents.keySet()) {
      allSubEvents.addAll(subEvents.get(threadId));
    }

    List<String> opNames = new ArrayList<>();
    for (Map<String, Object> theEvent : allSubEvents) {
      opNames.add((String) theEvent.get("name"));
    }

    assertThat(allSubEvents).hasSize(expectedSubEventNames.size());

    Collections.sort(opNames);
    Collections.sort(expectedSubEventNames);

    assertThat(opNames).isEqualTo(expectedSubEventNames);
  }

  private void validateSubEventsSize(int logIndex, int threadPoolSize, int expectedSubEventSize) {
    Map<String, List<Map<String, Object>>> subEvents = getSubEventsAtIndex(logIndex);
    assertThat(subEvents).hasSize(threadPoolSize);

    List<Map<String, Object>> allSubEvents = new ArrayList<>();
    for (String threadId : subEvents.keySet()) {
      allSubEvents.addAll(subEvents.get(threadId));
    }

    List<String> opNames = new ArrayList<>();
    for (Map<String, Object> theEvent : allSubEvents) {
      opNames.add((String) theEvent.get("name"));
    }

    assertThat(allSubEvents).hasSize(expectedSubEventSize);
  }

  private void assertEventAtIndex(int logIndex, int eventIndex, Map<String, String> expected) {
    ArrayList<Object> events = getEventAtLogIndex(logIndex);
    Map<Object, Object> theEvent = (Map<Object, Object>) events.get(eventIndex);

    for (String key : expected.keySet()) {
      assertThat(theEvent.get(key)).isEqualTo(expected.get(key));
    }

    System.out.println(theEvent);
  }

  private ArrayList<Object> getEventAtLogIndex(int index) {
    return (ArrayList<Object>) this.assertingHandler.logs.get(index).get("events");
  }

  private Map<String, List<Map<String, Object>>> getSubEventsAtIndex(int index) {
    return (Map<String, List<Map<String, Object>>>)
        this.assertingHandler.logs.get(index).get("subEvents");
  }

  private static void assertThreadTraceIsNull() {
    assertThat(getThreadTrace()).isNull();
  }

  private String getRandomString() {
    return Integer.toString(ThreadLocalRandom.current().nextInt());
  }

  private static ThreadTrace getThreadTrace() {
    return TraceOperation.current();
  }

  static class TestLogHandler extends Handler {
    private static final Gson gson = new Gson();

    private List<HashMap<String, Object>> logs = new ArrayList<>();

    @Override
    public void publish(LogRecord record) {
      HashMap<String, Object> parsed =
          gson.fromJson(record.getMessage(), new TypeToken<HashMap<String, Object>>() {}.getType());
      this.logs.add(parsed);

      System.out.println(parsed);
    }

    @Override
    public void flush() {}

    @Override
    public void close() throws SecurityException {}

    List<HashMap<String, Object>> getLogs() {
      return logs;
    }
  }
}
