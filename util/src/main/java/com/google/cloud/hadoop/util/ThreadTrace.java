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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.gson.Gson;
import com.google.gson.annotations.Expose;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Helps track a request. For eg. this can be used to track a HCFS API call (e.g. rename). This can help pinpoint the operation (e.g. a GCS API call) which is causing request to be slow.
 *
 * This also tracks operations which runs in the same thread or another thread, up to one level. Mmeaning if an main thread uses threadA, that is tracked. But if threadA uses threadB, it is not tracked. This case can be added if required.
 *
 * NOTE: Do not directly use this class. Use the factory methods in {@code ThreadOperation}. This class is for internal use and the API signatures can change.
 */
public class ThreadTrace {

  // TODO: Make this configurable
  static final int MAX_EVENTS_SIZE = 20;
  private static Gson gson = new Gson();
  private final String trackingId;
  private final List<ThreadTraceEvent> events;

  private final Map<Long, List<ThreadTraceEvent>> subEvents = new HashMap<>();

  public ThreadTrace(String trackingId) {
    this(trackingId, new ArrayList<>());
  }

  private ThreadTrace(String trackingId, List<ThreadTraceEvent> events) {
    checkNotNull(trackingId, "trackingId cannot be null");
    checkNotNull(events, "events cannot be null. trackingId=%s", trackingId);
    this.trackingId = trackingId;
    this.events = events;
  }

  @Override
  public String toString() {
    return gson.toJson(this);
  }

  ThreadTraceEvent starOperationEvent(String name) {
    if (isFull()) {
      return null;
    }

    ThreadTraceEvent theEvent = ThreadTraceEvent.startEvent(name);
    this.events.add(theEvent);

    return theEvent;
  }

  void endOperationEvent(ThreadTraceEvent startEvent) {
    if (isFull() || startEvent == null) {
      return;
    }

    int eventsSize = events.size();
    if (eventsSize != 0) {
      ThreadTraceEvent lastEvent = events.get(eventsSize - 1);
      if (lastEvent == startEvent) {
        startEvent.markEnd();
        return;
      }
    }

    this.events.add(startEvent.endEvent());
  }

  public String getTrackingId() {
    return this.trackingId;
  }

  ThreadTrace getSubTrace() {
    long threadId = Thread.currentThread().getId();

    subEvents.putIfAbsent(threadId, new ArrayList<>());

    int size = subEvents.get(threadId).size();
    if (size >= MAX_EVENTS_SIZE) {
      return null;
    }

    return new ThreadTrace(this.trackingId, subEvents.get(threadId).subList(size, size));
  }

  public void annotate(String key, Object value) {
    if (this.events.size() == 0) {
      return;
    }

    this.events.get(this.events.size() - 1).annotate(key, value);
  }

  private boolean isFull() {
    return events.size() >= MAX_EVENTS_SIZE;
  }

  static class ThreadTraceEvent extends HashMap<String, Object> {

    static final int MAX_PROPERTY_SIZE = 10;

    private enum EventType {
      START,
      END,
      MERGED,
    }

    @Expose(serialize = false)
    private final long timeStamp;

    @Expose(serialize = false)
    private final String name;

    static ThreadTraceEvent startEvent(String name) {
      return new ThreadTraceEvent(EventType.START, name);
    }

    private ThreadTraceEvent(EventType type, String name) {
      super(5);
      this.setEventType(type);
      this.put("name", name);
      this.timeStamp = System.currentTimeMillis();
      this.name = name;
      if (type == EventType.START) {
        this.put("starttime", this.timeStamp);
      } else if (type == EventType.END) {
        this.put("endtime", this.timeStamp);
      } else {
        throw new IllegalStateException("Not expected to create event of type: " + type);
      }
    }

    public void markEnd() {
      setEventType(EventType.MERGED);
      setTimeTaken(System.currentTimeMillis() - this.timeStamp);
    }

    ThreadTraceEvent endEvent() {
      ThreadTraceEvent result = new ThreadTraceEvent(EventType.END, name);
      result.setTimeTaken(result.timeStamp - timeStamp);
      return result;
    }

    void annotate(String key, Object value) {
      if (this.size() >= MAX_PROPERTY_SIZE) {
        return;
      }

      this.put(key, value);
    }

    private void setTimeTaken(long timeTaken) {
      put("timetaken", timeTaken);
    }

    private void setEventType(EventType eventType) {
      put("type", eventType);
    }
  }
}
