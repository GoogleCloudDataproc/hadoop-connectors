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

import com.google.cloud.hadoop.util.ThreadTrace.ThreadTraceEvent;
import com.google.common.flogger.GoogleLogger;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Helps trace a block of code.
 *
 * <p>Use {@code TraceOperation.createRootWithLogging} to setup tracing on a thread. Use {@code
 * TraceOperation.addToExistingTrace to add to the existing tracer}. Use {@code
 * TraceOperation.getChildTrace to track operation which runs in another thread}
 *
 * <p>NOTE: Ensure that close() is called on each of the operation. NOTE: This class is for internal
 * use and can change without notice.
 */
public class TraceOperation implements ITraceOperation {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();
  static final ITraceOperation NOOP =
      new ITraceOperation() {
        @Override
        public void close() {}

        @Override
        public ThreadTrace getTrace() {
          return null;
        }

        @Override
        public void annotate(String resultSize, int size) {}
      };

  private static ThreadLocal<Deque<ThreadTrace>> traceStack =
      new ThreadLocal<Deque<ThreadTrace>>() {
        @Override
        protected Deque<ThreadTrace> initialValue() {
          return new ArrayDeque<>(1);
        }
      };
  private final boolean isRoot;

  private final boolean shouldLog;

  private final String name;

  private final ThreadTrace trace;

  private final ThreadTraceEvent startEvent;

  public static ITraceOperation createRootWithLogging(String name, Object context) {
    checkNotNull(name, "name cannot be null");
    long randomMs = System.currentTimeMillis() % 1000;
    String trackingId = String.format("%s(%s)_%s", name, context, randomMs);
    return new TraceOperation(name, true, true, new ThreadTrace(trackingId));
  }

  public static ITraceOperation addToExistingTrace(String name) {
    ThreadTrace trace = current();
    if (trace == null) {
      return TraceOperation.NOOP;
    }

    return new TraceOperation(name, false, false, trace);
  }

  public static ThreadTrace current() {
    if (traceStack.get().isEmpty()) {
      return null;
    }

    return traceStack.get().peekLast();
  }

  public static ITraceOperation getChildTrace(ThreadTrace parentTrace, String name) {
    if (parentTrace == null) {
      return TraceOperation.NOOP;
    }

    ThreadTrace childTrace = parentTrace.getSubTrace();
    if (childTrace == null) { // If max events size reached.
      return TraceOperation.NOOP;
    }

    return new TraceOperation(name, true, false, childTrace);
  }

  public static String getTrackingId() {
    ThreadTrace threadTrace = current();
    if (threadTrace == null) {
      return "";
    }

    return threadTrace.getTrackingId();
  }

  private TraceOperation(String name, boolean isRoot, boolean shouldLog, ThreadTrace trace) {
    checkNotNull(trace, String.format("Trace cannot be null. name=%s;", name));
    this.isRoot = isRoot;
    this.shouldLog = shouldLog;
    this.name = name;
    this.trace = trace;
    if (isRoot) {
      setupTracingForThread(trace);
    }

    this.startEvent = this.trace.starOperationEvent(name);
  }

  public ThreadTrace getTrace() {
    return this.trace;
  }

  @Override
  public void close() {
    if (isRoot) {
      removeTracingForThread();
      // TODO: Do a null check and log.
    }

    this.trace.endOperationEvent(this.startEvent);

    if (this.shouldLog) {
      logger.atInfo().log("%s", this.trace);
    }
  }

  public void annotate(String resultSize, int size) {
    this.trace.annotate(resultSize, size);
  }

  private static void setupTracingForThread(ThreadTrace trace) {
    traceStack.get().addLast(trace);
  }

  private static void removeTracingForThread() {
    if (traceStack.get().isEmpty()) {
      logger.atSevere().log("Did not expect the thread trace to be null");
      return;
    }

    traceStack.get().pollLast();
  }
}
