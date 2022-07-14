/*
 * Copyright 2022 Google LLC. All Rights Reserved.
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

package com.google.cloud.hadoop.gcsio;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Handler;
import java.util.logging.LogRecord;

public class AssertingLogHandler extends Handler {
  private static final Gson GSON = new Gson();
  private static final Type LOG_RECORD_TYPE = new TypeToken<Map<String, Object>>() {}.getType();

  private List<Map<String, Object>> logRecords = new ArrayList<>();
  private List<String> methods = new ArrayList<>();

  @Override
  public void publish(LogRecord record) {
    if (isLoggable(record)) {
      logRecords.add(logRecordToMap(record));
      methods.add(record.getSourceMethodName());
    }
  }

  @Override
  public void flush() {
    logRecords.clear();
    methods.clear();
  }

  @Override
  public void close() {
    logRecords = null;
    methods = null;
  }

  public void assertLogCount(int n) {
    assertThat(logRecords).hasSize(n);
    assertThat(methods).hasSize(n);
  }

  public Map<String, Object> getLogRecordAtIndex(int index) {
    return logRecords.get(index);
  }

  public List<Map<String, Object>> getAllLogRecords() {
    return logRecords;
  }

  public void verifyCommonTraceFields() {
    for (Map<String, Object> event : logRecords) {
      assertTrue(event.containsKey("initiatingthreadname"));
      assertTrue(event.containsKey("remoteaddress"));
      assertTrue(event.containsKey("elapsedmillis"));
      assertTrue(event.containsKey("requestinfo"));
      assertTrue(event.containsKey("eventtime"));
      assertTrue(event.containsKey("details"));
    }
  }

  String getMethodAtIndex(int index) {
    return methods.get(index);
  }

  private static Map<String, Object> logRecordToMap(LogRecord logRecord) {
    return GSON.fromJson(logRecord.getMessage(), LOG_RECORD_TYPE);
  }
}
