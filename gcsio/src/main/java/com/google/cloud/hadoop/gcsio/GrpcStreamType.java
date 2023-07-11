/*
 * Copyright 2023 Google LLC
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

package com.google.cloud.hadoop.gcsio;

import com.google.common.annotations.VisibleForTesting;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

@VisibleForTesting
public enum GrpcStreamType {
  START_RESUMABLE_WRITE("StartResumableWrite"),
  WRITE_OBJECT("WriteObject"),
  READ_OBJECT("ReadObject"),
  OTHER("Other");

  private final String name;

  GrpcStreamType(String name) {
    this.name = name;
  }

  private static final Map<String, GrpcStreamType> names =
      Arrays.stream(GrpcStreamType.values())
          .collect(Collectors.toMap(x -> x.name.toUpperCase(), x -> x));

  public static GrpcStreamType getTypeFromName(String name) {
    GrpcStreamType type = names.get(name.toUpperCase());
    if (type == null) {
      type = GrpcStreamType.OTHER;
    }
    return type;
  }
}
