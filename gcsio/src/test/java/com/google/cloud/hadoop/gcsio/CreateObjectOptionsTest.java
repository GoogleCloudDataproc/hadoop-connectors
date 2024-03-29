/*
 * Copyright 2015 Google Inc.
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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CreateObjectOptionsTest {

  @Test
  public void build_checksContentTypeMetadata() {
    CreateObjectOptions.builder()
        .setMetadata(ImmutableMap.of("Innocuous-Type", "".getBytes(UTF_8)))
        .build();

    assertThrows(
        IllegalArgumentException.class,
        () ->
            CreateObjectOptions.builder()
                .setMetadata(ImmutableMap.of("Content-Type", "".getBytes(UTF_8)))
                .build());
  }

  @Test
  public void build_checksContentEncodingMetadata() {
    CreateObjectOptions.builder()
        .setMetadata(ImmutableMap.of("Innocuous-Encoding", "".getBytes(UTF_8)))
        .build();

    assertThrows(
        IllegalArgumentException.class,
        () ->
            CreateObjectOptions.builder()
                .setMetadata(ImmutableMap.of("Content-Encoding", "".getBytes(UTF_8)))
                .build());
  }
}
