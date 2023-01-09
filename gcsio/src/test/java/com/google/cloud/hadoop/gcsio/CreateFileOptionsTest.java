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

import static com.google.common.truth.Truth.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CreateFileOptionsTest {
  @Test
  public void validOptions() {
    CreateFileOptions.builder()
        .setAttributes(ImmutableMap.of("Innocuous", "text".getBytes(UTF_8)))
        .setContentType("text")
        .setWriteMode(CreateFileOptions.WriteMode.OVERWRITE)
        .setOverwriteGenerationId(0)
        .build();
  }

  @Test
  public void invalidOptions_contentType_shouldNotBeSetViaAttributes() {
    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                CreateFileOptions.builder()
                    .setAttributes(ImmutableMap.of("Content-Type", "text".getBytes(UTF_8)))
                    .setContentType("text")
                    .build());

    assertThat(thrown)
        .hasMessageThat()
        .isEqualTo("The Content-Type attribute must be set via the contentType option");
  }

  @Test
  public void invalidOptions_createNew_overwriteGenerationShouldNotBeSet() {
    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                CreateFileOptions.builder()
                    .setWriteMode(CreateFileOptions.WriteMode.CREATE_NEW)
                    .setOverwriteGenerationId(0)
                    .build());

    assertThat(thrown)
        .hasMessageThat()
        .isEqualTo("overwriteGenerationId is set to 0 but it can be set only in OVERWRITE mode");
  }
}
