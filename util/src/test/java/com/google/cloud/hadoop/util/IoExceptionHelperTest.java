/*
 * Copyright 2024 Google Inc.
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

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedByInterruptException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class IoExceptionHelperTest {

  @Test
  public void testIsInterrupted() {
    assertThat(IoExceptionHelper.isInterrupted(new InterruptedException())).isTrue();
    assertThat(IoExceptionHelper.isInterrupted(new ClosedByInterruptException())).isTrue();
    assertThat(IoExceptionHelper.isInterrupted(new InterruptedIOException())).isTrue();

    // SocketTimeoutException should NOT be treated as interrupt
    assertThat(IoExceptionHelper.isInterrupted(new SocketTimeoutException())).isFalse();

    // Regular IOException should NOT be treated as interrupt
    assertThat(IoExceptionHelper.isInterrupted(new IOException())).isFalse();

    // Nested interruptions should be detected
    assertThat(IoExceptionHelper.isInterrupted(new IOException(new InterruptedException())))
        .isTrue();
    assertThat(
            IoExceptionHelper.isInterrupted(new RuntimeException(new ClosedByInterruptException())))
        .isTrue();

    // Nested non-interruptions
    assertThat(IoExceptionHelper.isInterrupted(new IOException(new SocketTimeoutException())))
        .isFalse();
  }
}
