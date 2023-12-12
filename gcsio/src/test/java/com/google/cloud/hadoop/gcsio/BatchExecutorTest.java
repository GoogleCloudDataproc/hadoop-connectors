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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.common.util.concurrent.FutureCallback;
import java.io.IOException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link BatchExecutor}. */
@RunWith(JUnit4.class)
public class BatchExecutorTest {

  private BatchExecutor batchExecutor;

  @Before
  public void setUp() {
    batchExecutor = new BatchExecutor(10);
  }

  @Test
  public void queue_succeeds() {
    batchExecutor.queue(() -> true, /* callback*/ assertCallBack());
  }

  @Test
  public void queue_throwsException_afterShutdownCalled() throws IOException {
    batchExecutor.shutdown();

    IllegalStateException e =
        assertThrows(IllegalStateException.class, () -> batchExecutor.queue(() -> null, null));

    assertThat(e)
        .hasMessageThat()
        .startsWith("requestExecutor should not be terminated to queue request");
  }

  private FutureCallback<Boolean> assertCallBack() {
    return new FutureCallback<>() {
      @Override
      public void onSuccess(Boolean result) {
        assertThat(result).isEqualTo(true);
      }

      @Override
      public void onFailure(Throwable throwable) {}
    };
  }
}
