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
import com.google.common.util.concurrent.SettableFuture;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
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

  @Test
  public void isIdle_forThreadPoolExecutor_returnsTrueBeforeExecution() throws Exception {
    BatchExecutor batchExecutor = new BatchExecutor(/* numThreads= */ 1);

    assertThat(batchExecutor.isIdle()).isTrue();
  }

  @Test
  public void isIdle_forThreadPoolExecutor_returnsCorrectState() throws Exception {
    BatchExecutor batchExecutor = new BatchExecutor(/* numThreads= */ 1);

    // Create mock tasks and finish in one second.
    SettableFuture<Void> taskStarted = SettableFuture.create();
    SettableFuture<Void> taskShouldFinish = SettableFuture.create();
    Callable<Void> longRunningTask =
        () -> {
          taskStarted.set(null);
          // Wait until the test signals to finish.
          taskShouldFinish.get(1, TimeUnit.SECONDS);
          return null;
        };

    // Queueing the tasks to BatchExecuter.
    batchExecutor.queue(longRunningTask, null);
    taskStarted.get(1, TimeUnit.SECONDS);

    assertThat(batchExecutor.isIdle()).isFalse();

    // Shutting down the BatchExecuter.
    taskShouldFinish.set(null);
    batchExecutor.shutdown();

    assertThat(batchExecutor.isIdle()).isTrue();
  }

  @Test
  public void isIdle_forDirectExecutorService_isAlwaysTrue() throws Exception {
    BatchExecutor batchExecutor = new BatchExecutor(/* numThreads= */ 0);

    assertThat(batchExecutor.isIdle()).isTrue();

    batchExecutor.queue(() -> null, null);

    assertThat(batchExecutor.isIdle()).isTrue();

    batchExecutor.shutdown();
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
