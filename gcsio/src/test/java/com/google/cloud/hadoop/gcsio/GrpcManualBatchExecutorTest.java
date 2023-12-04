package com.google.cloud.hadoop.gcsio;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.common.util.concurrent.FutureCallback;
import java.io.IOException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link GrpcManualBatchExecutor}. */
@RunWith(JUnit4.class)
public class GrpcManualBatchExecutorTest {

  private GrpcManualBatchExecutor grpcManualBatchExecutor;

  @Before
  public void setUp() {
    grpcManualBatchExecutor = new GrpcManualBatchExecutor(10);
  }

  @Test
  public void queue_succeeds() {
    grpcManualBatchExecutor.queue(() -> true, /* callback*/ assertCallBack());
  }

  @Test
  public void queue_throwsException_afterShutdownCalled() throws IOException {
    grpcManualBatchExecutor.shutdown();

    IllegalStateException e =
        assertThrows(
            IllegalStateException.class, () -> grpcManualBatchExecutor.queue(() -> null, null));

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
