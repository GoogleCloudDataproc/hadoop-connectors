/*
 * Copyright 2026 Google LLC
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpExecuteInterceptor;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.testing.http.MockHttpTransport;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ChecksumHeaderInterceptorTest {

  private ChecksumHeaderInterceptor interceptor;
  private HttpExecuteInterceptor delegate;
  private HttpRequest request;
  private HttpHeaders headers;

  @Before
  public void setUp() throws IOException {
    delegate = mock(HttpExecuteInterceptor.class);
    interceptor = new ChecksumHeaderInterceptor(delegate);
    request =
        new MockHttpTransport()
            .createRequestFactory()
            .buildRequest("PUT", new GenericUrl("http://test"), null);
    headers = new HttpHeaders();
    request.setHeaders(headers);
  }

  @After
  public void tearDown() {
    ChecksumContext.clear();
  }

  @Test
  public void intercept_delegatesExecution() throws IOException {
    interceptor.intercept(request);
    verify(delegate).intercept(request);
  }

  @Test
  public void intercept_noSupplier_skipsHeader() throws IOException {
    headers.setContentRange("bytes 0-100/101");

    interceptor.intercept(request);

    // header not set since no Supplier was set in context
    assertThat(headers.get("x-goog-hash")).isNull();
  }

  @Test
  public void intercept_notPutMethod_skipsHeader() throws IOException {
    setupChecksumSupplier("fakeChecksum==");
    request.setRequestMethod("POST");
    headers.setContentRange("bytes 0-100/101");

    interceptor.intercept(request);

    assertThat(headers.get("x-goog-hash")).isNull();
  }

  @Test
  public void intercept_intermediateChunk_skipsHeader() throws IOException {
    setupChecksumSupplier("fakeChecksum==");
    headers.setContentRange("bytes 0-100/*");

    interceptor.intercept(request);

    assertThat(headers.get("x-goog-hash")).isNull();
  }

  @Test
  public void intercept_finalChunk_addsHeader() throws IOException {
    String expectedChecksum = "AAAAAA==";
    setupChecksumSupplier(expectedChecksum);
    headers.setContentRange("bytes 0-3/4");

    interceptor.intercept(request);

    assertThat(headers.getFirstHeaderStringValue("x-goog-hash"))
        .isEqualTo("crc32c=" + expectedChecksum);
  }

  @Test
  public void intercept_multipleCalls_simulatesRetry() throws IOException {
    String expectedChecksum = "AAAAAA==";
    setupChecksumSupplier(expectedChecksum);
    headers.setContentRange("bytes 0-3/4");

    // Simulate a network retry by calling the interceptor twice
    interceptor.intercept(request);
    interceptor.intercept(request);

    // It should safely pull from the supplier both times without crashing
    assertThat(headers.getFirstHeaderStringValue("x-goog-hash"))
        .isEqualTo("crc32c=" + expectedChecksum);
  }

  @Test
  public void intercept_finerLogRedactsUploadIdWithoutMutatingUrl() throws IOException {
    String secretUploadId = "SECRET_CHECKSUM_INTERCEPTOR_UPLOAD_ID";
    GenericUrl uploadUrl =
        new GenericUrl(
            "https://storage.googleapis.com/upload/storage/v1/b/bkt/o"
                + "?uploadType=resumable&upload_id="
                + secretUploadId);
    HttpRequest uploadRequest =
        new MockHttpTransport().createRequestFactory().buildRequest("PUT", uploadUrl, null);
    Logger julLogger = Logger.getLogger(ChecksumHeaderInterceptor.class.getName());
    List<LogRecord> records = new ArrayList<>();
    Handler handler =
        new Handler() {
          @Override
          public void publish(LogRecord logRecord) {
            records.add(logRecord);
          }

          @Override
          public void flush() {
            // No-op stub for in-memory test log handler.
          }

          @Override
          public void close() {
            // No-op stub for in-memory test log handler.
          }
        };
    Level previousLevel = julLogger.getLevel();
    boolean previousUseParentHandlers = julLogger.getUseParentHandlers();
    julLogger.setUseParentHandlers(false);
    julLogger.addHandler(handler);
    julLogger.setLevel(Level.FINER);
    try {
      interceptor.intercept(uploadRequest);

      assertThat(
              records.stream()
                  .map(LogRecord::getMessage)
                  .anyMatch(
                      msg ->
                          msg.contains("Intercepting request")
                              && msg.contains("uploadType=resumable")
                              && msg.contains(
                                  "upload_id=" + LoggingUrlRedactor.digestToken(secretUploadId))))
          .isTrue();
      for (LogRecord logRecord : records) {
        assertThat(logRecord.getMessage()).doesNotContain(secretUploadId);
      }
      assertThat(uploadRequest.getUrl().getFirst("upload_id")).isEqualTo(secretUploadId);
    } finally {
      julLogger.removeHandler(handler);
      julLogger.setLevel(previousLevel);
      julLogger.setUseParentHandlers(previousUseParentHandlers);
    }
  }

  /** Helper method to set a dummy supplier in the context. */
  private void setupChecksumSupplier(String expectedChecksum) {
    ChecksumContext.setChecksumSupplier(() -> expectedChecksum);
  }
}
