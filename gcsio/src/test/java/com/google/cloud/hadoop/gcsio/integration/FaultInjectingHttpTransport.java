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

package com.google.cloud.hadoop.gcsio.integration;

import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.http.LowLevelHttpResponse;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A "Man-in-the-Middle" Transport that delegates to a real HttpTransport that can inject different
 * network failures based on request headers.
 */
public class FaultInjectingHttpTransport extends HttpTransport {

  // Header keys
  public static final String FAULT_HEADER_NAME = "X-GCS-TEST-FAULT";
  public static final String FAULT_OFFSET_HEADER_NAME = "X-GCS-FAULT-OFFSET";

  // Supported Fault Types
  public static final String FAULT_CONNECTION_RESET = "connection-reset";
  public static final String FAULT_PREMATURE_EOF = "premature-eof";
  public static final String FAULT_CORRUPT_BYTES = "corrupt-bytes";
  public static final String FAULT_SOCKET_TIMEOUT = "socket-timeout";

  private static final int DEFAULT_FAULT_OFFSET = 10;

  private final HttpTransport realTransport;
  public final AtomicInteger failureCount;

  public FaultInjectingHttpTransport(HttpTransport realTransport) {
    this.realTransport = realTransport;
    this.failureCount = new AtomicInteger(0);
  }

  @Override
  protected LowLevelHttpRequest buildRequest(String method, String url) throws IOException {
    try {
      Method buildRequestMethod =
          HttpTransport.class.getDeclaredMethod("buildRequest", String.class, String.class);
      buildRequestMethod.setAccessible(true);
      final LowLevelHttpRequest realRequest =
          (LowLevelHttpRequest) buildRequestMethod.invoke(realTransport, method, url);

      return new LowLevelHttpRequest() {
        final Map<String, String> headers = new HashMap<>();

        @Override
        public void addHeader(String name, String value) throws IOException {
          headers.put(name, value);
          // Filter out test headers so GCS doesn't reject the request
          if (!FAULT_HEADER_NAME.equalsIgnoreCase(name)
              && !FAULT_OFFSET_HEADER_NAME.equalsIgnoreCase(name)) {
            realRequest.addHeader(name, value);
          }
        }

        @Override
        public void setTimeout(int connectTimeout, int readTimeout) throws IOException {
          realRequest.setTimeout(connectTimeout, readTimeout);
        }

        @Override
        public LowLevelHttpResponse execute() throws IOException {
          LowLevelHttpResponse realResponse = realRequest.execute();

          String faultType = headers.get(FAULT_HEADER_NAME.toLowerCase());
          if (faultType != null) {
            failureCount.incrementAndGet();

            // Parse offset (Default to 10 bytes if missing)
            int offset = DEFAULT_FAULT_OFFSET;
            String offsetStr = headers.get(FAULT_OFFSET_HEADER_NAME.toLowerCase());
            if (offsetStr != null) {
              try {
                offset = Integer.parseInt(offsetStr);
              } catch (NumberFormatException ignored) {
              }
            }

            return new FaultInjectingHttpResponse(realResponse, faultType, offset);
          }
          return realResponse;
        }
      };
    } catch (Exception e) {
      throw new IOException("Failed to invoke buildRequest ", e);
    }
  }

  /** Wrapper that injects specific faults into the InputStream. */
  private static class FaultInjectingHttpResponse extends LowLevelHttpResponse {
    private final LowLevelHttpResponse delegate;
    private final String faultType;
    private final int faultOffset;

    public FaultInjectingHttpResponse(
        LowLevelHttpResponse delegate, String faultType, int faultOffset) {
      this.delegate = delegate;
      this.faultType = faultType;
      this.faultOffset = faultOffset;
    }

    @Override
    public InputStream getContent() throws IOException {
      InputStream realStream = delegate.getContent();
      return new FilterInputStream(realStream) {
        int bytesReadTotal = 0;

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
          int n = super.read(b, off, len);
          if (n > 0) {
            int previousTotal = bytesReadTotal;
            bytesReadTotal += n;

            // Check if we crossed the fault offset in this chunk
            if (bytesReadTotal >= faultOffset) {
              switch (faultType) {
                case FAULT_CONNECTION_RESET:
                  throw new SocketException("Connection reset");

                case FAULT_SOCKET_TIMEOUT:
                  try {
                    Thread.sleep(2000);
                  } catch (InterruptedException e) {
                  }
                  throw new SocketTimeoutException("Read timed out");

                case FAULT_PREMATURE_EOF:
                  // Calculate how many bytes we CAN return before cutting it off
                  int allowed = Math.max(0, faultOffset - previousTotal);
                  if (allowed == 0) return -1;
                  return allowed;

                case FAULT_CORRUPT_BYTES:
                  // Find the exact index in the buffer b that corresponds to faultOffset
                  int indexToCorrupt = faultOffset - previousTotal;
                  if (indexToCorrupt >= 0 && indexToCorrupt < n) {
                    b[off + indexToCorrupt] ^= 0xFF; // Flip bits
                  }
                  break;

                default:
                  // Unknown fault type, do nothing
                  break;
              }
            }
          }
          return n;
        }

        @Override
        public int read() throws IOException {
          int b = super.read();
          if (b != -1) {
            bytesReadTotal++;
            if (bytesReadTotal >= faultOffset) {
              switch (faultType) {
                case FAULT_CONNECTION_RESET:
                  throw new SocketException("Connection reset");
                case FAULT_SOCKET_TIMEOUT:
                  throw new SocketTimeoutException("Timeout");
                case FAULT_PREMATURE_EOF:
                  return -1;
                case FAULT_CORRUPT_BYTES:
                  return b ^ 0xFF;
              }
            }
          }
          return b;
        }
      };
    }

    @Override
    public String getContentEncoding() throws IOException {
      return delegate.getContentEncoding();
    }

    @Override
    public long getContentLength() throws IOException {
      return delegate.getContentLength();
    }

    @Override
    public String getContentType() throws IOException {
      return delegate.getContentType();
    }

    @Override
    public String getStatusLine() throws IOException {
      return delegate.getStatusLine();
    }

    @Override
    public int getStatusCode() throws IOException {
      return delegate.getStatusCode();
    }

    @Override
    public String getReasonPhrase() throws IOException {
      return delegate.getReasonPhrase();
    }

    @Override
    public int getHeaderCount() throws IOException {
      return delegate.getHeaderCount();
    }

    @Override
    public String getHeaderName(int i) throws IOException {
      return delegate.getHeaderName(i);
    }

    @Override
    public String getHeaderValue(int i) throws IOException {
      return delegate.getHeaderValue(i);
    }
  }
}
