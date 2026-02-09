package com.google.cloud.hadoop.fs.gcs;

import com.google.api.client.http.HttpContent;
import com.google.api.client.http.HttpExecuteInterceptor;
import com.google.api.client.http.HttpRequest;
import com.google.common.annotations.VisibleForTesting;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Interceptor to simulate data or checksum corruption for testing GCS integrity checks. Wraps an
 * existing interceptor to preserve original request initialization.
 */
@VisibleForTesting
public class CorruptionSimulatorInterceptor implements HttpExecuteInterceptor {

  public enum CorruptionType {
    NONE,
    DATA, // Corrupts the upload body
    CHECKSUM // Corrupts the x-goog-hash header
  }

  private final HttpExecuteInterceptor existingInterceptor;
  private final CorruptionType corruptionType;

  public CorruptionSimulatorInterceptor(
      HttpExecuteInterceptor existingInterceptor, CorruptionType corruptionType) {
    this.existingInterceptor = existingInterceptor;
    this.corruptionType = corruptionType;
  }

  @Override
  public void intercept(HttpRequest request) throws IOException {
    // Run the existing interceptor (e.g., auth, logging)
    if (existingInterceptor != null) {
      existingInterceptor.intercept(request);
    }

    // Apply corruption logic based on type
    if (corruptionType == CorruptionType.CHECKSUM) {
      corruptChecksum(request);
    } else if (corruptionType == CorruptionType.DATA) {
      corruptData(request);
    }
  }

  private void corruptChecksum(HttpRequest request) {
    // Only corrupt if the header exists
    if (request.getHeaders().containsKey("x-goog-hash")) {
      // Overwrite valid checksum with a bad one
      request.getHeaders().set("x-goog-hash", "crc32c=AAAAAA==");
    }
  }

  private void corruptData(HttpRequest request) {
    String method = request.getRequestMethod();
    boolean hasHeader = request.getHeaders().containsKey("x-goog-hash");

    // Only corrupt PUT requests that have a checksum header (final chunks/objects)
    if (hasHeader && "PUT".equals(method)) {
      HttpContent originalContent = request.getContent();
      if (originalContent != null) {
        request.setContent(new CorruptedHttpContent(originalContent));
      }
    }
  }

  /** Wrapper for HttpContent that corrupts the first byte of the stream. */
  private static class CorruptedHttpContent implements HttpContent {
    private final HttpContent delegate;

    public CorruptedHttpContent(HttpContent delegate) {
      this.delegate = delegate;
    }

    @Override
    public long getLength() throws IOException {
      return delegate.getLength();
    }

    @Override
    public String getType() {
      return delegate.getType();
    }

    @Override
    public boolean retrySupported() {
      return delegate.retrySupported();
    }

    @Override
    public void writeTo(OutputStream out) throws IOException {
      delegate.writeTo(new CorruptingOutputStream(out));
    }
  }

  /** FilterOutputStream that flips the bits of the first byte written. */
  private static class CorruptingOutputStream extends FilterOutputStream {
    private boolean corrupted = false;

    public CorruptingOutputStream(OutputStream out) {
      super(out);
    }

    @Override
    public void write(int b) throws IOException {
      if (!corrupted) {
        corrupted = true;
        // Flip bits to corrupt
        out.write(b ^ 1);
      } else {
        out.write(b);
      }
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      if (!corrupted && len > 0) {
        byte[] corruptedCopy = new byte[len];
        System.arraycopy(b, off, corruptedCopy, 0, len);
        // Flip first byte
        corruptedCopy[0] = (byte) (corruptedCopy[0] ^ 1);
        corrupted = true;
        out.write(corruptedCopy, 0, len);
      } else {
        out.write(b, off, len);
      }
    }
  }
}
