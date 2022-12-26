package com.google.cloud.hadoop.gcsio;

import com.google.cloud.RestorableState;
import com.google.cloud.WriteChannel;
import java.io.IOException;
import java.nio.ByteBuffer;

/** FakeWriterChannel which writes only half the passed in byteBuffer capacity at a time. */
public class FakeWriteChannel implements WriteChannel {

  private boolean isOpen = false;

  private boolean writeException = false;

  public FakeWriteChannel() {
    isOpen = true;
  }

  public FakeWriteChannel(Boolean writeException) {
    this();
    this.writeException = writeException;
  }

  @Override
  public void setChunkSize(int i) {}

  @Override
  public RestorableState<WriteChannel> capture() {
    return null;
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    if (writeException) {
      throw new IOException("Intentionally triggered");
    }
    int bytesWritten = 0;
    // always writes half or lesser from the provided byte buffer capacity
    int capacity = src.capacity();
    if ((src.limit() - src.position()) <= capacity / 2) {
      bytesWritten = src.limit();
      src.position(src.limit());
    } else {
      bytesWritten = capacity / 2;
      src.position(src.position() + capacity / 2);
    }
    return bytesWritten;
  }

  @Override
  public boolean isOpen() {
    return isOpen;
  }

  @Override
  public void close() throws IOException {
    isOpen = false;
  }
}
