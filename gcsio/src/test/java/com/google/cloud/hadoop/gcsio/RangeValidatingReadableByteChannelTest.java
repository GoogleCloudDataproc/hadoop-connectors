package com.google.cloud.hadoop.gcsio;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.common.flogger.GoogleLogger;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class RangeValidatingReadableByteChannelTest {

  @Rule public final TestName testName = new TestName();

  @Test
  public void copy_happyPath() throws IOException {
    int beginOffset = 13;
    int endOffsetExclusive = 31;
    TestReadableByteChannel delegate = new TestReadableByteChannel(beginOffset, endOffsetExclusive);
    RangeValidatingReadableByteChannel r =
        RangeValidatingReadableByteChannel.of(objName(), beginOffset, endOffsetExclusive, delegate);

    int expectedCopiedBytes = endOffsetExclusive - beginOffset;
    byte[] expected = new byte[expectedCopiedBytes];
    for (int i = beginOffset; i < endOffsetExclusive; i++) {
      expected[i - beginOffset] = base64Byte(i);
    }

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    WritableByteChannel w = Channels.newChannel(baos);

    long copied = copyUsingBuffer(ByteBuffer.allocate(3), r, w);

    assertThat(copied).isEqualTo(expectedCopiedBytes);
    assertThat(delegate.position).isEqualTo(endOffsetExclusive);
    assertThat(baos.toByteArray()).isEqualTo(expected);
  }

  @Test
  public void copy_overRead() throws IOException {
    int beginOffset = 13;
    int endOffsetExclusive = 31;
    TestReadableByteChannel delegate =
        new TestReadableByteChannel(beginOffset, endOffsetExclusive + 1);
    RangeValidatingReadableByteChannel r =
        RangeValidatingReadableByteChannel.of(objName(), beginOffset, endOffsetExclusive, delegate);

    int expectedCopiedBytes = (endOffsetExclusive - beginOffset) + 1;
    byte[] expected = new byte[expectedCopiedBytes];
    for (int i = beginOffset; i < endOffsetExclusive + 1; i++) {
      expected[i - beginOffset] = base64Byte(i);
    }

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    WritableByteChannel w = Channels.newChannel(baos);

    long copied = copyUsingBuffer(ByteBuffer.allocate(3), r, w);

    // we're fully consuming the stream, so our assertion should be to the point of EOF
    assertThat(copied).isEqualTo(expectedCopiedBytes);
    assertThat(delegate.position).isEqualTo(endOffsetExclusive + 1);
    assertThat(baos.toByteArray()).isEqualTo(expected);
  }

  @Test
  public void copy_underRead() {
    int beginOffset = 13;
    int endOffsetExclusive = 31;
    TestReadableByteChannel delegate =
        new TestReadableByteChannel(beginOffset, endOffsetExclusive - 1);
    RangeValidatingReadableByteChannel r =
        RangeValidatingReadableByteChannel.of(objName(), beginOffset, endOffsetExclusive, delegate);

    int expectedCopiedBytes = (endOffsetExclusive - beginOffset) - 1;
    byte[] expected = new byte[expectedCopiedBytes];
    for (int i = beginOffset; i < endOffsetExclusive - 1; i++) {
      expected[i - beginOffset] = base64Byte(i);
    }

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    WritableByteChannel w = Channels.newChannel(baos);

    IOException ioException =
        assertThrows(IOException.class, () -> copyUsingBuffer(ByteBuffer.allocate(3), r, w));
    assertThat(ioException).hasMessageThat().contains("under-read of object ");
    assertThat(ioException)
        .hasMessageThat()
        .contains(
            "detected. Channel opened with {beginOffset: 13, endOffset: 31}, EOF detected at position: 30 (missing 1 bytes)");

    assertThat(delegate.position).isEqualTo(endOffsetExclusive - 1);
    assertThat(baos.toByteArray()).isEqualTo(expected);
  }

  @Test
  public void byteBuffer_happyPath() throws IOException {
    int beginOffset = 13;
    int endOffsetExclusive = 31;
    TestReadableByteChannel delegate = new TestReadableByteChannel(beginOffset, endOffsetExclusive);
    RangeValidatingReadableByteChannel r =
        RangeValidatingReadableByteChannel.of(objName(), beginOffset, endOffsetExclusive, delegate);

    int expectedCopiedBytes = (endOffsetExclusive - beginOffset);

    ByteBuffer buf = ByteBuffer.allocate(37);
    long copied = r.read(buf);
    assertThat(copied).isEqualTo(expectedCopiedBytes);
    assertThat(r.read(buf)).isEqualTo(-1);

    assertThat(delegate.position).isEqualTo(endOffsetExclusive);
    assertThat(buf.position()).isEqualTo(expectedCopiedBytes);
  }

  @Test
  public void byteBuffer_overRead() throws IOException {
    int beginOffset = 13;
    int endOffsetExclusive = 31;
    TestReadableByteChannel delegate =
        new TestReadableByteChannel(beginOffset, endOffsetExclusive + 1);
    RangeValidatingReadableByteChannel r =
        RangeValidatingReadableByteChannel.of(objName(), beginOffset, endOffsetExclusive, delegate);

    int expectedCopiedBytes = (endOffsetExclusive - beginOffset) + 1;

    ByteBuffer buf = ByteBuffer.allocate(37);
    long copied = r.read(buf);
    assertThat(r.read(buf)).isEqualTo(-1);

    assertThat(copied).isEqualTo(expectedCopiedBytes);
    assertThat(delegate.position).isEqualTo(endOffsetExclusive + 1);
    assertThat(buf.position()).isEqualTo(expectedCopiedBytes);
  }

  @Test
  public void byteBuffer_underRead() throws IOException {
    int beginOffset = 13;
    int endOffsetExclusive = 31;
    TestReadableByteChannel delegate =
        new TestReadableByteChannel(beginOffset, endOffsetExclusive - 1);
    RangeValidatingReadableByteChannel r =
        RangeValidatingReadableByteChannel.of(objName(), beginOffset, endOffsetExclusive, delegate);

    int expectedCopiedBytes = (endOffsetExclusive - beginOffset) - 1;

    ByteBuffer buf = ByteBuffer.allocate(37);
    long copied = r.read(buf);
    assertThat(copied).isEqualTo(expectedCopiedBytes);

    // EOF is only observed on a second read
    IOException ioException = assertThrows(IOException.class, () -> r.read(buf));
    assertThat(ioException).hasMessageThat().contains("under-read of object ");
    assertThat(ioException)
        .hasMessageThat()
        .contains(
            "detected. Channel opened with {beginOffset: 13, endOffset: 31}, EOF detected at position: 30 (missing 1 bytes)");

    assertThat(delegate.position).isEqualTo(endOffsetExclusive - 1);
    assertThat(buf.position()).isEqualTo(expectedCopiedBytes);
  }

  private String objName() {
    return "gs://test-bucket/obj-" + testName.getMethodName();
  }

  private static byte base64Byte(long i) {
    return (byte) (0x41 + i % 64);
  }

  private static long copyUsingBuffer(
      ByteBuffer buf, ReadableByteChannel from, WritableByteChannel to) throws IOException {
    long total = 0;
    while (from.read(buf) != -1) {
      buf.flip();
      while (buf.hasRemaining()) {
        total += to.write(buf);
      }
      buf.clear();
    }
    return total;
  }

  private static class TestReadableByteChannel implements ReadableByteChannel {
    private static final GoogleLogger LOGGER = GoogleLogger.forEnclosingClass();

    private long position;
    private boolean open;
    private final long eofAt;

    public TestReadableByteChannel(long position, long eofAt) {
      this.position = position;
      this.open = true;
      this.eofAt = eofAt;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
      if (!open) {
        throw new ClosedChannelException();
      }
      final long positionBegin = position;
      LOGGER.atInfo().log("positionBegin = %s", positionBegin);
      long channelRemaining = eofAt - positionBegin;
      LOGGER.atInfo().log("channelRemaining = %s", channelRemaining);
      if (channelRemaining == 0) {
        return -1;
      }
      int bufferRemaining = dst.remaining();
      LOGGER.atInfo().log("bufferRemaining = %s", bufferRemaining);
      int toWrite = Math.toIntExact(Math.min(channelRemaining, bufferRemaining));
      LOGGER.atInfo().log("toWrite = %s", toWrite);
      int written = 0;
      for (int i = 0; i < toWrite; i++) {
        byte base64Byte = base64Byte(position + i);
        dst.put(base64Byte);
        written++;
      }
      long newPosition = positionBegin + written;
      LOGGER.atInfo().log("newPosition = %s", newPosition);
      position = newPosition;
      return written;
    }

    @Override
    public boolean isOpen() {
      return open;
    }

    @Override
    public void close() throws IOException {
      open = false;
    }
  }
}
