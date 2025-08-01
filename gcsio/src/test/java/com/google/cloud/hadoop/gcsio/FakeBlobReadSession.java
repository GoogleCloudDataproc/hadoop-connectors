package com.google.cloud.hadoop.gcsio;

import static com.google.common.truth.Truth.assertThat;

import com.google.api.core.ApiFutures;
import com.google.cloud.storage.*;
import com.google.cloud.storage.ZeroCopySupport.DisposableByteString;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;

public class FakeBlobReadSession implements BlobReadSession {

  public static final String TEST_STRING =
      "Lorem ipsum dolor sit amet. Qui esse voluptatum qui tempora quia quo maiores galisum. Et officia cum";

  public static final int TEST_STRING_SIZE = TEST_STRING.length();
  // The following are substring used in Test cases following the format SUBSTRING_{Offset}_{Length}
  public static final String SUBSTRING_20_10 = TEST_STRING.substring(20, 30);
  public static final String SUBSTRING_50_7 = TEST_STRING.substring(50, 57);
  public static final String SUBSTRING_65_17 = TEST_STRING.substring(65, 82);

  @Override
  public BlobInfo getBlobInfo() {
    return null;
  }

  @Override
  public <Projection> Projection readAs(ReadProjectionConfig<Projection> readProjectionConfig) {
    if (readProjectionConfig instanceof ReadAsSeekableChannel) {
      return (Projection) createFakeSeekableByteChannel();
    } else {
      assertThat(readProjectionConfig).isInstanceOf(ReadAsFutureByteString.class);
      RangeSpec range = ((ReadAsFutureByteString) readProjectionConfig).getRange();
      return (Projection)
          ApiFutures.immediateFuture(
              new DisposableByteString() {
                @Override
                public ByteString byteString() {
                  return ByteString.copyFrom(getSubString(range).getBytes(StandardCharsets.UTF_8));
                }

                @Override
                public void close() throws IOException {}
              });
    }
  }

  private SeekableByteChannel createFakeSeekableByteChannel() {
    return new SeekableByteChannel() {
      private final byte[] data = TEST_STRING.getBytes(StandardCharsets.UTF_8);
      private int position = 0;
      private boolean open = true;

      @Override
      public int read(ByteBuffer dst) throws IOException {
        if (!open) {
          throw new ClosedChannelException();
        }
        if (position >= data.length) {
          return -1; // EOF
        }
        int bytesToRead = Math.min(dst.remaining(), data.length - position);
        dst.put(data, position, bytesToRead);
        position += bytesToRead;
        return bytesToRead;
      }

      @Override
      public long position() throws IOException {
        if (!open) {
          throw new ClosedChannelException();
        }
        return position;
      }

      @Override
      public SeekableByteChannel position(long newPosition) throws IOException {
        if (!open) {
          throw new ClosedChannelException();
        }
        this.position = (int) newPosition;
        return this;
      }

      @Override
      public long size() throws IOException {
        if (!open) {
          throw new ClosedChannelException();
        }
        return data.length;
      }

      @Override
      public boolean isOpen() {
        return open;
      }

      @Override
      public void close() {
        open = false;
      }

      @Override
      public int write(ByteBuffer src) {
        throw new NonWritableChannelException();
      }

      @Override
      public SeekableByteChannel truncate(long size) {
        throw new NonWritableChannelException();
      }
    };
  }

  private String getSubString(RangeSpec range) {
    return TEST_STRING.substring(
        Math.toIntExact(range.begin()),
        Math.toIntExact(range.begin() + range.maxLength().getAsLong()));
  }

  @Override
  public void close() throws IOException {}
}
