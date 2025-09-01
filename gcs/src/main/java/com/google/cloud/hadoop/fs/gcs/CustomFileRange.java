package com.google.cloud.hadoop.fs.gcs;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.fs.FileRange;

class CustomFileRange implements FileRange {
  long offset;
  int length;

  CompletableFuture<ByteBuffer> data;

  CustomFileRange(long offset, int length) {
    this.offset = offset;
    this.length = length;
  }

  @Override
  public long getOffset() {
    return offset;
  }

  @Override
  public int getLength() {
    return length;
  }

  @Override
  public CompletableFuture<ByteBuffer> getData() {
    return data;
  }

  @Override
  public void setData(CompletableFuture<ByteBuffer> completableFuture) {
    data = completableFuture;
  }

  @Override
  public Object getReference() {
    return null;
  }
}
