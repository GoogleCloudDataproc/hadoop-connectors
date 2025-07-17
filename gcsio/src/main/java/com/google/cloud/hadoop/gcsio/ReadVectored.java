package com.google.cloud.hadoop.gcsio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.IntFunction;

public interface ReadVectored {
  void readVectored(List<VectoredIORange> ranges, IntFunction<ByteBuffer> allocate)
      throws IOException;
}
