package com.google.cloud.hadoop.gcsio;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class BlockCache {
  private final Map<String, RangeMap<Long, byte[]>> blocks = new ConcurrentHashMap<>();

  public Map.Entry<Range<Long>, byte[]> get(String resourceId, long position) {
    RangeMap<Long, byte[]> resourceBlocks = blocks.get(resourceId);
    if (resourceBlocks == null) {
      return null;
    }
    synchronized (resourceBlocks) {
      return resourceBlocks.getEntry(position);
    }
  }

  public RangeMap<Long, byte[]> get(String resourceId, Long start, Long end) {
    RangeMap<Long, byte[]> resourceBlocks = blocks.get(resourceId);
    if (resourceBlocks == null) {
      return ImmutableRangeMap.of();
    }
    synchronized (resourceBlocks) {
      return resourceBlocks.subRangeMap(Range.closed(start, end));
    }
  }

  public void put(String resourceId, Long start, Long end, byte[] data) {
    checkNotNull(start, "block start could not be null");
    checkNotNull(end, "block end could not be null");
    checkNotNull(data, "block data could not be null");
    checkArgument(
        data.length == end - start + 1,
        "block data length (%s) not equal to range ([%s, %s])", data.length, start, end);

    RangeMap<Long, byte[]> resourceBlocks =
        blocks.computeIfAbsent(resourceId, k -> TreeRangeMap.create());
    synchronized (resourceBlocks) {
      resourceBlocks.put(Range.closed(start, end), data);
    }
  }
}
