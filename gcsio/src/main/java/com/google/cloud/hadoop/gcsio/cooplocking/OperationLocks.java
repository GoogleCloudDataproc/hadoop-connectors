package com.google.cloud.hadoop.gcsio.cooplocking;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.base.MoreObjects;
import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;

public class OperationLocks {
  /** Supported version of operation locks */
  public static final long FORMAT_VERSION = 1;

  private long formatVersion = -1;
  private Set<OperationLock> locks =
      new TreeSet<>(Comparator.comparing(OperationLock::getOperationId));

  public long getFormatVersion() {
    return formatVersion;
  }

  public OperationLocks setFormatVersion(long formatVersion) {
    this.formatVersion = formatVersion;
    return this;
  }

  public Set<OperationLock> getLocks() {
    checkState(
        FORMAT_VERSION == formatVersion,
        "%s operation lock version is not supported, supported version is %s",
        formatVersion, FORMAT_VERSION);
    return locks;
  }

  public OperationLocks setLocks(Set<OperationLock> locks) {
    this.locks = new TreeSet<>(Comparator.comparing(OperationLock::getOperationId));
    this.locks.addAll(locks);
    return this;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("formatVersion", formatVersion)
        .add("operations", locks)
        .toString();
  }
}
