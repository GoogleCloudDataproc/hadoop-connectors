package com.google.cloud.hadoop.gcsio.atomic;

import com.google.common.base.MoreObjects;
import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;

public class OperationLocks {
  public static final long FORMAT_VERSION = 1;

  private long formatVersion = -1;
  private Set<Operation> operations =
      new TreeSet<>(Comparator.comparing(Operation::getOperationId));

  public long getFormatVersion() {
    return formatVersion;
  }

  public OperationLocks setFormatVersion(long formatVersion) {
    this.formatVersion = formatVersion;
    return this;
  }

  public Set<Operation> getOperations() {
    return operations;
  }

  public OperationLocks setOperations(Set<Operation> operations) {
    this.operations = new TreeSet<>(Comparator.comparing(Operation::getOperationId));
    this.operations.addAll(operations);
    return this;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("formatVersion", formatVersion)
        .add("operations", operations)
        .toString();
  }
}
