package com.google.cloud.hadoop.fs.gcs.auth;

public enum DelegationTokenInstantiationStrategy {
  // Token instance is created per service. Default behavior.
  INSTANCE_PER_SERVICE,
  // Token instance is shared across  instances of GCS services. Different renewers still get unique
  // token instances.
  SHARED,
}
