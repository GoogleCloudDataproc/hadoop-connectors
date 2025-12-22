package com.google.cloud.hadoop.util;

import com.google.common.hash.Hasher;

public class ChecksumContext {
  // Stores the Hasher for the current upload thread
  public static final ThreadLocal<Hasher> CURRENT_HASHER = new ThreadLocal<>();
}
