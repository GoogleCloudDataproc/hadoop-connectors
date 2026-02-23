/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.util;

import java.util.function.Supplier;

public class ChecksumContext {
  // Stores a Supplier that provides the final checksum string for the current upload thread
  private static final ThreadLocal<Supplier<String>> CHECKSUM_SUPPLIER = new ThreadLocal<>();

  public static void setChecksumSupplier(Supplier<String> supplier) {
    CHECKSUM_SUPPLIER.set(supplier);
  }

  public static Supplier<String> getChecksumSupplier() {
    return CHECKSUM_SUPPLIER.get();
  }

  public static void clear() {
    CHECKSUM_SUPPLIER.remove();
  }
}
