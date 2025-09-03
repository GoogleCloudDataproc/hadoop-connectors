/*
 * Copyright 2025 Google LLC
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

package com.google.cloud.hadoop.gcsio;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

/**
 * A {@link java.nio.channels.WritableByteChannel} for writing to appendable objects, offering an
 * optional finalization step before closing.
 */
public interface FinalizableWritableByteChannel extends WritableByteChannel {
  /**
   * Finalizes the Appendable object and then closes the channel.
   *
   * <p>This method should be called to explicitly finalize the appendable object before closing the
   * channel Use {@link #close()} method instead for the default behaviour of the channel. The
   * default behaviour is closing without finalizing unless modified by setting the
   * fs.gs.bidi.finalize.on.close flag to true.
   *
   * @throws IOException if the finalization or close operation fails.
   */
  void finalizeAndClose() throws IOException;
}
