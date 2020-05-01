package com.google.cloud.hadoop.gcsio;

import com.google.auto.value.AutoValue;
import java.time.Duration;

@AutoValue
public abstract class SyncableOutputStreamOptions {
  public static final SyncableOutputStreamOptions DEFAULT = builder().build();

  public static Builder builder() {
    return new AutoValue_SyncableOutputStreamOptions.Builder()
        .setAppendEnabled(false)
        .setSyncOnFlushEnabled(false)
        .setMinSyncInterval(Duration.ZERO);
  }

  public abstract Builder toBuilder();

  /** See {@link Builder#setAppendEnabled} * */
  public abstract boolean isAppendEnabled();

  /** See {@link Builder#setSyncOnFlushEnabled} * */
  public abstract boolean isSyncOnFlushEnabled();

  /** See {@link Builder#setMinSyncInterval} * */
  public abstract Duration getMinSyncInterval();

  /** Mutable builder for the SyncableOutputStreamOptions. */
  @AutoValue.Builder
  public abstract static class Builder {

    /** Whether the syncable output stream will append to existing files. */
    public abstract Builder setAppendEnabled(boolean appendEnabled);

    /** Whether to implement flush using the sync implementation. */
    public abstract Builder setSyncOnFlushEnabled(boolean syncOnFlushEnabled);

    /** The minimal interval between two consecutive sync()/hsync()/hflush() calls. */
    public abstract Builder setMinSyncInterval(Duration minSyncInterval);

    public abstract SyncableOutputStreamOptions build();
  }
}
