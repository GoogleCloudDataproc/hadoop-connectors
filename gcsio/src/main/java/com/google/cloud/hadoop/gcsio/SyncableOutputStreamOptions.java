package com.google.cloud.hadoop.gcsio;

import com.google.auto.value.AutoValue;

@AutoValue
public abstract class SyncableOutputStreamOptions {
  public static final SyncableOutputStreamOptions DEFAULT = builder().build();

  public static Builder builder() {
    return new AutoValue_SyncableOutputStreamOptions.Builder()
        .setAppendEnabled(false)
        .setSyncOnFlushEnabled(false)
        .setMinSyncTimeIntervalMs(0);
  }

  public abstract Builder toBuilder();

  /** See {@link Builder#setAppendEnabled} * */
  public abstract boolean getAppendEnabled();

  /** See {@link Builder#setSyncOnFlushEnabled} * */
  public abstract boolean getSyncOnFlushEnabled();

  /** See {@link Builder#setMinSyncTimeIntervalMs} * */
  public abstract int getMinSyncTimeIntervalMs();

  /** Mutable builder for the SyncableOutputStreamOptions. */
  @AutoValue.Builder
  public abstract static class Builder {

    /** Whether the syncable output stream will append to existing files. */
    public abstract Builder setAppendEnabled(boolean appendEnabled);

    /** Whether to implement flush using the sync implementation. */
    public abstract Builder setSyncOnFlushEnabled(boolean syncOnFlushEnabled);

    /**
     * The minimal time interval (in milliseconds) between two consecutive sync()/hsync()/hflush()
     * calls.
     */
    public abstract Builder setMinSyncTimeIntervalMs(int minSyncTimeIntervalMs);

    abstract SyncableOutputStreamOptions autoBuild();

    public SyncableOutputStreamOptions build() {
      SyncableOutputStreamOptions options = autoBuild();
      return options;
    }
  }
}
