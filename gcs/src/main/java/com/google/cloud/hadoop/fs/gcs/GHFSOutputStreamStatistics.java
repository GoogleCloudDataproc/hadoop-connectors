package com.google.cloud.hadoop.fs.gcs;


import java.io.Closeable;

public interface GHFSOutputStreamStatistics extends Closeable, GHFSStatisticInterface {
    /**
     * Record bytes written.
     * @param count number of bytes
     */
    void writeBytes(long count);

    /**
     * Get the current count of bytes written.
     * @return the counter value.
     */
    long getBytesWritten();

    /**
     * Get the value of a counter.
     * @param name counter name
     * @return the value or null if no matching counter was found.
     */
    Long lookupCounterValue(String name);

    /**
     * Get the value of a gauge.
     * @param name gauge name
     * @return the value or null if no matching gauge was found.
     */
    Long lookupGaugeValue(String name);

    /**
     * Syncable.hflush() has been invoked.
     */
    void hflushInvoked();

    /**
     * Syncable.hsync() has been invoked.
     */
    void hsyncInvoked();
}
