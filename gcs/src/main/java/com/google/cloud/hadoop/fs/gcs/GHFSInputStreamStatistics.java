package com.google.cloud.hadoop.fs.gcs;

import org.apache.hadoop.fs.statistics.DurationTracker;

/**
 * Statistics updated by a
 * {@link com.google.cloud.hadoop.fs.gcs.GoogleHadoopFSInputStream} during its use.
 * It also contains getters for tests.
 */

public interface GHFSInputStreamStatistics extends AutoCloseable, GHFSStatisticInterface{
    /**
     * Seek backwards, incrementing the seek and backward seek counters.
     * @param negativeOffset how far was the seek?
     * This is expected to be negative.
     */
    void seekBackwards(long negativeOffset);
    /**
     * Record a forward seek, adding a seek operation, a forward
     * seek operation, and any bytes skipped.
     * @param skipped bytes moved forward in stream
     * @param bytesReadInSeek number of bytes skipped by reading from the stream.
     * If the seek was implemented by a close + reopen, set this to zero.
     */
    void seekForwards(long skipped, long bytesReadInSeek);
    /**
     * The inner stream was opened.
     * The return value is used in the input stream to decide whether it is
     * the initial vs later count.
     * @return the previous count or zero if this is the first opening.
     */
    long streamOpened();
    /**
     * The inner stream was closed.
     * @param abortedConnection flag to indicate the stream was aborted,
     * rather than closed cleanly
     * @param remainingInCurrentRequest the number of bytes remaining in
     * the current request.
     */
    void streamClose(boolean abortedConnection,
                     long remainingInCurrentRequest);

    /**
     * An ignored stream read exception was received.
     */
    void readException();
    /**
     * Increment the bytes read counter by the number of bytes;
     * no-op if the argument is negative.
     * @param bytes number of bytes read
     */
    void bytesRead(long bytes);

    /**
     * A {@code read(byte[] buf, int off, int len)} operation has started.
     * @param pos starting position of the read
     * @param len length of bytes to read
     */
    void readOperationStarted(long pos, long len);

    /**
     * A {@code PositionedRead.read(position, buffer, offset, length)}
     * operation has just started.
     * @param pos starting position of the read
     * @param len length of bytes to read
     */
    void readFullyOperationStarted(long pos, long len);

    /**
     * A read operation has completed.
     * @param requested number of requested bytes
     * @param actual the actual number of bytes
     */
    void readOperationCompleted(int requested, int actual);

    @Override
    void close();

    /**
     * The input policy has been switched.
     * @param updatedPolicy enum value of new policy.
     */
    void inputPolicySet(int updatedPolicy);

    /**
     * Get a reference to the change tracker statistics for this
     * stream.
     * @return a reference to the change tracker statistics
     */


    //ChangeTrackerStatistics getChangeTrackerStatistics();

    /**
     * A stream {@code unbuffer()} call has been made.
     */
    void unbuffered();

    long getCloseOperations();

    long getClosed();

    long getAborted();

    long getForwardSeekOperations();

    long getBackwardSeekOperations();

    /**
     * The bytes read in read() operations.
     * @return the number of bytes returned to the caller.
     */
    long getBytesRead();

    /**
     * The total number of bytes read, including
     * all read and discarded when closing streams
     * or skipped during seek calls.
     * @return the total number of bytes read from
     * S3.
     */
    long getTotalBytesRead();

    long getBytesSkippedOnSeek();

    long getBytesBackwardsOnSeek();

    long getBytesReadInClose();

    long getBytesDiscardedInAbort();

    long getOpenOperations();

    long getSeekOperations();

    long getReadExceptions();

    long getReadOperations();

    long getReadFullyOperations();

    long getReadsIncomplete();

    long getPolicySetCount();

   // long getVersionMismatches();

    long getInputPolicy();

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
     * Initiate a GET request.
     * @return duration tracker;
     */
    DurationTracker initiateGetRequest();

}
