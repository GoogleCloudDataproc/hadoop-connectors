package com.google.cloud.hadoop.fs.gcs;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceStability;
import com.google.cloud.hadoop.fs.gcs.GHFSStatisticTypeEnum;
import org.apache.hadoop.fs.statistics.StoreStatisticNames;
import org.apache.hadoop.fs.statistics.StreamStatisticNames;

import static com.google.cloud.hadoop.fs.gcs.GHFSStatisticTypeEnum.TYPE_COUNTER;
import static com.google.cloud.hadoop.fs.gcs.GHFSStatisticTypeEnum.TYPE_DURATION;
import static com.google.cloud.hadoop.fs.gcs.GHFSStatisticTypeEnum.TYPE_GAUGE;
import static com.google.cloud.hadoop.fs.gcs.GHFSStatisticTypeEnum.TYPE_QUANTILE;


@InterfaceStability.Unstable
public enum GHFSStatistic {
    /* Low-level duration counters */
    ACTION_EXECUTOR_ACQUIRED(
            StoreStatisticNames.ACTION_EXECUTOR_ACQUIRED,
            "Executor acquired.",
            TYPE_DURATION),
    ACTION_HTTP_HEAD_REQUEST(
            StoreStatisticNames.ACTION_HTTP_HEAD_REQUEST,
            "HEAD request.",
            TYPE_DURATION),
    ACTION_HTTP_GET_REQUEST(
            StoreStatisticNames.ACTION_HTTP_GET_REQUEST,
            "GET request.",
            TYPE_DURATION),

    /* FileSystem Level statistics */
    DIRECTORIES_CREATED("directories_created",
            "Total number of directories created through the object store.",
            TYPE_COUNTER),
    DIRECTORIES_DELETED("directories_deleted",
            "Total number of directories deleted through the object store.",
            TYPE_COUNTER),
    FILES_COPIED("files_copied",
            "Total number of files copied within the object store.",
            TYPE_COUNTER),
    FILES_COPIED_BYTES("files_copied_bytes",
            "Total number of bytes copied within the object store.",
            TYPE_COUNTER),
    FILES_CREATED("files_created",
            "Total number of files created through the object store.",
            TYPE_COUNTER),
    FILES_DELETED("files_deleted",
            "Total number of files deleted from the object store.",
            TYPE_COUNTER),
    FILES_DELETE_REJECTED("files_delete_rejected",
            "Total number of files whose delete request was rejected",
            TYPE_COUNTER),
    FAKE_DIRECTORIES_CREATED("fake_directories_created",
            "Total number of fake directory entries created in the object store.",
            TYPE_COUNTER),
    FAKE_DIRECTORIES_DELETED("fake_directories_deleted",
            "Total number of fake directory deletes submitted to object store.",
            TYPE_COUNTER),
    IGNORED_ERRORS("ignored_errors", "Errors caught and ignored",
            TYPE_COUNTER),

    INVOCATION_ABORT(
            StoreStatisticNames.OP_ABORT,
            "Calls of abort()",
            TYPE_DURATION),
    INVOCATION_COPY_FROM_LOCAL_FILE(
            StoreStatisticNames.OP_COPY_FROM_LOCAL_FILE,
            "Calls of copyFromLocalFile()",
            TYPE_COUNTER),
    INVOCATION_CREATE(
            StoreStatisticNames.OP_CREATE,
            "Calls of create()",
            TYPE_COUNTER),
    INVOCATION_CREATE_NON_RECURSIVE(
            StoreStatisticNames.OP_CREATE_NON_RECURSIVE,
            "Calls of createNonRecursive()",
            TYPE_COUNTER),
    INVOCATION_DELETE(
            StoreStatisticNames.OP_DELETE,
            "Calls of delete()",
            TYPE_COUNTER),
    INVOCATION_EXISTS(
            StoreStatisticNames.OP_EXISTS,
            "Calls of exists()",
            TYPE_COUNTER),
    INVOCATION_GET_DELEGATION_TOKEN(
            StoreStatisticNames.OP_GET_DELEGATION_TOKEN,
            "Calls of getDelegationToken()",
            TYPE_COUNTER),
    INVOCATION_GET_FILE_CHECKSUM(
            StoreStatisticNames.OP_GET_FILE_CHECKSUM,
            "Calls of getFileChecksum()",
            TYPE_COUNTER),
    INVOCATION_GET_FILE_STATUS(
            StoreStatisticNames.OP_GET_FILE_STATUS,
            "Calls of getFileStatus()",
            TYPE_COUNTER),
    INVOCATION_GLOB_STATUS(
            StoreStatisticNames.OP_GLOB_STATUS,
            "Calls of globStatus()",
            TYPE_COUNTER),
    INVOCATION_IS_DIRECTORY(
            StoreStatisticNames.OP_IS_DIRECTORY,
            "Calls of isDirectory()",
            TYPE_COUNTER),
    INVOCATION_IS_FILE(
            StoreStatisticNames.OP_IS_FILE,
            "Calls of isFile()",
            TYPE_COUNTER),
    INVOCATION_HFLUSH(
            StoreStatisticNames.OP_HFLUSH,
            "Calls of hflush()",
            TYPE_COUNTER),
    INVOCATION_HSYNC(
            StoreStatisticNames.OP_HSYNC,
            "Calls of hsync()",
            TYPE_COUNTER),
    INVOCATION_LIST_FILES(
            StoreStatisticNames.OP_LIST_FILES,
            "Calls of listFiles()",
            TYPE_COUNTER),
    INVOCATION_LIST_LOCATED_STATUS(
            StoreStatisticNames.OP_LIST_LOCATED_STATUS,
            "Calls of listLocatedStatus()",
            TYPE_COUNTER),
    INVOCATION_LIST_STATUS(
            StoreStatisticNames.OP_LIST_STATUS,
            "Calls of listStatus()",
            TYPE_COUNTER),
    INVOCATION_MKDIRS(
            StoreStatisticNames.OP_MKDIRS,
            "Calls of mkdirs()",
            TYPE_COUNTER),
    INVOCATION_OPEN(
            StoreStatisticNames.OP_OPEN,
            "Calls of open()",
            TYPE_COUNTER),
    INVOCATION_RENAME(
            StoreStatisticNames.OP_RENAME,
            "Calls of rename()",
            TYPE_COUNTER),

    /* The XAttr API metrics are all durations */
    INVOCATION_XATTR_GET_MAP(
            StoreStatisticNames.OP_XATTR_GET_MAP,
            "Calls of getXAttrs(Path path)",
            TYPE_DURATION),
    INVOCATION_XATTR_GET_NAMED(
            StoreStatisticNames.OP_XATTR_GET_NAMED,
            "Calls of getXAttr(Path, String)",
            TYPE_DURATION),
    INVOCATION_XATTR_GET_NAMED_MAP(
            StoreStatisticNames.OP_XATTR_GET_NAMED_MAP,
            "Calls of xattr()",
            TYPE_DURATION),
    INVOCATION_OP_XATTR_LIST(
            StoreStatisticNames.OP_XATTR_LIST,
            "Calls of getXAttrs(Path path, List<String> names)",
            TYPE_DURATION),

    /* Object IO */
    OBJECT_COPY_REQUESTS(StoreStatisticNames.OBJECT_COPY_REQUESTS,
            "Object copy requests",
            TYPE_COUNTER),
    OBJECT_DELETE_REQUEST(StoreStatisticNames.OBJECT_DELETE_REQUEST,
            "Object delete requests",
            TYPE_DURATION),
    OBJECT_BULK_DELETE_REQUEST(StoreStatisticNames.OBJECT_BULK_DELETE_REQUEST,
            "Object bulk delete requests",
            TYPE_DURATION),
    OBJECT_DELETE_OBJECTS(StoreStatisticNames.OBJECT_DELETE_OBJECTS,
            "Objects deleted in delete requests",
            TYPE_COUNTER),
    OBJECT_LIST_REQUEST(StoreStatisticNames.OBJECT_LIST_REQUEST,
            "Count of object listings made",
            TYPE_DURATION),
    OBJECT_CONTINUE_LIST_REQUESTS(
            StoreStatisticNames.OBJECT_CONTINUE_LIST_REQUEST,
            "Count of continued object listings made",
            TYPE_DURATION),
    OBJECT_METADATA_REQUESTS(
            StoreStatisticNames.OBJECT_METADATA_REQUESTS,
            "Count of requests for object metadata",
            TYPE_COUNTER),
    OBJECT_MULTIPART_UPLOAD_INITIATED(
            StoreStatisticNames.OBJECT_MULTIPART_UPLOAD_INITIATED,
            "Object multipart upload initiated",
            TYPE_COUNTER),
    OBJECT_MULTIPART_UPLOAD_ABORTED(
            StoreStatisticNames.OBJECT_MULTIPART_UPLOAD_ABORTED,
            "Object multipart upload aborted",
            TYPE_COUNTER),
    OBJECT_PUT_REQUESTS(
            StoreStatisticNames.OBJECT_PUT_REQUEST,
            "Object put/multipart upload count",
            TYPE_COUNTER),
    OBJECT_PUT_REQUESTS_COMPLETED(
            StoreStatisticNames.OBJECT_PUT_REQUEST_COMPLETED,
            "Object put/multipart upload completed count",
            TYPE_COUNTER),
    OBJECT_PUT_REQUESTS_ACTIVE(
            StoreStatisticNames.OBJECT_PUT_REQUEST_ACTIVE,
            "Current number of active put requests",
            TYPE_GAUGE),
    OBJECT_PUT_BYTES(
            StoreStatisticNames.OBJECT_PUT_BYTES,
            "number of bytes uploaded",
            TYPE_COUNTER),
    OBJECT_PUT_BYTES_PENDING(
            StoreStatisticNames.OBJECT_PUT_BYTES_PENDING,
            "number of bytes queued for upload/being actively uploaded",
            TYPE_GAUGE),
    OBJECT_SELECT_REQUESTS(
            StoreStatisticNames.OBJECT_SELECT_REQUESTS,
            "Count of gcs Select requests issued",
            TYPE_COUNTER),
    STREAM_READ_ABORTED(
            StreamStatisticNames.STREAM_READ_ABORTED,
            "Count of times the TCP stream was aborted",
            TYPE_COUNTER),

    /* Stream Reads */
    STREAM_READ_BYTES(
            StreamStatisticNames.STREAM_READ_BYTES,
            "Bytes read from an input stream in read() calls",
            TYPE_COUNTER),
    STREAM_READ_BYTES_DISCARDED_ABORT(
            StreamStatisticNames.STREAM_READ_BYTES_DISCARDED_ABORT,
            "Count of bytes discarded by aborting an input stream",
            TYPE_COUNTER),
    STREAM_READ_BYTES_READ_CLOSE(
            StreamStatisticNames.STREAM_READ_BYTES_DISCARDED_CLOSE,
            "Count of bytes read and discarded when closing an input stream",
            TYPE_COUNTER),
    STREAM_READ_CLOSED(
            StreamStatisticNames.STREAM_READ_CLOSED,
            "Count of times the TCP stream was closed",
            TYPE_COUNTER),
    STREAM_READ_CLOSE_OPERATIONS(
            StreamStatisticNames.STREAM_READ_CLOSE_OPERATIONS,
            "Total count of times an attempt to close an input stream was made",
            TYPE_COUNTER),
    STREAM_READ_EXCEPTIONS(
            StreamStatisticNames.STREAM_READ_EXCEPTIONS,
            "Count of exceptions raised during input stream reads",
            TYPE_COUNTER),
    STREAM_READ_FULLY_OPERATIONS(
            StreamStatisticNames.STREAM_READ_FULLY_OPERATIONS,
            "Count of readFully() operations in an input stream",
            TYPE_COUNTER),
    STREAM_READ_OPENED(
            StreamStatisticNames.STREAM_READ_OPENED,
            "Total count of times an input stream to object store data was opened",
            TYPE_COUNTER),
    STREAM_READ_OPERATIONS(
            StreamStatisticNames.STREAM_READ_OPERATIONS,
            "Count of read() operations in an input stream",
            TYPE_COUNTER),
    STREAM_READ_OPERATIONS_INCOMPLETE(
            StreamStatisticNames.STREAM_READ_OPERATIONS_INCOMPLETE,
            "Count of incomplete read() operations in an input stream",
            TYPE_COUNTER),
    STREAM_READ_VERSION_MISMATCHES(
            StreamStatisticNames.STREAM_READ_VERSION_MISMATCHES,
            "Count of version mismatches encountered while reading an input stream",
            TYPE_COUNTER),
    STREAM_READ_SEEK_BACKWARD_OPERATIONS(
            StreamStatisticNames.STREAM_READ_SEEK_BACKWARD_OPERATIONS,
            "Count of executed seek operations which went backwards in a stream",
            TYPE_COUNTER),
    STREAM_READ_SEEK_BYTES_BACKWARDS(
            StreamStatisticNames.STREAM_READ_SEEK_BYTES_BACKWARDS,
            "Count of bytes moved backwards during seek operations"
                    + " in an input stream",
            TYPE_COUNTER),
    STREAM_READ_SEEK_BYTES_DISCARDED(
            StreamStatisticNames.STREAM_READ_SEEK_BYTES_DISCARDED,
            "Count of bytes read and discarded during seek() in an input stream",
            TYPE_COUNTER),
    STREAM_READ_SEEK_BYTES_SKIPPED(
            StreamStatisticNames.STREAM_READ_SEEK_BYTES_SKIPPED,
            "Count of bytes skipped during forward seek operations"
                    + " an input stream",
            TYPE_COUNTER),
    STREAM_READ_SEEK_FORWARD_OPERATIONS(
            StreamStatisticNames.STREAM_READ_SEEK_FORWARD_OPERATIONS,
            "Count of executed seek operations which went forward in"
                    + " an input stream",
            TYPE_COUNTER),
    STREAM_READ_SEEK_OPERATIONS(
            StreamStatisticNames.STREAM_READ_SEEK_OPERATIONS,
            "Count of seek operations in an input stream",
            TYPE_COUNTER),
    STREAM_READ_SEEK_POLICY_CHANGED(
            StreamStatisticNames.STREAM_READ_SEEK_POLICY_CHANGED,
            "Count of times the seek policy was dynamically changed"
                    + " in an input stream",
            TYPE_COUNTER),
    STREAM_READ_TOTAL_BYTES(
            StreamStatisticNames.STREAM_READ_TOTAL_BYTES,
            "Total count of bytes read from an input stream",
            TYPE_COUNTER),

    /* Stream Write statistics */

    STREAM_WRITE_EXCEPTIONS(
            StreamStatisticNames.STREAM_WRITE_EXCEPTIONS,
            "Count of stream write failures reported",
            TYPE_COUNTER),
    STREAM_WRITE_EXCEPTIONS_COMPLETING_UPLOADS(
            StreamStatisticNames.STREAM_WRITE_EXCEPTIONS_COMPLETING_UPLOADS,
            "Count of failures when finalizing a multipart upload",
            TYPE_COUNTER),
    STREAM_WRITE_BLOCK_UPLOADS(
            StreamStatisticNames.STREAM_WRITE_BLOCK_UPLOADS,
            "Count of block/partition uploads completed",
            TYPE_COUNTER),
    STREAM_WRITE_BLOCK_UPLOADS_ACTIVE(
            StreamStatisticNames.STREAM_WRITE_BLOCK_UPLOADS_ACTIVE,
            "Count of block/partition uploads active",
            TYPE_GAUGE),
    STREAM_WRITE_BLOCK_UPLOADS_COMMITTED(
            StreamStatisticNames.STREAM_WRITE_BLOCK_UPLOADS_COMMITTED,
            "Count of number of block uploads committed",
            TYPE_COUNTER),
    STREAM_WRITE_BLOCK_UPLOADS_ABORTED(
            StreamStatisticNames.STREAM_WRITE_BLOCK_UPLOADS_ABORTED,
            "Count of number of block uploads aborted",
            TYPE_COUNTER),

    STREAM_WRITE_BLOCK_UPLOADS_PENDING(
            StreamStatisticNames.STREAM_WRITE_BLOCK_UPLOADS_PENDING,
            "Gauge of block/partitions uploads queued to be written",
            TYPE_GAUGE),
    STREAM_WRITE_BLOCK_UPLOADS_BYTES_PENDING(
            StreamStatisticNames.STREAM_WRITE_BLOCK_UPLOADS_BYTES_PENDING,
            "Gauge of data queued to be written",
            TYPE_GAUGE),
    STREAM_WRITE_TOTAL_TIME(
            StreamStatisticNames.STREAM_WRITE_TOTAL_TIME,
            "Count of total time taken for uploads to complete",
            TYPE_COUNTER),
    STREAM_WRITE_TOTAL_DATA(StreamStatisticNames.STREAM_WRITE_TOTAL_DATA,
            "Count of total data uploaded",
            TYPE_COUNTER),
    STREAM_WRITE_BYTES(
            StreamStatisticNames.STREAM_WRITE_BYTES,
            "Count of bytes written to output stream"
                    + " (including all not yet uploaded)",
            TYPE_COUNTER),
    STREAM_WRITE_QUEUE_DURATION(
            StreamStatisticNames.STREAM_WRITE_QUEUE_DURATION,
            "Total queue duration of all block uploads",
            TYPE_DURATION),

    


    /* General Store operations */
    STORE_IO_REQUEST(StoreStatisticNames.STORE_IO_REQUEST,
            "requests made of the remote store",
            TYPE_COUNTER),

    STORE_IO_RETRY(StoreStatisticNames.STORE_IO_RETRY,
            "retried requests made of the remote store",
            TYPE_COUNTER),

    STORE_IO_THROTTLED(
            StoreStatisticNames.STORE_IO_THROTTLED,
            "Requests throttled and retried",
            TYPE_COUNTER),
    STORE_IO_THROTTLE_RATE(
            StoreStatisticNames.STORE_IO_THROTTLE_RATE,
            "Rate of gcs request throttling",
            TYPE_QUANTILE),

    /*
     * Delegation Token Operations.
     */
    DELEGATION_TOKENS_ISSUED(
            StoreStatisticNames.DELEGATION_TOKENS_ISSUED,
            "Count of delegation tokens issued",
            TYPE_DURATION),

    /* Multipart Upload API */

    MULTIPART_UPLOAD_INSTANTIATED(
            StoreStatisticNames.MULTIPART_UPLOAD_INSTANTIATED,
            "Multipart Uploader Instantiated",
            TYPE_COUNTER),
    MULTIPART_UPLOAD_PART_PUT(
            StoreStatisticNames.MULTIPART_UPLOAD_PART_PUT,
            "Multipart Part Put Operation",
            TYPE_COUNTER),
    MULTIPART_UPLOAD_PART_PUT_BYTES(
            StoreStatisticNames.MULTIPART_UPLOAD_PART_PUT_BYTES,
            "Multipart Part Put Bytes",
            TYPE_COUNTER),
    MULTIPART_UPLOAD_ABORTED(
            StoreStatisticNames.MULTIPART_UPLOAD_ABORTED,
            "Multipart Upload Aborted",
            TYPE_COUNTER),
    MULTIPART_UPLOAD_ABORT_UNDER_PATH_INVOKED(
            StoreStatisticNames.MULTIPART_UPLOAD_ABORT_UNDER_PATH_INVOKED,
            "Multipart Upload Abort Unner Path Invoked",
            TYPE_COUNTER),
    MULTIPART_UPLOAD_COMPLETED(
            StoreStatisticNames.MULTIPART_UPLOAD_COMPLETED,
            "Multipart Upload Completed",
            TYPE_COUNTER),
    MULTIPART_UPLOAD_STARTED(
            StoreStatisticNames.MULTIPART_UPLOAD_STARTED,
            "Multipart Upload Started",
            TYPE_COUNTER);


    /**
     * A map used to support the {@link #fromSymbol(String)} call.
     */
    private static final Map<String, GHFSStatistic> SYMBOL_MAP =
            new HashMap<>(GHFSStatistic.values().length);
    static {
        for (GHFSStatistic stat : values()) {
            SYMBOL_MAP.put(stat.getSymbol(), stat);
        }
    }


    /**
     * Statistic definition.
     * @param symbol name
     * @param description description.
     * @param type type
     */
    GHFSStatistic(String symbol, String description, GHFSStatisticTypeEnum type) {
        this.symbol = symbol;
        this.description = description;
        this.type = type;
    }

    /** Statistic name. */
    private final String symbol;

    /** Statistic description. */
    private final String description;

    /** Statistic type. */
    private final GHFSStatisticTypeEnum type;

    public String getSymbol() {
        return symbol;
    }

    /**
     * Get a statistic from a symbol.
     * @param symbol statistic to look up
     * @return the value or null.
     */
    public static GHFSStatistic fromSymbol(String symbol) {
        return SYMBOL_MAP.get(symbol);
    }

    public String getDescription() {
        return description;
    }

    /**
     * The string value is simply the symbol.
     * This makes this operation very low cost.
     * @return the symbol of this statistic.
     */
    @Override
    public String toString() {
        return symbol;
    }

    /**
     * What type is this statistic?
     * @return the type.
     */
    public GHFSStatisticTypeEnum getType() {
        return type;
    }
}
