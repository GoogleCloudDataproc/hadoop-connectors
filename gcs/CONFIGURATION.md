## Configuration properties

### General configuration

*   `fs.gs.project.id` (not set by default)

    Google Cloud Project ID with access to GCS buckets. Required only for list
    buckets and create bucket operations.

*   `fs.gs.working.dir` (default: `/`)

    The directory relative `gs:` uris resolve in inside of the default bucket.

*   `fs.gs.implicit.dir.repair.enable` (default: `true`)

    Whether or not to create objects for the parent directories of objects with
    `/` in their path e.g. creating `gs://bucket/foo/` upon deleting or renaming
    `gs://bucket/foo/bar`.

*   `fs.gs.copy.with.rewrite.enable` (default: `true`)

    Whether or not to perform copy operation using Rewrite requests. Allows to
    copy files between different locations and storage classes.

*   `fs.gs.rewrite.max.bytes.per.call` (default: `536870912`)

    Maximum number of bytes rewritten in a single rewrite request when
    `fs.gs.copy.with.rewrite.enable` is set to `true`.

*   `fs.gs.reported.permissions` (default: `700`)

    Permissions that are reported for a file or directory to have regardless of
    actual Cloud Storage permissions. Can be either in octal or symbolic format
    that accepted by FsPermission class. This permission is important when the
    default file system is set to `gs` instead of `hdfs` in `yarn-site.xml`.

*   `fs.gs.delegation.token.binding` (not set by default)

    Delegation Token binding class.

*   `fs.gs.bucket.delete.enable` (default: `false`)

    If `true`, recursive delete on a path that refers to a Cloud Storage bucket
    itself or delete on that path when it is empty will result in deletion of
    the bucket itself. If `false`, any operation that normally would have
    deleted the bucket will be ignored. Setting to `false` preserves the typical
    behavior of `rm -rf /` which translates to deleting everything inside of
    root, but without clobbering the filesystem authority corresponding to that
    root path in the process.

*   `fs.gs.checksum.type` (default: `NONE`)

    Configuration of object checksum type to return; if a particular file
    doesn't support the requested type, then getFileChecksum() method will
    return `null` for that file. Supported checksum types are `NONE`, `CRC32C`
    and `MD5`

*   `fs.gs.status.parallel.enable` (default: `true`)

    If `true`, executes Cloud Storage object requests in `FileSystem`'s
    `listStatus` and `getFileStatus` methods in parallel to reduce latency.

*   `fs.gs.lazy.init.enable` (default: `false`)

    Enables lazy initialization of `GoogleHadoopFileSystem` instances.

*   `fs.gs.block.size` (default: `67108864`)

    The reported block size of the file system. This does not change any
    behavior of the connector or the underlying GCS objects. However it will
    affect the number of splits Hadoop MapReduce uses for a given input.

*   `fs.gs.create.items.conflict.check.enable` (default: `true`)

    Enables a check that ensures that conflicting directories do not exist when
    creating files and conflicting files do not exist when creating directories.

*   `fs.gs.glob.algorithm` (default: `CONCURRENT`)

    Glob search algorithm to use in Hadoop
    [FileSystem.globStatus](https://hadoop.apache.org/docs/r3.3.0/api/org/apache/hadoop/fs/FileSystem.html#globStatus-org.apache.hadoop.fs.Path-)
    method.

    Valid values:

    *   `FLAT` - fetch potential glob matches in a single list request to
        minimize calls to GCS in nested glob cases.

    *   `DEFAULT` - use default Hadoop glob search algorithm implementation.

    *   `CONCURRENT` - enables concurrent execution of flat and default glob
        search algorithms in two parallel threads to improve globbing
        performance. Whichever algorithm will finish first that result will be
        returned, and the other algorithm execution will be interrupted.

*   `fs.gs.max.requests.per.batch` (default: `15`)

    Maximum number of Cloud Storage requests that could be sent in a single
    batch request.

*   `fs.gs.batch.threads` (default: `15`)

    Maximum number of threads used to execute batch requests in parallel. Each
    thread batches at most `fs.gs.max.requests.per.batch` Cloud Storage requests
    in a single batch request. These threads are used to execute the Class A,
    Class B and Free Cloud Storage operations as copy, list, delete, etc. These
    operations are part of typical `hdfs` CLI commands such as `hdfs mv`, `hdfs
    cp`, etc.

    Depending on the number of requests the connector evenly distributes the
    number of requests across batch threads.

*   `fs.gs.list.max.items.per.call` (default: `5000`)

    Maximum number of items to return in response for list Cloud Storage
    requests.

*   `fs.gs.max.wait.for.empty.object.creation.ms` (default: `3000`)

    Maximum amount of time to wait after exception during empty object creation.

*   `fs.gs.marker.file.pattern` (not set by default)

    If set, files that match specified pattern are copied last during folder
    rename operation.

*   `fs.gs.storage.http.headers.<HEADER>=<VALUE>` (not set by default)

    Custom HTTP headers added to Cloud Storage API requests.

    Example:

    ```
    fs.gs.storage.http.headers.some-custom-header=custom_value
    fs.gs.storage.http.headers.another-custom-header=another_custom_value
    ```

### Encryption ([CSEK](https://cloud.google.com/storage/docs/encryption/customer-supplied-keys))

*   `fs.gs.encryption.algorithm` (not set by default)

    The encryption algorithm to use. For CSEK only `AES256` value is supported.

*   `fs.gs.encryption.key` (not set by default)

    An RFC 4648 Base64-encoded string of the source object's AES-256 encryption
    key.

*   `fs.gs.encryption.key.hash` (not set by default)

    An RFC 4648 Base64-encoded string of the SHA256 hash of the source object's
    encryption key.

### Authentication

When one of the following two properties is set, it will precede all other
credential settings, and credentials will be obtained from the access token
provider.

*   `fs.gs.auth.access.token.provider.impl` (not set by default)

    The implementation of the `AccessTokenProvider` interface used for GCS
    Connector.

*   `fs.gs.auth.service.account.enable` (default: `true`)

    Whether to use a service account for GCS authorization. If an email and
    keyfile are provided (see `fs.gs.auth.service.account.email` and
    `fs.gs.auth.service.account.keyfile`), then that service account will be
    used. Otherwise the connector will look to see if it is running on a GCE VM
    with some level of GCS access in it's service account scope, and use that
    service account.

#### Service account authentication

The following properties are required only when running not on a GCE VM and
`fs.gs.auth.service.account.enable` is `true`. There are 4 ways to configure
these credentials, which are mutually exclusive.

*   `fs.gs.auth.service.account.email` (not set by default)

    The email address is associated with the service account used for GCS access
    when `fs.gs.auth.service.account.enable` is `true`. Required when
    authentication key specified in the Configuration file (Method 1) or a
    PKCS12 certificate (Method 3) is being used.

##### Method 1

Configure service account details directly in the Configuration file or via
[Hadoop Credentials](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/CredentialProviderAPI.html).

*   `fs.gs.auth.service.account.private.key.id` (not set by default)

    The private key id associated with the service account used for GCS access.
    This can be extracted from the json keyfile generated via the Google Cloud
    Console.

*   `fs.gs.auth.service.account.private.key` (not set by default)

    The private key associated with the service account used for GCS access.
    This can be extracted from the json keyfile generated via the Google Cloud
    Console.

##### Method 2

Configure service account credentials using a json keyfile. The file must exist
at the same path on all nodes

*   `fs.gs.auth.service.account.json.keyfile` (not set by default)

    The path to the json keyfile for the service account.

##### Method 3

Configure service account credentials using a P12 certificate. The file must
exist at the same path on all nodes

*   `fs.gs.auth.service.account.keyfile` (not set by default)

    The PKCS12 (p12) certificate file of the service account used for GCS access
    when `fs.gs.auth.service.account.enable` is `true`.

##### Method 4

Configure service account credentials using OAuth client secret and refresh token.

To achieve this the connector will use the
[refresh token grant flow](https://oauth.net/2/grant-types/refresh-token/) to
retrieve a new access tokens when necessary.

In order to use this authentication type, you will first need to retrieve a
refresh token using the
[authorization code grant flow](https://oauth.net/2/grant-types/authorization-code)
and pass it to the connector with OAuth client ID and secret:

*   `fs.gs.auth.client.id` (not set by default)

    The OAuth2 client ID.

*   `fs.gs.auth.client.secret` (not set by default)

    The OAuth2 client secret.

*   `fs.gs.auth.refresh.token` (not set by default)

    The refresh token.


#### Service account impersonation

Service account impersonation can be configured for a specific user name and a
group name, or for all users by default using below properties:

*   `fs.gs.auth.impersonation.service.account.for.user.<USER_NAME>` (not set by
    default)

    The service account impersonation for a specific user.

*   `fs.gs.auth.impersonation.service.account.for.group.<GROUP_NAME>` (not set
    by default)

    The service account impersonation for a specific group.

*   `fs.gs.auth.impersonation.service.account` (not set by default)

    Default service account impersonation for all users.

If any of the above properties are set then the service account specified will
be impersonated by generating a short-lived credential when accessing Google
Cloud Storage.

Configured authentication method will be used to authenticate the request to
generate this short-lived credential.

If more than one property is set then the service account associated with the
user name will take precedence over the service account associated with the
group name for a matching user and group, which in turn will take precedence
over default service account impersonation.

### Authorization

When configured, a specified authorization handler will be used to authorize
Cloud Storage API requests before executing them. The handler will throw
`AccessDeniedException` for rejected requests if user does not have enough
permissions (not authorized) to execute these requests.

*   `fs.gs.authorization.handler.impl` (not set by default)

    Enable authorization handler. If this property is set, the specified
    authorization handler will be used. GCS connector will use the specified
    authorization handler to check if a user has enough permission to perform a
    GCS resource access request before granting access.

*   `fs.gs.authorization.handler.properties.<PROPERTY>=<VALUE>` (not set by
    default)

    Properties for the authorization handler. All the properties set with this
    prefix will be set to the handler after instantiation before calling any
    Cloud Storage requests handling methods.

### IO configuration

*   `fs.gs.inputstream.fast.fail.on.not.found.enable` (default: `true`)

    If `true`, on opening a file connector will proactively send a Cloud Storage
    metadata request to check whether the object exists, even though the
    underlying channel will not open a data stream until `read()` method is
    called so that streams can seek to nonzero file positions without incurring
    an extra stream creation. This is necessary to technically match the
    expected behavior of HCFS, but incurs extra latency overhead on `open()`
    call. If the client code can handle late failures on not-found errors, or
    has independently already ensured that a file exists before calling open(),
    then set this property to false for more efficient reads.

*   `fs.gs.inputstream.support.gzip.encoding.enable` (default: `false`)

    If set to `false` then reading files with GZIP content encoding (HTTP header
    `Content-Encoding: gzip`) will result in failure (`IOException` is thrown).

    This feature is disabled by default because processing of
    [GZIP encoded](https://cloud.google.com/storage/docs/transcoding#decompressive_transcoding)
    files is inefficient and error-prone in Hadoop and Spark.

*   `fs.gs.outputstream.buffer.size` (default: `8388608`)

    Write buffer size used by the file system API to send the data to be
    uploaded to Cloud Storage upload thread via pipes. The various pipe types
    are documented below.

*   `fs.gs.outputstream.pipe.type` (default: `IO_STREAM_PIPE`)

    Pipe type used for uploading Cloud Storage objects.

    Valid values:

    *   `NIO_CHANNEL_PIPE` - use
        [Java NIO Pipe](https://docs.oracle.com/javase/8/docs/api/java/nio/channels/Pipe.html)
        in output stream that writes to Cloud Storage. When using this pipe type
        client can reliably write in the output stream from multiple threads
        without *"Pipe broken"* exceptions. Note that when using this pipe type
        Cloud Storage upload throughput can decrease by 10%;

    *   `IO_STREAM_PIPE` - use
        [PipedInputStream](https://docs.oracle.com/javase/8/docs/api/java/io/PipedInputStream.html)
        and
        [PipedOutputStream](https://docs.oracle.com/javase/8/docs/api/java/io/PipedOutputStream.html)
        in output stream that writes to Cloud Storage. When using this pipe type
        client cannot reliably write in the output stream from multiple threads
        without triggering *"Pipe broken"* exceptions;

*   `fs.gs.outputstream.pipe.buffer.size` (default: `1048576`)

    Pipe buffer size used for uploading Cloud Storage objects. This pipe is an
    intermediate channel which is used to receive the data on one side and allow
    for reading of the data by the Cloud Storage upload thread on the other
    side.

*   `fs.gs.outputstream.upload.chunk.size` (default: `67108864`)

    The number of bytes in one Google Cloud Storage upload request via the
    [`MediaHttUploader` class](https://cloud.google.com/java/docs/reference/google-api-client/latest/com.google.api.client.googleapis.media.MediaHttpUploader).
    This is used only for JSON API and for best performance should be a multiple
    of 8 MiB.

    Having a large value like 64 MiB allows the upload to Cloud Storage to be
    faster due to smaller number of HTTP requests needed for upload. But on the
    other side if there are many files (partitions) being written at the same
    time then each file will hold 64 MiB buffer in memory, i.e. if 250 files are
    written at once then the total memory requirement will be 250 * 64 MiB = 16
    GiB of memory, which may result in OOM.

    To arrive to the optimal value this parameter needs to be tweaked based on
    the upload performance and number of concurrent files being written.

*   `fs.gs.outputstream.upload.cache.size` (default: `0`)

    The upload cache size in bytes used for high-level upload retries. To
    disable this feature set this property to zero or negative value. Retry will
    be performed if total size of written/uploaded data to the object is less
    than or equal to the cache size.

*   `fs.gs.outputstream.direct.upload.enable` (default: `false`)

    Enables Cloud Storage direct uploads.

*   `fs.gs.outputstream.type` (default: `BASIC`)

    Output stream type to use; different options may have different degrees of
    support for advanced features like `hsync()` and different performance
    characteristics.

    Valid values:

    *   `BASIC` - stream is closest analogue to direct wrapper around low-level
        HTTP stream into GCS.

    *   `SYNCABLE_COMPOSITE` - stream behaves similarly to `BASIC` when used
        with basic create/write/close patterns, but supports `hsync()` by
        creating discrete temporary GCS objects which are composed onto the
        destination object.

    *   `FLUSHABLE_COMPOSITE` - stream behaves similarly to
        `SYNCABLE_COMPOSITE`, except `hflush()` is also supported. It will use
        the same implementation as `hsync()`.

    For streaming applications where checkpoint files are used,
    `FLUSHABLE_COMPOSITE` should be used as the upstream frameworks typically
    use `hflush()` or `hsync()` calls before closing a file and if
    `FLUSHABLE_COMPOSITE` is not used then data is not written to these files.

*   `fs.gs.outputstream.sync.min.interval.ms` (default: `0`)

    `SYNCABLE_COMPOSITE` and `FLUSHABLE_COMPOSITE` streams configuration that
    controls the minimum interval (milliseconds) between consecutive syncs. This
    is to avoid getting rate-limited by GCS. Default is `0` - no wait between
    syncs. Note that `hflush()` for `FLUSHABLE_COMPOSITE` stream will be no-op
    if called more frequently than minimum sync interval and `hsync()` will
    block for both streams until an end of a min sync interval.

### HTTP transport configuration

*   `fs.gs.http.transport.type` (default: `JAVA_NET`)

    HTTP transport to use for sending Cloud Storage requests. Valid values are
    `APACHE` or `JAVA_NET`.

*   `fs.gs.application.name.suffix` (not set by default)

    Suffix that will be added to HTTP `User-Agent` header set in all Cloud
    Storage requests.

*   `fs.gs.proxy.address` (not set by default)

    Proxy address that connector can use to send Cloud Storage requests. The
    proxy must be an HTTP proxy and address should be in the `host:port` form.

*   `fs.gs.proxy.username` (not set by default)

    Proxy username that connector can use to send Cloud Storage requests.

*   `fs.gs.proxy.password` (not set by default)

    Proxy password that connector can use to send Cloud Storage requests.

*   `fs.gs.http.max.retry` (default: `10`)

    The maximum number of retries for low-level HTTP requests to GCS when server
    errors (code: `5XX`) or I/O errors are encountered.

*   `fs.gs.http.connect-timeout` (default: `20000`)

    Timeout in milliseconds to establish a connection. Use `0` for an infinite
    timeout.

*   `fs.gs.http.read-timeout` (default: `20000`)

    Timeout in milliseconds to read from an established connection. Use `0` for
    an infinite timeout.

### API client configuration

*   `fs.gs.storage.root.url` (default: `https://storage.googleapis.com/`)

    Google Cloud Storage root URL.

*   `fs.gs.storage.service.path` (default: `storage/v1/`)

    Google Cloud Storage service path.

*   `fs.gs.token.server.url` (default: `https://oauth2.googleapis.com/token`)

    Google Token Server root URL.

### Fadvise feature configuration

*   `fs.gs.inputstream.fadvise` (default: `AUTO`)

    Tunes reading objects behavior to optimize HTTP GET requests for various use
    cases.

    This property controls fadvise feature that allows to read objects in
    different modes:

    *   `SEQUENTIAL` - in this mode connector sends a single streaming
        (unbounded) Cloud Storage request to read object from a specified
        position sequentially.

    *   `RANDOM` - in this mode connector will send bounded Cloud Storage range
        requests (specified through HTTP Range header) which are more efficient
        in some cases (e.g. reading objects in row-columnar file formats like
        ORC, Parquet, etc).

        Range request size is limited by whatever is greater, `fs.gs.io.buffer`
        or read buffer size passed by a client.

        To avoid sending too small range requests (couple bytes) - could happen
        if `fs.gs.io.buffer` is 0 and client passes very small read buffer,
        minimum range request size is limited to 1 MiB by default configurable
        through `fs.gs.inputstream.min.range.request.size` property

    *   `AUTO` - in this mode (adaptive range reads) connector starts to send
        bounded range requests when reading non gzip-encoded objects instead of
        streaming requests as soon as first backward read or forward read for
        more than `fs.gs.inputstream.inplace.seek.limit` bytes was detected.

    *   `AUTO_RANDOM` - It is complementing `AUTO` mode which uses sequential
        mode to start with and adapts to bounded range requests. `AUTO_RANDOM`
        mode uses bounded channel initially and adapts to sequential requests if
        consecutive requests are within `fs.gs.inputstream.min.range.request.size`.
        gzip-encode object will bypass this adoption, it will always be a
        streaming(unbounded) channel. This helps in cases where egress limits is
        getting breached for customer because `AUTO` mode will always lead to
        one unbounded channel for a file. `AUTO_RANDOM` will avoid such unwanted
        unbounded channels.

*   `fs.gs.fadvise.request.track.count` (default: `3`)

    Self adaptive fadvise mode uses distance between the served requests to
    decide the access pattern. This property controls how many such requests
    need to be tracked.

*   `fs.gs.inputstream.inplace.seek.limit` (default: `8388608`)

    If forward seeks are within this many bytes of the current position, seeks
    are performed by reading and discarding bytes in-place rather than opening a
    new underlying stream.

*   `fs.gs.inputstream.min.range.request.size` (default: `2097152`)

    Minimum size in bytes of the read range for Cloud Storage request when
    opening a new stream to read an object.

### grpc configuration

gRPC is an optimized way to connect with gcs backend. It offers
better latency and increased bandwidth. Currently supported only for read/write operations.

* `fs.gs.client.type` (default: `HTTP_API_CLIENT`)

    Valid values:

    * `HTTP_API_CLIENT` uses json api to connect to gcs backend. Uses http
      over cloudpath.

    * `STORAGE_CLIENT` uses Java-storage client to connect to gcs backend. Uses
      gRPC.

*  `fs.gs.grpc.write.enable` (default: `false`)
   Is effective only of if `STORAGE_CLIENT` is selected. Enables write to go over
   grpc.

*  `fs.gs.client.upload.type` (default: `CHUNK_UPLOAD`)
   This is only effective if `STORAGE_CLIENT` is selected.

   Valid values:

   * `CHUNK_UPLOAD` uploads file in chunks, size of chunks are configurable via
     `fs.gs.outputstream.upload.chunk.size`

### Performance cache configuration

*   `fs.gs.performance.cache.enable` (default: `false`)

    Enables a performance cache that temporarily stores successfully queried
    Cloud Storage objects in memory. Caching provides a faster access to the
    recently queried objects, but because objects metadata is cached,
    modifications made outside of this connector instance may not be immediately
    reflected.

*   `fs.gs.performance.cache.max.entry.age.ms` (default: `5000`)

    Maximum number of milliseconds to store a cached metadata in the performance
    cache before it's invalidated.

### Cloud Storage [Requester Pays](https://cloud.google.com/storage/docs/requester-pays) feature configuration:

*   `fs.gs.requester.pays.mode` (default: `DISABLED`)

    Valid values:

    *   `AUTO` - Requester Pays feature enabled only for GCS buckets that
        require it;

    *   `CUSTOM` - Requester Pays feature enabled only for GCS buckets that are
        specified in the `fs.gs.requester.pays.buckets`;

    *   `DISABLED` - Requester Pays feature disabled for all GCS buckets;

    *   `ENABLED` - Requester Pays feature enabled for all GCS buckets.

*   `fs.gs.requester.pays.project.id` (not set by default)

    Google Cloud Project ID that will be used for billing when GCS Requester
    Pays feature is active (in `AUTO`, `CUSTOM` or `ENABLED` mode). If not
    specified and GCS Requester Pays is active then value of the
    `fs.gs.project.id` property will be used.

*   `fs.gs.requester.pays.buckets` (not set by default)

    Comma-separated list of Google Cloud Storage Buckets for which GCS Requester
    Pays feature should be activated if `fs.gs.requester.pays.mode` property
    value is set to `CUSTOM`.

### Cooperative Locking feature configuration

*   `fs.gs.cooperative.locking.enable` (default: `false`)

    Enables cooperative locking to achieve isolation of directory mutation
    operations.

*   `fs.gs.cooperative.locking.expiration.timeout.ms` (default: `120000`)

    Lock expiration timeout used by cooperative locking feature to lock
    directories.

*   `fs.gs.cooperative.locking.max.concurrent.operations` (default: `20`)

    Maximum number of concurrent directory modification operations per bucket
    guarded by cooperative locking feature.

### Logging Configuration

*   `fs.gs.tracelog.enable` (default: `false`)

    Enables or disables the logging of trace logs. The trace logs contain
    additional details (latency, headers etc) about the operations that are
    performed on the Google Cloud Storage.

*   `fs.gs.tracelog.time.filter.threshold.ms` (default: `0`)

    When this configuration is set, only operations that take longer than the
    specified duration will be logged. This flag can be used to reduce the size
    of the trace logs by only logging operations that take a significant amount
    of time. This config is applied only if `fs.gs.tracelog.enable` is set to true.

*   `fs.gs.tracelog.exclude.properties` (not set by default)

    A comma-separated list of property names to exclude from trace logs. When
    this flag is set, the specified properties are removed before logging. This
    flag can be used to reduce the size of the trace logs by excluding properties
    that are not relevant to your debugging session. This config is applied only
    if `fs.gs.tracelog.enable` is set to true.

    Example:

    ```
    fs.gs.tracelog.exclude.properties=request_headers,response_headers
    ```
