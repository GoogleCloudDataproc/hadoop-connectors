### Introducing GCS Connector Cooperative Locking

We are excited to release Cooperative Locking support for GCS Connector. With
this release you will be able to isolate and recover directory modification
operations performed through hadoop fs command line interface on Cloud Storage.

The key motivation behind this feature is a fact that directory mutations are
not atomic on GCS, which means that competing directory mutations are
susceptible to race conditions that can lead to data integrity issues (loss,
duplication, etc).

The key benefits to this feature are:

*   Write-only isolation of move and delete directory operations performed on
    GCS through `hadoop fs` command line interface.
*   Logging of modified objects in directory modification operations.
*   Recovery of failed directory operations using FSCK command line tool.

### Enabling Cooperative Locking

To enable this feature set `fs.gs.cooperative.locking.enable` Hadoop property to
true in `core-site.xml`:

```xml
<configuration>
  <property>
    <name>fs.gs.cooperative.locking.enable</name>
    <value>true</value>
  </property>
</configuration>
```

or specify it in directly `hadoop fs` command:

```shell
hadoop fs -Dfs.gs.cooperative.locking.enable=true ...
```

### Using Cooperative Locking

To perform isolated directory modification operations inside the same bucket you
should use `hadoop fs` command with enabled Cooperative Locking:

```shell
hadoop fs -{mv|rm} gs://bucket/path/to/dir1 [gs://bucket/path/to/dir2]
```

### Using FSCK tool

To recover failed directory modification operations performed with enabled
Cooperative Locking you should use FSCK tool:

```shell
hadoop jar /usr/lib/hadoop/lib/gcs-connector.jar \
  com.google.cloud.hadoop.fs.gcs.CoopLockFsck \
  --{check|rollBack|rollForward} gs://<bucket_name> [all|<operation-id>]
```

This command will recover (roll back or roll forward) all failed directory
modification operations based on operation log.

### Configuration

Cooperative Locking could be configured using Hadoop options:

*   `fs.gs.cooperative.locking.expiration.timeout.ms` - time interval in
    milliseconds after which directory modification operation lock will expire
    if not updated.

### Limitations

Because Cooperative Locking feature is intended to be used by human operators
when modifying Cloud Storage through `hadoop fs` interface, it has multiple
limitations that you should take into account before using this feature:

*   It supports isolation of directory modification operations only in the same
    bucket.
*   It has a configurable limit (20 by default) of simultaneous directory
    modification operations per-bucket.
*   It requires write and list access to the bucket for users that use
    Cooperative Locking to modify directories in the bucket.
*   It supports only `hadoop fs` tool and does not support any other GCS clients
    (gsutil, API libraries, etc).
*   To work as intended, if cooperative locking is enabled on one of the clients
    it also should be enabled on all other clients that modify the same bucket.

### Troubleshooting

To troubleshoot Cooperative Locking feature you can use extensive debug logs
that can be enabled with `--logglevel` flag:

```shell
hadoop --loglevel debug fs -Dfs.gs.cooperative.locking.enable=true -{mv|rm} \
  gs://bucket/path/to/dir1 [gs://gs://bucket/path/to/dir2]
hadoop --loglevel debug jar /usr/lib/hadoop/lib/gcs-connector.jar \
  com.google.cloud.hadoop.fs.gcs.CoopLockFsck \
  --{check|rollBack|rollForward} gs://<bucket_name> [all|<operation-id>]
```

Also you may want to inspect `*.log` and `*.lock` files for specific directory
operations in `_lock/` folder:

```shell
gsutil cat gs://bucket/_lock/<date>_{RENAME|DELETE}_<operation-id>.lock
gsutil cat gs://bucket/_lock/<date>_{RENAME|DELETE}_<operation-id>.log
```
