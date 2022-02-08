# Installing the connector

To install the connector manually, complete the following steps.

## Ensure authenticated Cloud Storage access

Depending on where the machines which comprise your cluster are located, you
must do one of the following:

*   **Google Cloud Platform** - Each Google Compute Engine VM must be
    [configured to have access](https://cloud.google.com/compute/docs/authentication#using)
    to the
    [Cloud Storage scope](https://cloud.google.com/storage/docs/authentication#oauth)
    you intend to use the connector for. When running inside of Google Compute
    Engine VMs, including Dataproc clusters, `google.cloud.auth.type` is set to
    `COMPUTE_ENGINE` by default, which means you don't need to manually
    configure a service account for the connector; it will automatically get the
    service account credentials from the metadata server of the GCE VM. But you
    must need to make sure the VM service account has permission to access the
    GCS bucket.
*   **non-Google Cloud Platform** - Obtain an
    [OAuth 2.0 private key](https://cloud.google.com/storage/docs/authentication#generating-a-private-key).
    Installing the connector on a machine other than a GCE VM can lead to higher
    Cloud Storage access costs. For more information, see
    [Cloud Storage Pricing](https://cloud.google.com/storage/pricing).

## Add the connector jar to Hadoop's classpath

Placing the connector jar in the `HADOOP_COMMON_LIB_JARS_DIR` directory should
be sufficient to have Hadoop load the jar. Alternatively, to be certain that the
jar is loaded, you can add
`HADOOP_CLASSPATH=$HADOOP_CLASSPATH:</path/to/gcs-connector.jar>` to
`hadoop-env.sh` in the Hadoop configuration directory.

## Configuring the connector

To begin, you will need a JSON keyfile so the connector can authenticate to
Google Cloud Storage. You can follow
[these directions](https://cloud.google.com/storage/docs/authentication#service_accounts)
to obtain a JSON keyfile.

Once you have the JSON keyfile, you can configure framework that you use to use
GCS connector when accessing data on Google Cloud Storage.

Additional properties can be specified for the Cloud Storage connector,
including alternative authentication options. For more information, see the
documentation in the [CONFIGURATION.md](/gcs/CONFIGURATION.md).

### Configuring Hadoop

To configure Hadoop to use GCS connector you need to configure following
properties in `core-site.xml` on your Hadoop cluster:

```xml
<property>
  <name>fs.AbstractFileSystem.gs.impl</name>
  <value>com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS</value>
  <description>The AbstractFileSystem for 'gs:' URIs.</description>
</property>
<property>
  <name>fs.gs.project.id</name>
  <value></value>
  <description>
    Optional. Google Cloud Project ID with access to GCS buckets.
    Required only for list buckets and create bucket operations.
  </description>
</property>
<property>
  <name>google.cloud.auth.type</name>
  <value>SERVICE_ACCOUNT_JSON_KEYFILE</value>
  <description>
    Authentication type to use for GCS access.
  </description>
</property>
<property>
  <name>google.cloud.auth.service.account.json.keyfile</name>
  <value>/path/to/keyfile</value>
  <description>
    The JSON keyfile of the service account used for GCS
    access when google.cloud.auth.type is SERVICE_ACCOUNT_JSON_KEYFILE.
  </description>
</property>
```

### Configuring Spark

Note that you do not need to configure Hadoop `core-site.xml` in order to use
the GCS connector with Spark.

To configure Spark to use GCS connector you need to configure following
`spark.hadoop.` properties in `spark-defaults.conf` on your Spark cluster (see
[Custom Hadoop/Hive Configuration](https://spark.apache.org/docs/latest/configuration.html#custom-hadoophive-configuration)):

```
# The AbstractFileSystem for 'gs:' URIs
spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS

# Optional. Google Cloud Project ID with access to GCS buckets.
# Required only for list buckets and create bucket operations.
spark.hadoop.fs.gs.project.id=

# Whether to use a service account for GCS authorization. Setting this
# property to `false` will disable use of service accounts for authentication.
spark.hadoop.google.cloud.auth.service.account.enable=true

# The JSON keyfile of the service account used for GCS
# access when google.cloud.auth.service.account.enable is true.
spark.hadoop.google.cloud.auth.service.account.json.keyfile=/path/to/keyfile
```

## Test the installation

On the command line, type `hadoop fs -ls gs://<some-bucket>`, where
`<some-bucket>` is the Google Cloud Storage bucket to which you gave the
connector read access. The command should output the top-level directories and
objects contained in `<some-bucket>`. If there is a problem, see Troubleshooting
the installation.

## Troubleshooting the installation

*   If the installation test reported `No FileSystem for scheme: gs`, make sure
    that you correctly set the two properties in the correct `core-site.xml`.

*   If the test reported `java.lang.ClassNotFoundException:
    com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem`, check that you added
    the connector to the Hadoop/Spark classpath. If this error caused by
    `java.lang.NoSuchMethodError:
    com.google.common.base.Preconditions.checkState(ZLjava/lang/String;J)V`, it
    is likely due to a conflicting version of transitive dependencies (in this
    case, Guava); using a
    [shaded version](https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop2-latest.jar)
    of the `gcs-connector` jar can resolve this.

*   If the test issued a message related to authorization, make sure that you
    have access to Cloud Storage using
    [gsutil](https://cloud.google.com/storage/docs/gsutil) (`gsutil ls -b
    gs://<some-bucket>`), and that the credentials in your configuration are
    correct.

*   To troubleshoot other issues run `hadoop fs` command with debug logs:

    ```
    $ cat <<EOF >"/tmp/gcs-connector-logging.properties"
    handlers = java.util.logging.ConsoleHandler
    java.util.logging.ConsoleHandler.level = ALL
    com.google.level = FINE
    sun.net.www.protocol.http.HttpURLConnection.level = ALL
    EOF

    $ export HADOOP_CLIENT_OPTS="-Djava.util.logging.config.file=/tmp/gcs-connector-logging.properties"

    $ hadoop --loglevel debug fs -ls gs://<some-bucket>
    ```
