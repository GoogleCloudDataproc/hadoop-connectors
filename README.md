# Apache Hadoop Connectors

[![GitHub release](https://img.shields.io/github/release/GoogleCloudDataproc/hadoop-connectors.svg)](https://github.com/GoogleCloudDataproc/hadoop-connectors/releases/latest)
[![GitHub release date](https://img.shields.io/github/release-date/GoogleCloudDataproc/hadoop-connectors.svg)](https://github.com/GoogleCloudDataproc/hadoop-connectors/releases/latest)
[![codecov](https://codecov.io/gh/GoogleCloudDataproc/hadoop-connectors/branch/master/graph/badge.svg)](https://codecov.io/gh/GoogleCloudDataproc/hadoop-connectors)

Libraries and tools for interoperability between Apache Hadoop related
open-source software and Google Cloud Platform.

## Google Cloud Storage connector for Apache Hadoop (HCFS)

[![Maven Central](https://img.shields.io/maven-central/v/com.google.cloud.bigdataoss/gcs-connector/hadoop1.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:com.google.cloud.bigdataoss%20AND%20a:gcs-connector%20AND%20v:hadoop1-*)
[![Maven Central](https://img.shields.io/maven-central/v/com.google.cloud.bigdataoss/gcs-connector/hadoop2.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:com.google.cloud.bigdataoss%20AND%20a:gcs-connector%20AND%20v:hadoop2-*)
[![Maven Central](https://img.shields.io/maven-central/v/com.google.cloud.bigdataoss/gcs-connector.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22com.google.cloud.bigdataoss%22%20AND%20a:%22gcs-connector%22)

The Google Cloud Storage connector for Hadoop enables running MapReduce jobs
directly on data in Google Cloud Storage by implementing the Hadoop FileSystem
interface. For details, see [the README](gcs/README.md).

## Building the Cloud Storage connector

> Note that build requires Java 17+ and fails with older Java versions.

To build the connector for specific Hadoop version, run the following commands
from the main directory:

```bash
./mvnw clean package
```

In order to verify test coverage for specific Hadoop version, run the following
commands from the main directory:

```bash
./mvnw -P coverage clean verify
```

The Cloud Storage connector JAR can be found in `gcs/target/` directory.

## Adding the Cloud Storage connector to your build

Maven group ID is `com.google.cloud.bigdataoss` and artifact ID for Cloud
Storage connector is `gcs-connector`.

To add a dependency on Cloud Storage connector using Maven, use the following:

```xml
<dependency>
  <groupId>com.google.cloud.bigdataoss</groupId>
  <artifactId>gcs-connector</artifactId>
  <version>4.0.0</version>
</dependency>
```

## Resources

On **Stack Overflow**, use the tag
[`google-cloud-dataproc`](https://stackoverflow.com/tags/google-cloud-dataproc)
for questions about the connectors in this repository. This tag receives
responses from the Stack Overflow community and Google engineers, who monitor
the tag and offer unofficial support.
