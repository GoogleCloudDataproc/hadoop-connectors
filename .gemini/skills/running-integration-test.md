---
name: running-integration-test
description: >-
  Use this skill whenever the user asks to run integration tests locally,
  or mentions running GCS connector tests with Application Default Credentials (ADC)
  or the GCS_TEST_APPLICATION_DEFAULT_ENABLE flag.
---

# Running Integration Tests with Application Default Credentials (ADC)

This guide and skill provides step-by-step instructions for running Google Cloud Hadoop Connector integration tests locally using Application Default Credentials (ADC).

## 1. Prerequisites

### Authenticate with ADC
Ensure your local environment has valid Application Default Credentials:
```bash
gcloud auth application-default login
```
*(Verify your active token anytime using `gcloud auth application-default print-access-token`)*.

### Configure Google Cloud Project ID
Set the `GCS_TEST_PROJECT_ID` environment variable (default: `gcs-hyd-connector-benchmarks`):
```bash
export GCS_TEST_PROJECT_ID="${GCS_TEST_PROJECT_ID:-gcs-hyd-connector-benchmarks}"
```

### Ensure Compatible Java Environment (JDK 17)
Ensure that `JAVA_HOME` points to a valid JDK 17 installation:
- Check `java -version` and `echo $JAVA_HOME`.
- If unset or invalid, locate an installed JDK 17 on the system (or install if missing) and export `JAVA_HOME`.

## 2. Enable ADC for Integration Tests

Set the `GCS_TEST_APPLICATION_DEFAULT_ENABLE` environment variable to `true`. This instructs `TestConfiguration` to use ADC when a service account keyfile is not configured:
```bash
export GCS_TEST_APPLICATION_DEFAULT_ENABLE=true
```

## 3. Running Integration Tests via Maven

Integration tests are executed using Maven's `integration-test` profile (configured via `maven-failsafe-plugin`).

### Run all integration tests in `gcs` module:
```bash
mvn verify -Pintegration-test -pl gcs
```

### Run a specific integration test class:
```bash
mvn verify -Pintegration-test -Dit.test=GoogleHadoopFSInputStreamAnalyticsIntegrationTest -pl gcs
```

### Run a single test method:
```bash
mvn verify -Pintegration-test -Dit.test=GoogleHadoopFSInputStreamAnalyticsIntegrationTest#testRead -pl gcs
```

### Run integration tests in other modules (e.g. `gcsio`):
```bash
mvn verify -Pintegration-test -pl gcsio
```

## 4. Verification & Troubleshooting
- **Token Check:** Verify valid credentials using `gcloud auth application-default print-access-token`.
- **Permissions:** Verify the authenticated account has appropriate IAM permissions on the test bucket (e.g., `roles/storage.objectAdmin`).
