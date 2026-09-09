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
Verify that the `GCS_TEST_PROJECT_ID` environment variable is set:
```bash
export GCS_TEST_PROJECT_ID="<your-gcp-project-id>"
```

### Ensure Compatible Java Environment
Integration tests should run with JDK 17 (or JDK 11+):
```bash
export JAVA_HOME=/usr/lib/jvm/java-1.17.0-openjdk-amd64
```

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
- **Reference:** [Connector FAQs: Using ADC for local testing](http://go/connector-faqs#can-we-use-application-default-credentials-adc-for-local-testing??)
