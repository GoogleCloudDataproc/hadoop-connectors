#!/bin/bash

# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
set -euxo pipefail

readonly TEST_TYPE="${1:-unittest}"

cd /hadoop-connectors

if [[ $TEST_TYPE == unittest ]]; then
  # Run unit tests and generate test coverage report
  ./mvnw -B -e -Pcoverage clean verify
else
  # This section handles all integration tests.

  # Start with the base Maven arguments.
  MVN_ARGS="-B -e -Pintegration-test -Pcoverage"

  # If the zonal bucket name is passed from Cloud Build, add it as a system property.
  # The Java test code should check for the presence of this property
  # to decide whether to run the zonal bucket-specific tests.
  if [[ -n "${GCS_ZONAL_TEST_BUCKET:-}" ]]; then
    echo "Zonal test bucket '${GCS_ZONAL_TEST_BUCKET}' detected. Passing to Maven."
    MVN_ARGS+=" -Dtest.gcs.zonal_test_bucket=${GCS_ZONAL_TEST_BUCKET}"
  fi

  # Execute the Maven command with any additional arguments.
  # shellcheck disable=SC2086
  ./mvnw ${MVN_ARGS} clean verify
fi

# Upload test coverage report to Codecov
bash <(curl -s https://codecov.io/bash) -K -F "${TEST_TYPE}"

#set -euxo pipefail
#
#readonly TEST_TYPE="${1:-unittest}"
#
#cd /hadoop-connectors
#
## Run unit or integration tests and generate test coverage report
#if [[ $TEST_TYPE == unittest ]]; then
#  ./mvnw -B -e -Pcoverage clean verify
#else
#  ./mvnw -B -e -Pintegration-test -Pcoverage clean verify
#fi
#
## Upload test coverage report to Codecov
#bash <(curl -s https://codecov.io/bash) -K -F "${TEST_TYPE}"
