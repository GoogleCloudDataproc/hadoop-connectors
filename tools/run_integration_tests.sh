#!/bin/bash
#
# Copyright 2013 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS-IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# tools/run_integration_tests.sh <project_id> <path_to_json_keyfile> [optional_maven_parameters, [...]]

set -Eeuo pipefail

export GCS_TEST_PROJECT_ID=$1
GCS_TEST_JSON_KEYFILE=$2

print_usage() {
  echo -en "Usage:\n  $0 <project_id> <path_to_json_keyfile> [optional_maven_parameters, [...]]"
}

check_required_param() {
  local -r value=$1
  local -r error_msg=$2
  if [[ -z ${value} ]]; then
    echo "${error_msg}"
    print_usage
    exit 1
  fi
}

check_required_params() {
  check_required_param "${GCS_TEST_PROJECT_ID}" "Project ID parameter is required."
  check_required_param "${GCS_TEST_JSON_KEYFILE}" "Keyfile parameter is required."

  if [[ ! -f ${GCS_TEST_JSON_KEYFILE} ]]; then
    echo "Can't find keyfile: ${GCS_TEST_JSON_KEYFILE}"
    print_usage
    exit 1
  fi
}

check_required_params

# When tests run, they run in the root of the module directory. Anything
# relative to our current directory won't work properly
if [[ $(uname -s) == Darwin ]]; then
  # On MacOS `readlink` does not support `-f` parameter
  # so we need manually resolve absolute path and symlink after that
  abs_path() {
    echo "$(cd $(dirname "$1"); pwd)/$(basename "$1")"
  }
  GCS_TEST_JSON_KEYFILE=$(abs_path "${GCS_TEST_JSON_KEYFILE}")
  while [[ -L ${GCS_TEST_JSON_KEYFILE} ]]; do
    GCS_TEST_JSON_KEYFILE=$(readlink -n "${GCS_TEST_JSON_KEYFILE}")
  done
else
  GCS_TEST_JSON_KEYFILE=$(readlink -n -f "${GCS_TEST_JSON_KEYFILE}")
fi
if [[ ! -f ${GCS_TEST_JSON_KEYFILE} ]]; then
  echo "Resolved keyfile does not exist: ${GCS_TEST_JSON_KEYFILE}"
  exit 1
fi

export GCS_TEST_JSON_KEYFILE
export HDFS_ROOT=file:///tmp
export RUN_INTEGRATION_TESTS=true

./mvnw -B -e -T1C -Pintegration-test clean verify "${@:3}"
