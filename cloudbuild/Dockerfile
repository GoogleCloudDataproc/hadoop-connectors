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

# This Dockerfile creates an image for running presubmit tests.
# The package below is for OpenJDK 17.
FROM us-docker.pkg.dev/artifact-foundry-prod/docker-3p-trusted/openjdk@sha256:2fd12c42c12bf707f7ac0f5fa630ff9c59868dfc4428daaf34df9d82a0c5b101

# Copy everything into the container to allow concurrent build execution
COPY . /hadoop-connectors
