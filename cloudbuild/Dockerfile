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
# The image below is for OpenJDK 11.
FROM us-docker.pkg.dev/artifact-foundry-prod/docker-3p-trusted/openjdk@sha256:e81b7f317654b0f26d3993e014b04bcb29250339b11b9de41e130feecd4cd43c

# Copy everything into the container to allow concurrent build execution
COPY . /hadoop-connectors
