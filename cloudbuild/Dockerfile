# This Dockerfile creates an image for running presubmit tests.
# The image below is for OpenJDK 8.
FROM us-docker.pkg.dev/artifact-foundry-prod/docker-3p-trusted/openjdk@sha256:3af2ac94130765b73fc8f1b42ffc04f77996ed8210c297fcfa28ca880ff0a217

# Copy everything into the container to allow concurrent build execution
COPY . /hadoop-connectors
