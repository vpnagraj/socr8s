#!/usr/bin/env bash
set -euo pipefail
#
# enqueue-build.sh: Shell script to demo populating a build request into MongoDB
#
# Usage:
#   ./enqueue-build.sh <repo_url> [image_tag] [dockerfile] [context]
#
# Examples:
#
#   ## Dockerfile at repo root and context is "."
#   ./enqueue-build.sh https://github.com/org/repo
#
#   ## adding a custom tag
#   ./enqueue-build.sh https://github.com/org/repo  org/repo:v1.2
#
#   ## Dockerfile in a subdirector and context = that same subdirectory
#   ./enqueue-build.sh https://github.com/org/repo  myapp:latest  services/api/Dockerfile  services/api
#
#   ## Dockerfile in a subdirectory but context is still the repo root
#   ./enqueue-build.sh https://github.com/org/repo  myapp:latest  services/api/Dockerfile  .
#
# Requires: mongosh and MongoDB is visible from session running the script
#

## get inputs for repo details to add positionally
REPO_URL="${1:?Usage: $0 <repo_url> [image_tag] [dockerfile] [context]}"
IMAGE_TAG="${2:-latest}"
DOCKERFILE="${3:-Dockerfile}"
CONTEXT="${4:-.}"

## NOTE: this requires that the MongoDB is visible
## if running outside the cluster use port-forward first:
## kubectl -n build-system port-forward svc/mongo 27017:27017 &
MONGO_HOST="${MONGO_HOST:-localhost}"
MONGO_PORT="${MONGO_PORT:-27017}"

## get credentials for MongoDB from k8s API using kubectl
## NOTE: this will try to access secret from (hardcodes namespace and secret name)
if [[ -z "${MONGO_USER:-}" ]]; then
  MONGO_USER=$(kubectl -n build-system get secret mongo-creds \
    -o jsonpath='{.data.MONGO_USERNAME}' | base64 -d)
fi
if [[ -z "${MONGO_PASS:-}" ]]; then
  MONGO_PASS=$(kubectl -n build-system get secret mongo-creds \
    -o jsonpath='{.data.MONGO_PASSWORD}' | base64 -d)
fi

if [[ -z "$MONGO_USER" || -z "$MONGO_PASS" ]]; then
  echo "ERROR: Could not determine Mongo credentials."
  echo "Set MONGO_USER and MONGO_PASS env vars, or ensure kubectl can access the build-system namespace."
  exit 1
fi

## mongosh command to insert and include some logging
mongosh "mongodb://${MONGO_USER}:${MONGO_PASS}@${MONGO_HOST}:${MONGO_PORT}" --quiet --eval "
  use('builddb');
  const result = db.builds.insertOne({
    repo_url:    '${REPO_URL}',
    image_tag:   '${IMAGE_TAG}',
    dockerfile:  '${DOCKERFILE}',
    context:     '${CONTEXT}',
    status:      'queued',
    created_at:  new Date(),
  });
  print('Build queued for ${REPO_URL}');
  print('  _id       : ' + result.insertedId);
  print('  dockerfile: ${DOCKERFILE}');
  print('  context   : ${CONTEXT}');
"
