#!/usr/bin/env bash
set -euo pipefail
#
# enqueue-pull.sh: Shell script to demo populating a pull request into MongoDB
#
# Usage:
#   ./enqueue-pull.sh <image_ref>
#
# Examples:
#
#   ## pull an image from Docker Hub
#   ./enqueue-pull.sh biocontainers/samtools:1.9--h91753b0_8
#
#   ## fully qualified reference from another registry
#   ./enqueue-pull.sh quay.io/biocontainers/bwa:0.7.17--hed695b0_7
#
# Requires: mongosh and MongoDB is visible from session running the script
#

## get input for image reference to add positionally
IMAGE_REF="${1:?Usage: $0 <image_ref>}"

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
  const result = db.pulls.insertOne({
    image_ref:   '${IMAGE_REF}',
    status:      'queued',
    created_at:  new Date(),
  });
  print('Pull queued for ${IMAGE_REF}');
  print('  _id       : ' + result.insertedId);
  print('  image_ref : ${IMAGE_REF}');
"