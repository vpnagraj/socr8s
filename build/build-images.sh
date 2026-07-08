#!/usr/bin/env bash
set -euo pipefail

## required env vars (injected by the dispatcher via the k8s job spec)
## BUILD_REPO_URL    – URL from which the repo should be cloned
## BUILD_IMAGE_TAG   – tag for the built image  (default: "latest")
## BUILD_DOCKERFILE  – path to Dockerfile inside the repo (default: "Dockerfile")
## BUILD_CONTEXT     – Docker build context, relative to repo root (default: ".")
## BUILD_JOB_ID      – the MongoDB _id for this build job (informational)

WORKDIR=/workspace
REPO_DIR="$WORKDIR/repo"
IMAGE_TAG="${BUILD_IMAGE_TAG:-latest}"
DOCKERFILE="${BUILD_DOCKERFILE:-Dockerfile}"
CONTEXT="${BUILD_CONTEXT:-.}"

## define helper function to emit a JSON summary line the dispatcher can parse from logs
emit_result() {
  local status="$1" message="$2" image_id="${3:-}" image_size="${4:-}"
  ## print machine readable JSON ... 
  ## ... dispatcher will grep for this pattern from the stdout and use for DB insert
  echo "@@BUILD_RESULT@@$(cat <<EOF
{"status":"${status}","message":"${message}","image_id":"${image_id}","image_size_bytes":${image_size:-0},"repo":"${BUILD_REPO_URL}","tag":"${IMAGE_TAG}"}
EOF
)"
}

## validate inputs
if [[ -z "${BUILD_REPO_URL:-}" ]]; then
  echo "ERROR: BUILD_REPO_URL is not set."
  emit_result "failed" "BUILD_REPO_URL not set"
  exit 1
fi

echo "========================================"
echo "Build Job: ${BUILD_JOB_ID:-unknown}"
echo "  Repo    : $BUILD_REPO_URL"
echo "  Tag     : $IMAGE_TAG"
echo "  File    : $DOCKERFILE"
echo "  Context : $CONTEXT"
echo "========================================"

## start Docker-in-Docker daemon
## need this to build the image within the container
echo "Starting Docker daemon..."
dockerd-entrypoint.sh &>/dev/null &

attempts=0
until docker info >/dev/null 2>&1; do
  sleep 1
  attempts=$((attempts + 1))
  if [[ $attempts -ge 30 ]]; then
    echo "ERROR: Docker daemon did not start within 30 s"
    emit_result "failed" "Docker daemon timeout"
    exit 1
  fi
done
echo "Docker daemon ready (${attempts}s)"

## clone tool repo
echo "Cloning $BUILD_REPO_URL ..."
## clean just in case anything is there
rm -rf "$REPO_DIR"
if ! git clone --depth 1 "$BUILD_REPO_URL" "$REPO_DIR" 2>&1; then
  emit_result "failed" "git clone failed"
  exit 1
fi
## change to repo to do the build
cd "$REPO_DIR"

## build
## resolve paths (see below) to enable more informative error messaging
## DOCKERFILE is relative to the repo root  (e.g. "services/api/Dockerfile")
## CONTEXT    is relative to the repo root  (e.g. "services/api" or ".")
BUILD_CONTEXT_DIR="$REPO_DIR/$CONTEXT"

if [[ ! -d "$BUILD_CONTEXT_DIR" ]]; then
  echo "ERROR: context directory '$CONTEXT' does not exist in the repo."
  emit_result "failed" "context dir not found: $CONTEXT"
  exit 1
fi

if [[ ! -f "$REPO_DIR/$DOCKERFILE" ]]; then
  echo "ERROR: Dockerfile '$DOCKERFILE' does not exist in the repo."
  emit_result "failed" "dockerfile not found: $DOCKERFILE"
  exit 1
fi

## logging ahead of the build
echo "Building image ${IMAGE_TAG} ..."
echo "  Dockerfile : $REPO_DIR/$DOCKERFILE"
echo "  Context    : $BUILD_CONTEXT_DIR"

## wrap the docker build in an 'if' to capture output if the build succeeds
## NOTE: the emit_result helper actually interprets image id and image size *positionally ...
## ... and from there sucks in the values and populates in the JSON that is captured by dispatcher
if docker build -f "$REPO_DIR/$DOCKERFILE" -t "$IMAGE_TAG" "$BUILD_CONTEXT_DIR" 2>&1; then
  IMAGE_ID=$(docker inspect --format='{{.Id}}' "$IMAGE_TAG" 2>/dev/null || echo "")
  IMAGE_SIZE=$(docker inspect --format='{{.Size}}' "$IMAGE_TAG" 2>/dev/null || echo "0")
  echo "Build succeeded.  Image ID: $IMAGE_ID  Size: $IMAGE_SIZE bytes"
  emit_result "succeeded" "build ok" "$IMAGE_ID" "$IMAGE_SIZE"
  exit 0
else
  emit_result "failed" "docker build exited non-zero"
  exit 1
fi