#!/usr/bin/env bash
set -euo pipefail

## required env vars (injected by the pull dispatcher via the Job spec)
## PULL_IMAGE_REF    – full image reference to pull (e.g. docker.io/biocontainers/samtools:1.9)
## PULL_JOB_ID       – the MongoDB _id for this pull job (informational)

IMAGE_REF="${PULL_IMAGE_REF:-}"

## define helper function to emit a JSON summary line the dispatcher can parse from logs
emit_result() {
  local status="$1" message="$2" image_id="${3:-}" image_size="${4:-}" pull_duration="${5:-0}"
  ## print machine readable JSON ... 
  ## ... dispatcher will grep for this pattern from the stdout and use for DB insert
  echo "@@PULL_RESULT@@$(cat <<EOF
{"status":"${status}","message":"${message}","image_id":"${image_id}","image_size_bytes":${image_size:-0},"pull_duration_seconds":${pull_duration},"image_ref":"${IMAGE_REF}"}
EOF
)"
}

## validate inputs
if [[ -z "$IMAGE_REF" ]]; then
  echo "ERROR: PULL_IMAGE_REF is not set."
  emit_result "failed" "PULL_IMAGE_REF not set"
  exit 1
fi

echo "========================================"
echo "Pull Job: ${PULL_JOB_ID:-unknown}"
echo "  Image : $IMAGE_REF"
echo "========================================"

## start Docker-in-Docker daemon
## need this to pull the image within the container
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

## try pulling the image
echo "Pulling $IMAGE_REF ..."
PULL_START=$(date +%s)

## conditionally grab image pull status and "emit" the JSON to be ingested by dispatcher
if docker pull "$IMAGE_REF" 2>&1; then
  PULL_END=$(date +%s)
  PULL_DURATION=$(( PULL_END - PULL_START ))

  IMAGE_ID=$(docker inspect --format='{{.Id}}' "$IMAGE_REF" 2>/dev/null || echo "")
  IMAGE_SIZE=$(docker inspect --format='{{.Size}}' "$IMAGE_REF" 2>/dev/null || echo "0")

  echo ""
  echo "Pull succeeded."
  echo "  Image ID : $IMAGE_ID"
  echo "  Size     : $IMAGE_SIZE bytes"
  echo "  Duration : ${PULL_DURATION}s"

  emit_result "succeeded" "pull ok" "$IMAGE_ID" "$IMAGE_SIZE" "$PULL_DURATION"
  exit 0
else
  PULL_END=$(date +%s)
  PULL_DURATION=$(( PULL_END - PULL_START ))

  echo ""
  echo "Pull failed after ${PULL_DURATION}s"

  emit_result "failed" "docker pull exited non-zero" "" "0" "$PULL_DURATION"
  exit 1
fi
