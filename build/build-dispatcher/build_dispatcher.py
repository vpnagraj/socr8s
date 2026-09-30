"""
build_dispatcher.py – Polls MongoDB for queued build jobs, creates Kubernetes
Jobs, watches their completion, and writes results back to MongoDB.

The main loop is NON-BLOCKING: it dispatches up to MAX_CONCURRENT jobs, then
polls all running jobs for completion each cycle, rather than blocking (waiting) on one
job at a time.

Env vars:
  MONGO_USERNAME      (required) from K8s Secret mongo-creds
  MONGO_PASSWORD      (required) from K8s Secret mongo-creds
  MONGO_HOST          mongo.build-system.svc.cluster.local:27017
  MONGO_URI           (optional) overrides the above three if set directly
  MONGO_DB            builddb
  MONGO_COLLECTION    builds
  BUILDER_IMAGE       socr8s-builder:latest
  BUILD_NAMESPACE     build-system
  POLL_INTERVAL       10            (seconds between queue checks)
  JOB_TIMEOUT         3600          (seconds before we consider a job hung)
  MAX_CONCURRENT      3             (max build jobs running at once)
"""

import json
import logging
import os
import re
import time
from datetime import datetime, timezone

from kubernetes import client, config, watch
from pymongo import MongoClient
from bson import ObjectId


def utcnow() -> datetime:
    """Naive UTC datetime — matches what MongoDB stores and returns.

    MongoDB strips timezone info, so using timezone-aware datetimes causes
    'can't subtract offset-naive and offset-aware' errors on comparison.
    """
    return datetime.now(timezone.utc).replace(tzinfo=None)

## config
_mongo_uri_override = os.getenv("MONGO_URI")
if _mongo_uri_override:
    MONGO_URI = _mongo_uri_override
else:
    from urllib.parse import quote_plus
    _user = os.environ["MONGO_USERNAME"]
    _pass = os.environ["MONGO_PASSWORD"]
    _host = os.getenv("MONGO_HOST", "mongo.build-system.svc.cluster.local:27017")
    MONGO_URI = f"mongodb://{quote_plus(_user)}:{quote_plus(_pass)}@{_host}"

## get environment variables for config from dispatcher.yaml ... 
## except for GRACE_PERIOD which is set here
MONGO_DB = os.getenv("MONGO_DB", "builddb")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "builds")
BUILDER_IMAGE = os.getenv("BUILDER_IMAGE", "vpnagraj/socr8s-builder:latest")
BUILD_NAMESPACE = os.getenv("BUILD_NAMESPACE", "build-system")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "10"))
JOB_TIMEOUT = int(os.getenv("JOB_TIMEOUT", "3600"))
## maximum number of jobs that can run concurrently
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", "3"))
## minimum seconds after started_at before we check the job status (gives time for pod to be scheduled/start up)
GRACE_PERIOD = 30

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("dispatcher")

## k8s client init
try:
    config.load_incluster_config()
except config.ConfigException:
    config.load_kube_config()
batch_v1 = client.BatchV1Api()
core_v1 = client.CoreV1Api()

## MongoDB client init
mongo = MongoClient(MONGO_URI)
db = mongo[MONGO_DB]
builds = db[MONGO_COLLECTION]

builds.create_index([("status", 1)])


# ── Document schema (for reference) ─────────────────────────────────────────
#
# {
#   "_id":            ObjectId,
#   "repo_url":       "https://github.com/org/repo",
#   "image_tag":      "org/repo:latest",          (optional, default = latest)
#   "dockerfile":     "Dockerfile",               (optional)
#   "context":        ".",                         (optional)
#   "status":         "queued" | "running" | "succeeded" | "failed" | "timeout",
#   "k8s_job_name":   "build-<hex>",              (set when dispatched)
#   "created_at":     datetime,
#   "started_at":     datetime,
#   "finished_at":    datetime,
#   "result":         { ... parsed from pod logs },
#   "logs":           "full build output",
#   "error":          "human-readable error",
# }
# ─────────────────────────────────────────────────────────────────────────────

## define functions

## ensures job name is lowercase
def sanitize_job_name(build_id: str) -> str:
    """K8s job names must be lowercase RFC-1123 subdomains."""
    return f"build-{build_id.lower()}"

## launches the job
## NOTE: specs for the builder job (e.g., resources available) are defined here
def create_k8s_job(build_doc: dict) -> str:
    """Create a K8s Job for the given build document.  Returns the job name."""
    build_id = str(build_doc["_id"])
    job_name = sanitize_job_name(build_id)

    env = [
        client.V1EnvVar(name="BUILD_REPO_URL", value=build_doc["repo_url"]),
        client.V1EnvVar(name="BUILD_IMAGE_TAG", value=build_doc.get("image_tag", "latest")),
        client.V1EnvVar(name="BUILD_DOCKERFILE", value=build_doc.get("dockerfile", "Dockerfile")),
        client.V1EnvVar(name="BUILD_CONTEXT", value=build_doc.get("context", ".")),
        client.V1EnvVar(name="BUILD_JOB_ID", value=build_id),
    ]

    container = client.V1Container(
        name="builder",
        image=BUILDER_IMAGE,
        image_pull_policy="IfNotPresent",
        env=env,
        security_context=client.V1SecurityContext(privileged=True),
        volume_mounts=[
            client.V1VolumeMount(name="workspace", mount_path="/workspace"),
        ],
        resources=client.V1ResourceRequirements(
            requests={"cpu": "500m", "memory": "512Mi"},
            limits={"cpu": "2", "memory": "4Gi"},
        ),
    )

    pod_spec = client.V1PodSpec(
        restart_policy="Never",
        containers=[container],
        volumes=[
            client.V1Volume(name="workspace", empty_dir=client.V1EmptyDirVolumeSource()),
        ],
    )

    job = client.V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=client.V1ObjectMeta(
            name=job_name,
            namespace=BUILD_NAMESPACE,
            labels={
                "app": "image-builder",
                "build-id": build_id,
            },
        ),
        spec=client.V1JobSpec(
            backoff_limit=0,
            ttl_seconds_after_finished=600,
            template=client.V1PodTemplateSpec(
                metadata=client.V1ObjectMeta(
                    labels={"app": "image-builder", "build-id": build_id},
                ),
                spec=pod_spec,
            ),
        ),
    )

    batch_v1.create_namespaced_job(namespace=BUILD_NAMESPACE, body=job)
    log.info("Created K8s Job %s for build %s", job_name, build_id)
    return job_name

## claims the next build to run by setting queued to running
def claim_next_build() -> dict | None:
    """Atomically claim the oldest queued build (status queued → running)."""
    doc = builds.find_one_and_update(
        {"status": "queued"},
        {
            "$set": {
                "status": "running",
                "started_at": utcnow(),
            }
        },
        sort=[("created_at", 1)],
        return_document=True,
    )
    return doc

## counts how many are running
def count_running() -> int:
    """Return the number of builds currently in 'running' status."""
    return builds.count_documents({"status": "running"})

## parses the bespoke JSON that the builder image outputs with its emit_result() bash helper
def extract_build_result(logs: str) -> dict | None:
    """Parse the @@BUILD_RESULT@@{...} line from pod logs."""
    match = re.search(r"@@BUILD_RESULT@@(\{.*\})", logs)
    if match:
        try:
            return json.loads(match.group(1))
        except json.JSONDecodeError:
            pass
    return None

## goes to the API to get pod logs
def read_pod_logs(job_name: str) -> str:
    """Return the combined logs of all pods belonging to a job."""
    try:
        pods = core_v1.list_namespaced_pod(
            namespace=BUILD_NAMESPACE,
            label_selector=f"job-name={job_name}",
        )
    except client.ApiException as exc:
        return f"[could not list pods for {job_name}: {exc.reason}]"

    all_logs = []
    for pod in pods.items:
        try:
            pod_log = core_v1.read_namespaced_pod_log(
                name=pod.metadata.name,
                namespace=BUILD_NAMESPACE,
            )
            all_logs.append(pod_log)
        except client.ApiException:
            all_logs.append(f"[could not read logs for {pod.metadata.name}]")
    return "\n".join(all_logs)


## checks job status in k8s API
def get_k8s_job_status(job_name: str) -> str | None:
    """Check a K8s Job's current status.

    Returns 'succeeded', 'failed', or None if still running / status unknown.
    Transient API errors return None so the caller retries next cycle
    rather than marking the build as failed.
    """
    try:
        job = batch_v1.read_namespaced_job_status(
            name=job_name, namespace=BUILD_NAMESPACE
        )
        if job.status.succeeded and job.status.succeeded > 0:
            return "succeeded"
        if job.status.failed and job.status.failed > 0:
            return "failed"
        return None
    except client.ApiException as exc:
        if exc.status == 404:
            ## job was deleted (TTL cleanup) or never created
            return "failed"
        ## transient API error (500, timeout, etc.) — don't assume failure
        log.warning("K8s API error checking job %s (status %s): %s",
                     job_name, exc.status, exc.reason)
        return None

## stitches it all together to update the database after build
def finalize_build(build_id: str, job_name: str, k8s_status: str):
    """Read pod logs, parse the result, and update MongoDB."""
    logs = read_pod_logs(job_name)
    result = extract_build_result(logs)

    update: dict = {
        "status": k8s_status,
        "finished_at": utcnow(),
        "k8s_job_name": job_name,
        ## NOTE: logs are stored in full
        "logs": logs,
    }

    if result:
        update["result"] = result

    builds.update_one({"_id": ObjectId(build_id)}, {"$set": update})
    log.info("Build %s finished: %s", build_id, k8s_status)

## overrides and marks a build as failure
def fail_build(build_id: str, error_msg: str, job_name: str = ""):
    """Mark a build as failed in MongoDB (used for crash recovery)."""
    update = {
        "status": "failed",
        "finished_at": utcnow(),
        "error": error_msg,
    }
    if job_name:
        update["k8s_job_name"] = job_name
    builds.update_one({"_id": ObjectId(build_id)}, {"$set": update})
    log.warning("Build %s marked failed: %s", build_id, error_msg)

## checks timeout condition
def is_timed_out(doc: dict) -> bool:
    """Check if a running build has exceeded JOB_TIMEOUT."""
    started = doc.get("started_at")
    if not started:
        return False
    elapsed = (utcnow() - started).total_seconds()
    return elapsed > JOB_TIMEOUT

## checks all running builds for completion
def check_running_builds():
    """Non-blocking: check every 'running' build and finalize any that finished."""
    running = list(builds.find({"status": "running"}))

    for doc in running:
        build_id = str(doc["_id"])
        job_name = doc.get("k8s_job_name", "")

        if not job_name:
            ## no k8s job name yet ....
            ## could be a race with dispatch_new_builds from the previous cycle ...
            ## give it one more cycle before failing.
            started = doc.get("started_at")
            if started:
                age = (utcnow() - started).total_seconds()
                if age > GRACE_PERIOD:
                    fail_build(build_id, "No K8s Job was created for this build")
            continue

        ## don't check brand-new jobs ... let the pod start up first
        started = doc.get("started_at")
        if started:
            age = (utcnow() - started).total_seconds()
            if age < GRACE_PERIOD:
                continue

        try:
            status = get_k8s_job_status(job_name)

            if status is not None:
                finalize_build(build_id, job_name, status)
            elif is_timed_out(doc):
                log.warning("Build %s timed out after %ds", build_id, JOB_TIMEOUT)
                finalize_build(build_id, job_name, "timeout")
            ## NOTE: there is no else ...
            ## because if it's still running, we should do nothing (check again next cycle)

        except Exception as exc:
            ## do not mark the build as failed just because we couldn't check it
            ## log the error and retry on the next cycle
            ## timeout will eventually catch truly stuck builds
            log.warning("Transient error checking build %s, will retry: %s", build_id, exc)


## dispatches new builds (up to the concurrency cap)
def dispatch_new_builds():
    """Claim and launch queued builds until we hit MAX_CONCURRENT."""
    while count_running() < MAX_CONCURRENT:
        doc = claim_next_build()
        ## queue empty condition
        if doc is None:
            break

        build_id = str(doc["_id"])
        log.info("Claimed build %s  repo=%s", build_id, doc["repo_url"])

        try:
            job_name = create_k8s_job(doc)

            ## record the job name immediately so recovery can find it
            builds.update_one(
                {"_id": doc["_id"]},
                {"$set": {"k8s_job_name": job_name}},
            )
            ## no waiting ... return to the loop and let check_running_builds pick up the next cycle

        except Exception as exc:
            log.exception("Failed to create K8s Job for build %s", build_id)
            fail_build(
                build_id,
                f"Job creation failed: {type(exc).__name__}: {exc}",
            )


## main while loop
def main():
    log.info(
        "Dispatcher starting.  max_concurrent=%d  poll=%ds  timeout=%ds",
        MAX_CONCURRENT, POLL_INTERVAL, JOB_TIMEOUT,
    )

    while True:
        try:
            ## 1. finalize any running builds that have completed
            check_running_builds()

            ## 2. dispatch new builds (if we have capacity)
            dispatch_new_builds()

        except Exception as exc:
            ## catch-all so the loop never dies
            log.exception("Unexpected error in main loop: %s", exc)

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
