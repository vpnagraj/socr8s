"""
pull_dispatcher.py polls MongoDB 'pulls' collection for queued jobs,
creates k8s jobs to pull images, watches completion, and writes
results back to MongoDB.

env vars:
  MONGO_USERNAME      (required) from K8s Secret mongo-creds
  MONGO_PASSWORD      (required) from K8s Secret mongo-creds
  MONGO_HOST          mongo.build-system.svc.cluster.local:27017
  MONGO_URI           (optional) overrides the above three if set directly
  MONGO_DB            builddb
  MONGO_COLLECTION    pulls
  PULLER_IMAGE        image-puller:latest
  PULL_NAMESPACE      build-system
  POLL_INTERVAL       10            (seconds between queue checks)
  JOB_TIMEOUT         3600          (seconds before we consider a job hung)
  MAX_CONCURRENT      3             (max pull jobs running at once)
"""

import json
import logging
import os
import re
import time
from datetime import datetime, timezone

from kubernetes import client, config
from pymongo import MongoClient
from bson import ObjectId


def utcnow() -> datetime:
    """Naive UTC datetime — matches what MongoDB stores and returns."""
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
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "pulls")
PULLER_IMAGE = os.getenv("PULLER_IMAGE", "image-puller:latest")
PULL_NAMESPACE = os.getenv("PULL_NAMESPACE", "build-system")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "10"))
JOB_TIMEOUT = int(os.getenv("JOB_TIMEOUT", "3600"))
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", "3"))
## minimum seconds after started_at before we check the job status (gives time for pod to be scheduled/start up)
GRACE_PERIOD = 30

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("pull-dispatcher")

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
pulls = db[MONGO_COLLECTION]

pulls.create_index([("status", 1)])


# ── Document schema (for reference) ─────────────────────────────────────────
#
# {
#   "_id":                ObjectId,
#   "tool":               "samtools",
#   "image_ref":          "docker.io/biocontainers/samtools:1.9",
#   "status":             "queued" | "running" | "succeeded" | "failed" | "timeout",
#   "k8s_job_name":       "pull-<hex>",
#   "created_at":         datetime,
#   "started_at":         datetime,
#   "finished_at":        datetime,
#   "pull_duration_seconds": int,
#   "image_size_bytes":   int,
#   "image_id":           "sha256:...",
#   "result":             { ... parsed from pod logs },
#   "logs":               "full pull output",
# }
# ─────────────────────────────────────────────────────────────────────────────

## define functions

## ensures job name is lowercase
def sanitize_job_name(pull_id: str) -> str:
    return f"pull-{pull_id.lower()}"

## launches the job
## NOTE: specs for the pull job (e.g., resources available) are defined here
def create_k8s_job(pull_doc: dict) -> str:
    """Create a K8s Job for the given pull document. Returns the job name."""
    pull_id = str(pull_doc["_id"])
    job_name = sanitize_job_name(pull_id)

    env = [
        client.V1EnvVar(name="PULL_IMAGE_REF", value=pull_doc["image_ref"]),
        client.V1EnvVar(name="PULL_JOB_ID", value=pull_id),
    ]

    container = client.V1Container(
        name="puller",
        image=PULLER_IMAGE,
        image_pull_policy="Never",
        env=env,
        security_context=client.V1SecurityContext(privileged=True),
        resources=client.V1ResourceRequirements(
            requests={"cpu": "250m", "memory": "256Mi"},
            limits={"cpu": "1", "memory": "2Gi"},
        ),
    )

    pod_spec = client.V1PodSpec(
        restart_policy="Never",
        containers=[container],
    )

    job = client.V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=client.V1ObjectMeta(
            name=job_name,
            namespace=PULL_NAMESPACE,
            labels={
                "app": "socr8s-puller",
                "pull-id": pull_id,
            },
        ),
        spec=client.V1JobSpec(
            backoff_limit=0,
            ttl_seconds_after_finished=600,
            template=client.V1PodTemplateSpec(
                metadata=client.V1ObjectMeta(
                    labels={"app": "socr8s-puller", "pull-id": pull_id},
                ),
                spec=pod_spec,
            ),
        ),
    )

    batch_v1.create_namespaced_job(namespace=PULL_NAMESPACE, body=job)
    log.info("Created K8s Job %s for pull %s", job_name, pull_id)
    return job_name

## claims the next pull to run by setting queued to running
def claim_next_pull() -> dict | None:
    """Atomically claim the oldest queued pull (status queued → running)."""
    doc = pulls.find_one_and_update(
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
    return pulls.count_documents({"status": "running"})

## parses the bespoke JSON that the pull image outputs with its emit_result() bash helper
def extract_pull_result(logs: str) -> dict | None:
    """Parse the @@PULL_RESULT@@{...} line from pod logs."""
    match = re.search(r"@@PULL_RESULT@@(\{.*\})", logs)
    if match:
        try:
            return json.loads(match.group(1))
        except json.JSONDecodeError:
            pass
    return None

## goes to the API to get pod logs
def read_pod_logs(job_name: str) -> str:
    try:
        pods = core_v1.list_namespaced_pod(
            namespace=PULL_NAMESPACE,
            label_selector=f"job-name={job_name}",
        )
    except client.ApiException as exc:
        return f"[could not list pods for {job_name}: {exc.reason}]"

    all_logs = []
    for pod in pods.items:
        try:
            pod_log = core_v1.read_namespaced_pod_log(
                name=pod.metadata.name,
                namespace=PULL_NAMESPACE,
            )
            all_logs.append(pod_log)
        except client.ApiException:
            all_logs.append(f"[could not read logs for {pod.metadata.name}]")
    return "\n".join(all_logs)

## checks job status in k8s API
def get_k8s_job_status(job_name: str) -> str | None:
    try:
        job = batch_v1.read_namespaced_job_status(
            name=job_name, namespace=PULL_NAMESPACE
        )
        if job.status.succeeded and job.status.succeeded > 0:
            return "succeeded"
        if job.status.failed and job.status.failed > 0:
            return "failed"
        return None
    except client.ApiException as exc:
        if exc.status == 404:
            return "failed"
        log.warning("K8s API error checking job %s (status %s): %s",
                     job_name, exc.status, exc.reason)
        return None

## stitches it all together to update the database after pull
def finalize_pull(pull_id: str, job_name: str, k8s_status: str):
    """Read pod logs, parse result, update MongoDB."""
    logs = read_pod_logs(job_name)
    result = extract_pull_result(logs)

    update: dict = {
        "status": k8s_status,
        "finished_at": utcnow(),
        "k8s_job_name": job_name,
        "logs": logs,
    }

    if result:
        update["result"] = result
        ## insert key fields to the database
        if "image_size_bytes" in result:
            update["image_size_bytes"] = result["image_size_bytes"]
        if "image_id" in result:
            update["image_id"] = result["image_id"]
        if "pull_duration_seconds" in result:
            update["pull_duration_seconds"] = result["pull_duration_seconds"]

    pulls.update_one({"_id": ObjectId(pull_id)}, {"$set": update})
    log.info("Pull %s finished: %s", pull_id, k8s_status)

## overrides and marks a pull as failure
def fail_pull(pull_id: str, error_msg: str, job_name: str = ""):
    update = {
        "status": "failed",
        "finished_at": utcnow(),
        "error": error_msg,
    }
    if job_name:
        update["k8s_job_name"] = job_name
    pulls.update_one({"_id": ObjectId(pull_id)}, {"$set": update})
    log.warning("Pull %s marked failed: %s", pull_id, error_msg)

## checks timeout condition
def is_timed_out(doc: dict) -> bool:
    started = doc.get("started_at")
    if not started:
        return False
    elapsed = (utcnow() - started).total_seconds()
    return elapsed > JOB_TIMEOUT


## checks all running builds for completion
def check_running_pulls():
    running = list(pulls.find({"status": "running"}))

    for doc in running:
        pull_id = str(doc["_id"])
        job_name = doc.get("k8s_job_name", "")

        if not job_name:
            started = doc.get("started_at")
            if started:
                age = (utcnow() - started).total_seconds()
                if age > GRACE_PERIOD:
                    fail_pull(pull_id, "No K8s Job was created for this pull")
            continue

        started = doc.get("started_at")
        if started:
            age = (utcnow() - started).total_seconds()
            if age < GRACE_PERIOD:
                continue

        try:
            status = get_k8s_job_status(job_name)

            if status is not None:
                finalize_pull(pull_id, job_name, status)
            elif is_timed_out(doc):
                log.warning("Pull %s timed out after %ds", pull_id, JOB_TIMEOUT)
                finalize_pull(pull_id, job_name, "timeout")

        except Exception as exc:
            log.warning("Transient error checking pull %s, will retry: %s", pull_id, exc)


## dispatches new pulls (up to the concurrency cap)
def dispatch_new_pulls():
    while count_running() < MAX_CONCURRENT:
        doc = claim_next_pull()
        if doc is None:
            break

        pull_id = str(doc["_id"])
        log.info("Claimed pull %s  image=%s", pull_id, doc["image_ref"])

        try:
            job_name = create_k8s_job(doc)
            pulls.update_one(
                {"_id": doc["_id"]},
                {"$set": {"k8s_job_name": job_name}},
            )
        except Exception as exc:
            log.exception("Failed to create K8s Job for pull %s", pull_id)
            fail_pull(
                pull_id,
                f"Job creation failed: {type(exc).__name__}: {exc}",
            )


## main while loop
def main():
    log.info(
        "Pull dispatcher starting.  max_concurrent=%d  poll=%ds  timeout=%ds",
        MAX_CONCURRENT, POLL_INTERVAL, JOB_TIMEOUT,
    )

    while True:
        try:
            ## 1. finalize any running pulls that have completed
            check_running_pulls()

            ## 2. dispatch new pulls (if we have capacity)
            dispatch_new_pulls()
        except Exception as exc:
            log.exception("Unexpected error in main loop: %s", exc)

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
