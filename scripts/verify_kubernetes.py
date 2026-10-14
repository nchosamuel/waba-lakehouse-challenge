"""Record native deployment evidence without exporting Secrets or credentials."""

from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
from kubernetes import kube, CONTEXT

ROOT = Path(__file__).resolve().parents[1]
apps = json.loads(
    kube("get", "sparkapplications", "-n", "processing", "-o", "json", capture=True)
)["items"]
completed = [
    a
    for a in apps
    if a.get("status", {}).get("applicationState", {}).get("state") == "COMPLETED"
]
required = {
    "ingest_raw",
    "raw_to_bronze",
    "bronze_to_silver",
    "silver_to_gold",
    "regulatory_report",
    "seed_stream_context",
    "seed_analytics_context",
}
observed = {Path(a["spec"]["mainApplicationFile"]).stem for a in completed}
assert required <= observed, required - observed
proof = {}
for ns in ("ingestion", "processing", "serving"):
    claims = json.loads(kube("get", "pvc", "-n", ns, "-o", "json", capture=True))[
        "items"
    ]
    proof[ns] = {c["metadata"]["name"]: c["status"]["phase"] for c in claims}
    # A deferred workload's local-path PVC legitimately remains Pending until used.
for ns, name in (
    ("ingestion", "minio-data"),
    ("processing", "catalog-data"),
    ("processing", "stream-checkpoints"),
):
    assert proof[ns][name] == "Bound", (ns, name, proof[ns][name])
stream = next(a for a in apps if a["metadata"]["name"] == "waba-streaming")
assert stream["status"]["applicationState"]["state"] == "RUNNING"
driver = stream["status"]["driverInfo"]["podName"]
pod = json.loads(
    kube("get", "pod", driver, "-n", "processing", "-o", "json", capture=True)
)
assert pod["spec"]["containers"][0].get("readinessProbe") and pod["spec"]["containers"][
    0
].get("livenessProbe")
health = json.loads(
    kube(
        "exec",
        driver,
        "-n",
        "processing",
        "--",
        "cat",
        "/checkpoints/stream-health.json",
        capture=True,
    )
)
assert (
    datetime.now(timezone.utc) - datetime.fromisoformat(health["heartbeat_utc"])
).total_seconds() < 90
assert len(health["queries"]) == 2 and all(q["active"] for q in health["queries"])
executors = json.loads(
    kube(
        "get",
        "pods",
        "-n",
        "processing",
        "-l",
        "sparkoperator.k8s.io/app-name=waba-streaming,spark-role=executor",
        "-o",
        "json",
        capture=True,
    )
)["items"]
assert any(e["status"]["phase"] == "Running" for e in executors)
for executor in executors:
    if executor["status"]["phase"] == "Running":
        assert any(
            m["mountPath"] == "/checkpoints"
            for m in executor["spec"]["containers"][0].get("volumeMounts", [])
        )
report = {
    "verified_at_utc": datetime.now(timezone.utc).isoformat(),
    "context": CONTEXT,
    "server_version": json.loads(kube("version", "-o", "json", capture=True))[
        "serverVersion"
    ]["gitVersion"],
    "architecture": json.loads(kube("get", "nodes", "-o", "json", capture=True))[
        "items"
    ][0]["status"]["nodeInfo"]["architecture"],
    "native_spark_driver_and_executor_verified": True,
    "checkpoint_mounted_in_driver_and_executor": True,
    "two_queries_active": True,
    "pvc_status": proof,
    "completed_jobs": [
        {
            "name": a["metadata"]["name"],
            "job": Path(a["spec"]["mainApplicationFile"]).stem,
            "start": a["status"].get("lastSubmissionAttemptTime"),
            "finish": a["status"].get("terminationTime"),
        }
        for a in completed
    ],
    "initial_failed_attempts_retained": [
        a["metadata"]["name"]
        for a in apps
        if a.get("status", {}).get("applicationState", {}).get("state") == "FAILED"
    ],
    "remaining_level4": [
        "Keycloak SSO and access policies",
        "OpenMetadata catalogue and lineage",
        "Prometheus Grafana Loki and tested alerts",
        "Ingress controller/TLS validation",
    ],
}
(ROOT / "docs/kubernetes-verification.json").write_text(
    json.dumps(report, indent=2) + "\n"
)
print(json.dumps(report))
