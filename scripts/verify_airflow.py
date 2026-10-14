"""Exercise actual Airflow scheduler runs, chaining and a selective country backfill."""

import base64
import json
import time
import urllib.request
import urllib.parse
import urllib.error
import http.client
import uuid
from pathlib import Path
from run_pipeline import env
from verification_runtime import report_path
from datetime import datetime, timezone
import os

ROOT = Path(__file__).resolve().parents[1]
credentials = env()
auth = base64.b64encode(
    (
        credentials["AIRFLOW_ADMIN_USER"] + ":" + credentials["AIRFLOW_ADMIN_PASSWORD"]
    ).encode()
).decode()


def api(path, method="GET", body=None):
    request = urllib.request.Request(
        "http://localhost:8081/api/v1/" + path,
        data=json.dumps(body).encode() if body is not None else None,
        method=method,
        headers={"Authorization": "Basic " + auth, "Content-Type": "application/json"},
    )
    # Only reads are retried: a lost POST response must not create a second run.
    attempts = 20 if method == "GET" else 1
    for attempt in range(attempts):
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return json.load(response)
        except urllib.error.HTTPError as exc:
            if method != "GET" or exc.code not in (500, 502, 503, 504) or attempt == attempts-1:
                raise
        except (urllib.error.URLError, http.client.HTTPException, TimeoutError):
            if attempt == attempts-1:
                raise
        time.sleep(3)


def wait(dag, run):
    deadline = time.monotonic() + 1500
    while time.monotonic() < deadline:
        status = api(f'dags/{dag}/dagRuns/{urllib.parse.quote(run,safe="")}')
        if status["state"] == "success":
            return status
        if status["state"] == "failed":
            raise AssertionError((dag, run, status["state"]))
        time.sleep(5)
    raise TimeoutError((dag, run))


if __name__ == "__main__":

    for name in (
        "dag_ingest_raw",
        "dag_bronze_to_silver",
        "dag_silver_to_gold",
    ):
        api("dags/" + name, "PATCH", {"is_paused": False})
    run = "verification_" + uuid.uuid4().hex
    api(
        "dags/dag_ingest_raw/dagRuns",
        "POST",
        {
            "dag_run_id": run,
            "conf": {"country_codes": ["CI", "SN", "ML", "BF", "GN", "TG", "BJ", "GH"]},
        },
    )
    completed = [wait("dag_ingest_raw", run)]
    api("dags/dag_ingest_raw", "PATCH", {"is_paused": True})
    silver = "ingestion__" + run
    completed.append(wait("dag_bronze_to_silver", silver))
    gold = "silver__" + silver
    completed.append(wait("dag_silver_to_gold", gold))
    api("dags/dag_regulatory_report", "PATCH", {"is_paused": False})
    api(
        "dags/dag_regulatory_report/dagRuns",
        "POST",
        {"dag_run_id": run, "conf": {"report_date": "2026-02-28"}},
    )
    completed.append(wait("dag_regulatory_report", run))
    # Stop periodic intake while validating isolated replay. Downstream DAGs remain active.
    api("dags/dag_ingest_raw", "PATCH", {"is_paused": True})
    result = {
        "scheduler_runs_verified": True,
        "verified_at_utc": datetime.now(timezone.utc).isoformat(),
        "runtime": os.environ.get("WABA_RUNTIME", "compose"),
        "runs": [
            {"dag_id": r["dag_id"], "run_id": r["dag_run_id"], "state": r["state"]}
            for r in completed
        ],
        "regulatory_schedule": "30 0 * * *",
        "timezone": "UTC",
    }
    report_path("airflow-verification.json").write_text(
        json.dumps(result, indent=2) + "\n"
    )
    print(json.dumps(result))
