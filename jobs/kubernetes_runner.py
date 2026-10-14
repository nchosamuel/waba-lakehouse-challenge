"""Authenticated Airflow adapter submitting real Spark Operator applications."""

import json
import os
import ssl
import time
import urllib.error
import urllib.request
import uuid
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from waba.pipeline_config import scope
from waba.kubernetes import spark_application

JOBS = {
    "ingest_raw",
    "raw_to_bronze",
    "bronze_to_silver",
    "silver_to_gold",
    "regulatory_report",
}
WRITE_LOCK = threading.Lock()
AUTH = os.environ["SPARK_RUNNER_TOKEN"]
ACCOUNT = Path("/var/run/secrets/kubernetes.io/serviceaccount")
CONTEXT = ssl.create_default_context(cafile=str(ACCOUNT / "ca.crt"))
BASE = "https://kubernetes.default.svc/apis/sparkoperator.k8s.io/v1beta2/namespaces/processing/sparkapplications"


def api(method, suffix="", body=None):
    request = urllib.request.Request(
        BASE + suffix,
        method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={
            "Authorization": "Bearer " + (ACCOUNT / "token").read_text(),
            "Content-Type": "application/json",
        },
    )
    with urllib.request.urlopen(request, context=CONTEXT, timeout=30) as response:
        return json.load(response)


class Handler(BaseHTTPRequestHandler):
    def reply(self, status, data):
        payload = json.dumps(data).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self):
        self.reply(200 if self.path == "/health" else 404, {"status": "ready"})

    def do_POST(self):
        if not WRITE_LOCK.acquire(blocking=False):
            return self.reply(409, {"error": "Another job submission is active"})
        try:
            self.submit()
        finally:
            WRITE_LOCK.release()

    def submit(self):
        import hmac

        if self.path != "/run":
            return self.reply(404, {"error": "Unknown route"})
        if not hmac.compare_digest(
            self.headers.get("Authorization", ""), "Bearer " + AUTH
        ):
            return self.reply(401, {"error": "Unauthorized"})
        try:
            size = int(self.headers.get("Content-Length", "0"))
            if not 0 < size < 10000:
                raise ValueError("Invalid request size")
            payload = json.loads(self.rfile.read(size))
            job = payload["job"]
            if job not in JOBS:
                raise ValueError("Unknown job")
            config = payload.get("config", {})
            countries, start, end = scope(config)
            from datetime import date

            if config.get("report_date"):
                date.fromisoformat(config["report_date"])
        except (KeyError, TypeError, ValueError) as exc:
            return self.reply(400, {"error": str(exc)})
        # Refuse overlapping writers, including the long-running stream.
        existing = api("GET").get("items", [])
        if any(
            item.get("status", {}).get("applicationState", {}).get("state")
            not in ("COMPLETED", "FAILED")
            for item in existing
        ):
            return self.reply(
                409,
                {"error": "A Spark application is active; retry after it completes"},
            )
        name = "waba-" + job.replace("_", "-") + "-" + uuid.uuid4().hex[:8]
        env = {
            "COUNTRY_CODES": ",".join(countries),
            "DATE_FROM": start or "",
            "DATE_TO": end or "",
            "REPORT_DATE": config.get("report_date") or "",
        }
        try:
            api("POST", body=spark_application(name, job, env))
            deadline = time.monotonic() + 1200
            while time.monotonic() < deadline:
                resource = api("GET", "/" + name)
                state = (
                    resource.get("status", {}).get("applicationState", {}).get("state")
                )
                if state in ("COMPLETED", "FAILED"):
                    success = state == "COMPLETED"
                    return self.reply(
                        200 if success else 500,
                        {
                            "job": job,
                            "exit_code": 0 if success else 1,
                            "log_tail": json.dumps(
                                {
                                    "spark_application": name,
                                    "state": state,
                                    "error": resource.get("status", {})
                                    .get("applicationState", {})
                                    .get("errorMessage"),
                                }
                            ),
                        },
                    )
                time.sleep(5)
            self.reply(
                504,
                {
                    "job": job,
                    "error": "Spark application exceeded 20 minutes; resource retained for diagnosis",
                },
            )
        except Exception as exc:
            self.reply(500, {"job": job, "error": str(exc)})


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8090), Handler).serve_forever()
