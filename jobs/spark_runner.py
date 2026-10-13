"""Private Spark job endpoint: allowlisted jobs, auth and one local JVM at a time.

Avoids exposing the host Docker socket to Airflow. For Kubernetes replace this
adapter with Spark Operator submission; the pipeline job files stay unchanged.
"""

import fcntl
import hmac
import json
import os
import subprocess
import tempfile
from datetime import date
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from waba.pipeline_config import scope

JOBS = {
    "ingest_raw",
    "raw_to_bronze",
    "bronze_to_silver",
    "silver_to_gold",
    "regulatory_report",
}
TOKEN = os.environ["SPARK_RUNNER_TOKEN"]


class Handler(BaseHTTPRequestHandler):
    def reply(self, status, payload):
        data = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        self.reply(200 if self.path == "/health" else 404, {"status": "ready"})

    def do_POST(self):
        if self.path != "/run":
            return self.reply(404, {"error": "Unknown route"})
        if not hmac.compare_digest(
            self.headers.get("Authorization", ""), "Bearer " + TOKEN
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
            report_date = config.get("report_date")
            if report_date:
                date.fromisoformat(report_date)
        except (KeyError, TypeError, ValueError) as exc:
            return self.reply(400, {"error": str(exc)})
        with open("/tmp/waba-spark.lock", "w") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return self.reply(
                    409, {"error": "Another Spark job is running; retry later"}
                )
            env = os.environ.copy()
            env["COUNTRY_CODES"] = ",".join(countries)
            for key, value in (
                ("DATE_FROM", start),
                ("DATE_TO", end),
                ("REPORT_DATE", report_date),
            ):
                env[key] = value or ""
            command = [
                "/opt/spark/bin/spark-submit",
                "--master",
                "local[2]",
                "--driver-memory",
                "1g",
                f"/app/jobs/{job}.py",
            ]
            with tempfile.TemporaryFile() as log:
                try:
                    result = subprocess.run(
                        command, env=env, stdout=log, stderr=log, timeout=1200
                    )
                    log.seek(0, 2)
                    log.seek(max(0, log.tell() - 16000))
                    tail = log.read().decode(errors="replace")
                    self.reply(
                        200 if result.returncode == 0 else 500,
                        {"job": job, "exit_code": result.returncode, "log_tail": tail},
                    )
                except subprocess.TimeoutExpired:
                    self.reply(504, {"job": job, "error": "Job exceeded 20 minutes"})
            fcntl.flock(lock, fcntl.LOCK_UN)


ThreadingHTTPServer(("0.0.0.0", 8090), Handler).serve_forever()
