"""Switch local modes without deleting data or allowing competing batch writers."""

import time
import argparse, subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BASE = ["docker", "compose"]
STREAM = BASE + [
    "-f",
    "compose.yaml",
    "-f",
    "compose.streaming.yaml",
    "--profile",
    "streaming",
]


def call(args):
    subprocess.run(args, cwd=ROOT, check=True)


parser = argparse.ArgumentParser()
parser.add_argument("mode", choices=("start", "batch", "stop"))
args = parser.parse_args()
if args.mode == "start":
    call(STREAM + ["stop", "streaming"])
    call(BASE + ["stop", "airflow", "spark-runner"])
    call(STREAM + ["build", "streaming"])
    call(STREAM + ["up", "-d", "kafka", "nifi", "kafka-init"])
    call(
        STREAM
        + [
            "run",
            "--rm",
            "streaming",
            "--master",
            "local[2]",
            "--driver-memory",
            "1g",
            "/app/jobs/seed_stream_context.py",
        ]
    )
    # Membership is initialized once. Subsequent batch Gold jobs keep it current.
    check = subprocess.run(
        BASE
        + [
            "exec",
            "-T",
            "trino",
            "trino",
            "--execute",
            "SELECT 1 FROM iceberg.gold.batch_transaction_membership LIMIT 1",
        ],
        cwd=ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    if check.returncode:
        call(
            STREAM
            + [
                "run",
                "--rm",
                "streaming",
                "--master",
                "local[2]",
                "--driver-memory",
                "1g",
                "/app/jobs/seed_lambda_membership.py",
            ]
        )
    from nifi_api import NiFi

    for attempt in range(90):
        try:
            NiFi()
            break
        except Exception:
            if attempt == 89:
                raise
            time.sleep(1)
    call(["python3", str(ROOT / "scripts/configure_nifi.py")])
    call(STREAM + ["up", "-d", "streaming", "trino"])
    call(["python3", str(ROOT / "scripts/create_lambda_view.py")])
else:
    call(STREAM + ["stop", "streaming", "nifi"])
    if args.mode == "batch":
        # Keep Kafka reachable because the Lambda view refers to its connector.
        call(STREAM + ["up", "-d", "kafka", "trino"])
        call(BASE + ["up", "-d", "spark-runner", "airflow"])
    else:
        call(STREAM + ["stop", "kafka"])
