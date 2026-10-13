"""Local authenticated job submission, identical adapter used by Airflow."""

import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def env():
    return dict(
        line.split("=", 1)
        for line in (ROOT / ".env").read_text().splitlines()
        if "=" in line and not line.startswith("#")
    )


def run(job, config=None):
    # The private runner has no published port. Submit from its Docker network.
    import subprocess

    payload = json.dumps({"job": job, "config": config or {}})
    command = """import json,os,sys,urllib.request
payload=sys.stdin.buffer.read()
request=urllib.request.Request('http://spark-runner:8090/run',data=payload,headers={'Content-Type':'application/json','Authorization':'Bearer '+os.environ['SPARK_RUNNER_TOKEN']})
try:
    with urllib.request.urlopen(request,timeout=1260) as response: result=json.load(response)
    print(result['log_tail']); print(json.dumps({'job':result['job'],'exit_code':result['exit_code']}))
except urllib.error.HTTPError as exc:
    print(exc.read().decode()); raise
"""
    # stdin is data; command text is fixed. Credentials remain inside the container.
    subprocess.run(
        ["docker", "compose", "exec", "-T", "spark-runner", "python3", "-c", command],
        input=payload,
        text=True,
        cwd=ROOT,
        check=True,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--countries", default="CI,SN,ML,BF,GN,TG,BJ,GH")
    parser.add_argument("--date-from")
    parser.add_argument("--date-to")
    parser.add_argument("--report-date", default="2026-02-28")
    args = parser.parse_args()
    config = {
        "country_codes": args.countries.split(","),
        "date_from": args.date_from,
        "date_to": args.date_to,
        "report_date": args.report_date,
    }
    for job in (
        "ingest_raw",
        "raw_to_bronze",
        "bronze_to_silver",
        "silver_to_gold",
        "regulatory_report",
    ):
        run(job, config)
