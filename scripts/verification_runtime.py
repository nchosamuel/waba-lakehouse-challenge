"""Explicit runtime selection for the same integration assertions on Compose/K8s."""

import csv
import io
import json
import os
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]
KUBECTL = (
    str(ROOT / ".tools/kubectl") if (ROOT / ".tools/kubectl").is_file() else "kubectl"
)


def command(service, arguments):
    if os.environ.get("WABA_RUNTIME", "compose") == "kubernetes":
        namespace = "serving" if service in ("app", "trino") else "processing"
        target = "deployment/" + service
        if service == "streaming":
            resource = subprocess.check_output(
                [
                    KUBECTL,
                    "--context",
                    "kind-waba",
                    "-n",
                    namespace,
                    "get",
                    "sparkapplication",
                    "waba-streaming",
                    "-o",
                    "json",
                ],
                text=True,
            )
            target = json.loads(resource)["status"]["driverInfo"]["podName"]
        return [
            KUBECTL,
            "--context",
            "kind-waba",
            "-n",
            namespace,
            "exec",
            target,
            "--",
            *arguments,
        ]
    prefix = ["docker", "compose"]
    if service == "streaming":
        prefix += [
            "-f",
            "compose.yaml",
            "-f",
            "compose.streaming.yaml",
            "--profile",
            "streaming",
        ]
    return [*prefix, "exec", "-T", service, *arguments]


def output(service, arguments):
    try:
        return subprocess.check_output(
            command(service, arguments), cwd=ROOT, text=True, stderr=subprocess.PIPE
        )
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(exc.stderr.strip() or "Runtime command failed") from exc


def query(sql):
    # Avoid launching a second Java VM inside the Trino coordinator's memory cap.
    script = """import json,sys,urllib.request
request=urllib.request.Request('http://trino:8080/v1/statement',data=sys.argv[1].encode(),headers={'X-Trino-User':'waba-validation'})
with urllib.request.urlopen(request,timeout=120) as response: result=json.load(response)
rows=[]
while True:
    if 'error' in result: raise RuntimeError(result['error']['message'])
    rows.extend(result.get('data',[]))
    if 'nextUri' not in result: break
    with urllib.request.urlopen(result['nextUri'],timeout=120) as response: result=json.load(response)
print(json.dumps(rows))
"""
    return json.loads(output("app", ["python", "-c", script, sql]))


def report_path(default):
    override = os.environ.get("WABA_REPORT_PATH")
    return ROOT / (override or "docs/" + default)
