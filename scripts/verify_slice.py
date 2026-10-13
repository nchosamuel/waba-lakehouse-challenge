"""Verify separate generation, multi-country ingestion, replay and empty landing."""

import csv
import io
import json
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def run(*args):
    return subprocess.check_output(["docker", "compose", *args], cwd=ROOT, text=True)


def query(sql):
    return list(
        csv.reader(
            io.StringIO(
                run(
                    "exec",
                    "-T",
                    "trino",
                    "trino",
                    "--output-format",
                    "CSV",
                    "--execute",
                    sql,
                )
            )
        )
    )


# A new deterministic fixture namespace per test run, so baseline is independent of old demo data.
import uuid

batch = "check-" + uuid.uuid4().hex
code = f"""
import json
from datetime import datetime
from waba.publisher import publish, client
report = publish(['CI','GH'], 25, datetime(2026,1,2), datetime(2026,1,2), batch_id={batch!r}, seed=123)
print(json.dumps(report))
"""
run("run", "--rm", "spark")
before = int(query("SELECT count(*) FROM iceberg.raw.bank_transactions")[0][0])
report = json.loads(
    run("run", "--rm", "--entrypoint", "python3", "spark", "-c", code).strip()
)
results = []
for iteration in (1, 2):
    if iteration == 2:
        replay = f"""
from waba.publisher import client
s3=client()
for key in {report['files']!r}:
    s3.copy_object(Bucket='raw-landing', Key=key, CopySource={{'Bucket':'archive','Key':key}})
"""
        run("run", "--rm", "--entrypoint", "python3", "spark", "-c", replay)
    run("run", "--rm", "spark")
    counts = [
        int(v)
        for v in query(
            "SELECT count(*), count(DISTINCT transaction_id) FROM iceberg.raw.bank_transactions"
        )[0]
    ]
    assert counts == [before + 50, before + 50], counts
    results.append(
        {"iteration": iteration, "rows": counts[0], "distinct_ids": counts[1]}
    )
# A run with nothing pending must not generate or change data.
run("run", "--rm", "spark")
assert (
    int(query("SELECT count(*) FROM iceberg.raw.bank_transactions")[0][0])
    == before + 50
)
# An entirely invalid file is quarantined and archived, without changing the table.
invalid_key = f"CI/bank_transactions/2026-01-02/invalid-{batch}.csv"
invalid = f"""
import csv, io
from waba.publisher import client, csv_bytes
s3=client()
text=s3.get_object(Bucket='archive',Key={report['files'][0]!r})['Body'].read().decode()
row=next(csv.DictReader(io.StringIO(text)))
row['amount']='-1'
s3.put_object(Bucket='raw-landing',Key={invalid_key!r},Body=csv_bytes([row]))
"""
run("run", "--rm", "--entrypoint", "python3", "spark", "-c", invalid)
run("run", "--rm", "spark")
run(
    "run",
    "--rm",
    "--entrypoint",
    "python3",
    "spark",
    "-c",
    f"""
import json
from waba.publisher import client
s3=client()
report=json.loads(s3.get_object(Bucket='archive',Key='reports/ingestion/'+{invalid_key!r}+'.json')['Body'].read())
assert report['accepted_rows']==0 and report['rejected_rows']==1, report
s3.head_object(Bucket='archive',Key='quarantine/'+{invalid_key!r}+'.jsonl')
s3.head_object(Bucket='archive',Key={invalid_key!r})
""",
)
assert (
    int(query("SELECT count(*) FROM iceberg.raw.bank_transactions")[0][0])
    == before + 50
)
result = {
    "separate_generation_verified": True,
    "countries": ["CI", "GH"],
    "new_rows": 50,
    "replay_verified": True,
    "empty_landing_verified": True,
    "invalid_rows_quarantined_verified": True,
    "results": results,
}
(ROOT / "docs" / "verification.json").write_text(json.dumps(result, indent=2) + "\n")
print(json.dumps(result))
