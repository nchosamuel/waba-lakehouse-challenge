"""Verify isolated Spark regression snapshots through actual Trino SQL."""
import json
import argparse
import fcntl
import sys
from pathlib import Path
from datetime import datetime, timezone
from verification_runtime import query, output, report_path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from waba.lambda_snapshot import lambda_sql

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--run-spark", action="store_true", help="Run the isolated native Spark fixtures first; requires a prepared kind batch profile")
args = parser.parse_args()
if args.run_spark:
    from kubernetes import execute, ROOT
    lock = (ROOT / ".tools/kubernetes.lock").open("w")
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    execute("verify_review_fixes")

report = json.loads(output("app", ["python", "-c",
    "from waba.publisher import client; print(client().get_object(Bucket='archive',Key='verification/review-fixes.json')['Body'].read().decode())"]))
assert report["batch_table"] == "iceberg.gold.review_lambda_batch"
assert report["stream_table"] == "iceberg.gold.review_lambda_stream"
checks = {}
for name, version in [("before_publication", report["old_snapshot_id"]),
                      ("after_injected_interruption", report["after_interruption_snapshot_id"]),
                      ("after_retry", None)]:
    batch = report["batch_table"]
    if version is not None:
        batch += " FOR VERSION AS OF " + str(int(version))
    rows = query(lambda_sql(batch, report["stream_table"]))
    amounts = {r[0]: float(r[5]) for r in rows}
    assert amounts == {"CI": 30.0, "GH": 7.0}, (name, amounts)
    checks[name] = {"total_eur": sum(amounts.values()), "country_totals": amounts}
assert query("SELECT count(*) FROM " + report["batch_table"])[0][0] == 3
report["trino_snapshot_checks"] = checks
report["trino_verified_at_utc"] = datetime.now(timezone.utc).isoformat()
for table in (report["stream_table"], report["batch_table"]):
    query("DROP TABLE " + table)
report["isolated_fixtures_removed"] = True
report_path("review-fixes-verification.json").write_text(json.dumps(report, indent=2)+"\n")
print("Native Spark/Iceberg late-event rules and Trino interruption/retry totals verified.")
