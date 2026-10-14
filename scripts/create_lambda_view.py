"""Unified batch/stream totals from one atomic batch fact snapshot."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from waba.lambda_snapshot import lambda_sql
from verification_runtime import query

query("CREATE OR REPLACE VIEW iceberg.gold.lambda_daily_transaction_volume AS " + lambda_sql())
print("Lambda view created.")
