"""Seed all four raw flows on a fresh environment, with explicit accounting inputs."""

import json
import os
from datetime import datetime
from waba.business_scenario import publish_scenario
from waba.publisher import publish
from waba.generator import COUNTRIES

month = os.environ.get("SCENARIO_MONTH", "2026-02-01")
result = publish_scenario(month)
start = datetime.fromisoformat(month)
# One day of small bank/mobile activity supplies every country, including GN/GH.
# Loan and insurance flows come from the coherent accounting scenario above.
for name in ("bank_transactions", "mobile_money_payments"):
    report = publish(
        list(COUNTRIES),
        100,
        start,
        start,
        batch_id="demo-v1-" + month,
        seed=42,
        dataset=name,
    )
    result[name] = {"rows": report["rows"], "files": len(report["files"])}
print(json.dumps(result))
