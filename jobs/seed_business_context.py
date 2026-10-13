import json
import os
from waba.business_scenario import publish_scenario

print(json.dumps(publish_scenario(os.environ.get("SCENARIO_MONTH", "2026-02-01"))))
