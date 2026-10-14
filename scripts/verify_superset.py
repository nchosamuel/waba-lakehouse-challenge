"""Exercise all dashboard chart queries and country filters through Superset API."""

import copy
from datetime import datetime, timezone
import http.cookiejar
import json
from pathlib import Path
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
values = dict(
    line.split("=", 1)
    for line in (ROOT / ".env").read_text().splitlines()
    if "=" in line and not line.startswith("#")
)
base = "http://localhost:8088"
client = urllib.request.build_opener(
    urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar())
)
headers = {"Content-Type": "application/json"}


def request(path, data=None):
    req = urllib.request.Request(
        base + path,
        data=json.dumps(data).encode() if data is not None else None,
        headers=headers,
    )
    with client.open(req, timeout=180) as response:
        return json.load(response)


login = request(
    "/api/v1/security/login",
    {
        "username": values["SUPERSET_ADMIN_USER"],
        "password": values["SUPERSET_ADMIN_PASSWORD"],
        "provider": "db",
        "refresh": True,
    },
)
headers["Authorization"] = "Bearer " + login["access_token"]
headers["X-CSRFToken"] = request("/api/v1/security/csrf_token/")["result"]
charts = request(
    "/api/v1/chart/?q=" + urllib.parse.quote(json.dumps({"page_size": 100}))
)["result"]
checks = []
for chart in charts:
    detail = request("/api/v1/chart/" + str(chart["id"]))["result"]
    context = json.loads(detail["query_context"])
    context["force"] = True
    result = request("/api/v1/chart/data", context)["result"][0]
    assert result.get("status") == "success", (chart["slice_name"], result.get("error"))
    data = result["data"]
    if chart["slice_name"].startswith("NPL"):
        expected = {
            "BF": 6,
            "BJ": 4,
            "CI": 3,
            "GH": 5,
            "GN": 7,
            "ML": 5,
            "SN": 4,
            "TG": 8,
        }
        assert len(data) == 8
        for row in data:
            assert abs(float(row["NPL (%)"]) - expected[row["country_code"]]) < 0.00001
    assert len(data) > 0, chart["slice_name"]
    all_countries = {row["country_code"] for row in data}
    assert all_countries <= {
        "CI",
        "SN",
        "ML",
        "BF",
        "GN",
        "TG",
        "BJ",
        "GH",
    }, all_countries
    filtered = copy.deepcopy(context)
    filtered["queries"][0]["filters"] = [
        {"col": "country_code", "op": "IN", "val": ["CI"]}
    ]
    country_result = request("/api/v1/chart/data", filtered)["result"][0]
    assert country_result.get("status") == "success", country_result.get("error")
    assert country_result["data"] and {
        row["country_code"] for row in country_result["data"]
    } == {"CI"}, chart["slice_name"]
    checks.append(
        {
            "chart": chart["slice_name"],
            "rows": len(data),
            "countries": sorted(all_countries),
            "ci_filter_verified": True,
        }
    )
dashboards = request(
    "/api/v1/dashboard/?q=" + urllib.parse.quote(json.dumps({"page_size": 100}))
)["result"]
assert len(checks) == 11 and len(dashboards) == 3, (len(checks), len(dashboards))
for dashboard in dashboards:
    detail = request("/api/v1/dashboard/" + str(dashboard["id"]))["result"]
    metadata = json.loads(detail["json_metadata"])
    assert any(
        item["name"] == "Pays" for item in metadata["native_filter_configuration"]
    )
report = {
    "verified_at_utc": datetime.now(timezone.utc).isoformat(),
    "superset_version": "5.0.0",
    "dashboards": 3,
    "charts": checks,
    "trino_data_verified": True,
    "country_filter_verified": True,
    "interactive_sso_tested_by_this_script": False,
    "sso_verification_report": "docs/sso-verification.json",
    "subscription_model": "SUBSCRIPTIONS_SIMULATED_V1",
    "claims_sla_model": "SLA_SIMULATED_V1",
}
(ROOT / "docs/superset-verification.json").write_text(
    json.dumps(report, indent=2, ensure_ascii=False) + "\n"
)
print(json.dumps(report, ensure_ascii=False))
