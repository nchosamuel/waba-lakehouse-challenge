"""Coverage status is separate from successful technical report generation."""
from datetime import date

REGULATED_COUNTRIES = {"CI", "SN", "ML", "BF", "TG", "BJ"}


def country_coverage(country, report_date, snapshot_date, modelled_events,
                     unmodelled_events, max_snapshot_age_days=1):
    report_date = date.fromisoformat(str(report_date))
    snapshot = date.fromisoformat(str(snapshot_date)) if snapshot_date else None
    age = (report_date - snapshot).days if snapshot else None
    bceao = "MISSING_SNAPSHOT" if age is None else (
        "FUTURE_SNAPSHOT" if age < 0 else (
            "STALE_SNAPSHOT" if age > max_snapshot_age_days else "COVERED"
        )
    )
    cima = "NO_MODELLED_OPERATIONS" if modelled_events == 0 else (
        "PARTIAL_ACCOUNTING_COVERAGE" if unmodelled_events else "COVERED"
    )
    return {"country_code": country, "in_regulatory_scope": country in REGULATED_COUNTRIES,
            "snapshot_date": str(snapshot) if snapshot else None, "snapshot_age_days": age,
            "bceao_coverage": bceao, "cima_coverage": cima,
            "modelled_insurance_events": modelled_events,
            "unmodelled_insurance_events": unmodelled_events,
            "coverage_complete": bceao == "COVERED" and cima == "COVERED"}
