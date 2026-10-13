"""Validated runtime scope shared by Spark jobs and Airflow tasks."""

import os
from datetime import date
from waba.generator import COUNTRIES


def scope(config=None):
    config = config or {}
    selected = config.get(
        "country_codes", os.environ.get("COUNTRY_CODES", ",".join(COUNTRIES))
    )
    selected = selected.split(",") if isinstance(selected, str) else selected
    if (
        not selected
        or len(set(selected)) != len(selected)
        or any(c not in COUNTRIES for c in selected)
    ):
        raise ValueError(
            "country_codes must be a nonempty list of unique supported ISO codes"
        )
    start = config.get("date_from", os.environ.get("DATE_FROM")) or None
    end = config.get("date_to", os.environ.get("DATE_TO")) or None
    for value in (start, end):
        if value:
            date.fromisoformat(value)
    if start and end and start > end:
        raise ValueError("date_from must precede date_to")
    return selected, start, end
