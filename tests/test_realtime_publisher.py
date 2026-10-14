"""Realtime events use the publication clock and coherent repayment dates."""

import csv, io, unittest
from datetime import datetime, timezone, date
from unittest.mock import MagicMock, patch
from waba.publisher import publish


class RealtimePublisherTests(unittest.TestCase):
    def records(self, dataset):
        client = MagicMock()
        client.head_object.return_value = {
            "Metadata": {"profile": "full-v1", "rows": "1"}
        }
        before = datetime.now(timezone.utc).replace(tzinfo=None)
        with patch("waba.publisher.client", return_value=client):
            publish(
                ["CI"],
                100,
                datetime(2026, 1, 1),
                datetime(2026, 1, 1),
                dataset=dataset,
                realtime=True,
            )
        after = datetime.now(timezone.utc).replace(tzinfo=None)
        records = []
        for call in client.put_object.call_args_list:
            records.extend(csv.DictReader(io.StringIO(call.kwargs["Body"].decode())))
        self.assertEqual(len(records), 100)
        for record in records:
            timestamp = datetime.fromisoformat(record["timestamp"])
            self.assertLessEqual(before, timestamp)
            self.assertLessEqual(timestamp, after)
        return records

    def test_realtime_replaces_historical_simulation_dates(self):
        self.records("bank_transactions")

    def test_realtime_loan_dates_match_reported_arrears(self):
        for record in self.records("loan_repayments"):
            observed = datetime.fromisoformat(record["timestamp"]).date()
            due = date.fromisoformat(record["due_date"])
            self.assertEqual((observed - due).days, int(record["days_overdue"]))
            if record["repayment_status"] == "DEFAULT":
                self.assertEqual(record["payment_date"], "")
            else:
                self.assertEqual(record["payment_date"], observed.isoformat())
