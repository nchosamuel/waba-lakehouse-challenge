import unittest
import csv
import io
from unittest.mock import MagicMock, patch
from collections import defaultdict
from datetime import date
from waba.business_scenario import scenario, NPL_TARGET, LOSS_TARGET
from waba.pipeline_config import scope


class BusinessScenarioTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data = scenario()

    def test_weighted_npl_uses_outstanding_not_instalments(self):
        total, doubtful = defaultdict(float), defaultdict(float)
        for row in self.data["loan_positions"]:
            total[row["country_code"]] += row["outstanding_amount"]
            if row["default_flag"]:
                doubtful[row["country_code"]] += row["outstanding_amount"]
        for country, percentage in NPL_TARGET.items():
            self.assertAlmostEqual(doubtful[country] / total[country], percentage / 100)

    def test_earned_premiums_and_paid_claims_by_product(self):
        premium, claims = defaultdict(float), defaultdict(float)
        for row in self.data["flows"]["insurance_operations"]:
            key = (row["country_code"], row["product_line"])
            if row["operation_type"] == "PREMIUM_PAYMENT":
                premium[key] += row["amount"]
            if row["operation_type"] == "CLAIM_PAYMENT":
                claims[key] += row["amount"]
        for key, amount in premium.items():
            self.assertAlmostEqual(claims[key] / amount, LOSS_TARGET[key[0]], places=6)
        for row in self.data["accounting_context"]:
            if row["claim_paid_date"]:
                start, end = map(
                    date.fromisoformat,
                    (row["claim_submitted_date"], row["claim_paid_date"]),
                )
                days = sum(
                    date.fromordinal(day).weekday() < 5
                    for day in range(start.toordinal() + 1, end.toordinal() + 1)
                )
                self.assertEqual(days, 10)

    def test_cross_border_receiver_matches_actual_reference_country(self):
        for row in self.data["flows"]["mobile_money_payments"]:
            self.assertNotEqual(row["sender_country"], row["receiver_country"])
            self.assertNotIn(row["receiver_country"], ("GH", "GN"))
            self.assertTrue(
                row["receiver_id"].startswith("WABA-" + row["receiver_country"] + "-")
            )
        identifiers = [
            r["operation_id"] for r in self.data["flows"]["insurance_operations"]
        ]
        self.assertEqual(len(identifiers), len(set(identifiers)))
        self.assertEqual(self.data, scenario())

    def test_loan_dates_match_recorded_arrears(self):
        for row in self.data["flows"]["loan_repayments"]:
            observed = date.fromisoformat(row["timestamp"][:10])
            due = date.fromisoformat(row["due_date"])
            if row["repayment_status"] == "DEFAULT":
                self.assertEqual((observed - due).days, row["days_overdue"])
            else:
                self.assertEqual(observed.isoformat(), row["payment_date"])
                self.assertEqual(row["days_overdue"], 0)

    def test_published_dates_support_selective_backfill(self):
        from waba.business_scenario import publish_scenario

        client = MagicMock()
        client.head_object.return_value = {"Metadata": {"profile": "full-v1"}}
        with patch("waba.publisher.client", return_value=client):
            publish_scenario()
        event_files = 0
        for call in client.put_object.call_args_list:
            values = call.kwargs
            if values["Key"].endswith(".csv"):
                day = values["Key"].split("/")[2]
                for row in csv.DictReader(io.StringIO(values["Body"].decode())):
                    self.assertEqual(row["timestamp"][:10], day)
                event_files += 1
        self.assertGreater(event_files, 24)
        client.reset_mock()
        client.head_object.return_value = {"Metadata": {}}
        with patch("waba.publisher.client", return_value=client):
            with self.assertRaises(ValueError):
                publish_scenario()
        client.put_object.assert_not_called()

    def test_scope_rejects_invalid_and_reversed_backfills(self):
        for config in (
            {"country_codes": ["ZZ"]},
            {"country_codes": ["CI", "CI"]},
            {"country_codes": []},
            {"date_from": "2026-03-01", "date_to": "2026-02-01"},
        ):
            with self.assertRaises(ValueError):
                scope(config)
        self.assertEqual(
            scope({"country_codes": ["CI"], "date_from": "2026-02-01"})[:2],
            (["CI"], "2026-02-01"),
        )
