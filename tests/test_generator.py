import tempfile
import unittest
from waba.generator import generate


class GeneratorTests(unittest.TestCase):
    def test_referential_integrity_and_country_currency(self):
        with tempfile.TemporaryDirectory() as directory:
            for country in ("CI", "GH"):
                data = generate(directory, country, 1000)
                accounts = {r["account_id"]: r for r in data["accounts"]}
                customers = {r["customer_id"] for r in data["customers"]}
                branches = {r["branch_id"] for r in data["branches"]}
                ids = set()
                for row in data["bank_transactions"]:
                    self.assertIn(row["account_id"], accounts)
                    self.assertIn(row["beneficiary_account"], accounts)
                    self.assertIn(row["customer_id"], customers)
                    self.assertEqual(
                        accounts[row["account_id"]]["customer_id"], row["customer_id"]
                    )
                    self.assertIn(row["branch_id"], branches)
                    self.assertEqual(
                        row["currency"], "GHS" if country == "GH" else "XOF"
                    )
                    self.assertGreater(row["amount"], 0)
                    ids.add(row["transaction_id"])
                self.assertEqual(len(ids), 1000)

    def test_reproducible_and_cross_country_distinct(self):
        with tempfile.TemporaryDirectory() as directory:
            first = generate(directory, "CI", 100)
            self.assertEqual(first, generate(directory, "CI", 100))
            second = generate(directory, "SN", 100)
            self.assertTrue(
                {r["transaction_id"] for r in first["bank_transactions"]}.isdisjoint(
                    {r["transaction_id"] for r in second["bank_transactions"]}
                )
            )

    def test_batch_ids_and_requested_dates(self):
        from datetime import datetime

        with tempfile.TemporaryDirectory() as directory:
            kwargs = dict(start=datetime(2026, 2, 1), end=datetime(2026, 2, 3))
            first = generate(directory, "CI", 100, batch_id="one", **kwargs)
            second = generate(directory, "CI", 100, batch_id="two", **kwargs)
            self.assertTrue(
                {r["transaction_id"] for r in first["bank_transactions"]}.isdisjoint(
                    {r["transaction_id"] for r in second["bank_transactions"]}
                )
            )
            for row in first["bank_transactions"]:
                self.assertGreaterEqual(row["timestamp"], "2026-02-01")
                self.assertLess(row["timestamp"], "2026-02-04")
            with self.assertRaises(ValueError):
                generate(
                    directory, start=datetime(2026, 2, 3), end=datetime(2026, 2, 1)
                )

    def test_all_flows_all_countries(self):
        from waba.generator import COUNTRIES
        from waba.contracts import SCHEMAS, KEYS, FLOW_PREFIX

        with tempfile.TemporaryDirectory() as directory:
            for country in COUNTRIES:
                data = generate(directory, country, 50)
                accounts = {r["account_id"]: r for r in data["accounts"]}
                customers = {r["customer_id"]: r for r in data["customers"]}
                for name, records in data.items():
                    self.assertEqual(
                        list(records[0]),
                        [x.split(":")[0] for x in SCHEMAS[name].split()],
                    )
                    self.assertEqual(
                        len({r[KEYS[name]] for r in records}), len(records)
                    )
                    for row in records:
                        self.assertEqual(row["country_code"], country)
                for row in data["insurance_operations"]:
                    account = accounts[row["account_id"]]
                    self.assertEqual(account["account_type"], "INSURANCE_POLICY")
                    self.assertEqual(account["customer_id"], row["customer_id"])
                    self.assertEqual(
                        customers[row["customer_id"]]["entity_type"], "INSURANCE"
                    )
                for row in data["mobile_money_payments"]:
                    for key in ("sender_id", "receiver_id"):
                        self.assertEqual(
                            customers[row[key]]["entity_type"], "MOBILE_MONEY"
                        )
                    self.assertEqual(
                        row["currency"], "GHS" if country == "GH" else "XOF"
                    )
                for row in data["loan_repayments"]:
                    account = accounts[row["loan_account_id"]]
                    self.assertEqual(account["account_type"], "LOAN")
                    self.assertEqual(account["customer_id"], row["customer_id"])
                    if row["repayment_status"] == "DEFAULT":
                        self.assertEqual(row["amount_paid"], 0)
                        self.assertEqual(row["payment_date"], "")

    def test_event_identifiers_have_uuid4_layout(self):
        import uuid
        from waba.contracts import FLOW_PREFIX, KEYS

        with tempfile.TemporaryDirectory() as directory:
            data = generate(directory, "CI", 10)
            for name in FLOW_PREFIX:
                for row in data[name]:
                    self.assertEqual(uuid.UUID(row[KEYS[name]]).version, 4)

    def test_microfinance_transactions_use_matching_references(self):
        with tempfile.TemporaryDirectory() as directory:
            for profile in ("demo", "full"):
                data = generate(
                    directory,
                    "CI",
                    30,
                    datasets_to_generate=["bank_transactions"],
                    reference_profile=profile,
                    bank_entity="MICROFINANCE",
                )
                if profile == "demo":
                    refs = {r["account_id"]: r for r in data["accounts"]}
                    for row in data["bank_transactions"]:
                        self.assertEqual(
                            refs[row["account_id"]]["entity_type"], "MICROFINANCE"
                        )
                self.assertTrue(
                    all(
                        r["entity_type"] == "MICROFINANCE"
                        for r in data["bank_transactions"]
                    )
                )
                first = generate(
                    directory, "CI", 30, datasets_to_generate=["bank_transactions"]
                )
                self.assertTrue(
                    {
                        r["transaction_id"] for r in first["bank_transactions"]
                    }.isdisjoint(
                        {r["transaction_id"] for r in data["bank_transactions"]}
                    )
                )
