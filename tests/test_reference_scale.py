import tempfile
import unittest
from pathlib import Path
from waba.reference_scale import TARGETS, count_for, reference_row, baseline, write_reference
from waba.generator import COUNTRIES
from waba.contracts import REFERENCES


class ReferenceScaleTests(unittest.TestCase):
    def test_exact_global_counts_and_preserved_keys(self):
        for name in REFERENCES:
            self.assertEqual(sum(count_for(name, cc) for cc in COUNTRIES), TARGETS[name])
        for cc in COUNTRIES:
            for name in REFERENCES:
                for i, row in enumerate(baseline(cc)[name]):
                    self.assertEqual(reference_row(name, cc, i), row)

    def test_large_account_customer_mapping(self):
        for cc in COUNTRIES:
            for i in (300, 62500, 99999):
                account = reference_row('accounts', cc, i)
                customer = reference_row('customers', cc, i % count_for('customers', cc))
                self.assertEqual(account['customer_id'], customer['customer_id'])
                self.assertEqual(account['entity_type'], customer['entity_type'])
                self.assertEqual(account['currency'], 'GHS' if cc == 'GH' else 'XOF')

    def test_small_streamed_file_and_checksum(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'branches.csv'
            report = write_reference(path, 'branches', 'CI')
            self.assertEqual(report['rows'], 25)
            self.assertEqual(len(report['sha256']), 64)
            self.assertEqual(len(path.read_text().splitlines()), 26)

    def test_transactions_sample_full_references(self):
        from waba.generator import generate
        with tempfile.TemporaryDirectory() as folder:
            for cc in ('CI','GH'):
                data = generate(folder, cc, 100, reference_profile='full')
                by_id = {r['account_id']:r for r in baseline(cc)['accounts']}
                def account(key):
                    if key in by_id:
                        return by_id[key]
                    return reference_row('accounts', cc, int(key.rsplit('-',1)[1]))
                self.assertTrue(any('-SA-' in r['account_id'] for r in data['bank_transactions']))
                for row in data['bank_transactions'] + data['insurance_operations']:
                    self.assertEqual(account(row['account_id'])['customer_id'], row['customer_id'])
                for row in data['loan_repayments']:
                    self.assertEqual(account(row['loan_account_id'])['account_type'], 'LOAN')
                    self.assertEqual(account(row['loan_account_id'])['customer_id'], row['customer_id'])
