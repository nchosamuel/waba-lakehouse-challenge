import unittest
from waba.report_coverage import country_coverage


class ReportCoverageTest(unittest.TestCase):
    def test_empty_october_cannot_certify_february_snapshot(self):
        value = country_coverage("CI", "2026-10-13", "2026-02-28", 0, 0)
        self.assertEqual(value["bceao_coverage"], "STALE_SNAPSHOT")
        self.assertEqual(value["cima_coverage"], "NO_MODELLED_OPERATIONS")
        self.assertFalse(value["coverage_complete"])

    def test_complete_period_and_partial_accounting(self):
        self.assertTrue(country_coverage("CI", "2026-02-28", "2026-02-28", 100, 0)["coverage_complete"])
        self.assertFalse(country_coverage("CI", "2026-02-28", "2026-02-28", 100, 1)["coverage_complete"])

    def test_missing_future_and_scope(self):
        self.assertEqual(country_coverage("CI", "2026-02-28", None, 1, 0)["bceao_coverage"], "MISSING_SNAPSHOT")
        self.assertEqual(country_coverage("CI", "2026-02-28", "2026-03-01", 1, 0)["bceao_coverage"], "FUTURE_SNAPSHOT")
        self.assertFalse(country_coverage("GH", "2026-02-28", "2026-02-28", 1, 0)["in_regulatory_scope"])
