"""Run deterministic and mocked tests without a Docker/S3 dependency."""

import sys
import unittest

suite = unittest.TestSuite()
for pattern in (
    "test_observability.py",
    "test_report_coverage.py",
    "test_generator.py",
    "test_kubernetes.py",
    "test_realtime_publisher.py",
    "test_reference_scale.py",
    "test_full_references.py",
    "test_business_scenario.py",
):
    suite.addTests(unittest.defaultTestLoader.discover("tests", pattern=pattern))
result = unittest.TextTestRunner(verbosity=2).run(suite)
sys.exit(not result.wasSuccessful())
