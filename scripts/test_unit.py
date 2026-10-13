"""Run deterministic and mocked tests without a Docker/S3 dependency."""

import sys
import unittest

suite = unittest.TestSuite()
for pattern in (
    "test_generator.py",
    "test_reference_scale.py",
    "test_full_references.py",
    "test_business_scenario.py",
):
    suite.addTests(unittest.defaultTestLoader.discover("tests", pattern=pattern))
result = unittest.TextTestRunner(verbosity=2).run(suite)
sys.exit(not result.wasSuccessful())
