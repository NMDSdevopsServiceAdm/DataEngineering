from dataclasses import asdict

from projects._03_independent_cqc._02_clean.utils.ascwds_filled_posts_calculator.calculation_constants import (
    ASCWDSFilledPostCalculationConstants,
)
from tests.base_test import SparkBaseTest


class TestASCWDSFilledPostCalculationConstants(SparkBaseTest):
    def test_calculation_constants(self):
        expected_values = {
            "MAX_ABSOLUTE_DIFFERENCE_BETWEEN_TOTAL_STAFF_AND_WORKER_RECORD_COUNT": 5,
            "MAX_PERCENTAGE_DIFFERENCE_BETWEEN_TOTAL_STAFF_AND_WORKER_RECORD_COUNT": 0.1,
            "MIN_ASCWDS_FILLED_POSTS_PERMITTED": 3,
        }

        self.assertEqual(
            expected_values, asdict(ASCWDSFilledPostCalculationConstants())
        )
