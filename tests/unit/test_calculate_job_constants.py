import unittest
from dataclasses import asdict

from utils.prepare_locations_utils.job_calculator.calculation_constants import JobCalculationConstants


class TestCalculateJobConstants(unittest.TestCase):
    def test_job_constants(self):
        expected_values = {'BEDS_IN_WORKPLACE_THRESHOLD': 0,
                           'BEDS_TO_JOB_COUNT_INTERCEPT': 8.40975704621392,
                           'BEDS_TO_JOB_COUNT_COEFFICIENT': 1.0010753137758377,
                           'MIN_ABS_DIFFERENCE_BETWEEN_TOTAL_STAFF_AND_WORKER_RECORD_COUNT': 5,
                           'MIN_PERCENTAGE_DIFFERENCE_BETWEEN_TOTAL_STAFF_AND_WORKER_RECORD_COUNT': 0.1,
                           'MIN_TOTAL_STAFF_VALUE_PERMITTED': 3,
                           'MIN_WORKER_RECORD_COUNT_PERMITTED': 3}

        self.assertEqual(expected_values, asdict(JobCalculationConstants()))
