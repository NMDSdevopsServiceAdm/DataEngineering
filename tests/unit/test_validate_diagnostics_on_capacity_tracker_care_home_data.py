import unittest
from unittest.mock import Mock, patch

import jobs.validate_diagnostics_on_capacity_tracker_care_home_data as job
from tests.test_file_data import (
    ValidateDiagnosticsOnCapacityTrackerCareHomeData as Data,  # add test data and then ready to commit
)
from tests.test_file_schemas import (
    ValidateDiagnosticsOnCapacityTrackerCareHomeData as Schemas,
)
from utils import utils


class ValidateDiagnosticsOnCapacityTrackerCareHomeDatasetTests(unittest.TestCase):
    TEST_DIAGNOSTICS_CT_CARE_HOME_SOURCE = "some/other/directory"
    TEST_DESTINATION = "some/other/other/directory"

    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.test_diagnostics_ct_care_home_df = self.spark.createDataFrame(
            Data.diagnostics_ct_care_home_rows, Schemas.diagnostics_ct_care_home_schema
        )

    def tearDown(self) -> None:
        if self.spark.sparkContext._gateway:
            self.spark.sparkContext._gateway.shutdown_callback_server()


class MainTests(ValidateDiagnosticsOnCapacityTrackerCareHomeDatasetTests):
    def setUp(self) -> None:
        return super().setUp()

    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main_runs(
        self,
        read_from_parquet_patch: Mock,
        write_to_parquet_patch: Mock,
    ):
        read_from_parquet_patch.side_effect = [
            self.test_diagnostics_ct_care_home_df,
        ]

        with self.assertRaises(ValueError):
            job.main(
                self.TEST_DIAGNOSTICS_CT_CARE_HOME_SOURCE,
                self.TEST_DESTINATION,
            )

            self.assertEqual(read_from_parquet_patch.call_count, 2)
            self.assertEqual(write_to_parquet_patch.call_count, 1)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
