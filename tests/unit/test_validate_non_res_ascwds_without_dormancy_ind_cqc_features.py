import unittest

from unittest.mock import Mock, patch

import jobs.validate_non_res_ascwds_without_dormancy_ind_cqc_features_data as job

from tests.test_file_data import (
    ValidateNonResASCWDSWithoutDormancyIndCqcFeaturesData as Data,
)
from tests.test_file_schemas import (
    ValidateNonResASCWDSIndCqcFeaturesData as Schemas,
)

from utils import utils


class ValidateNonResASCWDSWithoutDormancyIndCqcFeaturesDatasetTests(unittest.TestCase):
    TEST_CLEANED_IND_CQC_SOURCE = "some/directory"
    TEST_NON_RES_ASCWDS_INC_DORMANCY_IND_CQC_FEATURES_SOURCE = "some/other/directory"
    TEST_DESTINATION = "some/other/other/directory"

    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.test_cleaned_ind_cqc_df = self.spark.createDataFrame(
            Data.cleaned_ind_cqc_rows,
            Schemas.cleaned_ind_cqc_schema,
        )
        self.test_non_res_ascwds_without_dormancy_ind_cqc_features_df = (
            self.spark.createDataFrame(
                Data.non_res_ascwds_without_dormancy_ind_cqc_features_rows,
                Schemas.non_res_ascwds_ind_cqc_features_schema,
            )
        )

    def tearDown(self) -> None:
        if self.spark.sparkContext._gateway:
            self.spark.sparkContext._gateway.shutdown_callback_server()


class MainTests(ValidateNonResASCWDSWithoutDormancyIndCqcFeaturesDatasetTests):
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
            self.test_cleaned_ind_cqc_df,
            self.test_non_res_ascwds_without_dormancy_ind_cqc_features_df,
        ]

        with self.assertRaises(ValueError):
            job.main(
                self.TEST_CLEANED_IND_CQC_SOURCE,
                self.TEST_NON_RES_ASCWDS_INC_DORMANCY_IND_CQC_FEATURES_SOURCE,
                self.TEST_DESTINATION,
            )

            self.assertEqual(read_from_parquet_patch.call_count, 2)
            self.assertEqual(write_to_parquet_patch.call_count, 1)


class CalculateExpectedSizeofDataset(
    ValidateNonResASCWDSWithoutDormancyIndCqcFeaturesDatasetTests
):
    def setUp(self) -> None:
        return super().setUp()

    def test_calculate_expected_size_of_non_res_ascwds_without_dormancy_ind_cqc_features_dataset_returns_correct_row_count(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.calculate_expected_size_rows, Schemas.calculate_expected_size_schema
        )
        expected_row_count = 3
        returned_row_count = job.calculate_expected_size_of_non_res_ascwds_without_dormancy_ind_cqc_features_dataset(
            test_df
        )
        self.assertEqual(returned_row_count, expected_row_count)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
