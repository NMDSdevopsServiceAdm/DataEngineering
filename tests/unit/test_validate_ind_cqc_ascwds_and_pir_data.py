import unittest

from unittest.mock import Mock, patch

import jobs.validate_imputed_ind_cqc_ascwds_and_pir_data as job

from tests.test_file_data import ValidateImputedIndCqcAscwdsAndPir as Data
from tests.test_file_schemas import (
    ValidateImputedIndCqcAscwdsAndPir as Schemas,
)

from utils import utils


class ValidateImputedIndCqcAscwdsAndPirsetTests(unittest.TestCase):
    TEST_CLEANED_SOURCE = "some/directory"
    TEST_IMPUTED_IND_CQC_ASCWDS_AND_PIR_SOURCE = "some/other/directory"
    TEST_DESTINATION = "some/other/other/directory"

    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.test_cleaned_ind_cqc_df = self.spark.createDataFrame(
            Data.cleaned_ind_cqc_rows,
            Schemas.cleaned_ind_cqc_schema,
        )
        self.test_estimated_ind_cqc_filled_posts_df = self.spark.createDataFrame(
            Data.imputed_ind_cqc_ascwds_and_pir_rows,
            Schemas.imputed_ind_cqc_ascwds_and_pir_schema,
        )

    def tearDown(self) -> None:
        if self.spark.sparkContext._gateway:
            self.spark.sparkContext._gateway.shutdown_callback_server()


class MainTests(ValidateImputedIndCqcAscwdsAndPirsetTests):
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
            self.test_estimated_ind_cqc_filled_posts_df,
        ]

        with self.assertRaises(ValueError):
            job.main(
                self.TEST_CLEANED_SOURCE,
                self.TEST_IMPUTED_IND_CQC_ASCWDS_AND_PIR_SOURCE,
                self.TEST_DESTINATION,
            )

            self.assertEqual(read_from_parquet_patch.call_count, 2)
            self.assertEqual(write_to_parquet_patch.call_count, 1)


class CalculateExpectedSizeofDataset(ValidateImputedIndCqcAscwdsAndPirsetTests):
    def setUp(self) -> None:
        return super().setUp()

    def test_calculate_expected_size_of_estimated_ind_cqc_filled_posts_dataset_returns_correct_row_count(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.calculate_expected_size_rows, Schemas.calculate_expected_size_schema
        )
        expected_row_count = 1
        returned_row_count = (
            job.calculate_expected_size_of_imputed_ind_cqc_ascwds_and_pir_dataset(
                test_df
            )
        )
        self.assertEqual(returned_row_count, expected_row_count)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
