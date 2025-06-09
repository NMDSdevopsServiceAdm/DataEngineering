import unittest

from unittest.mock import Mock, patch

import projects._03_independent_cqc._06_estimate_filled_posts.jobs.validate_estimated_ind_cqc_filled_posts_data as job
from tests.test_file_data import ValidateEstimatedIndCqcFilledPostsData as Data
from tests.test_file_schemas import ValidateEstimatedIndCqcFilledPostsData as Schemas

from utils import utils


PATCH_PATH = "projects._03_independent_cqc._06_estimate_filled_posts.jobs.validate_estimated_ind_cqc_filled_posts_data"


class ValidateEstimatedIndCqcFilledPostsDatasetTests(unittest.TestCase):
    TEST_CLEANED_IND_CQC_SOURCE = "some/directory"
    TEST_ESTIMATED_IND_CQC_FILLED_POSTS_SOURCE = "some/other/directory"
    TEST_DESTINATION = "some/other/other/directory"

    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.test_cleaned_ind_cqc_df = self.spark.createDataFrame(
            Data.cleaned_ind_cqc_rows,
            Schemas.cleaned_ind_cqc_schema,
        )
        self.test_estimated_ind_cqc_filled_posts_df = self.spark.createDataFrame(
            Data.estimated_ind_cqc_filled_posts_rows,
            Schemas.estimated_ind_cqc_filled_posts_schema,
        )

    def tearDown(self) -> None:
        if self.spark.sparkContext._gateway:
            self.spark.sparkContext._gateway.shutdown_callback_server()


class MainTests(ValidateEstimatedIndCqcFilledPostsDatasetTests):
    def setUp(self) -> None:
        return super().setUp()

    @patch(f"{PATCH_PATH}.utils.write_to_parquet")
    @patch(f"{PATCH_PATH}.utils.read_from_parquet")
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
                self.TEST_CLEANED_IND_CQC_SOURCE,
                self.TEST_ESTIMATED_IND_CQC_FILLED_POSTS_SOURCE,
                self.TEST_DESTINATION,
            )

            self.assertEqual(read_from_parquet_patch.call_count, 2)
            self.assertEqual(write_to_parquet_patch.call_count, 1)


class CalculateExpectedSizeofDataset(ValidateEstimatedIndCqcFilledPostsDatasetTests):
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
            job.calculate_expected_size_of_estimated_ind_cqc_filled_posts_dataset(
                test_df
            )
        )
        self.assertEqual(returned_row_count, expected_row_count)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
