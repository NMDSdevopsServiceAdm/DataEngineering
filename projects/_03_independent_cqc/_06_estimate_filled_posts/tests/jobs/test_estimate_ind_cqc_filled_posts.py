import unittest
from unittest.mock import ANY, Mock, patch

import projects._03_independent_cqc._06_estimate_filled_posts.jobs.estimate_ind_cqc_filled_posts as job
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

PATCH_PATH = "projects._03_independent_cqc._06_estimate_filled_posts.jobs.estimate_ind_cqc_filled_posts"


class EstimateIndCQCFilledPostsTests(unittest.TestCase):
    TEST_BUCKET_NAME = "test-bucket"
    SOURCE_TEST_DATA = "some/cleaned/data"
    ESTIMATES_DESTINATION = "estimates/destination"
    partition_keys = [
        Keys.year,
        Keys.month,
        Keys.day,
        Keys.import_date,
    ]

    def setUp(self):
        self.spark = utils.get_spark()

        self.mock_ind_cqc_df = Mock(name="ind_cqc_df")

    @patch(f"{PATCH_PATH}.utils.write_to_parquet")
    @patch(f"{PATCH_PATH}.estimate_non_res_capacity_tracker_filled_posts")
    @patch(f"{PATCH_PATH}.merge_columns_in_order")
    @patch(f"{PATCH_PATH}.model_imputation_with_extrapolation_and_interpolation")
    @patch(f"{PATCH_PATH}.combine_non_res_with_and_without_dormancy_models")
    @patch(f"{PATCH_PATH}.join_model_predictions")
    @patch(f"{PATCH_PATH}.utils.read_from_parquet")
    def test_main_runs(
        self,
        read_from_parquet_patch: Mock,
        join_model_predictions_patch: Mock,
        combine_non_res_with_and_without_dormancy_models_patch: Mock,
        model_imputation_with_extrapolation_and_interpolation: Mock,
        merge_columns_in_order_mock: Mock,
        estimate_non_res_capacity_tracker_filled_posts_mock: Mock,
        write_to_parquet_patch: Mock,
    ):
        read_from_parquet_patch.return_value = self.mock_ind_cqc_df

        job.main(
            self.TEST_BUCKET_NAME,
            self.SOURCE_TEST_DATA,
            self.ESTIMATES_DESTINATION,
        )

        read_from_parquet_patch.assert_called_once_with(
            self.SOURCE_TEST_DATA, job.ind_cqc_columns
        )
        self.assertEqual(join_model_predictions_patch.call_count, 3)
        self.assertEqual(
            combine_non_res_with_and_without_dormancy_models_patch.call_count, 1
        )
        self.assertEqual(
            model_imputation_with_extrapolation_and_interpolation.call_count, 3
        )
        merge_columns_in_order_mock.assert_called_once()
        estimate_non_res_capacity_tracker_filled_posts_mock.assert_called_once()
        write_to_parquet_patch.assert_called_once_with(
            ANY,
            self.ESTIMATES_DESTINATION,
            mode="overwrite",
            partitionKeys=self.partition_keys,
        )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
