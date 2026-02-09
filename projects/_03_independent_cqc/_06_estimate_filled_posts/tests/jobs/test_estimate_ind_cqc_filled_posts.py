import unittest
import warnings
from unittest.mock import ANY, Mock, patch

import projects._03_independent_cqc._06_estimate_filled_posts.jobs.estimate_ind_cqc_filled_posts as job
from tests.test_file_data import EstimateIndCQCFilledPostsData as Data
from tests.test_file_schemas import EstimateIndCQCFilledPostsSchemas as Schemas
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

PATCH_PATH = "projects._03_independent_cqc._06_estimate_filled_posts.jobs.estimate_ind_cqc_filled_posts"


class EstimateIndCQCFilledPostsTests(unittest.TestCase):
    TEST_BUCKET_NAME = "test-bucket"
    CLEANED_IND_CQC_TEST_DATA = "some/cleaned/data"
    NON_RES_WITH_DORMANCY_FEATURES = "non res with dormancy features"
    NON_RES_WITH_DORMANCY_MODEL = (
        "tests/test_models/non_residential_with_dormancy_prediction/1.0.0/"
    )
    ESTIMATES_DESTINATION = "estimates destination"
    partition_keys = [
        Keys.year,
        Keys.month,
        Keys.day,
        Keys.import_date,
    ]

    def setUp(self):
        self.spark = utils.get_spark()
        self.test_cleaned_ind_cqc_df = self.spark.createDataFrame(
            Data.cleaned_ind_cqc_rows, Schemas.cleaned_ind_cqc_schema
        )

        warnings.filterwarnings("ignore", category=ResourceWarning)

    @patch(f"{PATCH_PATH}.utils.write_to_parquet")
    @patch(f"{PATCH_PATH}.estimate_non_res_capacity_tracker_filled_posts")
    @patch(f"{PATCH_PATH}.set_min_value")
    @patch(f"{PATCH_PATH}.merge_columns_in_order")
    @patch(f"{PATCH_PATH}.model_imputation_with_extrapolation_and_interpolation")
    @patch(f"{PATCH_PATH}.combine_non_res_with_and_without_dormancy_models")
    @patch(f"{PATCH_PATH}.model_non_res_with_dormancy")
    @patch(f"{PATCH_PATH}.enrich_with_model_predictions")
    @patch(f"{PATCH_PATH}.utils.read_from_parquet")
    def test_main_runs(
        self,
        read_from_parquet_patch: Mock,
        enrich_with_model_predictions_patch: Mock,
        model_non_res_with_dormancy_patch: Mock,
        combine_non_res_with_and_without_dormancy_models_patch: Mock,
        model_imputation_with_extrapolation_and_interpolation: Mock,
        merge_columns_in_order_mock: Mock,
        set_min_value_mock: Mock,
        estimate_non_res_capacity_tracker_filled_posts_mock: Mock,
        write_to_parquet_patch: Mock,
    ):
        read_from_parquet_patch.side_effect = [
            self.test_cleaned_ind_cqc_df,
            self.NON_RES_WITH_DORMANCY_FEATURES,
        ]

        job.main(
            self.TEST_BUCKET_NAME,
            self.CLEANED_IND_CQC_TEST_DATA,
            self.NON_RES_WITH_DORMANCY_FEATURES,
            self.NON_RES_WITH_DORMANCY_MODEL,
            self.ESTIMATES_DESTINATION,
        )

        self.assertEqual(read_from_parquet_patch.call_count, 2)
        self.assertEqual(enrich_with_model_predictions_patch.call_count, 2)
        self.assertEqual(model_non_res_with_dormancy_patch.call_count, 1)
        self.assertEqual(
            combine_non_res_with_and_without_dormancy_models_patch.call_count, 1
        )
        self.assertEqual(
            model_imputation_with_extrapolation_and_interpolation.call_count, 3
        )
        merge_columns_in_order_mock.assert_called_once()
        set_min_value_mock.assert_called_once_with(
            ANY, IndCQC.estimate_filled_posts, 1.0
        )
        estimate_non_res_capacity_tracker_filled_posts_mock.assert_called_once()
        write_to_parquet_patch.assert_called_once_with(
            ANY,
            self.ESTIMATES_DESTINATION,
            mode="overwrite",
            partitionKeys=self.partition_keys,
        )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
