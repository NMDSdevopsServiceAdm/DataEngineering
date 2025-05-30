import unittest
import warnings
from unittest.mock import ANY, Mock, patch

import jobs.estimate_ind_cqc_filled_posts as job
from tests.test_file_data import EstimateIndCQCFilledPostsData as Data
from tests.test_file_schemas import EstimateIndCQCFilledPostsSchemas as Schemas
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
    PartitionKeys as Keys,
)

PATCH_PATH: str = "jobs.estimate_ind_cqc_filled_posts"


class EstimateIndCQCFilledPostsTests(unittest.TestCase):
    CLEANED_IND_CQC_TEST_DATA = "some/cleaned/data"
    CARE_HOMES_FEATURES = "care home features"
    CARE_HOME_MODEL = (
        "pipeline-resources/models/care_home_filled_posts_prediction/0.0.1/"
    )
    NON_RES_WITH_DORMANCY_FEATURES = "non res with dormancy features"
    NON_RES_WITH_DORMANCY_MODEL = (
        "tests/test_models/non_residential_with_dormancy_prediction/1.0.0/"
    )
    NON_RES_WITHOUT_DORMANCY_FEATURES = "non res without dormancy features"
    NON_RES_WITHOUT_DORMANCY_MODEL = (
        "tests/test_models/non_residential_without_dormancy_prediction/1.0.0/"
    )
    ESTIMATES_DESTINATION = "estimates destination"
    METRICS_DESTINATION = "metrics destination"
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
    @patch(f"{PATCH_PATH}.merge_columns_in_order")
    @patch(f"{PATCH_PATH}.model_imputation_with_extrapolation_and_interpolation")
    @patch(f"{PATCH_PATH}.combine_non_res_with_and_without_dormancy_models")
    @patch(f"{PATCH_PATH}.model_non_res_without_dormancy")
    @patch(f"{PATCH_PATH}.model_non_res_with_dormancy")
    @patch(f"{PATCH_PATH}.model_care_homes")
    @patch(f"{PATCH_PATH}.utils.read_from_parquet")
    def test_main_runs(
        self,
        read_from_parquet_patch: Mock,
        model_care_homes_patch: Mock,
        model_non_res_with_dormancy_patch: Mock,
        model_non_res_without_dormancy_patch: Mock,
        combine_non_res_with_and_without_dormancy_models_patch: Mock,
        model_imputation_with_extrapolation_and_interpolation: Mock,
        merge_columns_in_order_mock: Mock,
        write_to_parquet_patch: Mock,
    ):
        read_from_parquet_patch.side_effect = [
            self.test_cleaned_ind_cqc_df,
            self.CARE_HOMES_FEATURES,
            self.NON_RES_WITH_DORMANCY_FEATURES,
            self.NON_RES_WITHOUT_DORMANCY_FEATURES,
        ]

        job.main(
            self.CLEANED_IND_CQC_TEST_DATA,
            self.CARE_HOMES_FEATURES,
            self.CARE_HOME_MODEL,
            self.NON_RES_WITH_DORMANCY_FEATURES,
            self.NON_RES_WITH_DORMANCY_MODEL,
            self.NON_RES_WITHOUT_DORMANCY_FEATURES,
            self.NON_RES_WITHOUT_DORMANCY_MODEL,
            self.ESTIMATES_DESTINATION,
            self.METRICS_DESTINATION,
        )

        self.assertEqual(read_from_parquet_patch.call_count, 4)
        self.assertEqual(model_care_homes_patch.call_count, 1)
        self.assertEqual(model_non_res_with_dormancy_patch.call_count, 1)
        self.assertEqual(model_non_res_without_dormancy_patch.call_count, 1)
        self.assertEqual(
            combine_non_res_with_and_without_dormancy_models_patch.call_count, 1
        )
        self.assertEqual(
            model_imputation_with_extrapolation_and_interpolation.call_count, 3
        )
        # self.assertEqual(merge_columns_in_order_mock.call_count, 1)
        self.assertEqual(write_to_parquet_patch.call_count, 1)
        write_to_parquet_patch.assert_any_call(
            ANY,
            self.ESTIMATES_DESTINATION,
            mode="overwrite",
            partitionKeys=self.partition_keys,
        )

    def test_null_large_initial_model_estimations(self):
        test_df = self.spark.createDataFrame(
            Data.null_large_initial_model_estimations_rows,
            Schemas.null_large_initial_model_estimations_schema,
        )
        returned_df = job.null_large_initial_model_estimations(
            test_df, IndCQC.ascwds_pir_merged, 100.0, "cleaned"
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_null_large_initial_model_estimations_rows,
            Schemas.expected_null_large_initial_model_estimations_schema,
        )

        returned_data = returned_df.sort(
            IndCQC.location_id, IndCQC.cqc_location_import_date
        ).collect()
        expected_data = expected_df.collect()

        self.assertEqual(returned_data, expected_data)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
