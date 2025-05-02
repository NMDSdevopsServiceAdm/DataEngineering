import unittest
import warnings
from unittest.mock import ANY, Mock, patch

import projects._03_independent_cqc._03_impute.jobs.impute_ind_cqc_ascwds_and_pir as job
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_data import (
    ImputeIndCqcAscwdsAndPirData as Data,
)
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_schemas import (
    ImputeIndCqcAscwdsAndPirSchemas as Schemas,
)
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

PATCH_PATH: str = (
    "projects._03_independent_cqc._03_impute.jobs.impute_ind_cqc_ascwds_and_pir"
)


class ImputeIndCqcAscwdsAndPirTests(unittest.TestCase):
    CLEANED_IND_CQC_TEST_DATA = "some/cleaned/data"
    ESTIMATES_DESTINATION = "estimates destination"
    NON_RES_PIR_MODEL = (
        "tests/test_models/non_res_pir_linear_regression_prediction/1.0.0/"
    )
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


class MainTests(ImputeIndCqcAscwdsAndPirTests):
    @patch(f"{PATCH_PATH}.utils.write_to_parquet")
    @patch(
        f"{PATCH_PATH}.convert_care_home_ratios_to_filled_posts_and_merge_with_filled_post_values"
    )
    @patch(f"{PATCH_PATH}.clean_number_of_beds_banded")
    @patch(f"{PATCH_PATH}.model_calculate_rolling_average")
    @patch(f"{PATCH_PATH}.model_imputation_with_extrapolation_and_interpolation")
    @patch(f"{PATCH_PATH}.merge_ascwds_and_pir_filled_post_submissions")
    @patch(f"{PATCH_PATH}.model_pir_filled_posts")
    @patch(f"{PATCH_PATH}.model_primary_service_rate_of_change_trendline")
    @patch(f"{PATCH_PATH}.combine_care_home_ratios_and_non_res_posts")
    @patch(f"{PATCH_PATH}.utils.create_unix_timestamp_variable_from_date_column")
    @patch(f"{PATCH_PATH}.utils.read_from_parquet")
    def test_main_runs(
        self,
        read_from_parquet_patch: Mock,
        create_unix_timestamp_variable_from_date_column_mock: Mock,
        combine_care_home_ratios_and_non_res_posts_mock: Mock,
        model_primary_service_rate_of_change_trendline_mock: Mock,
        model_pir_filled_posts_mock: Mock,
        merge_ascwds_and_pir_filled_post_submissions_mock: Mock,
        model_imputation_with_extrapolation_and_interpolation_mock: Mock,
        model_calculate_rolling_average_mock: Mock,
        clean_number_of_beds_banded_mock: Mock,
        convert_care_home_ratios_to_filled_posts_and_merge_with_filled_post_values_mock: Mock,
        write_to_parquet_patch: Mock,
    ):
        read_from_parquet_patch.return_value = self.test_cleaned_ind_cqc_df

        job.main(
            self.CLEANED_IND_CQC_TEST_DATA,
            self.ESTIMATES_DESTINATION,
            self.NON_RES_PIR_MODEL,
        )

        read_from_parquet_patch.assert_called_once()
        create_unix_timestamp_variable_from_date_column_mock.assert_called_once()
        combine_care_home_ratios_and_non_res_posts_mock.assert_called_once()
        model_primary_service_rate_of_change_trendline_mock.assert_called_once()
        model_pir_filled_posts_mock.assert_called_once()
        merge_ascwds_and_pir_filled_post_submissions_mock.assert_called_once()
        self.assertEqual(
            model_imputation_with_extrapolation_and_interpolation_mock.call_count, 3
        )
        self.assertEqual(model_calculate_rolling_average_mock.call_count, 2)
        clean_number_of_beds_banded_mock.assert_called_once()
        convert_care_home_ratios_to_filled_posts_and_merge_with_filled_post_values_mock.assert_called_once()
        write_to_parquet_patch.assert_called_once_with(
            ANY,
            self.ESTIMATES_DESTINATION,
            mode="overwrite",
            partitionKeys=self.partition_keys,
        )


class NumericalValuesTests(ImputeIndCqcAscwdsAndPirTests):
    def setUp(self) -> None:
        super().setUp()

    def test_number_of_days_in_window_value(self):
        self.assertEqual(job.NumericalValues.number_of_days_in_window, 95)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
