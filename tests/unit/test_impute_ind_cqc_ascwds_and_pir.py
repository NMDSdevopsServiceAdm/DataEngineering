import unittest
import warnings
from unittest.mock import ANY, Mock, patch


import jobs.impute_ind_cqc_ascwds_and_pir as job
from tests.test_file_data import ImputeIndCqcAscwdsAndPirData as Data
from tests.test_file_schemas import ImputeIndCqcAscwdsAndPirSchemas as Schemas
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys


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
    @patch("utils.utils.write_to_parquet")
    @patch("jobs.impute_ind_cqc_ascwds_and_pir.model_calculate_rolling_average")
    @patch(
        "jobs.impute_ind_cqc_ascwds_and_pir.model_imputation_with_extrapolation_and_interpolation"
    )
    @patch(
        "jobs.impute_ind_cqc_ascwds_and_pir.blend_pir_and_ascwds_when_ascwds_out_of_date"
    )
    @patch(
        "jobs.impute_ind_cqc_ascwds_and_pir.primary_service_rate_of_change_trendline"
    )
    @patch(
        "jobs.impute_ind_cqc_ascwds_and_pir.combine_care_home_ratios_and_non_res_posts"
    )
    @patch("utils.utils.create_unix_timestamp_variable_from_date_column")
    @patch("utils.utils.read_from_parquet")
    def test_main_runs(
        self,
        read_from_parquet_patch: Mock,
        create_unix_timestamp_variable_from_date_column_mock: Mock,
        combine_care_home_ratios_and_non_res_posts_mock: Mock,
        primary_service_rate_of_change_trendline_mock: Mock,
        blend_pir_and_ascwds_when_ascwds_out_of_date_mock: Mock,
        model_imputation_with_extrapolation_and_interpolation_mock: Mock,
        model_calculate_rolling_average_mock: Mock,
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
        self.assertEqual(combine_care_home_ratios_and_non_res_posts_mock.call_count, 2)
        primary_service_rate_of_change_trendline_mock.assert_called_once()
        blend_pir_and_ascwds_when_ascwds_out_of_date_mock.assert_called_once()
        self.assertEqual(
            model_imputation_with_extrapolation_and_interpolation_mock.call_count, 3
        )
        model_calculate_rolling_average_mock.assert_called_once()
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
        self.assertEqual(job.NumericalValues.NUMBER_OF_DAYS_IN_WINDOW, 185)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
