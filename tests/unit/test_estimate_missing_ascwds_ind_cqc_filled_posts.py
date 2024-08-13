import unittest
import warnings
from unittest.mock import ANY, Mock, patch


import jobs.estimate_missing_ascwds_ind_cqc_filled_posts as job
from tests.test_file_data import EstimateMissingAscwdsFilledPostsData as Data
from tests.test_file_schemas import EstimateMissingAscwdsFilledPostsSchemas as Schemas
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
    PartitionKeys as Keys,
)


class EstimateMissingAscwdsFilledPostsTests(unittest.TestCase):
    CLEANED_IND_CQC_TEST_DATA = "some/cleaned/data"
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


class MainTests(EstimateMissingAscwdsFilledPostsTests):
    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main_runs(
        self,
        read_from_parquet_patch: Mock,
        write_to_parquet_patch: Mock,
    ):
        read_from_parquet_patch.return_value = self.test_cleaned_ind_cqc_df

        job.main(
            self.CLEANED_IND_CQC_TEST_DATA,
            self.ESTIMATES_DESTINATION,
        )

        self.assertEqual(read_from_parquet_patch.call_count, 1)
        self.assertEqual(write_to_parquet_patch.call_count, 1)
        write_to_parquet_patch.assert_any_call(
            ANY,
            self.ESTIMATES_DESTINATION,
            mode="overwrite",
            partitionKeys=self.partition_keys,
        )


class NumericalValuesTests(EstimateMissingAscwdsFilledPostsTests):
    def setUp(self) -> None:
        super().setUp()

    def test_number_of_days_in_care_home_rolling_average_value(self):
        self.assertEqual(
            job.NumericalValues.NUMBER_OF_DAYS_IN_CARE_HOME_ROLLING_AVERAGE, 185
        )

    def test_number_of_days_in_non_res_rolling_average_value(self):
        self.assertEqual(
            job.NumericalValues.NUMBER_OF_DAYS_IN_NON_RES_ROLLING_AVERAGE, 185
        )


class ModelCareHomePostsPerBedRollingAverageTests(
    EstimateMissingAscwdsFilledPostsTests
):
    def setUp(self):
        self.spark = utils.get_spark()

        self.number_of_days = 88
        self.test_care_home_ratio_rolling_avg_df = self.spark.createDataFrame(
            Data.care_home_ratio_rolling_avg_rows,
            Schemas.care_home_ratio_rolling_avg_schema,
        )

    @patch(
        "jobs.estimate_missing_ascwds_ind_cqc_filled_posts.model_primary_service_rolling_average"
    )
    def test_model_care_home_posts_per_bed_rolling_average_calls_primary_service_rolling_avg_once(
        self,
        model_primary_service_rolling_average_patch: Mock,
    ):
        model_primary_service_rolling_average_patch.return_value = (
            self.test_care_home_ratio_rolling_avg_df
        )

        job.model_care_home_posts_per_bed_rolling_average(
            self.test_care_home_ratio_rolling_avg_df,
            self.number_of_days,
            IndCQC.rolling_average_care_home_posts_per_bed_model,
        )

        self.assertEqual(model_primary_service_rolling_average_patch.call_count, 1)

    @patch(
        "jobs.estimate_missing_ascwds_ind_cqc_filled_posts.model_primary_service_rolling_average"
    )
    def test_model_care_home_posts_per_bed_rolling_average_returns_expected_rows(
        self,
        model_primary_service_rolling_average_patch: Mock,
    ):
        model_primary_service_rolling_average_patch.return_value = (
            self.test_care_home_ratio_rolling_avg_df
        )

        returned_df = job.model_care_home_posts_per_bed_rolling_average(
            self.test_care_home_ratio_rolling_avg_df,
            self.number_of_days,
            IndCQC.rolling_average_care_home_posts_per_bed_model,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_care_home_ratio_rolling_avg_rows,
            Schemas.expected_care_home_ratio_rolling_avg_schema,
        )
        returned_row_object = returned_df.sort(IndCQC.location_id).collect()
        expected_row_object = expected_df.sort(IndCQC.location_id).collect()

        for i in range(len(returned_row_object)):
            self.assertEqual(
                returned_row_object[i][
                    IndCQC.rolling_average_care_home_posts_per_bed_model
                ],
                expected_row_object[i][
                    IndCQC.rolling_average_care_home_posts_per_bed_model
                ],
                f"Returned row {i} does not match expected",
            )


class ModelNonResFilledPostRollingAverageTests(EstimateMissingAscwdsFilledPostsTests):
    def setUp(self):
        self.spark = utils.get_spark()

        self.number_of_days = 88
        self.test_non_res_rolling_avg_df = self.spark.createDataFrame(
            Data.non_res_rolling_avg_rows,
            Schemas.non_res_rolling_avg_schema,
        )

    @patch(
        "jobs.estimate_missing_ascwds_ind_cqc_filled_posts.model_primary_service_rolling_average"
    )
    def test_model_non_res_filled_post_rolling_average_calls_primary_service_rolling_avg_once(
        self,
        model_primary_service_rolling_average_patch: Mock,
    ):
        model_primary_service_rolling_average_patch.return_value = (
            self.test_non_res_rolling_avg_df
        )

        job.model_non_res_filled_post_rolling_average(
            self.test_non_res_rolling_avg_df,
            self.number_of_days,
            IndCQC.rolling_average_non_res_model,
        )

        self.assertEqual(model_primary_service_rolling_average_patch.call_count, 1)

    @patch(
        "jobs.estimate_missing_ascwds_ind_cqc_filled_posts.model_primary_service_rolling_average"
    )
    def test_model_non_res_filled_post_rolling_average_returns_expected_rows(
        self,
        model_primary_service_rolling_average_patch: Mock,
    ):
        model_primary_service_rolling_average_patch.return_value = (
            self.test_non_res_rolling_avg_df
        )

        returned_df = job.model_non_res_filled_post_rolling_average(
            self.test_non_res_rolling_avg_df,
            self.number_of_days,
            IndCQC.rolling_average_non_res_model,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_non_res_rolling_avg_rows,
            Schemas.expected_non_res_rolling_avg_schema,
        )
        returned_row_object = returned_df.sort(IndCQC.location_id).collect()
        expected_row_object = expected_df.sort(IndCQC.location_id).collect()

        for i in range(len(returned_row_object)):
            self.assertEqual(
                returned_row_object[i][IndCQC.rolling_average_non_res_model],
                expected_row_object[i][IndCQC.rolling_average_non_res_model],
                f"Returned row {i} does not match expected",
            )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
