import unittest
from unittest.mock import patch, Mock
import warnings

import utils.estimate_filled_posts.models.primary_service_rolling_average as job
from tests.test_file_data import ModelPrimaryServiceRollingAverage as Data
from tests.test_file_schemas import ModelPrimaryServiceRollingAverage as Schemas
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
)
from utils import utils


class ModelPrimaryServiceRollingAverageTests(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()

        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)


class MainTests(ModelPrimaryServiceRollingAverageTests):
    def setUp(self) -> None:
        super().setUp()

        number_of_days: int = 3
        self.estimates_df = self.spark.createDataFrame(
            Data.primary_service_rolling_average_rows,
            Schemas.primary_service_rolling_average_schema,
        )
        self.returned_df = job.model_primary_service_rolling_average_and_rate_of_change(
            self.estimates_df,
            IndCqc.filled_posts_per_bed_ratio,
            IndCqc.ascwds_filled_posts_dedup_clean,
            number_of_days,
            IndCqc.rolling_average_model,
            IndCqc.rolling_rate_of_change_model,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_primary_service_rolling_average_rows,
            Schemas.expected_primary_service_rolling_average_schema,
        )
        self.returned_data = self.returned_df.sort(
            IndCqc.location_id, IndCqc.unix_time
        ).collect()
        self.expected_data = self.expected_df.collect()

    def test_row_count_unchanged_after_running_full_job(self):
        self.assertEqual(self.estimates_df.count(), self.returned_df.count())

    def test_model_primary_service_rolling_average_and_rate_of_change_returns_expected_columns(
        self,
    ):
        self.assertEqual(
            sorted(self.returned_df.columns),
            sorted(self.expected_df.columns),
        )

    def test_returned_rolling_average_model_values_match_expected(
        self,
    ):
        for i in range(len(self.returned_data)):
            self.assertAlmostEqual(
                self.returned_data[i][IndCqc.rolling_average_model],
                self.expected_data[i][IndCqc.rolling_average_model],
                3,
                f"Returned row {i} does not match expected",
            )

    def test_returned_rolling_rate_of_change_model_values_match_expected(
        self,
    ):
        for i in range(len(self.returned_data)):
            self.assertAlmostEqual(
                self.returned_data[i][IndCqc.rolling_rate_of_change_model],
                self.expected_data[i][IndCqc.rolling_rate_of_change_model],
                3,
                f"Returned row {i} does not match expected",
            )


class CreateSingleColumnToAverageTests(ModelPrimaryServiceRollingAverageTests):
    def setUp(self) -> None:
        super().setUp()

        test_df = self.spark.createDataFrame(
            Data.single_column_to_average_rows,
            Schemas.single_column_to_average_schema,
        )
        self.returned_df = job.create_single_column_to_average(
            test_df,
            IndCqc.filled_posts_per_bed_ratio,
            IndCqc.ascwds_filled_posts_dedup_clean,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_single_column_to_average_rows,
            Schemas.expected_single_column_to_average_schema,
        )
        self.returned_data = self.returned_df.sort(IndCqc.location_id).collect()
        self.expected_data = self.expected_df.collect()

    def test_create_single_column_to_average_returns_expected_columns(self):
        self.assertEqual(
            sorted(self.returned_df.columns),
            sorted(self.expected_df.columns),
        )

    def test_returned_column_to_average_values_match_expected(
        self,
    ):
        for i in range(len(self.returned_data)):
            self.assertEqual(
                self.returned_data[i][job.TempCol.column_to_average],
                self.expected_data[i][job.TempCol.column_to_average],
                f"Returned row {i} does not match expected",
            )


class CleanColumnToAverageTests(ModelPrimaryServiceRollingAverageTests):
    def setUp(self) -> None:
        super().setUp()

        test_df = self.spark.createDataFrame(
            Data.clean_column_to_average_rows,
            Schemas.clean_column_to_average_schema,
        )
        self.returned_df = job.clean_column_to_average(test_df)
        self.expected_df = self.spark.createDataFrame(
            Data.expected_clean_column_to_average_rows,
            Schemas.expected_clean_column_to_average_schema,
        )

    def test_clean_column_to_average_returns_expected_columns(self):
        self.assertEqual(self.returned_df.columns, self.expected_df.columns)

    def test_clean_column_to_average_is_not_nulled_when_submitted_more_than_once_and_consistent_care_home_status(
        self,
    ):
        returned_data = self.returned_df.sort(IndCqc.unix_time).collect()
        expected_data = self.expected_df.collect()
        self.assertEqual(returned_data, expected_data)

    def test_clean_column_to_average_is_nulled_when_location_only_submitted_once(self):
        one_submission_df = self.spark.createDataFrame(
            Data.clean_column_to_average_one_submission_rows,
            Schemas.clean_column_to_average_schema,
        )
        returned_df = job.clean_column_to_average(one_submission_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_clean_column_to_average_one_submission_rows,
            Schemas.expected_clean_column_to_average_schema,
        )
        returned_data = returned_df.sort(IndCqc.unix_time).collect()
        expected_data = expected_df.collect()
        self.assertEqual(returned_data, expected_data)

    def test_clean_column_to_average_is_nulled_when_location_switched_between_care_home_and_non_res(
        self,
    ):
        both_statuses_df = self.spark.createDataFrame(
            Data.clean_column_to_average_both_statuses_rows,
            Schemas.clean_column_to_average_schema,
        )
        returned_df = job.clean_column_to_average(both_statuses_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_clean_column_to_average_both_statuses_rows,
            Schemas.expected_clean_column_to_average_schema,
        )
        returned_data = returned_df.sort(IndCqc.unix_time).collect()
        expected_data = expected_df.collect()
        self.assertEqual(returned_data, expected_data)


class CalculateCareHomeStatusCountTests(ModelPrimaryServiceRollingAverageTests):
    def setUp(self) -> None:
        super().setUp()

        test_df = self.spark.createDataFrame(
            Data.calculate_care_home_status_count_rows,
            Schemas.calculate_care_home_status_count_schema,
        )
        self.returned_df = job.calculate_care_home_status_count(test_df)
        self.expected_df = self.spark.createDataFrame(
            Data.expected_calculate_care_home_status_count_rows,
            Schemas.expected_calculate_care_home_status_count_schema,
        )
        self.returned_data = self.returned_df.sort(IndCqc.location_id).collect()
        self.expected_data = self.expected_df.collect()

    def test_calculate_care_home_status_count_returns_expected_columns(self):
        self.assertEqual(
            sorted(self.returned_df.columns),
            sorted(self.expected_df.columns),
        )

    def test_returned_care_home_status_count_values_match_expected(
        self,
    ):
        for i in range(len(self.returned_data)):
            self.assertEqual(
                self.returned_data[i][job.TempCol.care_home_status_count],
                self.expected_data[i][job.TempCol.care_home_status_count],
                f"Returned row {i} does not match expected",
            )


class CalculateSubmissionCountTests(ModelPrimaryServiceRollingAverageTests):
    def setUp(self) -> None:
        super().setUp()

        test_df = self.spark.createDataFrame(
            Data.calculate_submission_count_same_care_home_status_rows,
            Schemas.calculate_submission_count_schema,
        )
        self.returned_df = job.calculate_submission_count(test_df)
        self.expected_df = self.spark.createDataFrame(
            Data.expected_calculate_submission_count_same_care_home_status_rows,
            Schemas.expected_calculate_submission_count_schema,
        )
        self.returned_data = self.returned_df.sort(IndCqc.location_id).collect()
        self.expected_data = self.expected_df.collect()

    def test_calculate_submission_count_returns_expected_columns(self):
        self.assertEqual(
            sorted(self.returned_df.columns),
            sorted(self.expected_df.columns),
        )

    def test_returned_submission_values_match_expected_when_location_does_not_have_multiple_care_home_statuses(
        self,
    ):
        for i in range(len(self.returned_data)):
            self.assertEqual(
                self.returned_data[i][job.TempCol.submission_count],
                self.expected_data[i][job.TempCol.submission_count],
                f"Returned row {i} does not match expected",
            )

    def test_returned_submission_values_match_expected_when_location_has_multiple_care_home_statuses(
        self,
    ):
        mixed_status_df = self.spark.createDataFrame(
            Data.calculate_submission_count_mixed_care_home_status_rows,
            Schemas.calculate_submission_count_schema,
        )
        returned_df = job.calculate_submission_count(mixed_status_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_submission_count_mixed_care_home_status_rows,
            Schemas.expected_calculate_submission_count_schema,
        )
        returned_data = returned_df.sort(IndCqc.care_home).collect()
        expected_data = expected_df.collect()

        for i in range(len(returned_data)):
            self.assertEqual(
                returned_data[i][job.TempCol.submission_count],
                expected_data[i][job.TempCol.submission_count],
                f"Returned row {i} does not match expected",
            )


class InterpolateColumnToAverageTests(ModelPrimaryServiceRollingAverageTests):
    def setUp(self) -> None:
        super().setUp()

        test_df = self.spark.createDataFrame(
            Data.interpolate_column_to_average_rows,
            Schemas.interpolate_column_to_average_schema,
        )
        self.returned_df = job.interpolate_column_to_average(test_df)
        self.expected_df = self.spark.createDataFrame(
            Data.expected_interpolate_column_to_average_rows,
            Schemas.expected_interpolate_column_to_average_schema,
        )
        self.returned_data = self.returned_df.sort(IndCqc.unix_time).collect()
        self.expected_data = self.expected_df.collect()

    def test_interpolate_column_to_average_returns_expected_columns(self):
        self.assertEqual(
            sorted(self.returned_df.columns),
            sorted(self.expected_df.columns),
        )

    def test_returned_column_to_average_interpolated_values_match_expected(
        self,
    ):
        for i in range(len(self.returned_data)):
            self.assertEqual(
                self.returned_data[i][job.TempCol.column_to_average_interpolated],
                self.expected_data[i][job.TempCol.column_to_average_interpolated],
                f"Returned row {i} does not match expected",
            )


class CalculateRollingAverageTests(ModelPrimaryServiceRollingAverageTests):
    def setUp(self) -> None:
        super().setUp()

        number_of_days: int = 2

        test_df = self.spark.createDataFrame(
            Data.calculate_rolling_average_rows,
            Schemas.calculate_rolling_average_schema,
        )
        self.returned_df = job.calculate_rolling_average(
            test_df, number_of_days, IndCqc.rolling_average_model
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_calculate_rolling_average_rows,
            Schemas.expected_calculate_rolling_average_schema,
        )
        self.returned_data = self.returned_df.sort(IndCqc.location_id).collect()
        self.expected_data = self.expected_df.collect()

    def test_calculate_rolling_average_returns_expected_columns(self):
        self.assertEqual(
            sorted(self.returned_df.columns),
            sorted(self.expected_df.columns),
        )

    def test_returned_rolling_average_values_match_expected(
        self,
    ):
        for i in range(len(self.returned_data)):
            self.assertAlmostEqual(
                self.returned_data[i][IndCqc.rolling_average_model],
                self.expected_data[i][IndCqc.rolling_average_model],
                2,
                f"Returned row {i} does not match expected",
            )


class CalculateRollingRateOfChangeTests(ModelPrimaryServiceRollingAverageTests):
    def setUp(self) -> None:
        super().setUp()

        self.number_of_days: int = 2
        self.calculate_roc_df = self.spark.createDataFrame(
            Data.calculate_rolling_rate_of_change_rows,
            Schemas.calculate_rolling_rate_of_change_schema,
        )

    @patch(
        "utils.estimate_filled_posts.models.primary_service_rolling_average.add_previous_value_column"
    )
    @patch(
        "utils.estimate_filled_posts.models.primary_service_rolling_average.add_rolling_sums"
    )
    @patch(
        "utils.estimate_filled_posts.models.primary_service_rolling_average.calculate_single_period_rate_of_change"
    )
    @patch(
        "utils.estimate_filled_posts.models.primary_service_rolling_average.calculate_rolling_rate_of_change_model"
    )
    def test_all_functions_called_in_calculate_rolling_rate_of_change_function(
        self,
        calculate_rolling_rate_of_change_model: Mock,
        calculate_single_period_rate_of_change: Mock,
        add_rolling_sums: Mock,
        add_previous_value_column: Mock,
    ):
        job.calculate_rolling_rate_of_change(
            self.calculate_roc_df,
            self.number_of_days,
            IndCqc.rolling_rate_of_change_model,
        )

        self.assertEqual(add_previous_value_column.call_count, 1)
        self.assertEqual(add_rolling_sums.call_count, 1)
        self.assertEqual(calculate_single_period_rate_of_change.call_count, 1)
        self.assertEqual(calculate_rolling_rate_of_change_model.call_count, 1)

    def test_rate_of_change_model_column_name_in_returned_column_list(self):
        returned_df = job.calculate_rolling_rate_of_change(
            self.calculate_roc_df,
            self.number_of_days,
            IndCqc.rolling_rate_of_change_model,
        )

        self.assertTrue(IndCqc.rolling_rate_of_change_model in returned_df.columns)
