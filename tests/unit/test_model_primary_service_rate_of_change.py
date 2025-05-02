import unittest
from unittest.mock import patch, Mock
import warnings

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc
import utils.estimate_filled_posts.models.primary_service_rate_of_change as job
from tests.test_file_data import ModelPrimaryServiceRateOfChange as Data
from tests.test_file_schemas import ModelPrimaryServiceRateOfChange as Schemas


class ModelPrimaryServiceRateOfChangeTests(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()

        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)


class MainTests(ModelPrimaryServiceRateOfChangeTests):
    def setUp(self) -> None:
        super().setUp()

        number_of_days: int = 3
        self.test_df = self.spark.createDataFrame(
            Data.primary_service_rate_of_change_rows,
            Schemas.primary_service_rate_of_change_schema,
        )
        self.returned_df = job.model_primary_service_rate_of_change(
            self.test_df,
            IndCqc.combined_ratio_and_filled_posts,
            number_of_days,
            IndCqc.single_period_rate_of_change,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_primary_service_rate_of_change_rows,
            Schemas.expected_primary_service_rate_of_change_schema,
        )
        self.returned_data = self.returned_df.sort(
            IndCqc.location_id, IndCqc.unix_time
        ).collect()
        self.expected_data = self.expected_df.collect()

    def test_row_count_unchanged_after_running_full_job(self):
        self.assertEqual(self.test_df.count(), self.returned_df.count())

    def test_primary_service_rate_of_change_returns_expected_columns(
        self,
    ):
        self.assertEqual(
            sorted(self.returned_df.columns),
            sorted(self.expected_df.columns),
        )

    def test_returned_rate_of_change_model_values_match_expected(
        self,
    ):
        for i in range(len(self.returned_data)):
            self.assertAlmostEqual(
                self.returned_data[i][IndCqc.single_period_rate_of_change],
                self.expected_data[i][IndCqc.single_period_rate_of_change],
                3,
                f"Returned row {i} does not match expected",
            )


class CleanColumnWithValuesTests(ModelPrimaryServiceRateOfChangeTests):
    def setUp(self) -> None:
        super().setUp()

        test_df = self.spark.createDataFrame(
            Data.clean_column_with_values_rows,
            Schemas.clean_column_with_values_schema,
        )
        self.returned_df = job.clean_column_with_values(test_df)
        self.expected_df = self.spark.createDataFrame(
            Data.expected_clean_column_with_values_rows,
            Schemas.expected_clean_column_with_values_schema,
        )

    def test_clean_column_with_values_returns_expected_columns(self):
        self.assertEqual(self.returned_df.columns, self.expected_df.columns)

    def test_clean_column_with_values_is_not_nulled_when_submitted_more_than_once_and_consistent_care_home_status(
        self,
    ):
        returned_data = self.returned_df.sort(IndCqc.unix_time).collect()
        expected_data = self.expected_df.collect()
        self.assertEqual(returned_data, expected_data)

    def test_clean_column_with_values_is_nulled_when_location_only_submitted_once(self):
        one_submission_df = self.spark.createDataFrame(
            Data.clean_column_with_values_one_submission_rows,
            Schemas.clean_column_with_values_schema,
        )
        returned_df = job.clean_column_with_values(one_submission_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_clean_column_with_values_one_submission_rows,
            Schemas.expected_clean_column_with_values_schema,
        )
        returned_data = returned_df.sort(IndCqc.unix_time).collect()
        expected_data = expected_df.collect()
        self.assertEqual(returned_data, expected_data)

    def test_clean_column_with_values_is_nulled_when_location_switched_between_care_home_and_non_res(
        self,
    ):
        both_statuses_df = self.spark.createDataFrame(
            Data.clean_column_with_values_both_statuses_rows,
            Schemas.clean_column_with_values_schema,
        )
        returned_df = job.clean_column_with_values(both_statuses_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_clean_column_with_values_both_statuses_rows,
            Schemas.expected_clean_column_with_values_schema,
        )
        returned_data = returned_df.sort(IndCqc.unix_time).collect()
        expected_data = expected_df.collect()
        self.assertEqual(returned_data, expected_data)


class CalculateCareHomeStatusCountTests(ModelPrimaryServiceRateOfChangeTests):
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


class CalculateSubmissionCountTests(ModelPrimaryServiceRateOfChangeTests):
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


class InterpolateColumnWithValuesTests(ModelPrimaryServiceRateOfChangeTests):
    def setUp(self) -> None:
        super().setUp()

        test_df = self.spark.createDataFrame(
            Data.interpolate_column_with_values_rows,
            Schemas.interpolate_column_with_values_schema,
        )
        self.returned_df = job.interpolate_column_with_values(test_df)
        self.expected_df = self.spark.createDataFrame(
            Data.expected_interpolate_column_with_values_rows,
            Schemas.expected_interpolate_column_with_values_schema,
        )
        self.returned_data = self.returned_df.sort(IndCqc.unix_time).collect()
        self.expected_data = self.expected_df.collect()

    def test_interpolate_column_with_values_returns_expected_columns(self):
        self.assertEqual(
            sorted(self.returned_df.columns),
            sorted(self.expected_df.columns),
        )

    def test_returned_column_with_values_interpolated_values_match_expected(
        self,
    ):
        for i in range(len(self.returned_data)):
            self.assertEqual(
                self.returned_data[i][job.TempCol.column_with_values_interpolated],
                self.expected_data[i][job.TempCol.column_with_values_interpolated],
                f"Returned row {i} does not match expected",
            )


class AddPreviousValueColumnTests(ModelPrimaryServiceRateOfChangeTests):
    def setUp(self) -> None:
        super().setUp()

        self.test_df = self.spark.createDataFrame(
            Data.add_previous_value_column_rows,
            Schemas.add_previous_value_column_schema,
        )
        self.returned_df = job.add_previous_value_column(self.test_df)
        self.expected_df = self.spark.createDataFrame(
            Data.expected_add_previous_value_column_rows,
            Schemas.expected_add_previous_value_column_schema,
        )

        self.returned_data = self.returned_df.sort(
            IndCqc.location_id, IndCqc.unix_time
        ).collect()
        self.expected_data = self.expected_df.collect()

    @patch(
        "utils.estimate_filled_posts.models.primary_service_rate_of_change.get_selected_value"
    )
    def test_functions_called_in_add_previous_value_column_function(
        self,
        get_selected_value: Mock,
    ):
        job.add_previous_value_column(self.test_df)

        self.assertEqual(get_selected_value.call_count, 1)

    def test_returned_column_names_match_expected(self):
        self.assertEqual(self.returned_df.columns, self.expected_df.columns)

    def test_returned_previous_interpolated_values_match_expected(
        self,
    ):
        for i in range(len(self.returned_data)):
            self.assertAlmostEqual(
                self.returned_data[i][
                    job.TempCol.previous_column_with_values_interpolated
                ],
                self.expected_data[i][
                    job.TempCol.previous_column_with_values_interpolated
                ],
                2,
                f"Returned row {i} does not match expected",
            )


class AddRollingSumColumnsTests(ModelPrimaryServiceRateOfChangeTests):
    def setUp(self) -> None:
        super().setUp()

        number_of_days: int = 2

        test_df = self.spark.createDataFrame(
            Data.add_rolling_sum_columns_rows,
            Schemas.add_rolling_sum_columns_schema,
        )
        self.returned_df = job.add_rolling_sum_columns(test_df, number_of_days)
        self.expected_df = self.spark.createDataFrame(
            Data.expected_add_rolling_sum_columns_rows,
            Schemas.expected_add_rolling_sum_columns_schema,
        )

        self.returned_data = self.returned_df.sort(
            IndCqc.location_id, IndCqc.unix_time
        ).collect()
        self.expected_data = self.expected_df.collect()

    def test_returned_column_names_match_expected(self):
        self.assertEqual(self.returned_df.columns, self.expected_df.columns)

    def test_returned_rolling_current_period_sum_values_match_expected(
        self,
    ):
        for i in range(len(self.returned_data)):
            self.assertAlmostEqual(
                self.returned_data[i][job.TempCol.rolling_current_period_sum],
                self.expected_data[i][job.TempCol.rolling_current_period_sum],
                2,
                f"Returned row {i} does not match expected",
            )

    def test_returned_rolling_previous_period_sum_values_match_expected(
        self,
    ):
        for i in range(len(self.returned_data)):
            self.assertAlmostEqual(
                self.returned_data[i][job.TempCol.rolling_previous_period_sum],
                self.expected_data[i][job.TempCol.rolling_previous_period_sum],
                2,
                f"Returned row {i} does not match expected",
            )


class CalculateRateOfChangeTests(ModelPrimaryServiceRateOfChangeTests):
    def setUp(self) -> None:
        super().setUp()

        test_df = self.spark.createDataFrame(
            Data.calculate_rate_of_change_rows,
            Schemas.calculate_rate_of_change_schema,
        )
        self.returned_df = job.calculate_rate_of_change(
            test_df, IndCqc.single_period_rate_of_change
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_calculate_rate_of_change_rows,
            Schemas.expected_calculate_rate_of_change_schema,
        )

        self.returned_data = self.returned_df.sort(IndCqc.location_id).collect()
        self.expected_data = self.expected_df.collect()

    def test_returned_column_names_match_expected(self):
        self.assertEqual(self.returned_df.columns, self.expected_df.columns)

    def test_returned_rate_of_change_values_match_expected(
        self,
    ):
        for i in range(len(self.returned_data)):
            self.assertAlmostEqual(
                self.returned_data[i][IndCqc.single_period_rate_of_change],
                self.expected_data[i][IndCqc.single_period_rate_of_change],
                2,
                f"Returned row {i} does not match expected",
            )
