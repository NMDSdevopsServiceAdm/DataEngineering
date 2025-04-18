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
        self.estimates_df = self.spark.createDataFrame(
            Data.primary_service_rate_of_change_rows,
            Schemas.primary_service_rate_of_change_schema,
        )
        self.returned_df = job.model_primary_service_rate_of_change(
            self.estimates_df,
            IndCqc.combined_ratio_and_filled_posts,
            number_of_days,
            IndCqc.ascwds_rate_of_change_trendline_model,
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
        self.assertEqual(self.estimates_df.count(), self.returned_df.count())

    def test_model_primary_service_rate_of_change_returns_expected_columns(
        self,
    ):
        self.assertEqual(
            sorted(self.returned_df.columns),
            sorted(self.expected_df.columns),
        )

    def test_returned_rolling_rate_of_change_model_values_match_expected(
        self,
    ):
        for i in range(len(self.returned_data)):
            self.assertAlmostEqual(
                self.returned_data[i][IndCqc.ascwds_rate_of_change_trendline_model],
                self.expected_data[i][IndCqc.ascwds_rate_of_change_trendline_model],
                3,
                f"Returned row {i} does not match expected",
            )


class CleanColumnToAverageTests(ModelPrimaryServiceRateOfChangeTests):
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


class InterpolateColumnToAverageTests(ModelPrimaryServiceRateOfChangeTests):
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


class CalculateRollingRateOfChangeTests(ModelPrimaryServiceRateOfChangeTests):
    def setUp(self) -> None:
        super().setUp()

        self.number_of_days: int = 2
        self.calculate_roc_df = self.spark.createDataFrame(
            Data.calculate_rolling_rate_of_change_rows,
            Schemas.calculate_rolling_rate_of_change_schema,
        )

    @patch(
        "utils.estimate_filled_posts.models.primary_service_rate_of_change.add_previous_value_column"
    )
    @patch(
        "utils.estimate_filled_posts.models.primary_service_rate_of_change.add_rolling_sum"
    )
    @patch(
        "utils.estimate_filled_posts.models.primary_service_rate_of_change.calculate_single_period_rate_of_change"
    )
    @patch(
        "utils.estimate_filled_posts.models.primary_service_rate_of_change.deduplicate_dataframe"
    )
    @patch(
        "utils.estimate_filled_posts.models.primary_service_rate_of_change.calculate_cumulative_rate_of_change"
    )
    def test_all_functions_called_in_calculate_rolling_rate_of_change_function(
        self,
        calculate_cumulative_rate_of_change: Mock,
        deduplicate_dataframe: Mock,
        calculate_single_period_rate_of_change: Mock,
        add_rolling_sum: Mock,
        add_previous_value_column: Mock,
    ):
        job.calculate_rolling_rate_of_change(
            self.calculate_roc_df,
            self.number_of_days,
            IndCqc.ascwds_rate_of_change_trendline_model,
        )

        self.assertEqual(add_previous_value_column.call_count, 1)
        self.assertEqual(add_rolling_sum.call_count, 2)
        self.assertEqual(calculate_single_period_rate_of_change.call_count, 1)
        self.assertEqual(deduplicate_dataframe.call_count, 1)
        self.assertEqual(calculate_cumulative_rate_of_change.call_count, 1)

    def test_rate_of_change_model_column_name_in_returned_column_list(self):
        returned_df = job.calculate_rolling_rate_of_change(
            self.calculate_roc_df,
            self.number_of_days,
            IndCqc.ascwds_rate_of_change_trendline_model,
        )

        self.assertTrue(
            IndCqc.ascwds_rate_of_change_trendline_model in returned_df.columns
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


class AddRollingSumTests(ModelPrimaryServiceRateOfChangeTests):
    def setUp(self) -> None:
        super().setUp()

        number_of_days: int = 2

        test_df = self.spark.createDataFrame(
            Data.add_rolling_sum_rows,
            Schemas.add_rolling_sum_schema,
        )
        self.returned_df = job.add_rolling_sum(
            test_df,
            number_of_days,
            job.TempCol.column_with_values_interpolated,
            job.TempCol.rolling_current_period_sum,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_add_rolling_sum_rows,
            Schemas.expected_add_rolling_sum_schema,
        )

        self.returned_data = self.returned_df.sort(
            IndCqc.location_id, IndCqc.unix_time
        ).collect()
        self.expected_data = self.expected_df.collect()

    def test_returned_column_names_match_expected(self):
        self.assertEqual(self.returned_df.columns, self.expected_df.columns)

    def test_returned_rolling_sum_values_match_expected(
        self,
    ):
        for i in range(len(self.returned_data)):
            self.assertAlmostEqual(
                self.returned_data[i][job.TempCol.rolling_current_period_sum],
                self.expected_data[i][job.TempCol.rolling_current_period_sum],
                2,
                f"Returned row {i} does not match expected",
            )


class CalculateSinglePeriodRateOfChangeTests(ModelPrimaryServiceRateOfChangeTests):
    def setUp(self) -> None:
        super().setUp()

        test_df = self.spark.createDataFrame(
            Data.single_period_rate_of_change_rows,
            Schemas.single_period_rate_of_change_schema,
        )
        self.returned_df = job.calculate_single_period_rate_of_change(test_df)
        self.expected_df = self.spark.createDataFrame(
            Data.expected_single_period_rate_of_change_rows,
            Schemas.expected_single_period_rate_of_change_schema,
        )

        self.returned_data = self.returned_df.sort(IndCqc.location_id).collect()
        self.expected_data = self.expected_df.collect()

    def test_returned_column_names_match_expected(self):
        self.assertEqual(self.returned_df.columns, self.expected_df.columns)

    def test_returned_single_period_rate_of_change_values_match_expected(
        self,
    ):
        for i in range(len(self.returned_data)):
            self.assertAlmostEqual(
                self.returned_data[i][job.TempCol.single_period_rate_of_change],
                self.expected_data[i][job.TempCol.single_period_rate_of_change],
                2,
                f"Returned row {i} does not match expected",
            )


class DeduplicateDataframeTests(ModelPrimaryServiceRateOfChangeTests):
    def setUp(self) -> None:
        super().setUp()

        test_df = self.spark.createDataFrame(
            Data.deduplicate_dataframe_rows,
            Schemas.deduplicate_dataframe_schema,
        )
        self.returned_df = job.deduplicate_dataframe(test_df)
        self.expected_df = self.spark.createDataFrame(
            Data.expected_deduplicate_dataframe_rows,
            Schemas.expected_deduplicate_dataframe_schema,
        )

        self.returned_data = self.returned_df.sort(
            IndCqc.primary_service_type, IndCqc.unix_time
        ).collect()
        self.expected_data = self.expected_df.collect()

    def test_returned_column_names_match_expected(self):
        self.assertEqual(self.returned_df.columns, self.expected_df.columns)

    def test_returned_deduplicated_dataframe_rows_match_expected(self):
        self.assertEqual(self.returned_data, self.expected_data)


class CalculateCumulativeRateOfChangeTests(ModelPrimaryServiceRateOfChangeTests):
    def setUp(self) -> None:
        super().setUp()

        test_df = self.spark.createDataFrame(
            Data.cumulative_rate_of_change_rows,
            Schemas.cumulative_rate_of_change_schema,
        )
        self.returned_df = job.calculate_cumulative_rate_of_change(
            test_df, IndCqc.ascwds_rate_of_change_trendline_model
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_cumulative_rate_of_change_rows,
            Schemas.expected_cumulative_rate_of_change_schema,
        )

        self.returned_data = self.returned_df.sort(
            IndCqc.primary_service_type, IndCqc.unix_time
        ).collect()
        self.expected_data = self.expected_df.collect()

    def test_returned_column_names_match_expected(self):
        self.assertEqual(self.returned_df.columns, self.expected_df.columns)

    def test_cumulative_rate_of_change_returns_correct_values_in_rolling_rate_of_change_model_column(
        self,
    ):
        for i in range(len(self.returned_data)):
            self.assertAlmostEqual(
                self.returned_data[i][IndCqc.ascwds_rate_of_change_trendline_model],
                self.expected_data[i][IndCqc.ascwds_rate_of_change_trendline_model],
                2,
                f"Returned row {i} does not match expected",
            )
