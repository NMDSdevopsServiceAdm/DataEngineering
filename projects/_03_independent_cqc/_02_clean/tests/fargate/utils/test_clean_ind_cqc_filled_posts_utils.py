import unittest

import polars as pl
import polars.testing as pl_testing

import projects._03_independent_cqc._02_clean.fargate.utils.clean_ind_cqc_filled_posts_utils as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    CleanIndCQCData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    CleanIndCQCSchema as Schemas,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


class ReplaceZeroBedsWithNullTests(unittest.TestCase):
    def test_replace_zero_beds_with_null(self):
        input_lf = pl.LazyFrame(
            data=Data.replace_zero_beds_with_null_rows,
            schema=Schemas.replace_zero_beds_with_null_schema,
            orient="row",
        )

        result = job.replace_zero_beds_with_null(input_lf)

        expected = pl.LazyFrame(
            data=Data.expected_replace_zero_beds_with_null_rows,
            schema=Schemas.replace_zero_beds_with_null_schema,
            orient="row",
        )

        pl_testing.assert_frame_equal(result, expected)


class PopulateMissingCareHomeNumberOfBedsTests(unittest.TestCase):
    def test_populate_missing_care_home_number_of_beds(self):
        input_lf = pl.LazyFrame(
            data=Data.populate_missing_care_home_number_of_beds_rows,
            schema=Schemas.populate_missing_care_home_number_of_beds_schema,
            orient="row",
        )

        result = job.populate_missing_care_home_number_of_beds(input_lf)

        expected = pl.LazyFrame(
            data=Data.expected_populate_missing_care_home_number_of_beds_rows,
            schema=Schemas.populate_missing_care_home_number_of_beds_schema,
            orient="row",
        )

        pl_testing.assert_frame_equal(result, expected)


class FilterToCareHomeWithKnownBedsTests(unittest.TestCase):
    def test_filter_to_care_homes_with_known_beds(self):
        input_lf = pl.LazyFrame(
            data=Data.filter_to_care_homes_with_known_beds_rows,
            schema=Schemas.filter_to_care_homes_with_known_beds_schema,
            orient="row",
        )

        result = job.filter_to_care_homes_with_known_beds(input_lf)

        expected = pl.LazyFrame(
            data=Data.expected_filter_to_care_homes_with_known_beds_rows,
            schema=Schemas.filter_to_care_homes_with_known_beds_schema,
            orient="row",
        )

        pl_testing.assert_frame_equal(result, expected)


class AverageBedsPerLocationTests(unittest.TestCase):
    def test_average_beds_per_location(self):
        input_lf = pl.LazyFrame(
            data=Data.average_beds_per_location_rows,
            schema=Schemas.average_beds_per_location_schema,
            orient="row",
        )

        result = job.average_beds_per_location(input_lf)

        expected = pl.LazyFrame(
            data=Data.expected_average_beds_per_location_rows,
            schema=Schemas.expected_average_beds_per_location_schema,
            orient="row",
        )

        pl_testing.assert_frame_equal(result, expected, check_row_order=False)


class ReplaceNullBedsWithAverageTests(unittest.TestCase):
    def test_replace_null_beds_with_average(self):

        input_lf = pl.LazyFrame(
            data=Data.replace_null_beds_with_average_rows,
            schema=Schemas.replace_null_beds_with_average_schema,
            orient="row",
        )

        result = job.replace_null_beds_with_average(input_lf)

        expected = pl.LazyFrame(
            data=Data.expected_replace_null_beds_with_average_rows,
            schema=Schemas.expected_replace_null_beds_with_average_schema,
            orient="row",
        )

        pl_testing.assert_frame_equal(result, expected)

    def test_replace_null_beds_with_average_doesnt_change_known_beds(self):
        input_lf = pl.LazyFrame(
            [
                ("1-000000001", 1, 2),
            ],
            schema=Schemas.replace_null_beds_with_average_schema,
            orient="row",
        )

        result = job.replace_null_beds_with_average(input_lf)

        expected = pl.LazyFrame(
            [
                ("1-000000001", 1),
            ],
            schema=Schemas.expected_replace_null_beds_with_average_schema,
            orient="row",
        )

        pl_testing.assert_frame_equal(result, expected)


class CalculateTimeRegisteredForTests(unittest.TestCase):
    def test_calculate_time_registered_returns_one_when_dates_are_on_the_same_day(
        self,
    ):

        expected_df = pl.LazyFrame(
            data=Data.expected_calculate_time_registered_same_day_rows,
            schema=Schemas.expected_calculate_time_registered_for_schema,
            orient="row",
        )
        returned_df = job.calculate_time_registered_for(
            expected_df.drop(IndCQC.time_registered)
        )

        pl_testing.assert_frame_equal(returned_df, expected_df)

    def test_calculate_time_registered_returns_expected_values_when_dates_are_exact_months_apart(
        self,
    ):
        expected_df = pl.LazyFrame(
            Data.expected_calculate_time_registered_exact_months_apart_rows,
            Schemas.expected_calculate_time_registered_for_schema,
            orient="row",
        )
        returned_df = job.calculate_time_registered_for(
            expected_df.drop(IndCQC.time_registered)
        )

        pl_testing.assert_frame_equal(returned_df, expected_df)

    def test_calculate_time_registered_returns_expected_values_when_dates_are_one_day_less_than_a_full_month_apart(
        self,
    ):
        expected_df = pl.LazyFrame(
            Data.expected_calculate_time_registered_one_day_less_than_a_full_month_apart_rows,
            Schemas.expected_calculate_time_registered_for_schema,
            orient="row",
        )
        returned_df = job.calculate_time_registered_for(
            expected_df.drop(IndCQC.time_registered)
        )

        pl_testing.assert_frame_equal(returned_df, expected_df)

    def test_calculate_time_registered_returns_expected_values_when_dates_are_one_day_more_than_a_full_month_apart(
        self,
    ):
        expected_df = pl.LazyFrame(
            Data.expected_calculate_time_registered_one_day_more_than_a_full_month_apart_rows,
            Schemas.expected_calculate_time_registered_for_schema,
            orient="row",
        )
        returned_df = job.calculate_time_registered_for(
            expected_df.drop(IndCQC.time_registered)
        )

        pl_testing.assert_frame_equal(returned_df, expected_df)


class CalculateTimeSinceDormantTests(unittest.TestCase):
    def test_calculate_time_since_dormant_returns_expected_values(self):
        expected_lf = pl.LazyFrame(
            Data.expected_calculate_time_since_dormant_rows,
            Schemas.expected_calculate_time_since_dormant_schema,
            orient="row",
        )
        returned_lf = job.calculate_time_since_dormant(
            expected_lf.drop(IndCQC.time_since_dormant)
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class RemoveDualRegistrationCqcCareHomes(unittest.TestCase):

    def test_remove_dual_registration_cqc_care_homes_returns_expected_values_when_carehome_and_asc_data_populated(
        self,
    ):
        test_df = pl.LazyFrame(
            Data.remove_cqc_dual_registrations_when_carehome_and_asc_data_populated_rows,
            Schemas.remove_cqc_dual_registrations_schema,
            orient="row",
        )
        expected_df = pl.LazyFrame(
            Data.expected_remove_cqc_dual_registrations_when_carehome_and_asc_data_populated_rows,
            Schemas.remove_cqc_dual_registrations_schema,
            orient="row",
        )
        returned_df = job.remove_dual_registration_cqc_care_homes(test_df)
        pl_testing.assert_frame_equal(returned_df, expected_df)

    def test_remove_dual_registration_cqc_care_homes_returns_expected_values_when_carehome_and_asc_data_missing_on_earlier_reg_date(
        self,
    ):
        test_df = pl.LazyFrame(
            Data.remove_cqc_dual_registrations_when_carehome_and_asc_data_missing_on_earlier_reg_date_rows,
            Schemas.remove_cqc_dual_registrations_schema,
            orient="row",
        )
        expected_df = pl.LazyFrame(
            Data.expected_remove_cqc_dual_registrations_when_carehome_and_asc_data_missing_on_earlier_reg_date_rows,
            Schemas.remove_cqc_dual_registrations_schema,
            orient="row",
        )
        returned_df = job.remove_dual_registration_cqc_care_homes(test_df)
        pl_testing.assert_frame_equal(returned_df, expected_df)

    def test_remove_dual_registration_cqc_care_homes_returns_expected_values_when_carehome_and_asc_data_missing_on_later_reg_date(
        self,
    ):
        test_df = pl.LazyFrame(
            Data.remove_cqc_dual_registrations_when_carehome_and_asc_data_missing_on_later_reg_date_rows,
            Schemas.remove_cqc_dual_registrations_schema,
            orient="row",
        )
        expected_df = pl.LazyFrame(
            Data.expected_remove_cqc_dual_registrations_when_carehome_and_asc_data_missing_on_later_reg_date_rows,
            Schemas.remove_cqc_dual_registrations_schema,
            orient="row",
        )
        returned_df = job.remove_dual_registration_cqc_care_homes(test_df)
        pl_testing.assert_frame_equal(returned_df, expected_df)

    def test_remove_dual_registration_cqc_care_homes_returns_expected_values_when_carehome_and_asc_data_missing_on_all_reg_dates(
        self,
    ):
        test_df = pl.LazyFrame(
            Data.remove_cqc_dual_registrations_when_carehome_and_asc_data_missing_on_all_reg_dates_rows,
            Schemas.remove_cqc_dual_registrations_schema,
            orient="row",
        )
        expected_df = pl.LazyFrame(
            Data.expected_remove_cqc_dual_registrations_when_carehome_and_asc_data_missing_on_all_reg_dates_rows,
            Schemas.remove_cqc_dual_registrations_schema,
            orient="row",
        )
        returned_df = job.remove_dual_registration_cqc_care_homes(test_df)
        pl_testing.assert_frame_equal(returned_df, expected_df)

    def test_remove_dual_registration_cqc_care_homes_returns_expected_values_when_carehome_and_asc_data_different_on_all_reg_dates(
        self,
    ):
        test_df = pl.LazyFrame(
            Data.remove_cqc_dual_registrations_when_carehome_and_asc_data_different_on_all_reg_dates_rows,
            Schemas.remove_cqc_dual_registrations_schema,
            orient="row",
        )
        expected_df = pl.LazyFrame(
            Data.expected_remove_cqc_dual_registrations_when_carehome_and_asc_data_different_on_all_reg_dates_rows,
            Schemas.remove_cqc_dual_registrations_schema,
            orient="row",
        )
        returned_df = job.remove_dual_registration_cqc_care_homes(test_df)
        pl_testing.assert_frame_equal(returned_df, expected_df)

    def test_remove_dual_registration_cqc_care_homes_returns_expected_values_when_carehome_and_registration_dates_the_same(
        self,
    ):
        test_df = pl.LazyFrame(
            Data.remove_cqc_dual_registrations_when_carehome_and_registration_dates_the_same_rows,
            Schemas.remove_cqc_dual_registrations_schema,
            orient="row",
        )
        expected_df = pl.LazyFrame(
            Data.expected_remove_cqc_dual_registrations_when_carehome_and_registration_dates_the_same_rows,
            Schemas.remove_cqc_dual_registrations_schema,
            orient="row",
        )
        returned_df = job.remove_dual_registration_cqc_care_homes(test_df)
        pl_testing.assert_frame_equal(returned_df, expected_df)

    def test_remove_dual_registration_cqc_care_homes_returns_expected_values_when_locations_not_sorted_numerically(
        self,
    ):
        test_df = pl.LazyFrame(
            Data.remove_cqc_dual_registrations_when_locations_not_sorted_numerically,
            Schemas.remove_cqc_dual_registrations_schema,
            orient="row",
        )
        expected_df = pl.LazyFrame(
            Data.expected_remove_cqc_dual_registrations_when_locations_not_sorted_numerically,
            Schemas.remove_cqc_dual_registrations_schema,
            orient="row",
        )
        returned_df = job.remove_dual_registration_cqc_care_homes(test_df)
        pl_testing.assert_frame_equal(returned_df, expected_df)

    def test_remove_dual_registration_cqc_care_homes_returns_expected_values_when_non_res(
        self,
    ):
        test_df = pl.LazyFrame(
            Data.remove_cqc_dual_registrations_when_non_res_rows,
            Schemas.remove_cqc_dual_registrations_schema,
            orient="row",
        )
        expected_df = pl.LazyFrame(
            Data.expected_remove_cqc_dual_registrations_when_non_res_rows,
            Schemas.remove_cqc_dual_registrations_schema,
            orient="row",
        )
        returned_df = job.remove_dual_registration_cqc_care_homes(test_df)

        pl_testing.assert_frame_equal(returned_df, expected_df)


class CalculateCareHomeStatusCountTests(unittest.TestCase):
    def test_returned_care_home_status_count_values_match_expected(
        self,
    ):
        expected_lf = pl.LazyFrame(
            Data.expected_calculate_care_home_status_count_rows,
            Schemas.expected_calculate_care_home_status_count_schema,
            orient="row",
        )
        returned_lf = job.calculate_care_home_status_count(
            expected_lf.drop(IndCQC.care_home_status_count)
        )
        pl_testing.assert_frame_equal(returned_lf, expected_lf)
