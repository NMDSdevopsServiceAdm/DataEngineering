import unittest
from unittest.mock import ANY, Mock, patch

import polars as pl
import polars.testing as pl_testing

import projects._03_independent_cqc._02_clean.fargate.clean_ind_cqc_filled_posts as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    CleanIndCQCData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    CleanIndCQCData as Schemas,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_values.categorical_column_values import Sector

PATCH_PATH = "projects._03_independent_cqc._02_clean.fargate.clean_ind_cqc_filled_posts"


class CleanIndFilledPostsTests(unittest.TestCase):
    MERGE_IND_CQC_SOURCE = "input_dir"
    CLEANED_IND_CQC_DESTINATION = "output_dir"
    partition_keys = [
        Keys.year,
        Keys.month,
        Keys.day,
        Keys.import_date,
    ]


class CalculateTimeRegisteredForTests(CleanIndFilledPostsTests):
    def test_calculate_time_registered_returns_one_when_dates_are_on_the_same_day(
        self,
    ):
        test_df = pl.LazyFrame(
            data=Data.calculate_time_registered_same_day_rows,
            schema=Schemas.calculate_time_registered_for_schema,
            orient="row",
        )
        returned_df = job.calculate_time_registered_for(test_df)

        expected_df = pl.LazyFrame(
            data=Data.expected_calculate_time_registered_same_day_rows,
            schema=Schemas.expected_calculate_time_registered_for_schema,
            orient="row",
        )
        returned_data = returned_df.collect()
        expected_data = expected_df.collect()

        pl_testing.assert_frame_equal(returned_data, expected_data)

    def test_calculate_time_registered_returns_expected_values_when_dates_are_exact_months_apart(
        self,
    ):
        test_df = pl.LazyFrame(
            Data.calculate_time_registered_exact_months_apart_rows,
            Schemas.calculate_time_registered_for_schema,
            orient="row",
        )
        returned_df = job.calculate_time_registered_for(test_df)

        expected_df = pl.LazyFrame(
            Data.expected_calculate_time_registered_exact_months_apart_rows,
            Schemas.expected_calculate_time_registered_for_schema,
            orient="row",
        )
        returned_data = returned_df.sort(CQCLClean.location_id).collect()
        expected_data = expected_df.collect()

        pl_testing.assert_frame_equal(returned_data, expected_data)

    def test_calculate_time_registered_returns_expected_values_when_dates_are_one_day_less_than_a_full_month_apart(
        self,
    ):
        test_df = pl.LazyFrame(
            Data.calculate_time_registered_one_day_less_than_a_full_month_apart_rows,
            Schemas.calculate_time_registered_for_schema,
            orient="row",
        )
        returned_df = job.calculate_time_registered_for(test_df)

        expected_df = pl.LazyFrame(
            Data.expected_calculate_time_registered_one_day_less_than_a_full_month_apart_rows,
            Schemas.expected_calculate_time_registered_for_schema,
            orient="row",
        )
        returned_data = returned_df.sort(CQCLClean.location_id).collect()
        expected_data = expected_df.collect()

        pl_testing.assert_frame_equal(returned_data, expected_data)

    def test_calculate_time_registered_returns_expected_values_when_dates_are_one_day_more_than_a_full_month_apart(
        self,
    ):
        test_df = pl.LazyFrame(
            Data.calculate_time_registered_one_day_more_than_a_full_month_apart_rows,
            Schemas.calculate_time_registered_for_schema,
            orient="row",
        )
        returned_df = job.calculate_time_registered_for(test_df)

        expected_df = pl.LazyFrame(
            Data.expected_calculate_time_registered_one_day_more_than_a_full_month_apart_rows,
            Schemas.expected_calculate_time_registered_for_schema,
            orient="row",
        )
        returned_data = returned_df.sort(CQCLClean.location_id).collect()
        expected_data = expected_df.collect()

        pl_testing.assert_frame_equal(returned_data, expected_data)


class CalculateTimeSinceDormantTests(CleanIndFilledPostsTests):
    def setUp(self):
        self.test_lf = pl.LazyFrame(
            Data.calculate_time_since_dormant_rows,
            Schemas.calculate_time_since_dormant_schema,
            orient="row",
        )
        self.returned_lf = job.calculate_time_since_dormant(self.test_lf)
        self.expected_lf = pl.LazyFrame(
            Data.expected_calculate_time_since_dormant_rows,
            Schemas.expected_calculate_time_since_dormant_schema,
            orient="row",
        )

        self.columns_added_by_function = [
            column
            for column in self.returned_lf.collect_schema().names()
            if column not in self.test_lf.collect_schema().names()
        ]

    def test_calculate_time_since_dormant_returns_new_column(self):
        self.assertEqual(len(self.columns_added_by_function), 1)
        self.assertEqual(self.columns_added_by_function[0], IndCQC.time_since_dormant)

    def test_calculate_time_since_dormant_returns_expected_values(self):
        returned_data = self.returned_lf.sort(IndCQC.cqc_location_import_date).collect()
        expected_data = self.expected_lf.collect()

        pl_testing.assert_frame_equal(returned_data, expected_data)


class RemoveDualRegistrationCqcCareHomes(CleanIndFilledPostsTests):

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
        pl_testing.assert_frame_equal(
            returned_df.sort(IndCQC.location_id).collect(), expected_df.collect()
        )

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
        pl_testing.assert_frame_equal(
            returned_df.sort(IndCQC.location_id).collect(), expected_df.collect()
        )

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
        pl_testing.assert_frame_equal(
            returned_df.sort(IndCQC.location_id).collect(), expected_df.collect()
        )

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
        pl_testing.assert_frame_equal(
            returned_df.sort(IndCQC.location_id).collect(), expected_df.collect()
        )

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
        pl_testing.assert_frame_equal(
            returned_df.sort(IndCQC.location_id).collect(), expected_df.collect()
        )

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
        pl_testing.assert_frame_equal(
            returned_df.sort(IndCQC.location_id).collect(), expected_df.collect()
        )

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
        pl_testing.assert_frame_equal(
            returned_df.sort(IndCQC.location_id).collect(), expected_df.collect()
        )

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

        pl_testing.assert_frame_equal(
            returned_df.sort(IndCQC.location_id).collect(), expected_df.collect()
        )
