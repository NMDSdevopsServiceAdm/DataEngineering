import unittest
from datetime import date
from unittest.mock import ANY, Mock, patch

import polars as pl
import polars.testing as pl_testing
import pytest

import projects._03_independent_cqc._01_filled_posts._02_clean.fargate.utils.clean_ascwds_filled_post_outliers.null_grouped_providers as job
from projects._03_independent_cqc._01_filled_posts.unittest_data.polars_ind_cqc_test_file_data import (
    NullGroupedProvidersData as Data,
)
from projects._03_independent_cqc._01_filled_posts.unittest_data.polars_ind_cqc_test_file_schemas import (
    NullGroupedProvidersSchema as Schemas,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

PATCH_PATH: str = (
    "projects._03_independent_cqc._01_filled_posts._02_clean.fargate.utils.clean_ascwds_filled_post_outliers.null_grouped_providers"
)


class NullGroupedProvidersConfigTests(unittest.TestCase):
    def test_minimum_size_of_care_home_location_to_identify(self):
        self.assertEqual(
            job.NullGroupedProvidersConfig.MINIMUM_SIZE_OF_CARE_HOME_LOCATION_TO_IDENTIFY,
            25.0,
        )

    def test_minimum_size_of_non_res_location_to_identify(self):
        self.assertEqual(
            job.NullGroupedProvidersConfig.MINIMUM_SIZE_OF_NON_RES_LOCATION_TO_IDENTIFY,
            50.0,
        )

    def test_posts_per_bed_at_location_multiplier(self):
        self.assertEqual(
            job.NullGroupedProvidersConfig.POSTS_PER_BED_AT_LOCATION_MULTIPLIER, 4
        )

    def test_posts_per_bed_at_provider_multiplier(self):
        self.assertEqual(
            job.NullGroupedProvidersConfig.POSTS_PER_BED_AT_PROVIDER_MULTIPLIER, 3
        )

    def test_posts_per_pir_posts_at_location_multiplier(self):
        self.assertEqual(
            job.NullGroupedProvidersConfig.POSTS_PER_PIR_LOCATION_THRESHOLD, 2.5
        )

    def test_posts_per_pir_posts_at_provider_multiplier(self):
        self.assertEqual(
            job.NullGroupedProvidersConfig.POSTS_PER_PIR_PROVIDER_THRESHOLD, 1.5
        )


class TestNullGroupedProviders:
    @pytest.fixture
    def test_lf(self):
        return pl.LazyFrame(
            Data.null_grouped_providers_rows,
            Schemas.null_grouped_providers_schema,
            orient="row",
        )

    @pytest.fixture
    def grouped_providers_lf(self):
        return pl.LazyFrame()

    def test_runs_and_returns_two_lazyframes(self, test_lf, grouped_providers_lf):
        returned_lf, grouped_providers = job.null_grouped_providers(
            test_lf, grouped_providers_lf
        )
        assert isinstance(returned_lf, pl.LazyFrame)
        assert isinstance(grouped_providers, pl.LazyFrame)

    def test_returns_same_number_of_rows_in_locations_data(
        self, test_lf, grouped_providers_lf
    ):
        returned_lf, _ = job.null_grouped_providers(test_lf, grouped_providers_lf)
        assert returned_lf.collect().height == test_lf.collect().height

    def test_returns_expected_rows_in_grouped_providers_data(
        self, test_lf, grouped_providers_lf
    ):
        _, grouped_providers = job.null_grouped_providers(test_lf, grouped_providers_lf)
        assert grouped_providers.collect().height == 2

    @patch(f"{PATCH_PATH}.update_grouped_providers_history")
    @patch(f"{PATCH_PATH}.select_grouped_providers")
    @patch(f"{PATCH_PATH}.select_locations_populated_this_month")
    @patch(f"{PATCH_PATH}.null_non_residential_grouped_providers")
    @patch(f"{PATCH_PATH}.null_care_home_grouped_providers")
    @patch(f"{PATCH_PATH}.identify_potential_grouped_providers")
    @patch(f"{PATCH_PATH}.calculate_data_for_grouped_provider_identification")
    def test_calls_all_grouped_provider_functions(
        self,
        calculate_data_for_grouped_provider_identification_mock: Mock,
        identify_potential_grouped_providers_mock: Mock,
        null_care_home_grouped_providers_mock: Mock,
        null_non_residential_grouped_providers_mock: Mock,
        select_locations_populated_this_month_mock: Mock,
        select_grouped_providers_mock: Mock,
        update_grouped_providers_history_mock: Mock,
        test_lf,
        grouped_providers_lf,
    ):
        job.null_grouped_providers(test_lf, grouped_providers_lf)

        calculate_data_for_grouped_provider_identification_mock.assert_called_once_with(
            test_lf
        )
        identify_potential_grouped_providers_mock.assert_called_once()
        null_care_home_grouped_providers_mock.assert_called_once()
        null_non_residential_grouped_providers_mock.assert_called_once()
        select_locations_populated_this_month_mock.assert_called_once()
        select_grouped_providers_mock.assert_called_once()
        update_grouped_providers_history_mock.assert_called_once_with(
            select_grouped_providers_mock.return_value,
            select_locations_populated_this_month_mock.return_value,
            grouped_providers_lf,
            ANY,
        )

    @patch(f"{PATCH_PATH}.update_grouped_providers_history")
    @patch(f"{PATCH_PATH}.select_grouped_providers")
    @patch(f"{PATCH_PATH}.select_locations_populated_this_month")
    @patch(f"{PATCH_PATH}.null_non_residential_grouped_providers")
    @patch(f"{PATCH_PATH}.null_care_home_grouped_providers")
    @patch(f"{PATCH_PATH}.identify_potential_grouped_providers")
    @patch(f"{PATCH_PATH}.calculate_data_for_grouped_provider_identification")
    def test_passes_latest_import_date_of_input_as_snapshot_date(
        self,
        calculate_data_for_grouped_provider_identification_mock: Mock,
        identify_potential_grouped_providers_mock: Mock,
        null_care_home_grouped_providers_mock: Mock,
        null_non_residential_grouped_providers_mock: Mock,
        select_locations_populated_this_month_mock: Mock,
        select_grouped_providers_mock: Mock,
        update_grouped_providers_history_mock: Mock,
        test_lf,
        grouped_providers_lf,
    ):
        job.null_grouped_providers(test_lf, grouped_providers_lf)

        snapshot_date = update_grouped_providers_history_mock.call_args.args[3]
        assert snapshot_date == date(2024, 2, 1)

    def test_stamps_non_null_fixed_date_when_no_grouped_providers_in_latest_month(
        self,
    ):
        test_lf = pl.LazyFrame(
            Data.null_grouped_providers_no_flags_in_latest_month_rows,
            Schemas.null_grouped_providers_schema,
            orient="row",
        )
        history_lf = pl.LazyFrame(
            Data.grouped_providers_history_before_no_flags_in_latest_month_rows,
            Schemas.final_grouped_providers_schema,
            orient="row",
        )
        expected_lf = pl.LazyFrame(
            Data.expected_grouped_providers_after_no_flags_in_latest_month_rows,
            Schemas.final_grouped_providers_schema,
            orient="row",
        )

        _, grouped_providers = job.null_grouped_providers(test_lf, history_lf)

        pl_testing.assert_frame_equal(grouped_providers, expected_lf)


class CalculateDataForGroupedProviderIdentificationTests(unittest.TestCase):
    def test_function_returns_expected_values(self):
        test_lf = pl.LazyFrame(
            Data.input_grouped_provider_rows,
            Schemas.grouped_provider_schema,
            orient="row",
        )
        returned_lf = job.calculate_data_for_grouped_provider_identification(test_lf)
        expected_lf = pl.LazyFrame(
            Data.expected_grouped_provider_rows,
            Schemas.expected_grouped_provider_schema,
            orient="row",
        )
        pl_testing.assert_frame_equal(expected_lf, returned_lf.sort(IndCQC.location_id))


class IdentifyPotentialGroupedProviderTests(unittest.TestCase):
    def test_function_returns_expected_values(self):
        test_lf = pl.LazyFrame(
            Data.identify_potential_grouped_providers_rows,
            Schemas.identify_potential_grouped_providers_schema,
            orient="row",
        )
        returned_lf = job.identify_potential_grouped_providers(test_lf)
        expected_lf = pl.LazyFrame(
            Data.expected_identify_potential_grouped_providers_rows,
            Schemas.expected_identify_potential_grouped_providers_schema,
            orient="row",
        )
        pl_testing.assert_frame_equal(expected_lf, returned_lf.sort(IndCQC.location_id))


class NullCareHomeGroupedProvidersTests(unittest.TestCase):
    def test_function_returns_null_when_criteria_met(self):
        test_lf = pl.LazyFrame(
            Data.null_care_home_grouped_providers_when_meets_criteria_rows,
            Schemas.null_care_home_grouped_providers_schema,
            orient="row",
        )

        returned_lf = job.null_care_home_grouped_providers(test_lf)
        expected_lf = pl.LazyFrame(
            Data.expected_null_care_home_grouped_providers_when_meets_criteria_rows,
            Schemas.null_care_home_grouped_providers_schema,
            orient="row",
        )
        pl_testing.assert_frame_equal(expected_lf, returned_lf.sort(IndCQC.location_id))

    def test_function_returns_original_data_when_criteria_not_met(self):
        test_lf = pl.LazyFrame(
            Data.null_care_home_grouped_providers_where_location_does_not_meet_criteria_rows,
            Schemas.null_care_home_grouped_providers_schema,
            orient="row",
        )

        returned_lf = job.null_care_home_grouped_providers(test_lf)

        pl_testing.assert_frame_equal(returned_lf.sort(IndCQC.location_id), test_lf)


class NullNonResidentialGroupedProvidersTests(unittest.TestCase):
    def test_function_returns_null_when_criteria_met(self):
        test_lf = pl.LazyFrame(
            Data.null_non_res_grouped_providers_when_meets_criteria_rows,
            Schemas.null_non_res_grouped_providers_schema,
            orient="row",
        )

        returned_lf = job.null_non_residential_grouped_providers(test_lf)
        expected_lf = pl.LazyFrame(
            Data.expected_null_non_res_grouped_providers_when_meets_criteria_rows,
            Schemas.null_non_res_grouped_providers_schema,
            orient="row",
        )
        pl_testing.assert_frame_equal(
            expected_lf,
            returned_lf.sort(IndCQC.location_id),
        )

    def test_function_returns_original_data_when_criteria_not_met(self):
        test_lf = pl.LazyFrame(
            Data.null_non_res_grouped_providers_when_does_not_meet_criteria_rows,
            Schemas.null_non_res_grouped_providers_schema,
            orient="row",
        )

        returned_lf = job.null_non_residential_grouped_providers(test_lf)

        pl_testing.assert_frame_equal(returned_lf.sort(IndCQC.location_id), test_lf)


class TestSelectGroupedProviders:
    CASES = [
        pytest.param(case, id=case.id)
        for case in Data.select_grouped_providers_test_cases
    ]

    @pytest.mark.parametrize("case", CASES)
    def test_function_returns_expected_rows(self, case):
        input_lf = pl.LazyFrame(
            case.input_rows,
            Schemas.select_grouped_providers_input_schema,
            orient="row",
        )
        expected_lf = pl.LazyFrame(
            case.expected_rows,
            Schemas.final_grouped_providers_schema,
            orient="row",
        )

        returned_lf = job.select_grouped_providers(input_lf)

        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)


class TestSelectLocationsPopulatedThisMonth:
    CASES = [
        pytest.param(case, id=case.id)
        for case in Data.select_locations_populated_this_month_test_cases
    ]

    @pytest.mark.parametrize("case", CASES)
    def test_function_returns_expected_location_ids(self, case):
        input_lf = pl.LazyFrame(
            case.input_rows,
            Schemas.select_grouped_providers_input_schema,
            orient="row",
        )
        expected_lf = pl.LazyFrame(
            {IndCQC.location_id: case.expected_rows},
            schema=Schemas.select_locations_populated_this_month_schema,
        )

        returned_lf = job.select_locations_populated_this_month(input_lf)

        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)


class TestUpdateGroupedProvidersHistory:
    def test_returns_new_snapshot_unchanged_when_no_history_exists(self):
        new_grouped_providers_lf = pl.LazyFrame(
            Data.new_grouped_providers_rows,
            Schemas.final_grouped_providers_schema,
            orient="row",
        )
        populated_location_ids_lf = pl.LazyFrame(
            schema=Schemas.select_locations_populated_this_month_schema
        )
        historical_grouped_providers_lf = pl.LazyFrame()

        returned_lf = job.update_grouped_providers_history(
            new_grouped_providers_lf,
            populated_location_ids_lf,
            historical_grouped_providers_lf,
            date(2026, 2, 1),
        )

        pl_testing.assert_frame_equal(returned_lf, new_grouped_providers_lf)

    CASES = [
        pytest.param(case, id=case.id)
        for case in Data.update_grouped_providers_history_test_cases
    ]

    @pytest.mark.parametrize("case", CASES)
    def test_function_returns_expected_rows(self, case):
        new_grouped_providers_lf = pl.LazyFrame(
            case.new_rows,
            Schemas.final_grouped_providers_schema,
            orient="row",
        )
        populated_location_ids_lf = pl.LazyFrame(
            {IndCQC.location_id: case.populated_location_ids},
            schema=Schemas.select_locations_populated_this_month_schema,
        )
        historical_grouped_providers_lf = pl.LazyFrame(
            case.history_rows,
            Schemas.final_grouped_providers_schema,
            orient="row",
        )
        expected_lf = pl.LazyFrame(
            case.expected_rows,
            Schemas.final_grouped_providers_schema,
            orient="row",
        )

        returned_lf = job.update_grouped_providers_history(
            new_grouped_providers_lf,
            populated_location_ids_lf,
            historical_grouped_providers_lf,
            case.snapshot_date,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)
