import unittest
from unittest.mock import Mock, patch

import polars as pl
import polars.testing as pl_testing

import projects._03_independent_cqc._02_clean.fargate.utils.clean_ascwds_filled_post_outliers.null_grouped_providers as job

from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    NullGroupedProvidersData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    NullGroupedProvidersSchema as Schemas,
)

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

PATCH_PATH: str = (
    "projects._03_independent_cqc._02_clean.fargate.utils.clean_ascwds_filled_post_outliers.null_grouped_providers"
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


class MainTests(unittest.TestCase):
    def setUp(self) -> None:
        self.test_lf = pl.LazyFrame(
            Data.null_grouped_providers_rows,
            Schemas.null_grouped_providers_schema,
            orient="row",
        )
        self.returned_lf = job.null_grouped_providers(self.test_lf)

    def test_null_grouped_providers_runs(self):
        self.assertIsInstance(self.returned_lf, pl.LazyFrame)

    def test_null_grouped_providers_returns_same_number_of_rows(self):
        self.assertEqual(
            self.returned_lf.collect().height, self.test_lf.collect().height
        )

    @patch(f"{PATCH_PATH}.null_non_residential_grouped_providers")
    @patch(f"{PATCH_PATH}.null_care_home_grouped_providers")
    @patch(f"{PATCH_PATH}.identify_potential_grouped_providers")
    @patch(f"{PATCH_PATH}.calculate_data_for_grouped_provider_identification")
    def test_null_grouped_providers_calls_functions(
        self,
        calculate_data_for_grouped_provider_identification_mock: Mock,
        identify_potential_grouped_providers_mock: Mock,
        null_care_home_grouped_providers_mock: Mock,
        null_non_residential_grouped_providers_mock: Mock,
    ):
        job.null_grouped_providers(self.test_lf)

        calculate_data_for_grouped_provider_identification_mock.assert_called_once_with(
            self.test_lf
        )
        identify_potential_grouped_providers_mock.assert_called_once()
        null_care_home_grouped_providers_mock.assert_called_once()
        null_non_residential_grouped_providers_mock.assert_called_once()


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
