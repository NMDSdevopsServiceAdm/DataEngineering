import unittest
from unittest.mock import Mock, patch
import polars as pl

import projects._03_independent_cqc._02_clean.fargate.utils.clean_ct_outliers.clean_ct_care_home_outliers as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    CleanCapacityTrackerCareHomeOutliersData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    CleanCapacityTrackerCareHomeOutliersSchema as Schemas,
)


PATCH_PATH: str = (
    "projects._03_independent_cqc._02_clean.fargate.utils.clean_ct_outliers.clean_ct_care_home_outliers"
)


class CleanCapacityTrackerCareHomeOutliersTests(unittest.TestCase):
    def setUp(self) -> None:

        self.test_lf = pl.LazyFrame(
            Data.ind_cqc_rows, Schemas.ind_cqc_schema, orient="row"
        )

    @patch(f"{PATCH_PATH}.clean_ct_values_after_consecutive_repetition")
    @patch(f"{PATCH_PATH}.clean_longitudinal_outliers")
    @patch(f"{PATCH_PATH}.null_posts_per_bed_outliers")
    @patch(f"{PATCH_PATH}.aggregate_values_to_provider_level")
    @patch(f"{PATCH_PATH}.add_filtering_rule_column")
    def test_functions_are_called(
        self,
        add_filtering_rule_column_mock: Mock,
        aggregate_values_to_provider_level_mock: Mock,
        null_posts_per_bed_outliers_mock: Mock,
        clean_longitudinal_outliers_mock: Mock,
        clean_ct_values_after_consecutive_repetition_mock: Mock,
    ):
        job.clean_capacity_tracker_care_home_outliers(self.test_lf)

        add_filtering_rule_column_mock.assert_called_once()
        aggregate_values_to_provider_level_mock.assert_called_once()
        null_posts_per_bed_outliers_mock.assert_called_once()
        clean_longitudinal_outliers_mock.assert_called_once()
        clean_ct_values_after_consecutive_repetition_mock.assert_called_once()
