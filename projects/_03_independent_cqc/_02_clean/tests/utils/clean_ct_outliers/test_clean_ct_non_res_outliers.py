import warnings
from unittest.mock import Mock, patch

import projects._03_independent_cqc._02_clean.utils.clean_ct_outliers.clean_ct_non_res_outliers as job
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_data import (
    CleanCapacityTrackerNonResOutliersData as Data,
)
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_schemas import (
    CleanCapacityTrackerNonResOutliersSchema as Schemas,
)
from tests.base_test import SparkBaseTest
from utils import utils

PATCH_PATH: str = (
    "projects._03_independent_cqc._02_clean.utils.clean_ct_outliers.clean_ct_non_res_outliers"
)


class CleanCapacityTrackerNonResOutliersTests(SparkBaseTest):
    def setUp(self) -> None:

        self.ind_cqc_df = self.spark.createDataFrame(
            Data.ind_cqc_rows, Schemas.ind_cqc_schema
        )

        warnings.filterwarnings("ignore", category=ResourceWarning)

    @patch(f"{PATCH_PATH}.clean_ct_values_after_consecutive_repetition")
    @patch(f"{PATCH_PATH}.clean_longitudinal_outliers")
    @patch(f"{PATCH_PATH}.aggregate_values_to_provider_level")
    @patch(f"{PATCH_PATH}.add_filtering_rule_column")
    def test_functions_are_called(
        self,
        add_filtering_rule_column_mock: Mock,
        aggregate_values_to_provider_level: Mock,
        clean_longitudinal_outliers_mock: Mock,
        clean_ct_values_after_consecutive_repetition_mock: Mock,
    ):
        job.clean_capacity_tracker_non_res_outliers(self.ind_cqc_df)

        add_filtering_rule_column_mock.assert_called_once()
        aggregate_values_to_provider_level.assert_called_once()
        clean_longitudinal_outliers_mock.assert_called_once()
        clean_ct_values_after_consecutive_repetition_mock.assert_called_once()
