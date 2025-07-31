import unittest
from unittest.mock import Mock, patch
import warnings

import projects._03_independent_cqc._02_clean.utils.clean_ascwds_filled_post_outliers.clean_ascwds_filled_post_outliers as job
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_data import (
    CleanAscwdsFilledPostOutliersData as Data,
)
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_schemas import (
    CleanAscwdsFilledPostOutliersSchema as Schemas,
)
from utils import utils

PATCH_PATH: str = "utils.ind_cqc_filled_posts_utils.clean_ascwds_filled_post_outliers.clean_ascwds_filled_post_outliers"


class CleanAscwdsFilledPostOutliersTests(unittest.TestCase):
    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.unfiltered_ind_cqc_df = self.spark.createDataFrame(
            Data.unfiltered_ind_cqc_rows,
            Schemas.unfiltered_ind_cqc_schema,
        )

        warnings.filterwarnings("ignore", category=ResourceWarning)

    @patch(f"{PATCH_PATH}.add_filtering_rule_column")
    @patch(
        f"{PATCH_PATH}.null_filled_posts_where_locations_use_invalid_missing_data_code"
    )
    @patch(f"{PATCH_PATH}.null_grouped_providers")
    @patch(f"{PATCH_PATH}.winsorize_care_home_filled_posts_per_bed_ratio_outliers")
    def test_functions_are_called(
        self,
        winsorize_care_home_filled_posts_per_bed_ratio_outliers_mock: Mock,
        null_grouped_provders_mock: Mock,
        null_filled_posts_where_locations_use_invalid_missing_data_code_mock: Mock,
        add_filtering_rule_column_mock: Mock,
    ):
        job.clean_ascwds_filled_post_outliers(self.unfiltered_ind_cqc_df)
        winsorize_care_home_filled_posts_per_bed_ratio_outliers_mock.assert_called_once()
        null_grouped_provders_mock.assert_called_once()
        null_filled_posts_where_locations_use_invalid_missing_data_code_mock.assert_called_once()
        add_filtering_rule_column_mock.assert_called_once()
