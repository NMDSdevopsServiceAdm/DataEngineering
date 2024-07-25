import unittest
from unittest.mock import Mock, patch
import warnings

from tests.test_file_data import NullAscwdsFilledPostOuliersData as Data
from tests.test_file_schemas import NullAscwdsFilledPostOuliersSchema as Schemas

from utils import utils
import utils.ind_cqc_filled_posts_utils.null_ascwds_filled_post_outliers.null_ascwds_filled_post_outliers as job
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)


class NullAscwdsFilledPostOuliersTests(unittest.TestCase):
    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.unfiltered_ind_cqc_df = self.spark.createDataFrame(
            Data.unfiltered_ind_cqc_rows,
            Schemas.unfiltered_ind_cqc_schema,
        )
        self.returned_filtered_df = job.null_ascwds_filled_post_outliers(
            self.unfiltered_ind_cqc_df
        )

        self.expected_filtered_ind_cqc_df = self.spark.createDataFrame(
            Data.expected_filtered_ind_cqc_rows,
            Schemas.expected_filtered_ind_cqc_schema,
        )

        self.returned_data = self.returned_filtered_df.sort(
            IndCQC.location_id
        ).collect()
        self.expected_data = self.expected_filtered_ind_cqc_df.sort(
            IndCQC.location_id
        ).collect()

        warnings.filterwarnings("ignore", category=ResourceWarning)

    @patch(
        "utils.ind_cqc_filled_posts_utils.null_ascwds_filled_post_outliers.null_care_home_filled_posts_per_bed_ratio_outliers.null_care_home_filled_posts_per_bed_ratio_outliers"
    )
    def test_functions_are_called(
        self, null_care_home_filled_posts_per_bed_ratio_outliers_mock: Mock
    ):
        job.null_ascwds_filled_post_outliers(self.unfiltered_ind_cqc_df)
        self.assertEqual(
            null_care_home_filled_posts_per_bed_ratio_outliers_mock.call_count,
            1,
        )

    @patch(
        "utils.ind_cqc_filled_posts_utils.null_ascwds_filled_post_outliers.null_care_home_filled_posts_per_bed_ratio_outliers.null_care_home_filled_posts_per_bed_ratio_outliers"
    )
    def test_returned_df_has_same_number_of_rows_as_input_df(
        self, null_care_home_filled_posts_per_bed_ratio_outliers_mock: Mock
    ):
        self.assertEqual(
            self.unfiltered_ind_cqc_df.count(),
            self.returned_filtered_df.count(),
        )

    @patch(
        "utils.ind_cqc_filled_posts_utils.null_ascwds_filled_post_outliers.null_care_home_filled_posts_per_bed_ratio_outliers.null_care_home_filled_posts_per_bed_ratio_outliers"
    )
    def test_returned_df_has_same_columns_as_expected_df(
        self, null_care_home_filled_posts_per_bed_ratio_outliers_mock: Mock
    ):
        self.assertEqual(
            sorted(self.returned_filtered_df.columns),
            sorted(self.expected_filtered_ind_cqc_df.columns),
        )

    @patch(
        "utils.ind_cqc_filled_posts_utils.null_ascwds_filled_post_outliers.null_care_home_filled_posts_per_bed_ratio_outliers.null_care_home_filled_posts_per_bed_ratio_outliers"
    )
    def test_returned_df_matches_expected_df(
        self, null_care_home_filled_posts_per_bed_ratio_outliers_patch: Mock
    ):
        self.assertEqual(self.returned_data, self.expected_data)
