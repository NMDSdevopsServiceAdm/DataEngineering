import unittest
import warnings

from tests.test_file_data import CleanIndCQCData as Data
from tests.test_file_schemas import CleanIndCQCData as Schemas

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)

import utils.ind_cqc_filled_posts_utils.ascwds_filled_posts_calculator.calculate_ascwds_filled_posts_return_only_permitted_value as job


class TestAscwdsFilledPostsOnlyPermittedValue(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()
        self.ascwds_total_staff_and_worker_record_df = self.spark.createDataFrame(
            Data.calculate_ascwds_filled_posts_rows,
            Schemas.calculate_ascwds_filled_posts_schema,
        )
        self.total_staff_only_permitted_df = job.calculate_ascwds_filled_posts_select_only_value_which_is_at_least_minimum_permitted_value(
            self.ascwds_total_staff_and_worker_record_df,
            IndCQC.total_staff_bounded,
            IndCQC.worker_records_bounded,
            IndCQC.ascwds_filled_posts,
        )
        self.worker_records_only_permitted_df = job.calculate_ascwds_filled_posts_select_only_value_which_is_at_least_minimum_permitted_value(
            self.ascwds_total_staff_and_worker_record_df,
            IndCQC.worker_records_bounded,
            IndCQC.total_staff_bounded,
            IndCQC.ascwds_filled_posts,
        )
        warnings.filterwarnings("ignore", category=ResourceWarning)

    def test_calculate_ascwds_filled_posts_row_count_correct(
        self,
    ):
        self.assertEqual(self.total_staff_only_permitted_df.count(), 9)

    def test_calculate_ascwds_filled_posts_absolute_difference_when_only_total_staff_within_range_values(
        self,
    ):
        df = self.total_staff_only_permitted_df.sort(IndCQC.location_id).collect()
        self.assertEqual(df[0][IndCQC.ascwds_filled_posts], None)
        self.assertEqual(df[1][IndCQC.ascwds_filled_posts], None)
        self.assertEqual(df[2][IndCQC.ascwds_filled_posts], 10.0)
        self.assertEqual(df[3][IndCQC.ascwds_filled_posts], 23.0)
        self.assertEqual(df[4][IndCQC.ascwds_filled_posts], None)
        self.assertEqual(df[5][IndCQC.ascwds_filled_posts], None)
        self.assertEqual(df[6][IndCQC.ascwds_filled_posts], None)
        self.assertEqual(df[7][IndCQC.ascwds_filled_posts], None)
        self.assertEqual(df[8][IndCQC.ascwds_filled_posts], 8.0)

    def test_calculate_ascwds_filled_posts_absolute_difference_when_only_total_staff_within_range_sources(
        self,
    ):
        df = self.total_staff_only_permitted_df.sort(IndCQC.location_id).collect()
        self.assertEqual(df[0][IndCQC.ascwds_filled_posts_source], None)
        self.assertEqual(df[1][IndCQC.ascwds_filled_posts_source], None)
        self.assertEqual(
            df[2][IndCQC.ascwds_filled_posts_source],
            job.ascwds_filled_posts_select_only_value_source_description(
                IndCQC.total_staff_bounded
            ),
        )
        self.assertEqual(
            df[3][IndCQC.ascwds_filled_posts_source],
            job.ascwds_filled_posts_select_only_value_source_description(
                IndCQC.total_staff_bounded
            ),
        )
        self.assertEqual(df[4][IndCQC.ascwds_filled_posts_source], None)
        self.assertEqual(df[5][IndCQC.ascwds_filled_posts_source], None)
        self.assertEqual(df[6][IndCQC.ascwds_filled_posts_source], None)
        self.assertEqual(df[7][IndCQC.ascwds_filled_posts_source], None)
        self.assertEqual(df[8][IndCQC.ascwds_filled_posts_source], "already populated")

    def test_calculate_ascwds_filled_posts_absolute_difference_when_only_worker_records_within_range_values(
        self,
    ):
        df = self.worker_records_only_permitted_df.sort(IndCQC.location_id).collect()
        self.assertEqual(df[0][IndCQC.ascwds_filled_posts], None)
        self.assertEqual(df[1][IndCQC.ascwds_filled_posts], None)
        self.assertEqual(df[2][IndCQC.ascwds_filled_posts], None)
        self.assertEqual(df[3][IndCQC.ascwds_filled_posts], None)
        self.assertEqual(df[4][IndCQC.ascwds_filled_posts], 100.0)
        self.assertEqual(df[5][IndCQC.ascwds_filled_posts], None)
        self.assertEqual(df[6][IndCQC.ascwds_filled_posts], None)
        self.assertEqual(df[7][IndCQC.ascwds_filled_posts], None)
        self.assertEqual(df[8][IndCQC.ascwds_filled_posts], 8.0)

    def test_calculate_ascwds_filled_posts_absolute_difference_when_only_worker_records_within_range_sources(
        self,
    ):
        df = self.worker_records_only_permitted_df.sort(IndCQC.location_id).collect()
        self.assertEqual(df[0][IndCQC.ascwds_filled_posts_source], None)
        self.assertEqual(df[1][IndCQC.ascwds_filled_posts_source], None)
        self.assertEqual(df[2][IndCQC.ascwds_filled_posts_source], None)
        self.assertEqual(df[3][IndCQC.ascwds_filled_posts_source], None)
        self.assertEqual(
            df[4][IndCQC.ascwds_filled_posts_source],
            job.ascwds_filled_posts_select_only_value_source_description(
                IndCQC.worker_records_bounded
            ),
        )
        self.assertEqual(df[5][IndCQC.ascwds_filled_posts_source], None)
        self.assertEqual(df[6][IndCQC.ascwds_filled_posts_source], None)
        self.assertEqual(df[7][IndCQC.ascwds_filled_posts_source], None)
        self.assertEqual(df[8][IndCQC.ascwds_filled_posts_source], "already populated")

    def test_source_description_string_compiles_correctly(self):
        compiled_string = job.ascwds_filled_posts_select_only_value_source_description(
            "this column"
        )
        expected_string = "only this column was provided"

        self.assertEqual(compiled_string, expected_string)
